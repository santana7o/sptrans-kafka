"""Cliente HTTP da API Olho Vivo (SPTrans) v2.1.

- ``authenticate()``: POST /Login/Autenticar?token=... (corpo ``true``/``false`` + cookie).
- ``get_positions()``: GET /Posicao, com reautenticacao automatica quando a sessao expira.
- Erros de rede, 5xx e 429 sao repetidos com backoff exponencial e jitter.

O token vai na query string da autenticacao; por isso nunca logamos URLs nem mensagens
de excecao sem antes passar por ``redact``.
"""

from __future__ import annotations

import random
import time
from typing import Any, Callable

import requests

from common.config import Settings
from common.log import get_logger
from common.runtime import ShutdownFlag

log = get_logger(__name__)

AUTH_PATH = "/Login/Autenticar"
POSITIONS_PATH = "/Posicao"
DENIED_MARKER = "authorization has been denied"
USER_AGENT = "sptrans-kafka-producer/0.1"


class OlhoVivoError(RuntimeError):
    """Falha ao obter dados da API (rede, status inesperado, resposta invalida)."""


class AuthenticationError(OlhoVivoError):
    """Token rejeitado ou sessao que nao pode ser restabelecida."""


# --------------------------------------------------------------------------- logica pura


def backoff_delay(
    attempt: int,
    base_s: float = 1.0,
    cap_s: float = 30.0,
    rand: Callable[[], float] = random.random,
) -> float:
    """Espera antes da tentativa ``attempt + 1`` (attempt comeca em 0).

    Exponencial (base * 2^attempt) limitada a ``cap_s``, com jitter: o valor final fica
    entre 50% e 100% do teto calculado, para que varios clientes nao sincronizem.
    """
    ceiling = min(cap_s, base_s * (2 ** max(0, attempt)))
    return ceiling * (0.5 + 0.5 * rand())


def parse_auth_response(body: str) -> bool:
    """A API responde ``true`` ou ``false`` (JSON). Qualquer outra coisa e falha."""
    return body.strip().strip('"').lower() == "true"


def is_session_expired(status_code: int, body: str) -> bool:
    """Sessao expirada/ausente: HTTP 401/403 ou corpo com 'Authorization has been denied'."""
    if status_code in (401, 403):
        return True
    # A mensagem de negacao e curta; nao varremos respostas grandes de /Posicao.
    return DENIED_MARKER in body[:300].lower()


def is_retryable_status(status_code: int) -> bool:
    """5xx e 429 sao transitorios; o resto e respondido ao chamador."""
    return status_code >= 500 or status_code == 429


def parse_positions_response(data: Any) -> dict[str, Any]:
    """Valida o envelope de /Posicao: objeto com ``l`` (lista) e ``hr``.

    ``l`` nulo vira lista vazia. Campos extras sao preservados (o flatten os ignora).
    """
    if not isinstance(data, dict):
        raise OlhoVivoError("resposta de /Posicao nao e um objeto JSON")
    lines = data.get("l")
    if lines is None:
        if "Message" in data:
            raise OlhoVivoError("resposta de erro da API em /Posicao")
        lines = []
    if not isinstance(lines, list):
        raise OlhoVivoError("campo 'l' de /Posicao nao e uma lista")
    out = dict(data)
    out["l"] = lines
    out["hr"] = str(data.get("hr") or "")
    return out


def redact(text: str, secret: str) -> str:
    """Remove o segredo de um texto (mensagens de excecao do requests incluem a URL)."""
    if secret:
        text = text.replace(secret, "***")
    return text


# --------------------------------------------------------------------------- cliente


class OlhoVivoClient:
    """Cliente com sessao HTTP persistente (cookie ``apiCredentials``)."""

    def __init__(
        self,
        base_url: str,
        token: str,
        timeout_s: float = 20.0,
        max_retries: int = 5,
        session: requests.Session | None = None,
        shutdown: ShutdownFlag | None = None,
        sleep: Callable[[float], object] | None = None,
    ) -> None:
        if not token:
            raise AuthenticationError("SPTRANS_TOKEN nao configurado")
        self.base_url = base_url.rstrip("/")
        self._token = token
        self.timeout_s = timeout_s
        self.max_attempts = max(1, max_retries)
        self.session = session or requests.Session()
        self.session.headers.setdefault("User-Agent", USER_AGENT)
        self._shutdown = shutdown
        if sleep is not None:
            self._sleep = sleep
        elif shutdown is not None:
            self._sleep = shutdown.wait
        else:
            self._sleep = time.sleep
        self.authenticated = False

    @classmethod
    def from_settings(cls, settings: Settings, shutdown: ShutdownFlag | None = None) -> "OlhoVivoClient":
        return cls(
            base_url=settings.sptrans_base_url,
            token=settings.sptrans_token,
            timeout_s=settings.http_timeout_seconds,
            max_retries=settings.http_max_retries,
            shutdown=shutdown,
        )

    def __repr__(self) -> str:  # nunca expor o token
        return f"OlhoVivoClient(base_url={self.base_url!r}, authenticated={self.authenticated})"

    def close(self) -> None:
        self.session.close()

    # ------------------------------------------------------------------ HTTP com retry

    def _request(self, method: str, path: str, params: dict[str, str] | None = None) -> requests.Response:
        url = f"{self.base_url}{path}"
        last_error = ""
        for attempt in range(self.max_attempts):
            if attempt > 0:
                delay = backoff_delay(attempt - 1)
                log.warning("nova tentativa na api", path=path, attempt=attempt + 1,
                            max_attempts=self.max_attempts, retry_in_s=round(delay, 2), error=last_error)
                self._sleep(delay)
                if self._shutdown is not None and self._shutdown.is_set():
                    raise OlhoVivoError("encerramento solicitado durante retry")
            try:
                response = self.session.request(method, url, params=params, timeout=self.timeout_s)
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_error = f"{type(exc).__name__}: {redact(str(exc), self._token)}"
                continue
            if is_retryable_status(response.status_code):
                last_error = f"HTTP {response.status_code}"
                continue
            return response
        raise OlhoVivoError(f"api indisponivel apos {self.max_attempts} tentativas em {path}: {last_error}")

    # ------------------------------------------------------------------ API

    def authenticate(self) -> None:
        """Autentica e guarda o cookie de sessao. Levanta AuthenticationError se recusado."""
        self.authenticated = False
        self.session.cookies.clear()
        response = self._request("POST", AUTH_PATH, params={"token": self._token})
        if response.status_code != 200 or not parse_auth_response(response.text):
            raise AuthenticationError(f"autenticacao recusada pela api (HTTP {response.status_code})")
        self.authenticated = True
        log.info("autenticado na api olho vivo")

    def get_positions(self) -> dict[str, Any]:
        """Retorna o payload cru de GET /Posicao (validado), reautenticando se preciso."""
        if not self.authenticated:
            self.authenticate()
        response = self._request("GET", POSITIONS_PATH)
        if is_session_expired(response.status_code, response.text):
            log.warning("sessao expirada, reautenticando", status=response.status_code)
            self.authenticate()
            response = self._request("GET", POSITIONS_PATH)
            if is_session_expired(response.status_code, response.text):
                self.authenticated = False
                raise AuthenticationError("sessao recusada mesmo apos reautenticar")
        if response.status_code != 200:
            raise OlhoVivoError(f"status inesperado em /Posicao: HTTP {response.status_code}")
        try:
            data = response.json()
        except ValueError as exc:
            raise OlhoVivoError("resposta de /Posicao nao e JSON valido") from exc
        return parse_positions_response(data)

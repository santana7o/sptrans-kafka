"""Encerramento limpo dos servicos (SIGTERM do Docker, Ctrl+C)."""

from __future__ import annotations

import signal
import threading


class ShutdownFlag:
    """Flag setada ao receber SIGTERM/SIGINT. Use ``while not flag.is_set(): ...``.

    ``flag.wait(segundos)`` substitui ``time.sleep`` e retorna antes se houver shutdown.
    """

    def __init__(self, install_handlers: bool = True) -> None:
        self._event = threading.Event()
        if install_handlers:
            signal.signal(signal.SIGTERM, self._handle)
            signal.signal(signal.SIGINT, self._handle)

    def _handle(self, signum, frame) -> None:  # noqa: ARG002
        self._event.set()

    def set(self) -> None:
        self._event.set()

    def is_set(self) -> bool:
        return self._event.is_set()

    def wait(self, seconds: float) -> bool:
        return self._event.wait(seconds)

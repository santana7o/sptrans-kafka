"""Terminais de onibus de Sao Paulo usados na regra de alerta.

A API Olho Vivo informa apenas o letreiro dos terminais (lt0/lt1), sem coordenadas.
Esta lista contem coordenadas APROXIMADAS (centro do terminal, precisao de ~100-200 m)
dos principais terminais municipais e terminais de integracao. Um veiculo a menos de
``TERMINAL_RADIUS_M`` metros de qualquer um deles e considerado "no terminal" e nao
gera alerta de parada. Ver limitacoes em docs/DECISIONS.md.
"""

from __future__ import annotations

from dataclasses import dataclass

from common.geo import haversine_m


@dataclass(frozen=True)
class Terminal:
    name: str
    lat: float
    lon: float


TERMINALS: tuple[Terminal, ...] = (
    Terminal("Parque Dom Pedro II", -23.5490, -46.6290),
    Terminal("Bandeira", -23.5503, -46.6400),
    Terminal("Princesa Isabel", -23.5340, -46.6440),
    Terminal("Amaral Gurgel", -23.5440, -46.6480),
    Terminal("Mercado", -23.5420, -46.6290),
    Terminal("Barra Funda", -23.5260, -46.6670),
    Terminal("Pinheiros", -23.5670, -46.7020),
    Terminal("Butanta", -23.5720, -46.7080),
    Terminal("Lapa", -23.5220, -46.7050),
    Terminal("Pirituba", -23.4890, -46.7270),
    Terminal("Casa Verde", -23.5090, -46.6560),
    Terminal("Vila Nova Cachoeirinha", -23.4720, -46.6700),
    Terminal("Santana", -23.5020, -46.6250),
    Terminal("Tucuruvi", -23.4800, -46.6030),
    Terminal("Armenia", -23.5250, -46.6290),
    Terminal("Penha", -23.5280, -46.5420),
    Terminal("Tatuape", -23.5400, -46.5760),
    Terminal("Vila Carrao", -23.5530, -46.5370),
    Terminal("Aricanduva", -23.5700, -46.5070),
    Terminal("Itaquera", -23.5420, -46.4710),
    Terminal("Cidade Tiradentes", -23.5820, -46.4060),
    Terminal("Sao Mateus", -23.6100, -46.4780),
    Terminal("Vila Prudente", -23.5830, -46.5820),
    Terminal("Sacoma", -23.6020, -46.6020),
    Terminal("Jabaquara", -23.6460, -46.6410),
    Terminal("Santo Amaro", -23.6540, -46.7120),
    Terminal("Joao Dias", -23.6430, -46.7270),
    Terminal("Campo Limpo", -23.6490, -46.7590),
    Terminal("Capelinha", -23.6610, -46.7600),
    Terminal("Guarapiranga", -23.6720, -46.7330),
    Terminal("Jardim Angela", -23.7120, -46.7700),
    Terminal("Grajau", -23.7400, -46.6980),
    Terminal("Varginha", -23.7840, -46.7000),
    Terminal("Parelheiros", -23.8250, -46.7280),
)


def nearest_terminal(lat: float, lon: float) -> tuple[Terminal, float]:
    """Terminal mais proximo e a distancia em metros."""
    best = min(TERMINALS, key=lambda t: haversine_m(lat, lon, t.lat, t.lon))
    return best, haversine_m(lat, lon, best.lat, best.lon)


def is_near_terminal(lat: float, lon: float, radius_m: float = 400.0) -> bool:
    """True se o ponto estiver dentro do raio de algum terminal conhecido."""
    _, distance = nearest_terminal(lat, lon)
    return distance <= radius_m

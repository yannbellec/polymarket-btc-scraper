"""
Historique des marchés terminés — contexte inter-marchés (séries d’outcomes, momentum BTC).
Les 3 fenêtres précédentes : outcome, mouvement % BTC sur la fenêtre, prix final YES.
"""
from collections import deque
from typing import Any, Dict, Optional

from monitoring.logger import log

# Fenêtres résolues (ordre chronologique d’insertion ; filtrage par window_ts au read)
_COMPLETIONS: deque = deque(maxlen=500)


def bootstrap_from_duckdb(con: Any) -> None:
    """
    Au démarrage (init_schema), recharge les derniers snapshots résolus depuis DuckDB
    pour que get_context_for_window fonctionne avant le prochain expiry.
    """
    global _COMPLETIONS
    try:
        rows = con.execute("""
            SELECT m.window_ts, s.market_id, s.winning_outcome, s.btc_move_pct, s.close_price_yes
            FROM market_snapshots s
            INNER JOIN btc_markets m ON s.market_id = m.market_id
            WHERE s.winning_outcome IS NOT NULL
            ORDER BY m.window_ts DESC
            LIMIT 100
        """).fetchall()
    except Exception as e:
        log.warning(f"inter_market_context bootstrap DuckDB: {e}")
        return
    if not rows:
        return
    _COMPLETIONS.clear()
    for r in sorted(rows, key=lambda x: int(x[0])):
        wts, mid, outcome, btc_m, close_y = r
        _COMPLETIONS.append({
            "window_ts": int(wts),
            "market_id": str(mid),
            "outcome": outcome,
            "btc_move_pct": float(btc_m) if btc_m is not None else 0.0,
            "close_price_yes": float(close_y) if close_y is not None else 0.0,
        })
    log.info(f"inter_market_context: {len(rows)} snapshots rechargés depuis DuckDB")


def record_completion(
    window_ts: int,
    market_id: str,
    outcome: Optional[str],
    btc_move_pct: float,
    close_price_yes: float,
) -> None:
    """Appelé après un snapshot réussi (outcome connu)."""
    if outcome is None:
        return
    _COMPLETIONS.append({
        "window_ts": int(window_ts),
        "market_id": str(market_id),
        "outcome": outcome,
        "btc_move_pct": float(btc_move_pct),
        "close_price_yes": float(close_price_yes),
    })


def _empty_context() -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for p in (1, 2, 3):
        prefix = f"prev{p}_"
        out[prefix + "market_id"] = None
        out[prefix + "outcome"] = None
        out[prefix + "btc_move_pct"] = None
        out[prefix + "close_price_yes"] = None
    return out


def empty_inter_market_fields() -> Dict[str, Any]:
    """Même clés que get_context_for_window, tout à NULL — pour lignes sans contexte."""
    return _empty_context()


def get_context_for_window(window_ts: int) -> Dict[str, Any]:
    """
    Pour une nouvelle fenêtre `window_ts`, retourne les 3 marchés précédents
    (fenêtres strictement antérieures, les plus récentes d’abord).
    """
    out = _empty_context()
    prev = [c for c in _COMPLETIONS if c["window_ts"] < window_ts]
    prev.sort(key=lambda x: -x["window_ts"])
    for i, rec in enumerate(prev[:3]):
        p = i + 1
        prefix = f"prev{p}_"
        out[prefix + "market_id"] = rec["market_id"]
        out[prefix + "outcome"] = rec["outcome"]
        out[prefix + "btc_move_pct"] = rec["btc_move_pct"]
        out[prefix + "close_price_yes"] = rec["close_price_yes"]
    return out

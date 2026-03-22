"""
Historique des marchés terminés — contexte inter-marchés (séries d’outcomes, momentum BTC).
Les 3 fenêtres précédentes : outcome, mouvement % BTC sur la fenêtre, prix final YES.
"""
from collections import deque
from typing import Any, Dict, Optional

# Fenêtres résolues (ordre chronologique d’insertion ; filtrage par window_ts au read)
_COMPLETIONS: deque = deque(maxlen=500)


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

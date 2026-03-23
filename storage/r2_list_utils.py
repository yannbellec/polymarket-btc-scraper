"""
Pagination list_objects_v2 (S3 / R2) : au-delà de ~1000 clés, une seule réponse est tronquée.
Utilisé par les scripts d’audit / maintenance sur le bucket.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


def list_objects_v2_all(
    s3_client: Any,
    bucket: str,
    prefix: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Retourne la liste complète des objets (équivalent de tous les ``Contents``),
    en enchaînant les requêtes avec ``ContinuationToken``.
    """
    out: List[Dict[str, Any]] = []
    kwargs: Dict[str, Any] = {"Bucket": bucket}
    if prefix:
        kwargs["Prefix"] = prefix

    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        out.extend(resp.get("Contents") or [])
        if not resp.get("IsTruncated"):
            break
        kwargs["ContinuationToken"] = resp["NextContinuationToken"]

    return out

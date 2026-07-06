from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable
from urllib.parse import urlparse

HttpGet = Callable[..., Any]


class OAuthValidationError(Exception):
    """Raised when an OAuth bearer token cannot authorize a request."""


@dataclass(frozen=True)
class OAuthPrincipal:
    subject: str
    jarvis_user: str
    scopes: frozenset[str]
    claims: dict[str, Any]


def auth_mode(value: str) -> str:
    mode = value.strip().lower()
    return mode if mode in {"legacy", "oauth", "hybrid"} else "hybrid"


def required_scopes(value: str) -> tuple[str, ...]:
    normalized = value.replace(",", " ")
    return tuple(scope for scope in (part.strip() for part in normalized.split()) if scope)


def oauth_is_configured(orchestration: Any) -> bool:
    return bool(
        str(orchestration.oauth_issuer).strip()
        and str(orchestration.oauth_audience).strip()
        and str(orchestration.oauth_jwks_url).strip()
    )


def oauth_metadata(orchestration: Any) -> dict[str, Any]:
    return {
        "auth_mode": auth_mode(str(orchestration.auth_mode)),
        "issuer": str(orchestration.oauth_issuer).strip(),
        "audience": str(orchestration.oauth_audience).strip(),
        "jwks_url": str(orchestration.oauth_jwks_url).strip(),
        "required_scopes": list(required_scopes(str(orchestration.oauth_required_scopes))),
        "jarvis_user_claim": str(orchestration.oauth_jarvis_user_claim).strip() or "jarvis_user",
    }


class OAuthTokenValidator:
    _NEG_KID_MAX = 1024

    def __init__(
        self,
        *,
        issuer: str,
        audience: str,
        jwks_url: str,
        scopes: tuple[str, ...],
        jarvis_user_claim: str,
        default_alg: str,
        jwks_ttl_s: float,
        jwks_min_refresh_s: float,
        http_get: HttpGet,
        require_jarvis_user: bool = True,
    ) -> None:
        self.issuer = issuer.strip()
        self.audience = audience.strip()
        self.jwks_url = jwks_url.strip()
        _require_secure_url("OAuth issuer", self.issuer)
        _require_secure_url("OAuth JWKS URL", self.jwks_url)
        self.scopes = scopes
        self.jarvis_user_claim = jarvis_user_claim.strip() or "jarvis_user"
        self.require_jarvis_user = require_jarvis_user
        self.default_alg = default_alg.strip() or "RS256"
        self.jwks_ttl_s = max(0.0, float(jwks_ttl_s))
        self.jwks_min_refresh_s = max(0.0, float(jwks_min_refresh_s))
        self._http_get = http_get
        self._lock = threading.Lock()
        self._jwks: dict[str, Any] | None = None
        self._jwks_loaded_at = 0.0
        self._last_unknown_refresh_at = -float("inf")
        self._negative_kids: dict[str, float] = {}

    def validate(self, token: str) -> OAuthPrincipal:
        if not token:
            raise OAuthValidationError("missing bearer token")
        header = self._unverified_header(token)
        kid = str(header.get("kid") or "")
        if not kid:
            raise OAuthValidationError("missing key id")
        key = self._key_for_kid(kid, refresh=False)
        if key is None:
            key = self._key_for_kid(kid, refresh=True)
        if key is None:
            raise OAuthValidationError("unknown key id")

        try:
            import jwt
            from jwt import PyJWK

            signing_key = PyJWK.from_dict(key).key
            algorithms = [str(key.get("alg") or self.default_alg)]
            claims = jwt.decode(
                token,
                signing_key,
                algorithms=algorithms,
                issuer=self.issuer,
                audience=self.audience,
                leeway=30,
                options={"require": ["exp", "iss", "sub"]},
            )
        except Exception as exc:  # noqa: BLE001 - surface all JWT failures as unauthorized.
            raise OAuthValidationError("invalid token") from exc

        subject = str(claims.get("sub") or "")
        if not subject:
            raise OAuthValidationError("missing subject")
        scopes = _claim_scopes(claims)
        missing = [scope for scope in self.scopes if scope not in scopes]
        if missing:
            raise OAuthValidationError("missing required scope")
        jarvis_user = str(claims.get(self.jarvis_user_claim) or "")
        if self.require_jarvis_user and not jarvis_user:
            raise OAuthValidationError("missing jarvis user")
        return OAuthPrincipal(
            subject=subject,
            jarvis_user=jarvis_user,
            scopes=frozenset(scopes),
            claims=dict(claims),
        )

    def _unverified_header(self, token: str) -> dict[str, Any]:
        try:
            import jwt

            header = jwt.get_unverified_header(token)
        except Exception as exc:  # noqa: BLE001
            raise OAuthValidationError("malformed token") from exc
        return header if isinstance(header, dict) else {}

    def _key_for_kid(self, kid: str, *, refresh: bool) -> dict[str, Any] | None:
        now = time.monotonic()
        should_fetch = False
        unknown_refresh = False
        with self._lock:
            self._prune_negative_kids_locked(now)
            missed_at = self._negative_kids.get(kid)
            if missed_at is not None and now - missed_at < self.jwks_min_refresh_s:
                return None

            if self._jwks_is_fresh_locked(now):
                key = self._find_key(self._jwks or {}, kid)
                if key is not None:
                    self._negative_kids.pop(kid, None)
                    return key

                if not refresh:
                    return None

                if now - self._last_unknown_refresh_at < self.jwks_min_refresh_s:
                    self._remember_negative_kid_locked(kid, now)
                    return None

                self._last_unknown_refresh_at = now
                should_fetch = True
                unknown_refresh = True
            else:
                should_fetch = True

        if not should_fetch:
            return None

        try:
            jwks = self._fetch_jwks()
        except OAuthValidationError:
            if unknown_refresh:
                failed_at = time.monotonic()
                with self._lock:
                    self._last_unknown_refresh_at = failed_at
                    self._remember_negative_kid_locked(kid, failed_at)
            raise

        fetched_at = time.monotonic()
        with self._lock:
            if unknown_refresh:
                self._last_unknown_refresh_at = fetched_at
            self._store_jwks_locked(jwks, fetched_at)
            key = self._find_key(self._jwks or {}, kid)
            if key is not None:
                self._negative_kids.pop(kid, None)
                return key
            if unknown_refresh:
                self._remember_negative_kid_locked(kid, fetched_at)
            return None

    def _find_key(self, jwks: dict[str, Any], kid: str) -> dict[str, Any] | None:
        keys = jwks.get("keys")
        if not isinstance(keys, list):
            raise OAuthValidationError("invalid jwks")
        for key in keys:
            if isinstance(key, dict) and str(key.get("kid") or "") == kid:
                return key
        return None

    def _jwks_is_fresh_locked(self, now: float) -> bool:
        return self._jwks is not None and (self.jwks_ttl_s <= 0.0 or now - self._jwks_loaded_at < self.jwks_ttl_s)

    def _fetch_jwks(self) -> dict[str, Any]:
        try:
            response = self._http_get(self.jwks_url, timeout=5)
            if hasattr(response, "raise_for_status"):
                response.raise_for_status()
            data = response.json()
        except Exception as exc:  # noqa: BLE001
            raise OAuthValidationError("jwks fetch failed") from exc
        if not isinstance(data, dict):
            raise OAuthValidationError("invalid jwks")
        return data

    def _store_jwks_locked(self, data: dict[str, Any], now: float) -> None:
        # Round-trip through JSON to detach any response-owned objects.
        self._jwks = json.loads(json.dumps(data))
        self._jwks_loaded_at = now

    def _remember_negative_kid_locked(self, kid: str, now: float) -> None:
        self._prune_negative_kids_locked(now)
        if len(self._negative_kids) >= self._NEG_KID_MAX:
            self._negative_kids.pop(min(self._negative_kids, key=self._negative_kids.get), None)
        self._negative_kids[kid] = now

    def _prune_negative_kids_locked(self, now: float) -> None:
        self._negative_kids = {kid: missed_at for kid, missed_at in self._negative_kids.items() if now - missed_at < self.jwks_min_refresh_s}


def _require_secure_url(label: str, value: str) -> None:
    parsed = urlparse(value)
    if parsed.scheme == "https":
        return
    if parsed.scheme == "http" and parsed.hostname in {"localhost", "127.0.0.1"}:
        return
    raise ValueError(f"{label} must use https:// outside localhost development")


def _claim_scopes(claims: dict[str, Any]) -> set[str]:
    scope = claims.get("scope")
    if isinstance(scope, str):
        return {part for part in scope.split() if part}
    scp = claims.get("scp")
    if isinstance(scp, list):
        return {str(part) for part in scp if str(part)}
    return set()

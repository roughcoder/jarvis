from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable

import jwt
from jwt import PyJWK


HttpGet = Callable[..., Any]


class OAuthValidationError(Exception):
    """Raised when a cockpit OAuth bearer token cannot authorize a request."""


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
        "legacy_token_available": bool(orchestration.api_token.get_secret_value()),
    }


class OAuthTokenValidator:
    def __init__(
        self,
        *,
        issuer: str,
        audience: str,
        jwks_url: str,
        scopes: tuple[str, ...],
        jarvis_user_claim: str,
        http_get: HttpGet,
    ) -> None:
        self.issuer = issuer.strip()
        self.audience = audience.strip()
        self.jwks_url = jwks_url.strip()
        self.scopes = scopes
        self.jarvis_user_claim = jarvis_user_claim.strip() or "jarvis_user"
        self._http_get = http_get
        self._jwks: dict[str, Any] | None = None

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
            signing_key = PyJWK.from_dict(key).key
            algorithms = [str(key.get("alg") or header.get("alg") or "RS256")]
            claims = jwt.decode(
                token,
                signing_key,
                algorithms=algorithms,
                issuer=self.issuer,
                audience=self.audience,
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
        return OAuthPrincipal(
            subject=subject,
            jarvis_user=jarvis_user,
            scopes=frozenset(scopes),
            claims=dict(claims),
        )

    def _unverified_header(self, token: str) -> dict[str, Any]:
        try:
            header = jwt.get_unverified_header(token)
        except Exception as exc:  # noqa: BLE001
            raise OAuthValidationError("malformed token") from exc
        return header if isinstance(header, dict) else {}

    def _key_for_kid(self, kid: str, *, refresh: bool) -> dict[str, Any] | None:
        jwks = self._load_jwks(refresh=refresh)
        keys = jwks.get("keys")
        if not isinstance(keys, list):
            raise OAuthValidationError("invalid jwks")
        for key in keys:
            if isinstance(key, dict) and str(key.get("kid") or "") == kid:
                return key
        return None

    def _load_jwks(self, *, refresh: bool = False) -> dict[str, Any]:
        if self._jwks is not None and not refresh:
            return self._jwks
        try:
            response = self._http_get(self.jwks_url, timeout=5)
            if hasattr(response, "raise_for_status"):
                response.raise_for_status()
            data = response.json()
        except Exception as exc:  # noqa: BLE001
            raise OAuthValidationError("jwks fetch failed") from exc
        if not isinstance(data, dict):
            raise OAuthValidationError("invalid jwks")
        # Round-trip through JSON to detach any response-owned objects.
        self._jwks = json.loads(json.dumps(data))
        return self._jwks


def _claim_scopes(claims: dict[str, Any]) -> set[str]:
    scope = claims.get("scope")
    if isinstance(scope, str):
        return {part for part in scope.split() if part}
    scp = claims.get("scp")
    if isinstance(scp, list):
        return {str(part) for part in scp if str(part)}
    return set()

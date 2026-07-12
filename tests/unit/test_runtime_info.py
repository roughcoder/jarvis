from jarvis import __version__
from jarvis.runtime_info import runtime_info


def test_runtime_info_defaults_to_production_release() -> None:
    assert runtime_info({}) == {
        "version": __version__,
        "channel": "production",
        "git_sha": "",
    }


def test_runtime_info_reports_valid_dogfood_identity() -> None:
    sha = "a" * 40
    assert runtime_info(
        {
            "JARVIS_RUNTIME_CHANNEL": "Dogfood",
            "JARVIS_RUNTIME_GIT_SHA": sha.upper(),
        }
    ) == {"version": __version__, "channel": "dogfood", "git_sha": sha}


def test_runtime_info_never_reflects_malformed_identity() -> None:
    assert runtime_info(
        {
            "JARVIS_RUNTIME_CHANNEL": "dogfood; printenv",
            "JARVIS_RUNTIME_GIT_SHA": "not-a-commit",
        }
    ) == {"version": __version__, "channel": "unknown", "git_sha": ""}

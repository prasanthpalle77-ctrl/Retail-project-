from pathlib import Path

import pytest

from retail_lakehouse.config.settings import SettingsError, load_settings

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_test_environment_overrides_development_baseline() -> None:
    settings = load_settings("test", PROJECT_ROOT)

    assert settings["application"]["environment"] == "test"
    assert settings["application"]["timezone"] == "UTC"
    assert settings["spark"]["master"] == "local[2]"
    assert settings["pipeline"]["default_currency"] == "USD"


def test_invalid_environment_is_rejected() -> None:
    with pytest.raises(SettingsError, match="Unsupported environment"):
        load_settings("personal", PROJECT_ROOT)


@pytest.mark.parametrize("environment", ["databricks_dev", "staging", "prod"])
def test_databricks_environments_use_managed_runtime_and_volume_paths(environment: str) -> None:
    settings = load_settings(environment, PROJECT_ROOT)

    assert settings["storage"]["mode"] == "databricks"
    assert settings["storage"]["bronze"].startswith("/Volumes/")
    assert settings["storage"]["gold"].startswith("/Volumes/")
    assert settings["spark"]["master"] is None
    assert settings["spark"]["enable_delta"] is False

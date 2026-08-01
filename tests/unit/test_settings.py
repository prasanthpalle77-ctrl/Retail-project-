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

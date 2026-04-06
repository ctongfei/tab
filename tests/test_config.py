import json
from unittest.mock import patch

from tab_cli import config as config_module
from tab_cli.cli import app
from tab_cli.config import Config, load_config_file

from tests.conftest import TEST_CSV, runner


class TestConfigFile:
    def setup_method(self):
        config_module.config = Config()

    def test_load_missing_file(self, tmp_path):
        loaded = load_config_file(tmp_path / "nonexistent.json")
        assert loaded.az_url_authority_is_account is False
        assert loaded.default_num_view_rows == 20
        assert loaded.log_level == "INFO"
        assert loaded.max_cell_length is None
        assert loaded.num_remote_workers == 8
        assert loaded.sampling_size_for_schema_inference == 32

    def test_load_valid_config(self, tmp_path):
        cfg = tmp_path / "config.json"
        cfg.write_text(
            json.dumps(
                {
                    "az_url_authority_is_account": True,
                    "default_num_view_rows": 10,
                    "log_level": "DEBUG",
                    "max_cell_length": 12,
                    "num_remote_workers": 2,
                    "sampling_size_for_schema_inference": 64,
                }
            )
        )
        loaded = load_config_file(cfg)
        assert loaded.az_url_authority_is_account is True
        assert loaded.default_num_view_rows == 10
        assert loaded.log_level == "DEBUG"
        assert loaded.max_cell_length == 12
        assert loaded.num_remote_workers == 2
        assert loaded.sampling_size_for_schema_inference == 64

    def test_load_partial_config(self, tmp_path):
        cfg = tmp_path / "config.json"
        cfg.write_text(
            json.dumps(
                {
                    "max_cell_length": None,
                    "sampling_size_for_schema_inference": 128,
                }
            )
        )
        loaded = load_config_file(cfg)
        assert loaded.az_url_authority_is_account is False
        assert loaded.default_num_view_rows == 20
        assert loaded.log_level == "INFO"
        assert loaded.max_cell_length is None
        assert loaded.num_remote_workers == 8
        assert loaded.sampling_size_for_schema_inference == 128

    def test_unknown_keys_ignored(self, tmp_path):
        cfg = tmp_path / "config.json"
        cfg.write_text(
            json.dumps(
                {"unknown_key": "value", "sampling_size_for_schema_inference": 16}
            )
        )
        loaded = load_config_file(cfg)
        assert loaded.sampling_size_for_schema_inference == 16
        assert not hasattr(loaded, "unknown_key")

    def test_invalid_json_raises(self, tmp_path):
        cfg = tmp_path / "config.json"
        cfg.write_text('"just a string"')
        try:
            load_config_file(cfg)
            assert False, "Expected ValueError"
        except ValueError as exc:
            assert "JSON object" in str(exc)

    def test_invalid_type_raises(self, tmp_path):
        cfg = tmp_path / "config.json"
        cfg.write_text(json.dumps({"num_remote_workers": "three"}))

        try:
            load_config_file(cfg)
            assert False, "Expected ValueError"
        except ValueError as exc:
            assert "num_remote_workers" in str(exc)

    def test_invalid_log_level_in_config_is_rejected_by_cli(self, tmp_path):
        cfg = tmp_path / "config.json"
        cfg.write_text(json.dumps({"log_level": "NOPE"}))

        with patch(
            "tab_cli.cli.load_config_file", side_effect=lambda: load_config_file(cfg)
        ):
            result = runner.invoke(app, ["view", TEST_CSV])

        assert result.exit_code != 0
        assert "Invalid value" in result.output
        assert "Invalid log level 'NOPE'" in result.output

    def test_invalid_cli_log_level_has_friendly_error(self):
        result = runner.invoke(app, ["--log-level", "NOPE", "view", TEST_CSV])

        assert result.exit_code != 0
        assert "Invalid value" in result.output
        assert "Invalid log level 'NOPE'" in result.output

    def test_view_uses_default_max_cell_length_from_config(self, tmp_path):
        cfg = tmp_path / "config.json"
        cfg.write_text(json.dumps({"max_cell_length": 5}))

        with patch(
            "tab_cli.cli.load_config_file", side_effect=lambda: load_config_file(cfg)
        ):
            result = runner.invoke(app, ["view", TEST_CSV])

        assert result.exit_code == 0
        assert "Contr..." in result.output

    def test_cli_max_cell_length_overrides_config(self, tmp_path):
        cfg = tmp_path / "config.json"
        cfg.write_text(json.dumps({"max_cell_length": 5}))

        with patch(
            "tab_cli.cli.load_config_file", side_effect=lambda: load_config_file(cfg)
        ):
            result = runner.invoke(
                app, ["view", TEST_CSV, "--max-cell-length", "10"]
            )

        assert result.exit_code == 0
        assert "Control" in result.output

    def test_cli_flag_overrides_config_file(self, tmp_path):
        cfg = tmp_path / "config.json"
        cfg.write_text(json.dumps({"az_url_authority_is_account": False}))
        with patch(
            "tab_cli.cli.load_config_file", side_effect=lambda: load_config_file(cfg)
        ):
            result = runner.invoke(app, ["--az-url-authority-is-account", "view", TEST_CSV])
            assert result.exit_code == 0
            assert config_module.config.az_url_authority_is_account is True

    def test_config_file_sets_log_level(self, tmp_path):
        cfg = tmp_path / "config.json"
        cfg.write_text(json.dumps({"log_level": "DEBUG"}))
        with patch(
            "tab_cli.cli.load_config_file", side_effect=lambda: load_config_file(cfg)
        ):
            with patch("tab_cli.cli.logger.remove"), patch(
                "tab_cli.cli.logger.add"
            ) as logger_add:
                result = runner.invoke(app, ["view", TEST_CSV])

        assert result.exit_code == 0
        assert logger_add.call_args.kwargs["level"] == "DEBUG"

    def test_cli_log_level_overrides_config(self, tmp_path):
        cfg = tmp_path / "config.json"
        cfg.write_text(json.dumps({"log_level": "DEBUG"}))
        with patch(
            "tab_cli.cli.load_config_file", side_effect=lambda: load_config_file(cfg)
        ):
            with patch("tab_cli.cli.logger.remove"), patch(
                "tab_cli.cli.logger.add"
            ) as logger_add:
                result = runner.invoke(app, ["--log-level", "ERROR", "view", TEST_CSV])

        assert result.exit_code == 0
        assert logger_add.call_args.kwargs["level"] == "ERROR"

    def test_config_file_sets_default(self, tmp_path):
        cfg = tmp_path / "config.json"
        cfg.write_text(json.dumps({"az_url_authority_is_account": True}))
        with patch(
            "tab_cli.cli.load_config_file", side_effect=lambda: load_config_file(cfg)
        ):
            result = runner.invoke(app, ["view", TEST_CSV])
            assert result.exit_code == 0
            assert config_module.config.az_url_authority_is_account is True

    def test_config_file_sets_default_view_rows(self, tmp_path):
        cfg = tmp_path / "config.json"
        cfg.write_text(json.dumps({"default_num_view_rows": 3}))
        with patch(
            "tab_cli.cli.load_config_file", side_effect=lambda: load_config_file(cfg)
        ):
            result = runner.invoke(app, ["view", TEST_CSV])
            assert result.exit_code == 0
            count = sum(1 for line in result.output.splitlines() if "P00" in line)
            assert count <= 3

    def test_config_file_sets_num_remote_workers(self, tmp_path):
        cfg = tmp_path / "config.json"
        cfg.write_text(json.dumps({"num_remote_workers": 3}))
        loaded = load_config_file(cfg)
        assert loaded.num_remote_workers == 3

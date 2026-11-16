# -*- coding: utf-8 -*-
"""Address search variants: off by default, with one-time legacy migration."""
from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[1] / "src" / "rosreestr_parser_app.py"
spec = importlib.util.spec_from_file_location("oss_variants_default_tests", SOURCE)
assert spec and spec.loader
app = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = app
spec.loader.exec_module(app)


class TestVariantsDisabledByDefault(unittest.TestCase):
    def test_program_default_and_example_are_off(self):
        self.assertIs(app.DEFAULT_CONFIG["try_address_variants"], False)
        example = json.loads((SOURCE.parents[1] / "config.example.json").read_text(encoding="utf-8"))
        self.assertIs(example["try_address_variants"], False)

    def test_old_saved_true_is_migrated_off_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.json"
            config_path.write_text(
                json.dumps({"try_address_variants": True, "start_flat": 5}),
                encoding="utf-8",
            )
            cfg = app.load_config(config_path)
            self.assertIs(cfg["try_address_variants"], False)
            self.assertIs(cfg["address_variants_default_off_applied"], True)
            self.assertEqual(cfg["start_flat"], 5)

            app.save_config(config_path, cfg)
            reloaded = app.load_config(config_path)
            self.assertIs(reloaded["try_address_variants"], False)

            # Explicitly enable and save after migration: respect this choice.
            reloaded["try_address_variants"] = True
            app.save_config(config_path, reloaded)
            self.assertIs(app.load_config(config_path)["try_address_variants"], True)

    def test_saved_profile_and_user_choice(self):
        old_profile = {"try_address_variants": True, "address_prefix": "г Тестоград ул Примерная д 42"}
        migrated = app.migrate_legacy_address_variants(old_profile)
        self.assertIs(migrated["try_address_variants"], False)
        self.assertEqual(migrated["address_prefix"], old_profile["address_prefix"])
        migrated["try_address_variants"] = True
        self.assertIs(app.migrate_legacy_address_variants(migrated)["try_address_variants"], True)

    def test_new_config_missing_option_defaults_off(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text("{}", encoding="utf-8")
            self.assertIs(app.load_config(path)["try_address_variants"], False)


if __name__ == "__main__":
    unittest.main()

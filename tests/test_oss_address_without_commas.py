# -*- coding: utf-8 -*-
"""OSS address matching: only fictional cities, streets and property identifiers."""
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[1] / "src" / "rosreestr_parser_app.py"
spec = importlib.util.spec_from_file_location("rosreestr_oss_address_test", SOURCE)
assert spec is not None and spec.loader is not None
app = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = app
spec.loader.exec_module(app)


class TestOssAddressWithoutCommas(unittest.TestCase):
    query = "г Тестоград ул Макетная д 42 кв 4"
    row = (
        "00:00:000000:1001\n"
        "Вымышленная область, г.о. город Тестоград, г. Тестоград, "
        "ул. Макетная, д. 42, кв. 4"
    )

    def test_city_before_unpunctuated_street_is_not_included_in_city(self):
        self.assertEqual(app.extract_query_settlement(self.query), "тестоград")
        self.assertEqual(app.extract_explicit_settlement(self.row), "тестоград")
        self.assertTrue(app.settlement_matches(self.query, self.row))

    def test_matches_exact_row_without_commas_or_abbreviation_periods(self):
        self.assertTrue(app.address_matches(
            self.query, self.row, "4", strict_house=True, strict_street=True
        ))
        self.assertTrue(app.address_matches(
            "г. Тестоград, ул. Макетная, д. 42, кв. 4",
            self.row, "4", strict_house=True, strict_street=True
        ))

    def test_multiword_city_is_preserved(self):
        query = "г Новая Тестоградия ул Макетная д 42 кв 4"
        row = "Вымышленная область, г. Новая Тестоградия, ул. Макетная, д. 42, кв. 4"
        self.assertEqual(app.extract_query_settlement(query), "новая тестоградия")
        self.assertTrue(app.address_matches(query, row, "4", strict_house=True))

    def test_rejects_other_city_street_flat_and_house(self):
        wrong = (
            "Вымышленная область, г. Демо-град, ул. Макетная, д. 42, кв. 4",
            "Вымышленная область, г. Тестоград, ул. Примерная, д. 42, кв. 4",
            "Вымышленная область, г. Тестоград, ул. Макетная, д. 42, кв. 5",
            "Вымышленная область, г. Тестоград, ул. Макетная, д. 42/1, кв. 4",
            "Вымышленная область, г. Тестоград, ул. Макетная, д. 43, кв. 4",
        )
        for address in wrong:
            with self.subTest(address=address):
                self.assertFalse(app.address_matches(
                    self.query, address, "4", strict_house=True, strict_street=True
                ))

    def test_fractional_houses_are_not_confused(self):
        self.assertEqual(app.extract_house_no("г Тестоград ул Макетная д 42/1 кв 4"), "42/1")
        self.assertEqual(app.extract_house_no("г Тестоград ул Макетная д 42/2 кв 4"), "42/2")
        self.assertEqual(app.extract_house_no("г Тестоград ул Макетная д 42А кв 4"), "42А")
        self.assertTrue(app.address_matches(
            "г Тестоград ул Макетная д 42/1 кв 4",
            "Вымышленная область, г. Тестоград, ул. Макетная, д. 42/1, кв. 4",
            "4", strict_house=True
        ))
        self.assertFalse(app.address_matches(
            "г Тестоград ул Макетная д 42/1 кв 4",
            "Вымышленная область, г. Тестоград, ул. Макетная, д. 42/2, кв. 4",
            "4", strict_house=True
        ))

    def test_without_explicit_unit_marker_cannot_pass_as_apartment(self):
        bare_building = "Вымышленная область, г. Тестоград, ул. Макетная, д. 42"
        self.assertFalse(app.address_matches(
            self.query, bare_building, "4", strict_house=True
        ))


if __name__ == "__main__":
    unittest.main()

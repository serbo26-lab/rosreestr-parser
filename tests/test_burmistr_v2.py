# -*- coding: utf-8 -*-
"""Burmistr V2: template and numeric area only. No actual property records."""
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

from openpyxl import load_workbook, Workbook

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "rosreestr_burmistr_v2_tests", ROOT / "src" / "rosreestr_parser_app.py"
)
assert spec is not None and spec.loader is not None
app = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = app
spec.loader.exec_module(app)


class TestBurmistrV2(unittest.TestCase):
    def make_writer(self, export_format="burmistr"):
        writer = app.ExcelOutput.__new__(app.ExcelOutput)
        writer.wb = Workbook()
        writer.ws = writer.wb.active
        writer.export_format = export_format
        writer.row_style = []
        writer.data_headers = list(app.REQUIRED_HEADERS)
        writer.header_map = {
            app.normalize_text(header): index + 1
            for index, header in enumerate(app.REQUIRED_HEADERS)
        }
        writer._seen_reg_keys = set()
        writer.ws.append(list(app.REQUIRED_HEADERS))
        return writer

    def test_template_contains_only_headers_without_example_records(self):
        template = load_workbook(ROOT / "template_burmistr.xlsx")
        self.assertEqual(template.sheetnames, ["Данные"])
        ws = template["Данные"]
        self.assertEqual(
            [ws.cell(1, column).value for column in range(1, 15)],
            list(app.REQUIRED_HEADERS),
        )
        for row in ws.iter_rows(min_row=2, max_col=14):
            self.assertTrue(all(cell.value is None for cell in row))

    def test_burmistr_area_converts_comma_decimal_to_number(self):
        writer = self.make_writer()
        writer.append_data({
            "Номер помещения (обяз.)": "1",
            "Площадь помещения, м2 (обяз.)": "60,7",
            "Кадастровый номер": "00:00:000000:1001",
        })
        self.assertEqual(writer.ws["B2"].value, 60.7)
        self.assertIsInstance(writer.ws["B2"].value, float)

    def test_roskvartal_area_is_not_changed(self):
        writer = self.make_writer(export_format="roskvartal")
        writer.append_data({
            "Номер помещения (обяз.)": "2",
            "Площадь помещения, м2 (обяз.)": "60,7",
        })
        self.assertEqual(writer.ws["B2"].value, "60,7")

    def test_area_conversion_rejects_malformed_data_without_guessing(self):
        self.assertIsNone(app.burmistr_area_number("площадь неизвестна"))
        self.assertIsNone(app.burmistr_area_number("0"))
        self.assertIsNone(app.burmistr_area_number("60,7 кв. м"))
        self.assertEqual(app.burmistr_area_number("60,7"), 60.7)

    def test_share_calculation_remains_unchanged_for_rosreestr(self):
        one = [app.RightInfo("Собственность", "номер права A", "")]
        self.assertEqual(app.share_for_right(one[0], one), ("", "1/1"))
        two = [
            app.RightInfo("Общая долевая собственность", "номер права A", ""),
            app.RightInfo("Общая долевая собственность", "номер права B", ""),
        ]
        self.assertEqual([app.share_for_right(r, two) for r in two], [
            ("", "1/2"), ("", "1/2")
        ])
        joint = [app.RightInfo("Общая совместная собственность", "номер права A", "")]
        self.assertEqual(app.share_for_right(joint[0], joint), ("да", "1/1"))


if __name__ == "__main__":
    unittest.main()

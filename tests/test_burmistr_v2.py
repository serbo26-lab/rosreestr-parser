# -*- coding: utf-8 -*-
"""Burmistr V2 regression tests. ALL names and cadastral identifiers are fictional."""
from __future__ import annotations

import importlib.util
import sys
import unittest
from fractions import Fraction
from pathlib import Path

from openpyxl import Workbook, load_workbook

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("rosreestr_burmistr_v2_tests", ROOT / "src" / "rosreestr_parser_app.py")
assert spec is not None and spec.loader is not None
app = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = app
spec.loader.exec_module(app)


class TestBurmistrV2(unittest.TestCase):
    def make_writer(self):
        writer = app.ExcelOutput.__new__(app.ExcelOutput)
        writer.wb = Workbook()
        writer.ws = writer.wb.active
        writer.ws.title = "Данные"
        writer.aux_ws = writer.wb.create_sheet("Здания и участки")
        writer.validation_ws = writer.wb.create_sheet("Проверка")
        writer.duplicates_ws = writer.wb.create_sheet("Дубли")
        writer.export_format = "burmistr"
        writer.row_style = []
        writer.data_headers = list(app.REQUIRED_HEADERS)
        writer.header_map = {
            app.normalize_text(header): index + 1
            for index, header in enumerate(app.REQUIRED_HEADERS)
        }
        writer._seen_reg_keys = set()
        writer.ws.append(list(app.REQUIRED_HEADERS))
        return writer

    def test_template_contains_only_headers_and_no_example_people(self):
        template = load_workbook(ROOT / "template_burmistr.xlsx")
        ws = template["Данные"]
        self.assertEqual(
            [ws.cell(1, col).value for col in range(1, 15)],
            list(app.REQUIRED_HEADERS)
        )
        for row in ws.iter_rows(min_row=2, max_col=14):
            self.assertTrue(all(cell.value is None for cell in row))

    def test_area_is_numeric_from_comma_decimal_string(self):
        writer = self.make_writer()
        writer.append_data({
            "Номер помещения (обяз.)": "1",
            "Площадь помещения, м2 (обяз.)": "60,7",
            "Кадастровый номер": "00:00:000000:1001",
        })
        self.assertEqual(writer.ws["B2"].value, 60.7)
        self.assertIsInstance(writer.ws["B2"].value, float)

    def test_joint_share_and_other_shares_total_one(self):
        reg_joint = "00:00:000000:1001-00/001/2020-1"
        items = [
            (2, Fraction(18,20), "18/20", "да", reg_joint),
            (3, Fraction(18,20), "18/20", "да", reg_joint),
            (4, Fraction(1,20), "1/20", "", "00:00:000000:1001-00/001/2020-2"),
            (5, Fraction(1,20), "1/20", "", "00:00:000000:1001-00/001/2020-3"),
        ]
        effective, conflicts = app.burmistr_effective_shares(items)
        self.assertEqual(conflicts, [])
        self.assertEqual(sum((frac for _r, frac, _share in effective), Fraction(0,1)), Fraction(1,1))
        self.assertEqual(len(effective), 3)

    def test_joint_shares_with_same_registration_must_match(self):
        items = [
            (2, Fraction(18,20), "18/20", "да", "00:00:000000:1001-00/001/2020-1"),
            (3, Fraction(17,20), "17/20", "да", "00:00:000000:1001-00/001/2020-1"),
        ]
        _effective, conflicts = app.burmistr_effective_shares(items)
        self.assertEqual(conflicts, [(3, "00:00:000000:1001-00/001/2020-1")])

    def test_joint_without_registered_right_is_not_silently_deduplicated(self):
        items = [
            (2, Fraction(1,2), "1/2", "да", "."),
            (3, Fraction(1,2), "1/2", "да", "."),
        ]
        effective, _conflicts = app.burmistr_effective_shares(items)
        self.assertEqual(len(effective), 2)

    def test_v2_joint_registration_validates_without_false_share_errors(self):
        writer = self.make_writer()
        record = "00:00:000000:1001"
        right = record + "-00/001/2020-1"
        rows = [
            ("Макетова", "Лада", "Фиктивовна", "да", "18/20", right),
            ("Макетов", "Лев", "Фиктивович", "да", "18/20", right),
            ("Макетова", "Вера", "Фиктивовна", "", "1/20", record+"-00/001/2020-2"),
            ("Макетов", "Герман", "Фиктивович", "", "1/20", record+"-00/001/2020-3"),
        ]
        for surname, first, middle, joint, share, reg in rows:
            writer.append_data({
                "Номер помещения (обяз.)": "1",
                "Площадь помещения, м2 (обяз.)": "60,7",
                "Кадастровый номер": record,
                'Признак совместной собс. ("да" для сов. собс.)': joint,
                "Доля (1/1, 1/2, 1/3 и т.д.) (обяз., сумма в помещении должна быть 1/1)": share,
                "Номер регистрации (обяз.)": reg,
                "Дата регистрации": "01.02.2020",
                "Фамилия / Наименование организации (обяз.)": surname,
                "Имя (обяз. для физ лиц)": first,
                "Отчество": middle,
            })
        writer.validate_result()
        self.assertEqual(writer.validation_ws.max_row, 1)
        for row in range(2, 6):
            self.assertIsInstance(writer.ws.cell(row, 2).value, float)


if __name__ == "__main__":
    unittest.main()

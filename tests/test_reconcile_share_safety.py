# -*- coding: utf-8 -*-
"""Регрессия безопасного переноса долей. Все данные полностью вымышленные."""
from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from fractions import Fraction
from pathlib import Path

from openpyxl import Workbook, load_workbook

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "rosreestr_share_safety_tests", ROOT / "src" / "rosreestr_parser_app.py"
)
assert spec and spec.loader
app = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = app
spec.loader.exec_module(app)

TEST_CAD = "00:00:000000:9001"
FAKE_REG = ["00-00/001/2020-001", "00-00/001/2020-002",
            "00-00/001/2020-003", "00-00/001/2020-004",
            "00-00/001/2020-005"]


def new_workbook(shares, *, flat="901", joint=False):
    wb = Workbook()
    ws = wb.active
    ws.title = "Данные"
    ws.append(list(app.REQUIRED_HEADERS))
    for index, share in enumerate(shares):
        ws.append([
            flat, 45.5, TEST_CAD, "да" if joint else "",
            share, "", FAKE_REG[index], "", ".", ".", ".", "", "", "",
        ])
    return wb, ws


def stage(ws, updates, flat="901"):
    cols = app.get_new_header_columns(ws, 1)
    match_log = []
    pending = []
    for row_idx, old_share in updates.items():
        match_log.append([
            row_idx, flat, ws.cell(row_idx, cols["reg"]).value,
            "найдено", "номер регистрации", "вымышленный владелец",
            old_share, "вымышленный источник",
        ])
        pending.append((ws.cell(row_idx, cols["share"]), old_share, flat, len(match_log) - 1))
    return cols, pending, match_log


def total_of(ws, cols):
    return sum(
        (app.parse_fraction_share(ws.cell(row, cols["share"]).value)
         for row in range(2, ws.max_row + 1)
         if str(ws.cell(row, cols["flat"]).value or "").strip()),
        Fraction(0, 1),
    )


class TestSafeShareTransfer(unittest.TestCase):
    def check_rejected(self, new_shares, old_shares, expected_projected):
        wb, ws = new_workbook(new_shares)
        cols, pending, log = stage(
            ws, {index + 2: share for index, share in enumerate(old_shares)}
        )
        conflicts = app.commit_old_share_transfers(ws, 1, cols, pending, log)
        self.assertEqual(len(conflicts), 1)
        self.assertIn(expected_projected, conflicts[0][1])
        self.assertEqual(
            [ws.cell(row, cols["share"]).value for row in range(2, ws.max_row + 1)],
            new_shares,
        )
        self.assertEqual(total_of(ws, cols), Fraction(1, 1))

        ambiguous = []
        app.mark_old_share_conflicts(ws, 1, cols, conflicts, ambiguous, log)
        self.assertEqual(len(ambiguous), 1)
        self.assertEqual(ambiguous[0][1], "901")
        self.assertTrue(all(
            ws.cell(row, cols["share"]).fill.fgColor.rgb.endswith("FFF2CC")
            for row in range(2, ws.max_row + 1)
        ))
        self.assertTrue(any("доли не перенесены" == row[3] for row in log))
        self.assertTrue(all(
            "Старые доли не применены" in row[7]
            for row in log if row[3] == "найдено"
        ))

    def test_five_new_rights_four_old_quarters_are_rejected_together(self):
        self.check_rejected(["1/5"] * 5, ["1/4"] * 4, "23/20")

    def test_three_new_rights_two_old_halves_are_rejected_together(self):
        self.check_rejected(["1/3"] * 3, ["1/2"] * 2, "7/6")

    def test_consistent_unequal_old_fractions_are_transferred(self):
        _, ws = new_workbook(["1/2", "1/2"])
        cols, pending, log = stage(ws, {2: "1/4", 3: "3/4"})
        conflicts = app.commit_old_share_transfers(ws, 1, cols, pending, log)
        self.assertEqual(conflicts, [])
        self.assertEqual([ws.cell(2, cols["share"]).value,
                          ws.cell(3, cols["share"]).value], ["1/4", "3/4"])
        self.assertEqual(total_of(ws, cols), Fraction(1, 1))

    def test_joint_ownership_is_not_checked_as_fractional_shares(self):
        _, ws = new_workbook(["1/1", "1/1"], joint=True)
        cols, pending, log = stage(ws, {2: "1/1", 3: "1/1"})
        self.assertEqual(app.commit_old_share_transfers(ws, 1, cols, pending, log), [])
        self.assertEqual(total_of(ws, cols), Fraction(2, 1))

    def test_cell_references_follow_inserted_rows(self):
        _, ws = new_workbook(["1/2", "1/2"])
        cols, pending, log = stage(ws, {2: "1/4", 3: "3/4"})
        ws.insert_rows(2)
        self.assertEqual(app.commit_old_share_transfers(ws, 1, cols, pending, log), [])
        self.assertEqual([ws.cell(3, cols["share"]).value,
                          ws.cell(4, cols["share"]).value], ["1/4", "3/4"])

    def test_removable_empty_placeholder_keeps_existing_cleanup(self):
        _, ws = new_workbook(["1/2", "1/2"])
        cols = app.get_new_header_columns(ws, 1)
        ws.cell(2, cols["last"]).value = "Примеров"
        ws.cell(2, cols["first"]).value = "Тест"
        ws.cell(3, cols["reg"]).value = "."
        cols, pending, log = stage(ws, {2: "1/1"})
        self.assertEqual(app.commit_old_share_transfers(ws, 1, cols, pending, log), [])
        self.assertEqual(ws.cell(2, cols["share"]).value, "1/1")
        ambiguous = []
        removed = app.remove_redundant_placeholder_rows(ws, 1, cols, ambiguous, log)
        self.assertEqual(removed, 1)
        self.assertEqual(ws.max_row, 2)


class TestSyntheticReconcileEndToEnd(unittest.TestCase):
    def test_old_halves_dont_corrupt_three_new_thirds(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            new_file = temp / "synthetic_new.xlsx"
            old_file = temp / "synthetic_old.xlsx"
            output_file = temp / "synthetic_matched.xlsx"
            new_wb, _ = new_workbook(["1/3"] * 3)
            new_wb.save(new_file)

            old_wb = Workbook()
            old_ws = old_wb.active
            old_ws.title = "Вымышленные данные"
            old_ws.append(["Номер помещения", "ФИО", "Сведения о праве", "Доля"])
            for index, fio in enumerate(["Примеров Тест Тестович",
                                         "Образцова Тест Тестовна"]):
                old_ws.append([
                    "901", fio, f"Собственность № {FAKE_REG[index]}", "1/2",
                ])
            old_wb.save(old_file)

            stats = app.reconcile_registries(new_file, old_file, output_file)
            self.assertEqual(stats["matched_by_reg"], 2)
            self.assertGreaterEqual(stats["ambiguous"], 1)
            matched = load_workbook(output_file)
            sheet = matched["Данные"]
            cols = app.get_new_header_columns(sheet, 1)
            self.assertEqual([sheet.cell(row, cols["share"]).value
                              for row in (2, 3, 4)], ["1/3"] * 3)
            self.assertEqual(total_of(sheet, cols), Fraction(1, 1))
            self.assertNotEqual(sheet.cell(2, cols["last"]).value, ".")

            log_wb = load_workbook(stats["log_output"])
            self.assertTrue(any(
                "Старые доли не применены" in str(row[4].value or "")
                for row in list(log_wb["Неоднозначные"].rows)[1:]
            ))
            self.assertTrue(any(
                row[3].value == "доли не перенесены"
                for row in list(log_wb["Сопоставление"].rows)[1:]
            ))


if __name__ == "__main__":
    unittest.main()

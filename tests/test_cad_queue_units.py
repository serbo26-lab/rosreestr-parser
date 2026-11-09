# -*- coding: utf-8 -*-
"""Regression checks using exclusively fictitious addresses and cadastre IDs (00:00:...)."""
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[1] / "src" / "rosreestr_parser_app.py"
spec = importlib.util.spec_from_file_location("rosreestr_parser_cad_queue_tests", SOURCE)
assert spec and spec.loader
app = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = app
spec.loader.exec_module(app)


class _Excel:
    def __init__(self):
        self.disputed = []
        self.errors = []

    def append_disputed(self, *values):
        self.disputed.append(values)

    def append_error(self, *values):
        self.errors.append(values)


class TestCadastralQueueNumbers(unittest.TestCase):
    @staticmethod
    def row(cad, street_address):
        return f"{cad}\n{street_address}\nПомещение"

    def make_worker(self):
        worker = app.ParserWorker.__new__(app.ParserWorker)
        worker.excel = _Excel()
        worker.metrics = {}
        worker.cfg = {
            "address_prefix": "Вымышленная область, город Тестоград, улица Макетная, дом 99",
            "cadastral_queue_validate_address": True,
        }
        worker.log = lambda message: None
        output = []
        worker.write_object_rows = lambda info: output.append((info.flat_no, info.address))
        return worker, output

    @staticmethod
    def card(cad, address=""):
        return app.ObjectInfo(
            flat_no=".", area="45", cadastral_number=cad,
            ownership_form="", address=address, object_type="Помещение",
            status="Актуально", purpose="", floor="", cadastral_cost="",
            rights=[],
        )

    def test_room_one_recovered_from_exact_kn_result(self):
        cad = "00:00:000000:1001"
        row = self.row(
            cad,
            "Вымышленная область, "
            "г. Тестоград, ул. Макетная, д. 99, кв. 1",
        )
        self.assertEqual(app.cad_queue_unit_from_result_row(row, cad)[0], "1")
        worker, output = self.make_worker()
        self.assertTrue(worker._write_cadastral_queue_info(
            self.card(cad), app.CadastralQueueItem("", cad), cad, row
        ))
        self.assertEqual(output[0][0], "1")
        self.assertEqual(worker.excel.disputed, [])

    def test_room_34_other_locality_is_not_an_error(self):
        cad = "00:00:000000:1002"
        row = self.row(
            cad,
            "Вымышленная область, г. Примероград, ул. Макетная, д. 99, квартира 34",
        )
        worker, output = self.make_worker()
        self.assertTrue(worker._write_cadastral_queue_info(
            self.card(cad), app.CadastralQueueItem("", cad), cad, row
        ))
        self.assertEqual(output[0][0], "34")
        self.assertFalse(worker.excel.disputed)
        self.assertEqual(worker.metrics.get("Спорных результатов", 0), 0)

    def test_wrong_kn_cannot_supply_unit(self):
        row = self.row(
            "00:00:000000:1001",
            "Вымышленная область, ул. Макетная, д. 99, кв. 1",
        )
        self.assertEqual(
            app.cad_queue_unit_from_result_row(row, "00:00:000000:1003")[0], ""
        )

    def test_missing_room_is_written_with_review_marker(self):
        cad = "00:00:000000:1001"
        worker, output = self.make_worker()
        row = self.row(cad, "Вымышленная область, ул. Макетная, д. 99")
        self.assertTrue(worker._write_cadastral_queue_info(
            self.card(cad), app.CadastralQueueItem("", cad), cad, row
        ))
        self.assertEqual(output[0][0], ".")
        self.assertEqual(len(worker.excel.disputed), 1)
        self.assertEqual(worker.excel.disputed[0][4], "Номер помещения не определён")

    def test_conflict_between_card_and_result_is_not_guessed(self):
        cad = "00:00:000000:1001"
        worker, output = self.make_worker()
        row = self.row(cad, "Вымышленная область, ул. Макетная, д. 99, кв. 1")
        self.assertTrue(worker._write_cadastral_queue_info(
            self.card(cad, "Вымышленная область, ул. Макетная, д. 99, кв. 2"),
            app.CadastralQueueItem("", cad), cad, row
        ))
        self.assertEqual(output[0][0], ".")
        self.assertEqual(len(worker.excel.disputed), 1)
        self.assertEqual(worker.excel.disputed[0][4], "Разные номера помещения")


if __name__ == "__main__":
    unittest.main()

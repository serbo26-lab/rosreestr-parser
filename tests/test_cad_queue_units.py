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
        self.updated_search_units = []

    def set_cadastral_queue_search_unit(self, search_cad, result_cad, unit_no):
        self.updated_search_units.append((search_cad, result_cad, unit_no))
        return True

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
        self.assertEqual(worker.excel.updated_search_units, [(cad, cad, "1")])

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

    def test_diagnostic_row_targets_only_accepted_exact_kn(self):
        from openpyxl import Workbook
        wb = Workbook()
        ws = wb.active
        ws.append(["Дата", "Поиск", "Помещение", "КН", "Текст", "Решение", "Причина"])
        ws.append(["-", "00:00:000000:1003", ".", "00:00:000000:1003", "", "Принят к открытию", ""])
        ws.append(["-", "00:00:000000:1001", ".", "00:00:000000:1001", "", "Отклонён", ""])
        ws.append(["-", "00:00:000000:1001", ".", "00:00:000000:1001", "", "Принят к открытию", ""])
        writer = app.ExcelOutput.__new__(app.ExcelOutput)
        writer.search_ws = ws
        self.assertTrue(writer.set_cadastral_queue_search_unit(
            "00:00:000000:1001", "00:00:000000:1001", "12/1"
        ))
        self.assertEqual(ws["C4"].value, "12/1")
        self.assertEqual(ws["C2"].value, ".")
        self.assertEqual(ws["C3"].value, ".")
        self.assertFalse(writer.set_cadastral_queue_search_unit(
            "00:00:000000:1002", "00:00:000000:1002", "34"
        ))



class TestStartupUpdateCheck(unittest.TestCase):
    """No external network, no real data, no Tk display required."""

    def manifest(self, version=14, channel="stable", checksum=None):
        return {
            "latest_version": "V1.4",
            "version_code": version,
            "channel": channel,
            "download_url": "https://example.invalid/releases/download/v1.4/rosreestr_parser.zip",
            "sha256": checksum if checksum is not None else "a" * 64,
        }

    def fake_app(self, channel="stable"):
        from types import SimpleNamespace
        state = {"messages": [], "schedules": [], "installations": []}
        fake = SimpleNamespace(
            cfg={
                "update_manifest_url": "https://example.invalid/latest.json",
                "update_channel": channel,
            },
            last_update_info={},
            update_status_var=SimpleNamespace(set=lambda value: state["messages"].append(value)),
            after=lambda ms, callback: state["schedules"].append((ms, callback)),
            download_and_install_update=lambda **kw: state["installations"].append(kw),
        )
        fake._poll_startup_update_result = lambda: app.App._poll_startup_update_result(fake)
        return fake, state

    def test_only_new_stable_downloadable_release_qualifies(self):
        self.assertIsNone(app.valid_new_stable_release(self.manifest(version=13), 13))
        self.assertIsNone(app.valid_new_stable_release(self.manifest(version=12), 13))
        self.assertIsNone(app.valid_new_stable_release(self.manifest(channel="dev"), 13))
        self.assertIsNone(app.valid_new_stable_release(self.manifest(checksum="bad"), 13))
        self.assertIsNone(app.valid_new_stable_release({"version_code": 14}, 13))
        self.assertEqual(
            app.valid_new_stable_release(self.manifest(), 13)["version_code"], 14
        )

    def test_silent_when_no_update_found(self):
        from unittest import mock
        from queue import Queue
        fake, state = self.fake_app()
        fake._startup_update_results = Queue()
        fake._startup_update_results.put(None)
        with mock.patch.object(app.messagebox, "askyesno") as prompt:
            app.App._poll_startup_update_result(fake)
        prompt.assert_not_called()
        self.assertEqual(state["messages"], [])
        self.assertEqual(state["installations"], [])

    def test_new_release_prompts_once_and_installs_without_second_confirmation(self):
        from unittest import mock
        from queue import Queue
        fake, state = self.fake_app()
        fake._startup_update_results = Queue()
        fake._startup_update_results.put(self.manifest())
        with mock.patch.object(app.messagebox, "askyesno", return_value=True) as prompt:
            app.App._poll_startup_update_result(fake)
        prompt.assert_called_once()
        self.assertEqual(state["installations"], [{"confirmed": True}])
        self.assertEqual(fake.last_update_info["version_code"], 14)

    def test_decline_keeps_running_without_download(self):
        from unittest import mock
        from queue import Queue
        fake, state = self.fake_app()
        fake._startup_update_results = Queue()
        fake._startup_update_results.put(self.manifest())
        with mock.patch.object(app.messagebox, "askyesno", return_value=False) as prompt:
            app.App._poll_startup_update_result(fake)
        prompt.assert_called_once()
        self.assertEqual(state["installations"], [])

    def test_dev_channel_skips_network_check(self):
        from unittest import mock
        fake, state = self.fake_app(channel="dev")
        with mock.patch.object(app.sys, "frozen", True, create=True):
            with mock.patch.object(app.urllib.request, "urlopen") as network:
                app.App._check_update_after_launch(fake)
        network.assert_not_called()
        self.assertEqual(state["schedules"], [])

    def test_startup_fetch_runs_silently_in_background(self):
        import io
        import json
        from unittest import mock
        fake, state = self.fake_app()
        payload = json.dumps(self.manifest(version=13)).encode("utf-8")
        with mock.patch.object(app.sys, "frozen", True, create=True):
            with mock.patch.object(
                app.urllib.request, "urlopen",
                side_effect=lambda request, timeout: io.BytesIO(payload),
            ) as network:
                app.App._check_update_after_launch(fake)
                candidate = fake._startup_update_results.get(timeout=5)
        self.assertIsNone(candidate)
        self.assertTrue(network.called)
        self.assertEqual(state["messages"], [])
        self.assertEqual(state["installations"], [])



if __name__ == "__main__":
    unittest.main()

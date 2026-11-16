# -*- coding: utf-8 -*-
"""Candidate selection tests: fictitious cadastral numbers and addresses only."""
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

SOURCE = Path(__file__).resolve().parents[1] / "src" / "rosreestr_parser_app.py"
spec = importlib.util.spec_from_file_location("rosreestr_oss_selection_tests", SOURCE)
assert spec and spec.loader
app = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = app
spec.loader.exec_module(app)

C1 = "00:00:000000:1001"
C2 = "00:00:000000:1002"
C3 = "00:00:000000:1003"


def card(cad, status="Актуально", registered=True):
    rights = [app.RightInfo(
        "Общая долевая собственность", "00-00/001/2020-1", "01.01.2020"
    )] if registered else []
    return app.ObjectInfo(
        flat_no="4", area="50.2", cadastral_number=cad, ownership_form="Частная",
        address="Вымышленная область, г. Тестоград, ул. Макетная, д. 42, кв. 4",
        object_type="Помещение", status=status, purpose="Жилое",
        floor="2", cadastral_cost="", rights=rights,
    )


def card_body(cad, status="Актуально"):
    return (
        "Общая информация\n"
        "Вид объекта недвижимости\n"
        "Помещение\n"
        "Статус объекта\n"
        f"{status}\n"
        "Кадастровый номер\n"
        f"{cad}\n"
        "Характеристики объекта\n"
        "Адрес (местоположение)\n"
        "Вымышленная область, г. Тестоград, ул. Макетная, д. 42, кв. 4\n"
        "Площадь, кв.м\n50.2\n"
    )


def simulated_worker(candidate_cards, cads=(C1, C2), full=False):
    state = SimpleNamespace(opened=[], written=[], disputed=[], errors=[], logs=[], backs=0)
    worker = app.ParserWorker.__new__(app.ParserWorker)
    worker.cfg = {"resume_enabled": False, "skip_done_cadastrals": False}
    worker.page = object()
    worker.metrics = {}
    worker.processed_cadastrals = set()
    worker.excel = SimpleNamespace(
        append_disputed=lambda *args: state.disputed.append(args),
        append_error=lambda *args: state.errors.append(args),
        append_building_land=lambda *args, **kw: None,
        append_full_object_info=lambda info, **kw: state.written.append(info.cadastral_number) or 1,
    )
    worker.log = lambda message: state.logs.append(message)
    worker.wait_control = lambda: True
    worker.open_service_page = lambda: None
    worker.fill_search_address = lambda address: None
    worker.click_find = lambda: None
    worker.wait_results = lambda: None
    worker.detect_captcha = lambda: False
    worker.collect_candidates = lambda *args: [{"cad": cad, "text": f"Результат {cad}"} for cad in cads]
    worker.is_cadastral_mode = lambda: False
    worker.is_full_info_mode = lambda: full
    worker.back_to_results = lambda: setattr(state, "backs", state.backs + 1)
    worker.write_object_rows = lambda info: state.written.append(info.cadastral_number)

    def open_card(candidate, flat, address):
        cad = candidate["cad"]
        state.opened.append(cad)
        return candidate_cards.get(cad)

    worker.open_candidate_and_parse = open_card
    return worker, state


class TestVerifiedCardIdentity(unittest.TestCase):
    def test_real_card_identity_is_required_not_cadastral_in_results(self):
        body = f"Результаты поиска\n{C2}\n" + card_body(C1)
        self.assertTrue(app.confirmed_card_section(body, C1))
        self.assertEqual(app.confirmed_card_section(body, C2), "")

    def test_search_result_alone_cannot_be_treated_as_open_card(self):
        body = f"Результаты поиска\n{C1}\nСтатус объекта\nАктуально\n"
        self.assertEqual(app.confirmed_card_section(body, C1), "")

    def test_old_card_is_not_accepted_when_another_card_is_visible(self):
        body = card_body(C1) + "\n" + card_body(C2)
        self.assertEqual(app.confirmed_card_section(body, C1), "")
        self.assertTrue(app.confirmed_card_section(body, C2))

    def test_card_status_and_identity_are_needed(self):
        self.assertEqual(
            app.confirmed_card_section(card_body(C1).replace("Статус объекта", "Описание"), C1),
            "",
        )
        self.assertEqual(app.confirmed_card_section(card_body(C1), C2), "")


class TestMultipleCadastralCandidates(unittest.TestCase):
    def test_current_trumps_redeemed_even_without_rights(self):
        worker, state = simulated_worker({
            C1: card(C1, status="Погашен", registered=True),
            C2: card(C2, status="Актуально", registered=False),
        })
        worker.process_flat("4", "г Тестоград ул Макетная д 42 кв 4")
        self.assertEqual(state.opened, [C1, C2])
        self.assertEqual(state.written, [C2])
        self.assertEqual(state.disputed, [])

    def test_only_one_current_has_registered_ownership(self):
        worker, state = simulated_worker({
            C1: card(C1, registered=False),
            C2: card(C2, registered=True),
        })
        worker.process_flat("4", "г Тестоград ул Макетная д 42 кв 4")
        self.assertEqual(state.opened, [C1, C2])
        self.assertEqual(state.written, [C2])
        self.assertTrue(any("зарегистрированное право" in x for x in state.logs))

    def test_unopenable_card_is_not_chosen(self):
        worker, state = simulated_worker({C1: None, C2: card(C2)})
        worker.process_flat("4", "г Тестоград ул Макетная д 42 кв 4")
        self.assertEqual(state.opened, [C1, C2])
        self.assertEqual(state.written, [C2])

    def test_both_current_and_both_registered_are_disputed(self):
        worker, state = simulated_worker({
            C1: card(C1, registered=True),
            C2: card(C2, registered=True),
        })
        worker.process_flat("4", "г Тестоград ул Макетная д 42 кв 4")
        self.assertEqual(state.opened, [C1, C2])
        self.assertEqual(state.written, [])
        self.assertEqual(len(state.disputed), 1)
        self.assertEqual(len(state.errors), 1)
        self.assertEqual(state.errors[0][2], "Спорно")

    def test_both_current_without_rights_are_disputed(self):
        worker, state = simulated_worker({
            C1: card(C1, registered=False),
            C2: card(C2, registered=False),
        })
        worker.process_flat("4", "г Тестоград ул Макетная д 42 кв 4")
        self.assertEqual(state.written, [])
        self.assertEqual(len(state.disputed), 1)

    def test_single_distinct_kn_with_duplicate_search_rows_is_processed_once(self):
        worker, state = simulated_worker({C1: card(C1)}, cads=(C1, C1))
        worker.process_flat("4", "г Тестоград ул Макетная д 42 кв 4")
        self.assertEqual(state.opened, [C1])
        self.assertEqual(state.written, [C1])

    def test_both_current_with_registered_rights_are_disputed_in_full_mode(self):
        worker, state = simulated_worker(
            {C1: card(C1), C2: card(C2)}, full=True
        )
        worker.process_flat("4", "г Тестоград ул Макетная д 42 кв 4")
        self.assertEqual(state.written, [])
        self.assertEqual(len(state.disputed), 1)

    def test_empty_registration_number_not_counted_as_ownership(self):
        info = card(C1, registered=True)
        info.rights[0].reg_number = ""
        self.assertFalse(app.has_registered_ownership(info))
        info.rights[0].reg_number = "."
        self.assertFalse(app.has_registered_ownership(info))


if __name__ == "__main__":
    unittest.main()

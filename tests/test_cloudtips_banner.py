# -*- coding: utf-8 -*-
"""CloudTips banner tests. All data is fictional except the public donation URL."""
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

SOURCE = Path(__file__).resolve().parents[1] / "src" / "rosreestr_parser_app.py"
spec = importlib.util.spec_from_file_location("rosreestr_parser_cloudtips_tests", SOURCE)
assert spec and spec.loader
app = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = app
spec.loader.exec_module(app)


class StubCanvas:
    instance = None

    def __init__(self, parent, **kwargs):
        self.opts = kwargs
        self.handlers = {}
        StubCanvas.instance = self

    def pack(self, **kwargs):
        pass

    def winfo_width(self):
        return 920

    def delete(self, *args, **kwargs):
        pass

    def create_rectangle(self, *args, **kwargs):
        pass

    def create_oval(self, *args, **kwargs):
        pass

    def create_line(self, *args, **kwargs):
        pass

    def create_text(self, *args, **kwargs):
        pass

    def bind(self, event, handler):
        self.handlers[event] = handler

    def focus_set(self):
        pass


class TestCloudTipsBanner(unittest.TestCase):
    def test_public_support_url_is_used(self):
        opened = []
        fake_app = SimpleNamespace(
            open_url_safe=lambda *args: opened.append(args)
        )
        app.App.open_support_page(fake_app)
        self.assertEqual(len(opened), 1)
        self.assertEqual(opened[0][0], "https://pay.cloudtips.ru/p/6b9c3555")

    def test_full_banner_and_keyboard_open_support(self):
        opened = []
        fake_app = SimpleNamespace(
            open_support_page=lambda: opened.append("opened")
        )
        with mock.patch.object(app.tk, "Canvas", StubCanvas):
            app.App._build_cloudtips_banner(fake_app, object())
        canvas = StubCanvas.instance
        self.assertIs(fake_app.cloudtips_banner, canvas)
        self.assertEqual(canvas.opts["cursor"], "hand2")
        self.assertEqual(canvas.opts["takefocus"], 1)
        for event in ("<Button-1>", "<Return>", "<space>"):
            self.assertIn(event, canvas.handlers)
            canvas.handlers[event](None)
        self.assertEqual(opened, ["opened", "opened", "opened"])

    def test_no_donation_card_details_are_shipped(self):
        source = SOURCE.read_text(encoding="utf-8")
        self.assertNotIn("copy_donate_card", source)
        self.assertNotIn("Скопировать номер карты", source)
        self.assertNotIn("Номер карты:", source)


if __name__ == "__main__":
    unittest.main()

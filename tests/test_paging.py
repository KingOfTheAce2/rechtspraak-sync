from types import SimpleNamespace

import importlib.util
import sys
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "rechtspraak_crawler", Path(__file__).resolve().parents[1] / "rechtspraak_crawler.py"
)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)  # type: ignore

fetch_ecli_page = module.fetch_ecli_page


class DummyResponse:
    def __init__(self, text: str):
        self.content = text.encode()

    def raise_for_status(self) -> None:
        pass


def test_fetch_ecli_page(monkeypatch):
    xml_feed = """
    <feed xmlns='http://www.w3.org/2005/Atom'>
        <entry><id>E1</id><updated>2020-01-01T00:00:00</updated></entry>
        <entry><id>E2</id><updated>2020-01-02T00:00:00</updated></entry>
    </feed>
    """

    called_params = {}

    def fake_get(url, params=None, timeout=None):
        called_params.update(params)
        return DummyResponse(xml_feed)

    session = SimpleNamespace(get=fake_get)
    batch = fetch_ecli_page(session, None, 0)
    assert len(batch) == 2
    assert batch[0]["ecli"] == "E1"
    assert called_params["from"] == 0


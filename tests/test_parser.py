from src.parser import parse_ruling_xml

def test_parse_uitspraak():
    xml = (
        '<rs:root xmlns:rs="http://www.rechtspraak.nl/schema/rechtspraak-1.0">'
        '<rs:uitspraak><p>Hello <b>world</b>.</p></rs:uitspraak>'
        '</rs:root>'
    )
    assert parse_ruling_xml(xml) == 'Hello world.'

def test_parse_conclusie():
    xml = (
        '<rs:root xmlns:rs="http://www.rechtspraak.nl/schema/rechtspraak-1.0">'
        '<rs:conclusie>Test</rs:conclusie>'
        '</rs:root>'
    )
    assert parse_ruling_xml(xml) == 'Test'

def test_invalid_xml():
    assert parse_ruling_xml('<invalid>') == ''

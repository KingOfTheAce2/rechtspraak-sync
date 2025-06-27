# src/parser.py

import xml.etree.ElementTree as ET

def parse_ruling_xml(xml_content: str) -> str:
    """
    Parses the ruling XML to extract the full text content.

    The content is located within <uitspraak> or <conclusie> tags,
    which are inside a default namespace.

    Args:
        xml_content: The raw XML string of a single court case.

    Returns:
        The extracted full text, or an empty string if not found.
    """
    if not xml_content:
        return ""

    try:
        root = ET.fromstring(xml_content)
        # Namespace is defined in the root element, we need to handle it
        namespaces = {'rs': 'http://www.rechtspraak.nl/schema/rechtspraak-1.0'}

        # Find the main content tags
        uitspraak = root.find('rs:uitspraak', namespaces)
        if uitspraak is not None:
            return _get_text_from_element(uitspraak)

        conclusie = root.find('rs:conclusie', namespaces)
        if conclusie is not None:
            return _get_text_from_element(conclusie)

        return ""
    except ET.ParseError:
        # Handle cases with invalid XML
        return ""

def _get_text_from_element(element: ET.Element) -> str:
    """Recursively extracts all text from an element and its children."""
    return "".join(element.itertext()).strip()

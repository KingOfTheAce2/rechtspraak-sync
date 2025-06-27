from src.name_scrubber import scrub_judge_names, scrub_gemachtigde_names

def test_scrub_judge_names():
    text = 'De rechter is dhr. mr. B.G.L. van der Aa.'
    assert scrub_judge_names(text) == 'De rechter is NAAM.'

def test_scrub_gemachtigde_names():
    text = 'Verschenen voor de gemachtigde mr. Jan Jansen.'
    cleaned = scrub_gemachtigde_names(text)
    assert 'NAAM' in cleaned

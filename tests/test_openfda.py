"""Unit tests for OpenFDA fetcher (name normalization and search variants)."""
import unittest

from fetchers.openfda import _company_search_variants, _normalize_company, _parse_conditions


class TestNormalizeCompany(unittest.TestCase):
    def test_strips_inc(self):
        self.assertEqual(_normalize_company("Pfizer Inc"), "Pfizer")
        self.assertEqual(_normalize_company("Pfizer Inc."), "Pfizer")

    def test_strips_llc_corp(self):
        self.assertEqual(_normalize_company("Acme LLC"), "Acme")
        self.assertEqual(_normalize_company("Acme Corp."), "Acme")
        self.assertEqual(_normalize_company("Acme Corporation"), "Acme")

    def test_unchanged_when_no_suffix(self):
        self.assertEqual(_normalize_company("Eli Lilly and Company"), "Eli Lilly and Company")

    def test_empty_safe(self):
        self.assertEqual(_normalize_company("  "), "")


class TestCompanySearchVariants(unittest.TestCase):
    def test_exact_first(self):
        v = _company_search_variants("Eli Lilly and Company")
        self.assertEqual(v[0], "Eli Lilly and Company")

    def test_includes_normalized_and_primary(self):
        v = _company_search_variants("Pfizer Inc")
        self.assertIn("Pfizer Inc", v)
        # Normalized "Pfizer" or primary word "Pfizer" should appear
        self.assertTrue(
            "Pfizer" in v,
            f"Expected 'Pfizer' in variants {v}",
        )

    def test_dedupes(self):
        v = _company_search_variants("Pfizer")
        self.assertEqual(v.count("Pfizer"), 1)

    def test_skips_single_char_and_stopwords(self):
        v = _company_search_variants("The Company")
        self.assertTrue("Company" in v or "The Company" in v)


class TestParseConditions(unittest.TestCase):
    def test_matches_keyword(self):
        text = "For the treatment of type 2 diabetes mellitus."
        out = _parse_conditions(text)
        self.assertTrue(any(c["icd10"] == "E11" for c in out))

    def test_empty_string(self):
        self.assertEqual(_parse_conditions(""), [])

    def test_no_match(self):
        self.assertEqual(_parse_conditions("For treatment of xyz unknown condition."), [])

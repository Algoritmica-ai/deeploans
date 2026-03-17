import sys
import unittest

sys.path.insert(0, "mcp-server")
from deeploans_mcp import server


class TestMCPServerTools(unittest.TestCase):
    def test_list_tables_sme(self):
        result = server.list_tables("sme")
        self.assertTrue(result["ok"])
        self.assertGreaterEqual(result["count"], 1)

    def test_list_tables_invalid_credit_type(self):
        result = server.list_tables("unknown")
        self.assertFalse(result["ok"])
        self.assertEqual(result["count"], 0)

    def test_describe_table_loans(self):
        result = server.describe_table("sme", "loans")
        self.assertTrue(result["ok"])
        self.assertGreater(result["column_count"], 0)
        self.assertGreater(result["filterable_count"], 0)

    def test_build_filter_examples(self):
        result = server.build_filter_examples("sme", "loans", limit=3)
        self.assertTrue(result["ok"])
        self.assertGreaterEqual(len(result["single_clause_examples"]), 1)

    def test_sample_rows_limit_validation(self):
        result = server.sample_rows("sme", "loans", limit="bad")
        self.assertFalse(result["ok"])
        self.assertIn("limit", result["error"])


if __name__ == "__main__":
    unittest.main()

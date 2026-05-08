import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import app


def sec_fact(value, *, unit="USD", year=2024):
    return {
        "val": value,
        "form": "10-K",
        "fy": year,
        "fp": "FY",
        "end": f"{year}-12-31",
        "filed": f"{year + 1}-02-15",
    }


class RangeScanCacheTests(unittest.TestCase):
    def test_us_companyfacts_record_contains_scan_fundamentals(self):
        facts = {
            "facts": {
                "us-gaap": {
                    "CashAndCashEquivalentsAtCarryingValue": {"units": {"USD": [sec_fact(1000)]}},
                    "MarketableSecuritiesCurrent": {"units": {"USD": [sec_fact(500)]}},
                    "DebtCurrent": {"units": {"USD": [sec_fact(200)]}},
                    "LongTermDebtNoncurrent": {"units": {"USD": [sec_fact(300)]}},
                    "CommonStockSharesOutstanding": {"units": {"shares": [sec_fact(100, unit="shares")]}},
                    "NetIncomeLoss": {
                        "units": {
                            "USD": [sec_fact(50, year=2022), sec_fact(75, year=2023), sec_fact(100, year=2024)]
                        }
                    },
                    "StockholdersEquity": {"units": {"USD": [sec_fact(2000)]}},
                    "Liabilities": {"units": {"USD": [sec_fact(1000)]}},
                    "Revenues": {
                        "units": {
                            "USD": [sec_fact(100, year=2022), sec_fact(150, year=2023), sec_fact(225, year=2024)]
                        }
                    },
                    "OperatingIncomeLoss": {
                        "units": {
                            "USD": [sec_fact(20, year=2022), sec_fact(30, year=2023), sec_fact(45, year=2024)]
                        }
                    },
                }
            }
        }

        fundamentals = app.extract_edgar_scan_fundamentals(facts)
        record = app.us_fundamentals_to_cache_record("ABC", "0000000001", "ABC Inc.", fundamentals)

        self.assertEqual(record["country"], "US")
        self.assertEqual(record["code"], "ABC")
        self.assertEqual(record["net_cash"], 1000)
        self.assertEqual(record["net_cash_per_share"], "10.00")
        self.assertEqual(record["liabilities_ratio_value"], 50.0)
        self.assertEqual(record["interest_bearing_debt_ratio_value"], 25.0)
        self.assertEqual(record["net_income"], 100)
        self.assertIsNotNone(record["sales_growth_5y_avg_pct"])

    def test_kr_cache_builder_writes_dart_detail_record(self):
        detail = {
            "corp_name": "Mock Corp",
            "bsns_year": "2024",
            "reprt_code": "11011",
            "liabilities": 700,
            "liabilities_ratio": "70.00",
            "liabilities_ratio_value": 70.0,
            "interest_bearing_debt": 200,
            "interest_bearing_debt_ratio": "20.00",
            "interest_bearing_debt_ratio_value": 20.0,
            "liquid_funds": 500,
            "net_cash": 300,
            "net_cash_display": "300",
            "float_shares": 10,
            "float_shares_display": "10",
            "net_cash_per_share": "30.00",
            "net_cash_per_share_value": 30.0,
            "net_income": 100,
            "equity": 1000,
            "sales_growth_5y_avg_pct": 12.0,
            "op_growth_5y_avg_pct": 10.0,
            "net_income_growth_5y_avg_pct": 8.0,
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "kr_cache.jsonl"
            with patch.object(app, "load_dart_corp_map", return_value=({}, {"000001": "12345678"}, {"12345678": "Mock Corp"})):
                with patch.object(app, "load_name_map", return_value={"mockcorp": "000001"}):
                    with patch.object(app, "fetch_dart_financials", return_value=detail):
                        with patch("builtins.print"):
                            written, total, last_error = app.build_kr_fundamentals_cache(output_path, force=True)

            self.assertEqual((written, total, last_error), (1, 1, None))
            payload = json.loads(output_path.read_text(encoding="utf-8").strip())
            self.assertEqual(payload["country"], "KR")
            self.assertEqual(payload["code"], "000001")
            self.assertEqual(payload["corp_code"], "12345678")
            self.assertEqual(payload["net_cash_per_share_value"], 30.0)

    def test_yahoo_missing_ratios_fall_back_to_cache_fundamentals(self):
        records = [
            {
                "country": "US",
                "code": "ABC",
                "shares": 100,
                "net_income": 50,
                "equity": 200,
                "net_cash_per_share_value": 5,
                "liabilities_ratio": "50.00",
            }
        ]

        def quote_fetcher(symbols):
            self.assertEqual(symbols, ["ABC"])
            return {"ABC": {"symbol": "ABC", "price": 10, "per": None, "pbr": None, "source": "yahoo"}}

        enriched = app.enrich_cache_records_with_yahoo(records, "US", quote_fetcher=quote_fetcher)

        self.assertEqual(app.parse_float(enriched[0]["per"]), 20.0)
        self.assertEqual(app.parse_float(enriched[0]["pbr"]), 5.0)
        self.assertEqual(enriched[0]["net_cash_per_share_ratio"], "50.00%")

    def test_cached_scan_batches_quotes_without_official_detail_calls(self):
        records = [
            {
                "country": "US",
                "code": f"AAA{i}",
                "name": f"Company {i}",
                "liabilities_ratio": "10.00",
                "interest_bearing_debt_ratio": "5.00",
                "net_cash_per_share_value": 1,
            }
            for i in range(201)
        ]
        calls = []

        def quote_fetcher(symbols):
            calls.append(list(symbols))
            return {
                symbol: {"symbol": symbol, "price": 10, "per": 8, "pbr": 1.2, "source": "yahoo"}
                for symbol in symbols
            }

        with patch.object(app, "fetch_edgar_financials", side_effect=AssertionError("official US call")):
            with patch.object(app, "fetch_dart_financials", side_effect=AssertionError("official KR call")):
                rows, total, last_error = app.scan_cached_fundamentals_records(
                    "US",
                    records,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    quote_fetcher=quote_fetcher,
                )

        self.assertEqual(total, 201)
        self.assertIsNone(last_error)
        self.assertEqual(len(rows), 201)
        self.assertEqual(len(calls), 3)
        self.assertEqual([len(call) for call in calls], [100, 100, 1])


if __name__ == "__main__":
    unittest.main()

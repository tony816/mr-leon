import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import app
import build_uk_cache_db
import collect_uk_ch_and_build_cache


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

    def test_kr_cache_builder_stops_on_dart_usage_limit(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "kr_cache.jsonl"
            stock_map = {"000001": "12345678", "000002": "87654321"}
            names = {"12345678": "One", "87654321": "Two"}
            with patch.object(app, "load_dart_corp_map", return_value=({}, stock_map, names)):
                with patch.object(app, "load_name_map", return_value={"one": "000001", "two": "000002"}):
                    with patch.object(app, "fetch_dart_financials", side_effect=app.DartError("020 사용한도를 초과하였습니다.")) as fetch_mock:
                        with patch("builtins.print"):
                            written, total, last_error = app.build_kr_fundamentals_cache(output_path, force=True)

            self.assertEqual(written, 0)
            self.assertEqual(total, 2)
            self.assertIn("020", last_error)
            self.assertEqual(fetch_mock.call_count, 1)

    def test_kr_cache_builder_stops_when_corp_map_hits_usage_limit(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "kr_cache.jsonl"
            with patch.object(app, "load_dart_corp_map", side_effect=app.DartError("020 사용한도를 초과하였습니다.")):
                with patch("builtins.print") as print_mock:
                    written, total, last_error = app.build_kr_fundamentals_cache(output_path, force=True)

        self.assertEqual((written, total), (0, 0))
        self.assertIn("020", last_error)
        self.assertTrue(print_mock.called)

    def test_jquants_v2_abbreviated_fields_populate_cache_metrics(self):
        class FakeJQuantsClient:
            def get_listed_info(self, code=None):
                return [{"Code": "7203", "CompanyNameEnglish": "TOYOTA MOTOR"}]

            def get_statements(self, code):
                return [
                    {
                        "Code": "7203",
                        "DiscDate": "2025-05-01",
                        "CurFYEn": "2025-03-31",
                        "Sales": "1000",
                        "OP": "120",
                        "NP": "80",
                        "Eq": "600",
                        "TA": "1000",
                        "CashEq": "700",
                        "ShOutFY": "10",
                        "EPS": "8",
                        "BPS": "60",
                    }
                ]

        snapshot, detail = app.fetch_jquants_financials_with_client("7203", FakeJQuantsClient(), include_price=False)
        record = app.detail_to_cache_record("JP", snapshot, detail)

        self.assertEqual(record["sales"], "1,000")
        self.assertEqual(record["op_income"], "120")
        self.assertEqual(record["equity"], "600")
        self.assertEqual(record["liabilities_ratio_value"], 400 / 600 * 100)
        self.assertEqual(record["interest_bearing_debt_source"], "total_liabilities_fallback")
        self.assertEqual(record["net_cash"], 300)
        self.assertEqual(record["net_cash_per_share_value"], 30)
        self.assertEqual(record["shares"], 10)
        self.assertEqual(record["bsns_year"], "2025")

    def test_jquants_missing_debt_defaults_to_zero_for_net_cash(self):
        class FakeJQuantsClient:
            def get_listed_info(self, code=None):
                return [{"Code": "1111", "CompanyNameEnglish": "CASH CO"}]

            def get_statements(self, code):
                return [
                    {
                        "Code": "1111",
                        "DiscDate": "2025-05-01",
                        "Sales": "1000",
                        "OP": "120",
                        "NP": "80",
                        "Eq": "600",
                        "CashEq": "700",
                        "ShOutFY": "10",
                    }
                ]

        snapshot, detail = app.fetch_jquants_financials_with_client("1111", FakeJQuantsClient(), include_price=False)
        record = app.detail_to_cache_record("JP", snapshot, detail)

        self.assertEqual(record["liquid_funds"], 700)
        self.assertEqual(record["interest_bearing_debt"], 0)
        self.assertEqual(record["net_cash"], 700)
        self.assertEqual(record["net_cash_per_share_value"], 70)

    def test_jquants_uses_latest_statement_with_cash_for_net_cash(self):
        class FakeJQuantsClient:
            def get_listed_info(self, code=None):
                return [{"Code": "1301", "CompanyNameEnglish": "KYOKUYO"}]

            def get_statements(self, code):
                return [
                    {
                        "Code": "1301",
                        "DiscDate": "2026-02-06",
                        "CurFYEn": "2026-03-31",
                        "Sales": "256910",
                        "OP": "9064",
                        "NP": "5682",
                        "Eq": "75639",
                        "TA": "222439",
                        "CashEq": "",
                        "ShOutFY": "10",
                    },
                    {
                        "Code": "1301",
                        "DiscDate": "2025-11-04",
                        "CurFYEn": "2026-03-31",
                        "Sales": "155996",
                        "OP": "4555",
                        "NP": "2814",
                        "Eq": "71992",
                        "TA": "200027",
                        "CashEq": "8071",
                        "ShOutFY": "10",
                    },
                ]

        snapshot, detail = app.fetch_jquants_financials_with_client("1301", FakeJQuantsClient(), include_price=False)
        record = app.detail_to_cache_record("JP", snapshot, detail)

        self.assertEqual(record["sales"], "256,910")
        self.assertEqual(record["equity"], "71,992")
        self.assertEqual(record["liquid_funds"], 8071)
        self.assertEqual(record["net_cash"], 8071 - (200027 - 71992))
        self.assertEqual(record["liquid_funds_source_date"], "2025-11-04")

    def test_normalize_jp_code_accepts_alphanumeric_tse_codes(self):
        self.assertEqual(app.normalize_jp_code("72030"), "7203")
        self.assertEqual(app.normalize_jp_code("7203.T"), "7203")
        self.assertEqual(app.normalize_jp_code("130A0"), "130A")
        self.assertEqual(app.normalize_jp_code("130A.T"), "130A")

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

    def test_yahoo_symbol_preserves_international_exchange_suffixes(self):
        self.assertEqual(app.yahoo_symbol_for_ticker("BP.L"), "BP.L")
        self.assertEqual(app.yahoo_symbol_for_ticker("7203.T"), "7203.T")
        self.assertEqual(app.yahoo_symbol_for_ticker("005930.KS"), "005930.KS")
        self.assertEqual(app.yahoo_symbol_for_ticker("005930.KQ"), "005930.KQ")
        self.assertEqual(app.yahoo_symbol_for_ticker("BRK.B"), "BRK-B")

    def test_cached_scan_uses_country_quote_symbols_and_cache_fallbacks(self):
        cases = [
            ("JP", "7203", ["7203.T"], "7203.T"),
            ("UK", "BP", ["BP.L"], "BP.L"),
        ]
        for country, code, expected_symbols, quote_symbol in cases:
            with self.subTest(country=country):
                records = [
                    {
                        "country": country,
                        "code": code,
                        "shares": 100,
                        "net_income": 50,
                        "equity": 200,
                        "net_cash_per_share_value": 5,
                    }
                ]
                calls = []

                def quote_fetcher(symbols):
                    calls.append(list(symbols))
                    return {
                        quote_symbol: {
                            "symbol": quote_symbol,
                            "price": 10,
                            "per": None,
                            "pbr": None,
                            "source": "yahoo",
                        }
                    }

                enriched = app.enrich_cache_records_with_yahoo(records, country, quote_fetcher=quote_fetcher)

                self.assertEqual(calls, [expected_symbols])
                self.assertEqual(app.parse_float(enriched[0]["per"]), 20.0)
                self.assertEqual(app.parse_float(enriched[0]["pbr"]), 5.0)
                self.assertEqual(enriched[0]["net_cash_per_share_ratio"], "50.00%")

    def test_kr_cached_scan_falls_back_to_kosdaq_when_kospi_quote_is_empty(self):
        records = [
            {
                "country": "KR",
                "code": "123456",
                "shares": 100,
                "net_income": 50,
                "equity": 200,
                "net_cash_per_share_value": 5,
            }
        ]
        calls = []

        def quote_fetcher(symbols):
            calls.append(list(symbols))
            if symbols == ["123456.KS"]:
                return {
                    "123456.KS": {
                        "symbol": "123456.KS",
                        "price": None,
                        "per": None,
                        "pbr": None,
                        "market_cap": None,
                        "source": "yahoo",
                    }
                }
            return {
                "123456.KQ": {
                    "symbol": "123456.KQ",
                    "price": 10,
                    "per": None,
                    "pbr": None,
                    "source": "yahoo",
                }
            }

        enriched = app.enrich_cache_records_with_yahoo(records, "KR", quote_fetcher=quote_fetcher)

        self.assertEqual(calls, [["123456.KS"], ["123456.KQ"]])
        self.assertEqual(app.parse_float(enriched[0]["per"]), 20.0)
        self.assertEqual(app.parse_float(enriched[0]["pbr"]), 5.0)
        self.assertEqual(enriched[0]["net_cash_per_share_ratio"], "50.00%")

    def test_net_cash_ratio_falls_back_to_market_cap_when_shares_missing(self):
        record = {
            "country": "UK",
            "code": "ABC",
            "net_cash": 25,
            "net_cash_per_share_value": None,
        }

        app.apply_quote_to_cache_record(
            record,
            {"symbol": "ABC.L", "price": 100, "market_cap": 200, "source": "yahoo"},
        )

        self.assertEqual(record["net_cash_per_share_ratio"], "12.50%")

    def test_uk_cache_builder_missing_debt_defaults_to_zero_for_net_cash(self):
        facts = {
            "CashAndCashEquivalents": [{"value": 500.0, "year": 2025, "end_ord": 1, "duration_days": 0}],
            "Equity": [{"value": 1000.0, "year": 2025, "end_ord": 1, "duration_days": 0}],
            "Liabilities": [{"value": 200.0, "year": 2025, "end_ord": 1, "duration_days": 0}],
            "NumberOfSharesOutstanding": [{"value": 10.0, "year": 2025, "end_ord": 1, "duration_days": 0}],
        }

        record = build_uk_cache_db.build_cache_record("ABC", "ABC PLC", facts, ["mock.xhtml"], 5)

        self.assertEqual(record["liquid_funds_total"], 500.0)
        self.assertEqual(record["interest_bearing_debt"], 0.0)
        self.assertEqual(record["net_cash"], 500.0)
        self.assertEqual(record["net_cash_per_share_value"], 50.0)

    def test_uk_cache_builder_uses_weighted_average_shares(self):
        facts = {
            "CashAndCashEquivalents": [{"value": 500.0, "year": 2025, "end_ord": 1, "duration_days": 0}],
            "Equity": [{"value": 1000.0, "year": 2025, "end_ord": 1, "duration_days": 0}],
            "WeightedAverageShares": [
                {"value": 25.0, "year": 2025, "end_ord": 1, "duration_days": 365, "unit": "shares"}
            ],
        }

        record = build_uk_cache_db.build_cache_record("ABC", "ABC PLC", facts, ["mock.xhtml"], 5)

        self.assertEqual(record["shares"], 25.0)
        self.assertEqual(record["shares_source"], "tag")
        self.assertEqual(record["net_cash_per_share_value"], 20.0)

    def test_uk_cache_builder_infers_shares_from_pence_eps(self):
        facts = {
            "CashAndCashEquivalents": [{"value": 500.0, "year": 2025, "end_ord": 1, "duration_days": 0}],
            "Equity": [{"value": 1000.0, "year": 2025, "end_ord": 1, "duration_days": 0}],
            "ProfitLoss": [{"value": 5_220_000.0, "year": 2025, "end_ord": 1, "duration_days": 365}],
            "BasicEarningsLossPerShareFromContinuingOperations": [
                {"value": 522.0, "year": 2025, "end_ord": 1, "duration_days": 365, "unit": "GBP,shares"}
            ],
        }

        record = build_uk_cache_db.build_cache_record("ABC", "ABC PLC", facts, ["mock.xhtml"], 5)

        self.assertEqual(record["shares_source"], "eps_inferred")
        self.assertAlmostEqual(record["shares"], 1_000_000.0)

    def test_uk_yahoo_timeseries_fallback_populates_missing_placeholder(self):
        placeholder = build_uk_cache_db.universe_placeholder_record(
            {
                "ticker": "ABC",
                "name": "ABC PLC",
                "isin": "GB00ABC",
                "market": "AIM",
                "instrument_type": "ORD 1P",
            }
        )
        chart_payload = {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "symbol": "ABC.L",
                            "longName": "ABC PLC",
                            "currency": "GBp",
                            "regularMarketPrice": 250.0,
                        }
                    }
                ],
                "error": None,
            }
        }

        def item(type_name, date, raw, currency="GBP"):
            return {
                "meta": {"type": [type_name]},
                type_name: [
                    {
                        "asOfDate": date,
                        "periodType": "12M",
                        "currencyCode": currency,
                        "reportedValue": {"raw": raw, "fmt": str(raw)},
                    }
                ],
            }

        timeseries_payload = {
            "timeseries": {
                "result": [
                    item("annualTotalRevenue", "2025-12-31", 1000),
                    item("annualOperatingIncome", "2025-12-31", 200),
                    item("annualNetIncome", "2025-12-31", 150),
                    item("annualStockholdersEquity", "2025-12-31", 500),
                    item("annualTotalLiabilitiesNetMinorityInterest", "2025-12-31", 300),
                    item("annualCashCashEquivalentsAndShortTermInvestments", "2025-12-31", 400),
                    item("annualTotalDebt", "2025-12-31", 100),
                    item("annualOrdinarySharesNumber", "2025-12-31", 100),
                    item("annualBasicEPS", "2025-12-31", 1.5),
                ],
                "error": None,
            }
        }

        class Response:
            status_code = 200

            def __init__(self, payload):
                self._payload = payload

            def json(self):
                return self._payload

        def fake_get(url, **_kwargs):
            if "/v8/finance/chart/" in url:
                return Response(chart_payload)
            return Response(timeseries_payload)

        with patch.object(build_uk_cache_db.requests, "get", side_effect=fake_get):
            record = build_uk_cache_db.build_yahoo_timeseries_cache_record(
                placeholder,
                timeout=5,
                window_years=5,
            )

        self.assertEqual(record["fundamentals_status"], "fallback_fundamentals_loaded")
        self.assertEqual(record["fundamentals_source"], "yahoo-timeseries")
        self.assertEqual(record["net_cash"], 300.0)
        self.assertEqual(record["net_cash_per_share_value"], 3.0)
        self.assertEqual(record["net_cash_per_share_ratio"], "120.00%")
        self.assertEqual(record["per"], "1.67")
        self.assertTrue(record["coverage"]["cash"])

    def test_nsm_name_matching_normalizes_dotted_plc(self):
        row = collect_uk_ch_and_build_cache.LseRow(
            ticker="BP",
            name="BP PLC",
            isin="GB0007980591",
            market="MAIN MARKET",
            instrument_type="Shares",
        )
        reports = [
            {
                "company": "BP p.l.c.",
                "lei": "213800LH1BZH3DI6G760",
                "download_link": "/download/bp.zip",
            }
        ]

        indexes = collect_uk_ch_and_build_cache.build_nsm_match_indexes(reports)
        self.assertIs(collect_uk_ch_and_build_cache.match_nsm_report_for_lse_row_indexed(row, indexes), reports[0])

    def test_uk_universe_placeholder_marks_missing_fundamentals(self):
        record = build_uk_cache_db.universe_placeholder_record(
            {
                "ticker": "ABC",
                "name": "ABC PLC",
                "isin": "GB00ABC",
                "lei": "ABCLEI",
                "market": "MAIN MARKET",
                "instrument_type": "Equity shares",
            }
        )

        self.assertEqual(record["code"], "ABC")
        self.assertEqual(record["fundamentals_status"], "missing_official_fundamentals")
        self.assertEqual(record["isin"], "GB00ABC")
        self.assertFalse(record["coverage"]["cash"])

    def test_cached_scan_excludes_missing_fundamentals_placeholders(self):
        self.assertFalse(
            app.cache_record_passes_filters(
                {
                    "fundamentals_status": "missing_official_fundamentals",
                    "per": 10,
                    "pbr": 1,
                },
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
            )
        )

    def test_lse_full_universe_matches_nsm_report_by_lei(self):
        reports = [
            {
                "company": "ABC PUBLIC LIMITED COMPANY",
                "lei": "ABCLEI",
                "download_link": "/download/abc.zip",
            }
        ]
        row = collect_uk_ch_and_build_cache.LseRow(
            ticker="ABC",
            name="ABC PLC",
            isin="GB00ABC",
            lei="ABCLEI",
            market="MAIN MARKET",
            instrument_type="Equity shares",
            source="lse",
            raw={},
        )

        indexes = collect_uk_ch_and_build_cache.build_nsm_match_indexes(reports)
        match = collect_uk_ch_and_build_cache.match_nsm_report_for_lse_row_indexed(row, indexes)

        self.assertIs(match, reports[0])

    def test_lse_full_universe_missing_rows_feed_ch_backfill(self):
        reports = [
            {
                "company": "MATCHED PUBLIC LIMITED COMPANY",
                "lei": "MATCHLEI",
                "download_link": "/download/matched.zip",
            }
        ]
        matched = collect_uk_ch_and_build_cache.LseRow(
            ticker="MAT",
            name="Matched PLC",
            isin="GB00MAT",
            lei="MATCHLEI",
            market="MAIN MARKET",
            instrument_type="Equity shares",
            source="lse",
            raw={},
        )
        missing = collect_uk_ch_and_build_cache.LseRow(
            ticker="MIS",
            name="Missing PLC",
            isin="GB00MIS",
            lei="MISSLEI",
            market="MAIN MARKET",
            instrument_type="Equity shares",
            source="lse",
            raw={},
        )

        rows = collect_uk_ch_and_build_cache.lse_rows_missing_nsm_esef([matched, missing], reports)

        self.assertEqual(rows, [missing])

    def test_lse_company_share_filter_excludes_etfs_and_non_uk_rows(self):
        company = collect_uk_ch_and_build_cache.LseRow(
            ticker="ABC",
            name="ABC PLC",
            isin="GB00ABC",
            market="AIM",
            instrument_type="ORD 1P",
            raw={
                "MiFIR Identifier Code": "SHRS",
                "MiFIR Indentifier Name": "Shares",
                "Country of Incorporation": "United Kingdom",
                "FCA Listing Category": "Equity shares (commercial companies)",
            },
        )
        etf = collect_uk_ch_and_build_cache.LseRow(
            ticker="ETF",
            name="ETF ISSUER PLC",
            isin="IE00ETF",
            market="MAIN MARKET",
            instrument_type="UCITS ETF",
            raw={
                "MiFIR Identifier Code": "ETFS",
                "MiFIR Indentifier Name": "Exchange Traded Funds",
                "Country of Incorporation": "Ireland",
            },
        )
        fund = collect_uk_ch_and_build_cache.LseRow(
            ticker="FND",
            name="FUND PLC",
            isin="GB00FND",
            market="MAIN MARKET",
            instrument_type="ORD 1P",
            raw={
                "MiFIR Identifier Code": "SHRS",
                "Country of Incorporation": "United Kingdom",
                "FCA Listing Category": "Closed-ended investment funds",
            },
        )
        income_fund = collect_uk_ch_and_build_cache.LseRow(
            ticker="IFD",
            name="INCOME FUND LIMITED",
            isin="GG00IFD",
            market="MAIN MARKET - SFS",
            instrument_type="ORD NPV",
            raw={
                "MiFIR Identifier Code": "SHRS",
                "Country of Incorporation": "Guernsey",
            },
        )
        preference = collect_uk_ch_and_build_cache.LseRow(
            ticker="PRF",
            name="PREFERENCE PLC",
            isin="GB00PRF",
            market="MAIN MARKET",
            instrument_type="3 1/2% GTD PRF STK",
            raw={
                "MiFIR Identifier Code": "SHRS",
                "Country of Incorporation": "United Kingdom",
                "FCA Listing Category": "Non-equity shares and non-voting equity shares",
            },
        )
        foreign_company = collect_uk_ch_and_build_cache.LseRow(
            ticker="FOR",
            name="FOREIGN PLC",
            isin="JE00FOR",
            market="MAIN MARKET",
            instrument_type="ORD NPV",
            raw={
                "MiFIR Identifier Code": "SHRS",
                "Country of Incorporation": "Jersey",
                "FCA Listing Category": "Equity shares (commercial companies)",
            },
        )

        self.assertTrue(collect_uk_ch_and_build_cache.is_lse_company_share_row(company))
        self.assertTrue(collect_uk_ch_and_build_cache.is_lse_uk_incorporated_row(company))
        self.assertFalse(collect_uk_ch_and_build_cache.is_lse_company_share_row(etf))
        self.assertFalse(collect_uk_ch_and_build_cache.is_lse_company_share_row(fund))
        self.assertFalse(collect_uk_ch_and_build_cache.is_lse_company_share_row(income_fund))
        self.assertFalse(collect_uk_ch_and_build_cache.is_lse_company_share_row(preference))
        self.assertTrue(collect_uk_ch_and_build_cache.is_lse_company_share_row(foreign_company))
        self.assertFalse(collect_uk_ch_and_build_cache.is_lse_uk_incorporated_row(foreign_company))

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
                    15,
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
        self.assertEqual(len(calls), 2)
        self.assertEqual([len(call) for call in calls], [200, 1])

    def test_uk_cached_scan_prefilters_before_quote_enrichment(self):
        records = [
            {
                "country": "UK",
                "code": "PASS",
                "name": "Pass PLC",
                "liabilities_ratio": "40.00",
                "interest_bearing_debt_ratio": "10.00",
                "net_cash_per_share_value": 2,
            },
            {
                "country": "UK",
                "code": "DEBT",
                "name": "Debt PLC",
                "liabilities_ratio": "140.00",
                "interest_bearing_debt_ratio": "10.00",
                "net_cash_per_share_value": 2,
            },
            {
                "country": "UK",
                "code": "CASH",
                "name": "Negative Cash PLC",
                "liabilities_ratio": "40.00",
                "interest_bearing_debt_ratio": "10.00",
                "net_cash_per_share_value": -2,
            },
        ]
        calls = []

        def quote_fetcher(symbols):
            calls.append(list(symbols))
            return {
                "PASS.L": {
                    "symbol": "PASS.L",
                    "price": 4,
                    "per": 8,
                    "pbr": 1.1,
                    "currency": "GBP",
                    "source": "test",
                }
            }

        rows, total, last_error = app.scan_cached_fundamentals_records(
            "UK",
            records,
            None,
            15,
            None,
            2,
            None,
            100,
            None,
            50,
            0.1,
            None,
            None,
            None,
            None,
            quote_fetcher=quote_fetcher,
        )

        self.assertEqual(total, 3)
        self.assertIsNone(last_error)
        self.assertEqual(len(rows), 1)
        self.assertEqual(calls, [["PASS.L"]])


if __name__ == "__main__":
    unittest.main()

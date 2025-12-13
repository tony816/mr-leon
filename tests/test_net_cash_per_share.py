import unittest

from app import compute_net_cash, format_per_share, parse_stock_totals


class NetCashPerShareTests(unittest.TestCase):
    def test_distb_stock_co_preferred_and_required(self):
        entries = [
            {"se": "우선주", "distb_stock_co": "111", "istc_totqy": "999"},
            {"se": "보통주", "distb_stock_co": "222", "istc_totqy": "888"},
        ]
        self.assertEqual(parse_stock_totals(entries), 222)

        fallback_entries = [
            {
                "se": "보통주",
                "distb_stock_co": None,
                "now_to_isu_stock_totqy": "8,208,283",
                "now_to_dcrs_stock_totqy": "0",
                "tesstk_co": "436,424",
            },
            {"se": "우선주", "distb_stock_co": "999"},
        ]
        self.assertEqual(parse_stock_totals(fallback_entries), 7_771_859)

    def test_net_cash_per_share_sample(self):
        liquid_funds = 53_705_579_000_000 + 58_909_334_000_000 + 0 + 36_877_000_000
        debt = 13_172_504_000_000 + 2_207_290_000_000 + 14_530_000_000 + 3_935_860_000_000
        net_cash = liquid_funds - debt

        self.assertEqual(net_cash, 93_321_606_000_000)
        self.assertEqual(format_per_share(net_cash, 5_940_082_550), "15,710.49")

    def test_net_cash_defaults_debt_to_zero(self):
        net_cash, debt_value = compute_net_cash(150, None)
        self.assertEqual(net_cash, 150)
        self.assertEqual(debt_value, 0)
        self.assertEqual(format_per_share(net_cash, 3), "50.00")


if __name__ == "__main__":
    unittest.main()

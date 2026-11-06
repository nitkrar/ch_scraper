"""iXBRL parser rules for balance-sheet fields, latest-year selection, headcount
scale and untagged turnover.

Fixtures are synthetic iXBRL documents run through the real ixbrlparse, so
dimension members reach the parser in the same ``segment:N`` shape a real
filing produces, under whichever namespace prefix the document declares.
"""

from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import patch

from ch_bulk.companies_house.financials_enricher import _parse_pdf_bytes
from ch_bulk.companies_house.financials_parsers import _parse_ixbrl_bytes
from tests.support.paths import fixture_path

CORE_NS = "http://xbrl.frc.org.uk/fr/2023-01-01/core"
PREFIXES = ("core", "uk-core", "frs-core", "ns5")

CY = "2025-12-31"
PY = "2024-12-31"
CY_DURATION = ("2025-01-01", "2025-12-31")
PY_DURATION = ("2024-01-01", "2024-12-31")

MATURITY = "MaturitiesOrExpirationPeriodsDimension"
INSTRUMENT = "FinancialInstrumentCurrentNon-currentDimension"


def within_one_year(prefix: str = "core") -> tuple[str, str]:
    return (f"{prefix}:{MATURITY}", f"{prefix}:WithinOneYear")


def after_one_year(prefix: str = "core") -> tuple[str, str]:
    return (f"{prefix}:{MATURITY}", f"{prefix}:AfterOneYear")


def current_instruments(prefix: str = "core") -> tuple[str, str]:
    return (f"{prefix}:{INSTRUMENT}", f"{prefix}:CurrentFinancialInstruments")


def non_current_instruments(prefix: str = "core") -> tuple[str, str]:
    return (f"{prefix}:{INSTRUMENT}", f"{prefix}:Non-currentFinancialInstruments")


def creditor_class(prefix: str = "core") -> tuple[str, str]:
    return (
        f"{prefix}:FinancialLiabilitiesClassesDimension",
        f"{prefix}:TradeCreditorsTradePayables",
    )


def fact(
    name: str,
    display: str,
    period: str | tuple[str, str],
    *members: tuple[str, str],
    scale: int | None = None,
    unit: str = "GBP",
    sign: str | None = None,
) -> dict:
    """One ix:nonFraction. ``name`` carries its prefix, e.g. ``ns5:Creditors``."""
    return {
        "name": name if ":" in name else f"core:{name}",
        "display": display,
        "period": period,
        "members": tuple(members),
        "scale": scale,
        "unit": unit,
        "sign": sign,
    }


def ixbrl_document(*facts: dict, text_rows: tuple[tuple[str, ...], ...] = ()) -> bytes:
    contexts: dict[tuple, str] = {}
    for item in facts:
        key = (item["period"], item["members"])
        contexts.setdefault(key, f"c{len(contexts)}")

    context_xml = []
    for (period, members), context_id in contexts.items():
        segment = ""
        if members:
            segment = "<xbrli:segment>" + "".join(
                f'<xbrldi:explicitMember dimension="{dimension}">{member}</xbrldi:explicitMember>'
                for dimension, member in members
            ) + "</xbrli:segment>"
        if isinstance(period, tuple):
            period_xml = (
                f"<xbrli:startDate>{period[0]}</xbrli:startDate>"
                f"<xbrli:endDate>{period[1]}</xbrli:endDate>"
            )
        else:
            period_xml = f"<xbrli:instant>{period}</xbrli:instant>"
        context_xml.append(
            f'<xbrli:context id="{context_id}"><xbrli:entity>'
            '<xbrli:identifier scheme="http://www.companieshouse.gov.uk/">01234567</xbrli:identifier>'
            f"{segment}</xbrli:entity><xbrli:period>{period_xml}</xbrli:period></xbrli:context>"
        )

    fact_xml = []
    for item in facts:
        context_id = contexts[(item["period"], item["members"])]
        scale = "" if item["scale"] is None else f' scale="{item["scale"]}"'
        if item["sign"] is not None:
            scale += f' sign="{item["sign"]}"'
        fact_xml.append(
            f'<p>Tagged figure: <ix:nonFraction name="{item["name"]}" contextRef="{context_id}" '
            f'unitRef="{item["unit"]}" decimals="0"{scale}>{item["display"]}</ix:nonFraction></p>'
        )

    row_xml = [
        "<tr>" + " ".join(f"<td>{cell}</td>" for cell in row) + "</tr>"
        for row in text_rows
    ]

    namespaces = " ".join(f'xmlns:{prefix}="{CORE_NS}"' for prefix in PREFIXES)
    document = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<html xmlns="http://www.w3.org/1999/xhtml" '
        'xmlns:ix="http://www.xbrl.org/2013/inlineXBRL" '
        'xmlns:xbrli="http://www.xbrl.org/2003/instance" '
        'xmlns:xbrldi="http://xbrl.org/2006/xbrldi" '
        'xmlns:iso4217="http://www.xbrl.org/2003/iso4217" '
        f"{namespaces}>\n<head><title>Fixture Ltd</title></head>\n<body>\n"
        '<div style="display:none"><ix:header><ix:resources>\n'
        + "\n".join(context_xml)
        + '\n<xbrli:unit id="GBP"><xbrli:measure>iso4217:GBP</xbrli:measure></xbrli:unit>'
        '\n<xbrli:unit id="pure"><xbrli:measure>xbrli:pure</xbrli:measure></xbrli:unit>'
        "\n</ix:resources></ix:header></div>\n"
        + "\n".join(fact_xml)
        + "\n<table>\n"
        + "\n".join(row_xml)
        + "\n</table>\n</body>\n</html>\n"
    )
    return document.encode("utf-8")


def parse(*facts: dict, text_rows: tuple[tuple[str, ...], ...] = ()):
    return _parse_ixbrl_bytes(ixbrl_document(*facts, text_rows=text_rows))


# Pins the balance-sheet date to CY in tests whose subject fact is absent or
# only present for the prior year.
CY_ANCHOR = fact("CurrentAssets", "1", CY)


class DebtorsTests(unittest.TestCase):
    def test_undimensioned_current_year_fact(self):
        facts = parse(fact("Debtors", "89,094", CY), fact("Debtors", "119,942", PY))
        self.assertEqual(facts.debtors, 89094.0)

    def test_within_one_year_or_current_instruments_member_under_any_prefix(self):
        for prefix in PREFIXES:
            for member in (within_one_year(prefix), current_instruments(prefix)):
                with self.subTest(prefix=prefix, member=member[1]):
                    facts = parse(fact(f"{prefix}:Debtors", "5,000", CY, member))
                    self.assertEqual(facts.debtors, 5000.0)

    def test_both_allowed_members_together_are_eligible(self):
        facts = parse(
            fact("ns5:Debtors", "5,000", CY, within_one_year("ns5"), current_instruments("ns5"))
        )
        self.assertEqual(facts.debtors, 5000.0)

    def test_prefers_undimensioned_over_within_one_year(self):
        facts = parse(
            fact("Debtors", "7,000", CY, within_one_year()),
            fact("Debtors", "9,000", CY),
        )
        self.assertEqual(facts.debtors, 9000.0)

    def test_after_one_year_member_is_ineligible(self):
        facts = parse(fact("Debtors", "5,000", CY, after_one_year()), CY_ANCHOR)
        self.assertIsNone(facts.debtors)

    def test_extra_dimension_member_is_ineligible(self):
        facts = parse(
            fact("Debtors", "5,000", CY, within_one_year(), creditor_class()),
            CY_ANCHOR,
        )
        self.assertIsNone(facts.debtors)

    def test_prior_year_only_is_none(self):
        facts = parse(fact("Debtors", "119,942", PY), CY_ANCHOR)
        self.assertIsNone(facts.debtors)

    def test_current_year_dimensioned_beats_prior_year_undimensioned(self):
        facts = parse(
            fact("Debtors", "7,000", CY, within_one_year()),
            fact("Debtors", "9,000", PY),
        )
        self.assertEqual(facts.debtors, 7000.0)


class CreditorsTests(unittest.TestCase):
    def test_splits_creditors_by_maturity_under_varying_prefixes(self):
        for prefix in PREFIXES:
            with self.subTest(prefix=prefix):
                facts = parse(
                    fact(f"{prefix}:Creditors", "108,212", CY, within_one_year(prefix)),
                    fact(f"{prefix}:Creditors", "55,583", CY, after_one_year(prefix)),
                    fact(f"{prefix}:Creditors", "125,292", PY, within_one_year(prefix)),
                    fact(f"{prefix}:Creditors", "72,595", PY, after_one_year(prefix)),
                )
                self.assertEqual(facts.creditors_within_one_year, 108212.0)
                self.assertEqual(facts.creditors_after_one_year, 55583.0)

    def test_financial_instrument_members(self):
        facts = parse(
            fact("uk-core:Creditors", "1,000", CY, current_instruments("uk-core")),
            fact("uk-core:Creditors", "2,000", CY, non_current_instruments("uk-core")),
        )
        self.assertEqual(facts.creditors_within_one_year, 1000.0)
        self.assertEqual(facts.creditors_after_one_year, 2000.0)

    def test_two_allowed_members_on_one_fact(self):
        facts = parse(
            fact("ns5:Creditors", "1,000", CY, within_one_year("ns5"), current_instruments("ns5")),
            fact("ns5:Creditors", "2,000", CY, after_one_year("ns5"), non_current_instruments("ns5")),
        )
        self.assertEqual(facts.creditors_within_one_year, 1000.0)
        self.assertEqual(facts.creditors_after_one_year, 2000.0)

    def test_mixed_maturity_members_are_eligible_for_neither(self):
        facts = parse(
            fact("Creditors", "1,000", CY, within_one_year(), non_current_instruments()),
            CY_ANCHOR,
        )
        self.assertIsNone(facts.creditors_within_one_year)
        self.assertIsNone(facts.creditors_after_one_year)

    def test_undimensioned_creditors_are_ignored(self):
        facts = parse(fact("Creditors", "163,795", CY), fact("Creditors", "150,000", PY))
        self.assertIsNone(facts.creditors_within_one_year)
        self.assertIsNone(facts.creditors_after_one_year)

    def test_undimensioned_creditors_do_not_displace_dimensioned(self):
        facts = parse(
            fact("Creditors", "163,795", CY),
            fact("Creditors", "108,212", CY, within_one_year()),
        )
        self.assertEqual(facts.creditors_within_one_year, 108212.0)
        self.assertIsNone(facts.creditors_after_one_year)

    def test_class_of_creditor_breakdown_is_ineligible(self):
        facts = parse(
            fact("Creditors", "40,000", CY, within_one_year(), creditor_class()),
            fact("Creditors", "100,000", CY, within_one_year()),
            fact("Creditors", "30,000", CY, after_one_year(), creditor_class()),
        )
        self.assertEqual(facts.creditors_within_one_year, 100000.0)
        self.assertIsNone(facts.creditors_after_one_year)

    def test_prior_year_only_is_none(self):
        facts = parse(
            fact("Creditors", "125,292", PY, within_one_year()),
            fact("Creditors", "72,595", PY, after_one_year()),
            CY_ANCHOR,
        )
        self.assertIsNone(facts.creditors_within_one_year)
        self.assertIsNone(facts.creditors_after_one_year)


class ProvisionsAndDeferredTaxTests(unittest.TestCase):
    PROVISION_NAMES = (
        "ProvisionsForLiabilitiesBalanceSheetSubtotal",
        "TaxationIncludingDeferredTaxationBalanceSheetSubtotal",
        "Provisions",
    )
    DEFERRED_TAX_NAMES = ("DeferredTaxLiabilities", "NetDeferredTaxLiabilityAsset")

    def test_each_provisions_fact_name_is_recognised(self):
        for name in self.PROVISION_NAMES:
            with self.subTest(name=name):
                facts = parse(fact(name, "12,000", CY))
                self.assertEqual(facts.provisions_for_liabilities, 12000.0)

    def test_provisions_fact_name_preference(self):
        facts = parse(
            fact("Provisions", "3,000", CY),
            fact("TaxationIncludingDeferredTaxationBalanceSheetSubtotal", "2,000", CY),
            fact("ProvisionsForLiabilitiesBalanceSheetSubtotal", "1,000", CY),
        )
        self.assertEqual(facts.provisions_for_liabilities, 1000.0)

        facts = parse(
            fact("Provisions", "3,000", CY),
            fact("TaxationIncludingDeferredTaxationBalanceSheetSubtotal", "2,000", CY),
        )
        self.assertEqual(facts.provisions_for_liabilities, 2000.0)

    def test_dimensioned_provisions_are_ineligible(self):
        facts = parse(
            fact("Provisions", "3,000", CY, within_one_year()),
            fact("ProvisionsForLiabilitiesBalanceSheetSubtotal", "1,000", CY, after_one_year()),
            CY_ANCHOR,
        )
        self.assertIsNone(facts.provisions_for_liabilities)

    def test_prior_year_provisions_are_never_returned(self):
        facts = parse(fact("ProvisionsForLiabilitiesBalanceSheetSubtotal", "1,000", PY), CY_ANCHOR)
        self.assertIsNone(facts.provisions_for_liabilities)

    def test_each_deferred_tax_fact_name_is_recognised(self):
        for name in self.DEFERRED_TAX_NAMES:
            with self.subTest(name=name):
                facts = parse(fact(name, "4,500", CY))
                self.assertEqual(facts.deferred_tax, 4500.0)

    def test_deferred_tax_fact_name_preference(self):
        facts = parse(
            fact("NetDeferredTaxLiabilityAsset", "9,999", CY),
            fact("DeferredTaxLiabilities", "4,500", CY),
        )
        self.assertEqual(facts.deferred_tax, 4500.0)

    def test_dimensioned_deferred_tax_is_ineligible(self):
        facts = parse(fact("DeferredTaxLiabilities", "4,500", CY, after_one_year()), CY_ANCHOR)
        self.assertIsNone(facts.deferred_tax)

    def test_prior_year_deferred_tax_is_never_returned(self):
        facts = parse(fact("DeferredTaxLiabilities", "4,500", PY), CY_ANCHOR)
        self.assertIsNone(facts.deferred_tax)

    def test_fields_absent_from_filing_are_none(self):
        facts = parse(fact("NetAssetsLiabilities", "10,000", CY))
        for field in (
            "debtors",
            "creditors_within_one_year",
            "creditors_after_one_year",
            "provisions_for_liabilities",
            "deferred_tax",
        ):
            with self.subTest(field=field):
                self.assertIsNone(getattr(facts, field))


class SignTests(unittest.TestCase):
    def test_liability_fields_are_stored_as_absolute_values(self):
        facts = parse(
            fact("Creditors", "17,938", CY, within_one_year(), sign="-"),
            fact("Creditors", "5,000", CY, after_one_year(), sign="-"),
            fact("ProvisionsForLiabilitiesBalanceSheetSubtotal", "1,200", CY, sign="-"),
        )
        self.assertEqual(facts.creditors_within_one_year, 17938.0)
        self.assertEqual(facts.creditors_after_one_year, 5000.0)
        self.assertEqual(facts.provisions_for_liabilities, 1200.0)

    def test_unsigned_liability_fields_unchanged(self):
        facts = parse(
            fact("Creditors", "17,938", CY, within_one_year()),
            fact("Provisions", "1,200", CY),
        )
        self.assertEqual(facts.creditors_within_one_year, 17938.0)
        self.assertEqual(facts.provisions_for_liabilities, 1200.0)

    def test_debtors_and_deferred_tax_keep_parsed_sign(self):
        facts = parse(
            fact("Debtors", "300", CY, sign="-"),
            fact("DeferredTaxLiabilities", "500", CY, sign="-"),
        )
        self.assertEqual(facts.debtors, -300.0)
        self.assertEqual(facts.deferred_tax, -500.0)


class ParseStatusTests(unittest.TestCase):
    def test_only_new_fields_found_is_partial(self):
        for found in (
            (fact("Debtors", "300", CY),),
            (fact("Creditors", "17,938", CY, within_one_year()),),
            (fact("Creditors", "17,938", CY, after_one_year()),),
            (fact("Provisions", "1,200", CY),),
            (fact("DeferredTaxLiabilities", "500", CY),),
        ):
            with self.subTest(fact=found[0]["name"]):
                facts = parse(*found)
                self.assertEqual(facts.parse_status, "partial")
                self.assertIsNone(facts.revenue)
                self.assertIsNone(facts.employee_count)

    def test_nothing_found_is_still_a_parse_error(self):
        facts = parse(fact("CashBankOnHand", "50", CY))
        self.assertEqual(facts.parse_status, "ixbrl_parse_error")


class LatestYearExistingInstantFieldsTests(unittest.TestCase):
    def test_prior_year_only_values_are_none(self):
        facts = parse(
            fact("Debtors", "10", CY),
            fact("NetAssetsLiabilities", "810", PY),
            fact("CurrentAssets", "500", PY),
            fact("FixedAssets", "700", PY),
            fact("NetCurrentAssetsLiabilities", "230", PY),
        )
        self.assertIsNone(facts.net_assets)
        self.assertIsNone(facts.current_assets)
        self.assertIsNone(facts.fixed_assets)
        self.assertIsNone(facts.net_current_assets)
        self.assertIsNone(facts.total_assets)

    def test_current_year_values_are_kept_alongside_comparatives(self):
        facts = parse(
            fact("NetAssetsLiabilities", "810", CY),
            fact("NetAssetsLiabilities", "999", PY),
            fact("CurrentAssets", "500", CY),
            fact("CurrentAssets", "444", PY),
        )
        self.assertEqual(facts.net_assets, 810.0)
        self.assertEqual(facts.current_assets, 500.0)

    def test_balance_sheet_date_is_latest_instant_among_all_facts(self):
        # Only the comparative CurrentAssets is tagged; the current-year date
        # comes from an unrelated fact.
        facts = parse(
            fact("CurrentAssets", "444", PY),
            fact("CashBankOnHand", "50", CY),
        )
        self.assertIsNone(facts.current_assets)

    def test_sample_filing_balance_sheet_fields(self):
        # Values read off the 07545840 fixture's 2026-02-28 balance sheet.
        sample = fixture_path("financials", "ixbrl", "07545840", "sample.ixbrl")
        facts = _parse_ixbrl_bytes(sample.read_bytes())
        self.assertEqual(facts.debtors, 89094.0)
        self.assertEqual(facts.creditors_within_one_year, 108212.0)
        self.assertEqual(facts.creditors_after_one_year, 55583.0)


class NetAssetsFallbackTests(unittest.TestCase):
    def test_total_assets_less_current_liabilities_is_not_net_assets(self):
        facts = parse(
            fact("TotalAssetsLessCurrentLiabilities", "1,500", CY),
            fact("CurrentAssets", "900", CY),
        )
        self.assertIsNone(facts.net_assets)
        self.assertEqual(facts.current_assets, 900.0)

    def test_net_assets_including_pension_is_recognised(self):
        facts = parse(
            fact("NetAssetsLiabilitiesIncludingPensionAssetLiability", "1,200", CY),
            fact("TotalAssetsLessCurrentLiabilities", "1,500", CY),
        )
        self.assertEqual(facts.net_assets, 1200.0)

    # The three period-bound tests below pin today's results for filings whose
    # only net-assets-like fact is TotalAssetsLessCurrentLiabilities.
    def test_period_end_falls_back_to_balance_sheet_date_without_duration_facts(self):
        facts = parse(
            fact("TotalAssetsLessCurrentLiabilities", "1,500", CY),
            fact("TotalAssetsLessCurrentLiabilities", "1,400", PY),
            fact("CurrentAssets", "900", CY),
        )
        self.assertIsNone(facts.filing_period_start)
        self.assertEqual(facts.filing_period_end, date(2025, 12, 31))

    def test_period_comes_from_duration_fact_ending_on_balance_sheet_date(self):
        facts = parse(
            fact("TotalAssetsLessCurrentLiabilities", "1,500", CY),
            fact("AverageNumberEmployeesDuringPeriod", "9", PY_DURATION, unit="pure"),
            fact("ProfitLoss", "300", CY_DURATION),
        )
        self.assertEqual(facts.filing_period_start, date(2025, 1, 1))
        self.assertEqual(facts.filing_period_end, date(2025, 12, 31))

    def test_period_prefers_current_year_segmented_headcount_over_prior_year(self):
        facts = parse(
            fact("TotalAssetsLessCurrentLiabilities", "1,500", CY),
            fact("AverageNumberEmployeesDuringPeriod", "9", PY_DURATION, unit="pure"),
            fact(
                "AverageNumberEmployeesDuringPeriod",
                "14",
                CY_DURATION,
                ("core:EntityOfficersDimension", "core:Director1"),
                unit="pure",
            ),
        )
        self.assertEqual(facts.filing_period_start, date(2025, 1, 1))
        self.assertEqual(facts.filing_period_end, date(2025, 12, 31))


class EmployeeCountScaleTests(unittest.TestCase):
    def test_negative_scale_whole_display_uses_displayed_number(self):
        facts = parse(
            fact("AverageNumberEmployeesDuringPeriod", "108", CY_DURATION, scale=-2, unit="pure")
        )
        self.assertEqual(facts.employee_count, 108)

    def test_ordinary_headcount_unchanged(self):
        for scale in (None, 0):
            with self.subTest(scale=scale):
                facts = parse(
                    fact("AverageNumberEmployeesDuringPeriod", "42", CY_DURATION, scale=scale, unit="pure")
                )
                self.assertEqual(facts.employee_count, 42)

    def test_headcount_tagged_negative_uses_its_magnitude(self):
        # 08649426 tags 25 employees with sign="-"; a headcount cannot be negative.
        facts = parse(
            fact("AverageNumberEmployeesDuringPeriod", "25", CY_DURATION, scale=0, unit="pure", sign="-")
        )
        self.assertEqual(facts.employee_count, 25)

    def test_displayed_number_is_still_bounded(self):
        facts = parse(
            fact("AverageNumberEmployeesDuringPeriod", "1000001", CY_DURATION, scale=-2, unit="pure")
        )
        self.assertIsNone(facts.employee_count)


class TextTurnoverTests(unittest.TestCase):
    def test_turnover_row_with_note_reference(self):
        facts = parse(
            fact("NetAssetsLiabilities", "2,500", CY),
            text_rows=(("Turnover", "3", "35,800,210", "31,000,000"),),
        )
        self.assertEqual(facts.revenue, 35800210.0)

    def test_revenue_row_without_note_reference(self):
        facts = parse(
            fact("NetAssetsLiabilities", "2,500", CY),
            text_rows=(("Revenue", "1,250,000", "1,100,000"),),
        )
        self.assertEqual(facts.revenue, 1250000.0)

    def test_label_is_case_insensitive_and_pound_sign_allowed(self):
        facts = parse(
            fact("NetAssetsLiabilities", "2,500", CY),
            text_rows=(("TURNOVER", "£35,800,210", "£31,000,000"),),
        )
        self.assertEqual(facts.revenue, 35800210.0)

    def test_thousands_scale_document_multiplies_text_amount(self):
        facts = parse(
            fact("NetAssetsLiabilities", "2,500", CY, scale=3),
            text_rows=(("Turnover", "4", "35,800", "31,000"),),
        )
        self.assertEqual(facts.net_assets, 2500000.0)
        self.assertEqual(facts.revenue, 35800000.0)

    def test_no_tagged_monetary_fact_uses_amount_as_written(self):
        facts = parse(
            fact("AverageNumberEmployeesDuringPeriod", "40", CY_DURATION, unit="pure"),
            text_rows=(("Turnover", "35,800", "31,000"),),
        )
        self.assertEqual(facts.revenue, 35800.0)

    def test_recovered_revenue_counts_like_tagged_revenue(self):
        facts = parse(
            fact("AverageNumberEmployeesDuringPeriod", "40", CY_DURATION, unit="pure"),
            fact("NetAssetsLiabilities", "2,500", CY),
            text_rows=(("Turnover", "3", "35,800,210", "31,000,000"),),
        )
        self.assertEqual(facts.revenue, 35800210.0)
        self.assertEqual(facts.parse_status, "ok")

    def test_rows_that_must_not_match(self):
        cases = {
            "single amount": ("Turnover", "35,800,210"),
            "narrative": ("Turnover of 16.0% to £35.8m",),
            "policy note": ("Turnover is recognised at the fair value of 1,000,000 2,000,000",),
            "percentages": ("Turnover", "16.0%", "12.5%"),
            "label not at start": ("Share of joint ventures' turnover", "35,800,210", "31,000,000"),
            "label not a whole word": ("Turnovers", "35,800,210", "31,000,000"),
            "no thousands separators": ("Turnover", "35800210", "31000000"),
        }
        for label, row in cases.items():
            with self.subTest(case=label):
                facts = parse(fact("NetAssetsLiabilities", "2,500", CY), text_rows=(row,))
                self.assertIsNone(facts.revenue)

    def test_largest_current_year_amount_wins_among_matching_rows(self):
        facts = parse(
            fact("NetAssetsLiabilities", "2,500", CY),
            text_rows=(
                ("Turnover of 16.0% to £35.8m",),
                ("Turnover", "34,000,000", "36,000,000"),
                ("Turnover", "3", "35,800,210", "31,000,000"),
                ("Turnover", "1,000,000", "900,000"),
            ),
        )
        self.assertEqual(facts.revenue, 35800210.0)

    def test_group_turnover_row_is_recognised(self):
        # 02463465: the P&L row is "GROUP TURNOVER"; a joint-venture note
        # later prints a much smaller "Turnover" row.
        facts = parse(
            fact("NetAssetsLiabilities", "2,500", CY),
            text_rows=(
                ("GROUP TURNOVER", "3", "20,519,248", "18,629,241"),
                ("Turnover", "541,669", "488,130"),
            ),
        )
        self.assertEqual(facts.revenue, 20519248.0)

    def test_thousands_summary_row_before_pl_row_loses(self):
        # An untagged £000 highlights row precedes the full-pounds P&L row in
        # a document whose tagged figures are unscaled.
        facts = parse(
            fact("NetAssetsLiabilities", "1,234,567", CY, scale=0),
            text_rows=(
                ("Turnover", "3,358", "3,305"),
                ("TURNOVER", "3", "3,358,088", "3,305,162"),
            ),
        )
        self.assertEqual(facts.revenue, 3358088.0)

    def test_largest_is_compared_after_unit_multiplier(self):
        facts = parse(
            fact("NetAssetsLiabilities", "2,500", CY, scale=3),
            text_rows=(
                ("Turnover", "1,500", "1,400"),
                ("Turnover", "4", "35,800", "31,000"),
            ),
        )
        self.assertEqual(facts.revenue, 35800000.0)

    def test_tagged_revenue_beats_text(self):
        facts = parse(
            fact("TurnoverRevenue", "999,999", CY_DURATION),
            fact("NetAssetsLiabilities", "2,500", CY),
            text_rows=(("Turnover", "3", "35,800,210", "31,000,000"),),
        )
        self.assertEqual(facts.revenue, 999999.0)


class PdfParserNewFieldsTests(unittest.TestCase):
    def test_pdf_parser_returns_none_for_new_fields(self):
        page_text = (
            "Turnover 1,000,000\nDebtors 50,000\nCreditors: amounts falling due within one year 40,000\n"
            "Creditors: amounts falling due after more than one year 30,000\n"
            "Provisions for liabilities 20,000\nDeferred tax 10,000\nNet assets 80,000\n"
            "Enough surrounding narrative to clear the sparse-scan threshold for a text PDF page."
        )

        class _Page:
            def extract_text(self):
                return page_text

        class _Pdf:
            pages = [_Page()]

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return None

        with patch("ch_bulk.companies_house.financials_enricher.pdfplumber.open", return_value=_Pdf()):
            facts = _parse_pdf_bytes(b"%PDF-1.4 fixture")

        self.assertEqual(facts.revenue, 1000000.0)
        for field in (
            "debtors",
            "creditors_within_one_year",
            "creditors_after_one_year",
            "provisions_for_liabilities",
            "deferred_tax",
        ):
            with self.subTest(field=field):
                self.assertIsNone(getattr(facts, field))


if __name__ == "__main__":
    unittest.main()

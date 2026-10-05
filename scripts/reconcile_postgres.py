import os
import sys

import psycopg2


EXPECTED_ROW_COUNT = 31_610_742
EXPECTED_MIN_DATE = "1995-01-01"
EXPECTED_MAX_DATE = "2026-08-28"

EXPECTED_YEAR_COUNTS = {
    1995: 797158,
    1996: 965427,
    1997: 1094673,
    1998: 1050706,
    1999: 1195090,
    2000: 1129596,
    2001: 1246054,
    2002: 1351982,
    2003: 1235660,
    2004: 1232193,
    2005: 1061836,
    2006: 1326330,
    2007: 1272507,
    2008: 649722,
    2009: 625377,
    2010: 663358,
    2011: 661265,
    2012: 668982,
    2013: 811189,
    2014: 985204,
    2015: 1011782,
    2016: 1046656,
    2017: 1067647,
    2018: 1037666,
    2019: 1012236,
    2020: 897978,
    2021: 1281983,
    2022: 1077205,
    2023: 861502,
    2024: 931453,
    2025: 974087,
    2026: 386238,
}


def get_connection():
    return psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="real_estate",
        user="postgres",
        password=os.environ["REAL_ESTATE_POSTGRES_PASSWORD"],
    )


def check_row_count(cursor):
    cursor.execute(
        "SELECT COUNT(*) FROM staging.price_paid;"
    )
    actual = cursor.fetchone()[0]

    passed = actual == EXPECTED_ROW_COUNT

    print(
        f"[{'PASS' if passed else 'FAIL'}] "
        f"Row count: expected={EXPECTED_ROW_COUNT:,}, actual={actual:,}"
    )

    return passed


def check_transaction_ids(cursor):
    cursor.execute(
        """
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT transaction_id) AS distinct_ids
        FROM staging.price_paid;
        """
    )

    total_rows, distinct_ids = cursor.fetchone()
    duplicate_ids = total_rows - distinct_ids

    passed = duplicate_ids == 0

    print(
        f"[{'PASS' if passed else 'FAIL'}] "
        f"Duplicate transaction IDs: {duplicate_ids:,}"
    )

    return passed


def check_date_range(cursor):
    cursor.execute(
        """
        SELECT
            MIN(transfer_date),
            MAX(transfer_date)
        FROM staging.price_paid;
        """
    )

    min_date, max_date = cursor.fetchone()

    actual_min = min_date.isoformat()
    actual_max = max_date.isoformat()

    passed = (
        actual_min == EXPECTED_MIN_DATE
        and actual_max == EXPECTED_MAX_DATE
    )

    print(
        f"[{'PASS' if passed else 'FAIL'}] "
        f"Date range: expected={EXPECTED_MIN_DATE} to {EXPECTED_MAX_DATE}, "
        f"actual={actual_min} to {actual_max}"
    )

    return passed


def check_data_validity(cursor):
    cursor.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE transaction_id IS NULL),
            COUNT(*) FILTER (WHERE transfer_date IS NULL),
            COUNT(*) FILTER (WHERE price IS NULL OR price <= 0),
            COUNT(*) FILTER (
                WHERE property_type NOT IN ('D', 'F', 'O', 'S', 'T')
            ),
            COUNT(*) FILTER (WHERE old_new NOT IN ('Y', 'N')),
            COUNT(*) FILTER (WHERE duration NOT IN ('F', 'L', 'U')),
            COUNT(*) FILTER (WHERE ppd_category NOT IN ('A', 'B')),
            COUNT(*) FILTER (WHERE record_status <> 'A')
        FROM staging.price_paid;
        """
    )

    results = cursor.fetchone()

    passed = all(value == 0 for value in results)

    print(
        f"[{'PASS' if passed else 'FAIL'}] "
        f"Critical data validation: {sum(results):,} violations"
    )

    return passed


def check_year_counts(cursor):
    cursor.execute(
        """
        SELECT
            transfer_year,
            COUNT(*)
        FROM staging.price_paid
        GROUP BY transfer_year
        ORDER BY transfer_year;
        """
    )

    actual = dict(cursor.fetchall())

    passed = actual == EXPECTED_YEAR_COUNTS

    print(
        f"[{'PASS' if passed else 'FAIL'}] "
        f"Yearly transaction distribution"
    )

    if not passed:
        expected_years = set(EXPECTED_YEAR_COUNTS)
        actual_years = set(actual)

        missing_years = sorted(expected_years - actual_years)
        unexpected_years = sorted(actual_years - expected_years)

        if missing_years:
            print(f"  Missing years: {missing_years}")

        if unexpected_years:
            print(f"  Unexpected years: {unexpected_years}")

        for year in sorted(expected_years & actual_years):
            if EXPECTED_YEAR_COUNTS[year] != actual[year]:
                print(
                    f"  {year}: "
                    f"expected={EXPECTED_YEAR_COUNTS[year]:,}, "
                    f"actual={actual[year]:,}"
                )

    return passed


def main():
    checks = []

    try:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                checks.append(check_row_count(cursor))
                checks.append(check_transaction_ids(cursor))
                checks.append(check_date_range(cursor))
                checks.append(check_data_validity(cursor))
                checks.append(check_year_counts(cursor))

    except Exception as exc:
        print(f"[FAIL] Reconciliation could not complete: {exc}")
        sys.exit(1)

    print()

    if all(checks):
        print("RECONCILIATION PASSED")
        sys.exit(0)

    print("RECONCILIATION FAILED")
    sys.exit(1)


if __name__ == "__main__":
    main()
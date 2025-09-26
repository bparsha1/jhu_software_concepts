"""
Module for date-related utility functions used in data scraping.

This module provides functions for handling date strings that may be missing
year information, particularly useful when processing scraped data that has
inconsistent date formats.
"""
from datetime import datetime


def _parse_single_date(date_str, formats_with_year, format_no_year):
    """Parse a single date string, detecting if it has a year or not.
    
    :param date_str: Date string to parse
    :param formats_with_year: List of formats that include year
    :param format_no_year: Format for dates without year
    :returns: Dict with 'date' and 'inferred' keys
    """
    # Try parsing with year formats
    for fmt in formats_with_year:
        try:
            date_obj = datetime.strptime(date_str, fmt)
            return {'date': date_obj, 'inferred': False}
        except (ValueError, TypeError):
            continue

    # Try parsing without year (using 1900 as placeholder)
    try:
        full_date_str = f"{date_str} 2000"
        date_with_dummy = datetime.strptime(full_date_str, f"{format_no_year} %Y")
        no_year_obj = date_with_dummy.replace(year=1900)
        return {'date': no_year_obj, 'inferred': True}
    except (ValueError, TypeError):
        return {'date': None, 'inferred': False}


def _forward_pass_years(parsed_dates):
    """Propagate years forward through the date list.
    
    :param parsed_dates: List of parsed date dictionaries
    :returns: None (modifies list in place)
    """
    last_known_year = None
    for i, item in enumerate(parsed_dates):
        if not item['date']:
            continue

        if not item['inferred']:
            last_known_year = item['date'].year
        elif last_known_year:
            temp_date = item['date'].replace(year=last_known_year)
            prev_date = parsed_dates[i-1]['date'] if i > 0 else None

            # Adjust year if date would be before previous
            if prev_date and temp_date < prev_date:
                last_known_year += 1

            item['date'] = item['date'].replace(year=last_known_year)
            item['inferred'] = False


def _backward_pass_years(parsed_dates):
    """Propagate years backward through the date list.
    
    :param parsed_dates: List of parsed date dictionaries
    :returns: None (modifies list in place)
    """
    next_known_year = None
    for i in range(len(parsed_dates) - 1, -1, -1):
        if not parsed_dates[i]['date']:
            continue

        if not parsed_dates[i]['inferred']:
            next_known_year = parsed_dates[i]['date'].year
        elif next_known_year:
            temp_date = parsed_dates[i]['date'].replace(year=next_known_year)
            next_date = parsed_dates[i+1]['date'] if i + 1 < len(parsed_dates) else None

            # Adjust year if date would be after next
            if next_date and temp_date > next_date:
                next_known_year -= 1

            parsed_dates[i]['date'] = parsed_dates[i]['date'].replace(year=next_known_year)
            parsed_dates[i]['inferred'] = False


def _format_results(parsed_dates):
    """Format parsed dates into standardized strings.
    
    :param parsed_dates: List of parsed date dictionaries
    :returns: List of formatted date strings
    """
    results = []
    current_year = datetime.now().year

    for item in parsed_dates:
        if not item['date']:
            results.append(None)
        elif item['inferred']:
            # Use current year for any remaining inferred dates
            item['date'] = item['date'].replace(year=current_year)
            results.append(item['date'].strftime('%Y-%m-%d'))
        else:
            results.append(item['date'].strftime('%Y-%m-%d'))

    return results


def infer_years(date_strings: list[str]) -> list[str]:
    """Infer missing years in date strings using chronological context from surrounding entries.

    This function handles cases where scraped date strings lack year information
    by analyzing the chronological sequence of entries. It uses a two-pass algorithm
    (forward and backward) to propagate known years to entries with missing years,
    ensuring chronological consistency.

    The function handles various date formats and uses surrounding entries to
    determine the most likely year for ambiguous dates, which is necessary because
    the scraped data is chronologically ordered but sometimes omits years.

    :param date_strings: List of date strings that may be missing year information.
    :type date_strings: list[str]
    :returns: List of standardized date strings in YYYY-MM-DD format, with inferred years.
    :rtype: list[str]
    """
    formats_with_year = ['%d %b %y', '%d %b %Y', '%B %d, %Y']
    format_no_year = '%d %b'

    # Parse all dates
    parsed_dates = [
        _parse_single_date(s, formats_with_year, format_no_year)
        for s in date_strings
    ]

    # Propagate years in both directions
    _forward_pass_years(parsed_dates)
    _backward_pass_years(parsed_dates)

    # Format and return results
    return _format_results(parsed_dates)


def format_decision_date(date_str, reference_year):
    """Format decision date string into standardized YYYY-MM-DD format.

    This function takes a decision date string (which may lack year information)
    and a reference year, then attempts to parse and format it into a standardized
    date format. It handles various input formats and uses the reference year
    when the date string doesn't include year information.

    :param date_str: Date string that may be in various formats, possibly missing year.
    :type date_str: str
    :param reference_year: Year to use when the date string doesn't include a year.
    :type reference_year: int
    :returns: Formatted date string in YYYY-MM-DD format, or None if parsing fails.
    :rtype: str
    """
    if not date_str or not reference_year:
        return None

    # First, try formats that might already include a year
    formats_to_try = ['%d %b %y', '%d %b %Y']
    for fmt in formats_to_try:
        try:
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue

    # If the above fail, combine the string and year BEFORE parsing
    try:
        full_date_str = f"{date_str} {reference_year}"
        return datetime.strptime(full_date_str, '%d %b %Y').strftime('%Y-%m-%d')
    except ValueError:
        return None

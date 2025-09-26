"""
Module for scraping and cleaning graduate school application data.

This module handles web scraping of graduate school admission results,
respecting robots.txt, parsing HTML data, and performing initial data
cleaning before further processing.
"""
import json
import re
from datetime import datetime, date
from urllib.robotparser import RobotFileParser
from urllib.parse import urljoin

import urllib3
import psycopg
from bs4 import BeautifulSoup

from .date_utils import infer_years, format_decision_date

# --- Constants ---
OUTPUT_FILE = 'new_structured_entries.json'
BASE_URL = 'https://www.thegradcafe.com/'
ROBOTS_URL = urljoin(BASE_URL, 'robots.txt')
TARGET_URL = urljoin(BASE_URL, 'survey/index.php')
USER_AGENT = 'Burch'
DB_CONN_STR = "dbname=grad_cafe user=postgres"

# --- Main Entry Point for the Pipeline ---
def main(conn):
    """Scrape latest data from website, starting from most recent database entry or Jan 2020.

    This function serves as the main entry point for the scraping pipeline. It checks
    robots.txt permissions, determines the starting point for scraping based on existing
    database data, performs the scraping operation, and saves results to a JSON file.

    The function will start scraping from the most recent date in the database, or from
    January 1, 2020 if the database is empty (which typically yields about 44k records).

    :param conn: Database connection for querying existing data and determining scrape start point.
    :type conn: psycopg.Connection
    :returns: Number of new entries found and saved, or 0 if no new data or robots.txt disallows.
    :rtype: int
    """
    print("--- Starting Scrape & Clean Step ---")

    # Check for robots.txt rules.
    rp = RobotFileParser()
    rp.set_url(ROBOTS_URL)
    rp.read()
    if not rp.can_fetch(USER_AGENT, TARGET_URL):
        print("Scraping disallowed by robots.txt. Aborting.")
        return 0

    latest_date, pids_on_latest_date = get_latest_day_info(conn)

    # Set a default latest date, currently gives about 44k records.
    if not latest_date:
        latest_date = date(2020, 1, 1)
        print(f"No existing data found. Starting initial scrape from {latest_date}.")

    new_data = scrape_and_clean(latest_db_date=latest_date, pids_on_latest_date=pids_on_latest_date)

    if new_data:
        print(f"Scraping complete. Found {len(new_data)} new entries.")
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(new_data, f, indent=4, default=str)
        print(f"Successfully saved structured data to {OUTPUT_FILE}")
        return len(new_data)

    print("No new entries were found.")
    return 0


def get_latest_day_info(conn):
    """Get the most recent entry date from database and all PIDs from that date.

    This function queries the database to find the latest date_added value and
    retrieves all PIDs that were added on that date. This information is used
    to determine where to resume scraping and which entries to skip to avoid
    duplicates.

    :param conn: Database connection for querying applicant data.
    :type conn: psycopg.Connection
    :returns: Tuple of (latest_date, set_of_pids) or (None, empty_set) if no data or error.
    :rtype: tuple[datetime.date, set[int]]
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(date_added) FROM applicants;")
            result = cur.fetchone()
            latest_date = result[0] if result and result[0] else None

            if not latest_date:
                return None, set()

            cur.execute("SELECT pid FROM applicants WHERE date_added = %s;", (latest_date,))
            pids_on_date = {row[0] for row in cur.fetchall()}
            print(f"Most recent entry date: {latest_date}. "
                  f"Found {len(pids_on_date)} existing entries for that day.")
            return latest_date, pids_on_date

    except psycopg.Error as e:
        print(f"Database error while fetching latest day info: {e}")
        return None, set()


def scrape_and_clean(latest_db_date=None, pids_on_latest_date=None, page_limit=100):
    """Scrape and parse graduate school application data from the target website.

    This function performs the core scraping logic, fetching pages sequentially
    and parsing HTML to extract structured application data. It stops scraping
    when it encounters entries older than the latest database date to avoid
    processing unnecessary historical data.

    The function handles duplicate detection by checking PIDs against those
    already present on the latest database date, and includes comprehensive
    error handling for network issues and malformed HTML.

    :param latest_db_date: Most recent date in database; scraping stops when older entries found.
    :type latest_db_date: datetime.date
    :param pids_on_latest_date: Set of PIDs already present on the latest database date.
    :type pids_on_latest_date: set[int]
    :param page_limit: Maximum number of pages to scrape before stopping.
    :type page_limit: int
    :returns: List of dictionaries containing structured entry data for new records.
    :rtype: list[dict]
    """
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    if pids_on_latest_date is None:
        pids_on_latest_date = set()

    http = urllib3.PoolManager()
    new_entries = []
    stop_scraping = False

    # Goes out to the page limit, the default is 100.
    for page_num in range(1, page_limit + 1):
        if stop_scraping:
            break

        url = f'{TARGET_URL}?page={page_num}'
        print(f"Scraping page {page_num}...")
        try:
            response = http.request('GET', url, headers={'User-Agent': USER_AGENT})
            if response.status != 200:
                print(f"Failed to fetch page {page_num}. Status: {response.status}")
                break
        except urllib3.exceptions.MaxRetryError as e:
            print(f"Network error while fetching page {page_num}: {e}")
            break

        # Parse out the page to get just the data in the table.
        soup = BeautifulSoup(response.data, 'html.parser')
        tbody = soup.find('tbody')
        if not tbody:
            print("Could not find the results table (tbody) on the page.")
            break

        all_rows = tbody.find_all('tr', recursive=False)
        main_rows = [row for row in all_rows if len(row.find_all('td', recursive=False)) > 1]

        if not main_rows:
            print("No valid entry rows found on this page. Reached the end.")
            break

        page_date_strings = [row.find_all('td')[2].get_text(strip=True) for row in main_rows]
        corrected_dates = infer_years(page_date_strings)

        # Look at the dates to make sure we are only adding new data.
        for i, row in enumerate(main_rows):
            date_added_str = corrected_dates[i]

            current_entry_date = datetime.strptime(date_added_str, '%Y-%m-%d').date()

            if latest_db_date and current_entry_date < latest_db_date:
                stop_scraping = True
                print(f"Found entry from {current_entry_date}, which is older than "
                      f"the cutoff date ({latest_db_date}). Stopping scrape.")
                break

            cells = row.find_all('td')

            url_tag = cells[4].find('a', href=re.compile(r'/result/\d+'))
            url = urljoin(BASE_URL, url_tag['href']) if url_tag else ''
            pid_match = re.search(r'/(\d+)', url)
            if not pid_match:
                continue

            pid = int(pid_match.group(1))

            if current_entry_date == latest_db_date and pid in pids_on_latest_date:
                continue

            # Process the entry
            entry = process_table_row(row, cells, pid, url, date_added_str)
            new_entries.append(entry)

    return new_entries

def process_table_row(row, cells, pid, url, date_added_str):
    """Process a single table row and extract entry data.

    :param row: BeautifulSoup row element
    :param cells: List of td cells from the row
    :param pid: Program ID
    :param url: URL for the entry
    :param date_added_str: Date string for when entry was added
    :returns: Dictionary containing the entry data
    """
    # Find detail and comment rows
    detail_row = row.find_next_sibling('tr')
    comment_row = None
    if detail_row and len(detail_row.find_all('td', recursive=False)) == 1:
        comment_row_candidate = detail_row.find_next_sibling('tr')
        if (comment_row_candidate and \
            len(comment_row_candidate.find_all('td', recursive=False)) == 1):
            comment_row = comment_row_candidate

    # Parse status and decision date
    status, decision_date_str = parse_status_and_date(cells[3])

    # Extract program information directly
    degree_spans = cells[1].find_all('span')

    # Build entry dictionary with inline extraction
    entry = {
        'pid': pid,
        'university': cells[0].get_text(strip=True),
        'program': cells[1].find('span').get_text(strip=True) if cells[1].find('span') else '',
        'degree': degree_spans[-1].get_text(strip=True) if len(degree_spans) > 1 else '',
        'status': status,
        'date_added': date_added_str,
        'decision_date': format_decision_date(
            decision_date_str,
            int(date_added_str.split('-')[0]) if date_added_str else None
        ),
        'url': url,
        'comments': (comment_row.find('p').get_text(strip=True) 
                    if comment_row and comment_row.find('p') else None)
    }

    # Update with parsed details
    entry.update(parse_details_from_badges(detail_row))
    return entry

def parse_status_and_date(tag):
    """Extract application status and decision date from HTML table cell.

    This function parses the status/decision cell from the scraped HTML table,
    extracting both the application status (Accepted, Rejected, Interview, etc.)
    and the decision date if present. It uses text analysis and regex patterns
    to identify these components from the formatted cell content.

    :param tag: BeautifulSoup tag containing the status and date information.
    :type tag: bs4.element.Tag
    :returns: Tuple of (status_string, decision_date_string) where date may be None.
    :rtype: tuple[str, str]
    """

    text = tag.get_text(strip=True)
    status, decision_date_str = 'Other', None
    if 'Accepted' in text:
        status = 'Accepted'
    elif 'Rejected' in text:
        status = 'Rejected'
    elif 'Interview' in text:
        status = 'Interview'
    elif 'Wait listed' in text:
        status = 'Wait listed'
    date_match = re.search(r'on\s+(.*)', text, re.IGNORECASE)
    if date_match:
        decision_date_str = date_match.group(1).strip()
    return status, decision_date_str

def parse_details_from_badges(row):
    """Extract applicant details (GPA, GRE scores, student type, etc.) from HTML badge elements.

    This function parses the detail row that contains applicant information displayed
    as badge elements (div tags with specific classes). It extracts numerical values
    like GPA and GRE scores, as well as categorical information like student type
    and semester/year information.

    :param row: BeautifulSoup element containing the detail row with badge information.
    :type row: bs4.element.Tag
    :returns: Dictionary containing parsed applicant details with None for missing values.
    :rtype: dict
    """
    details = {'gpa': None, 'gre': None, 'gre_v': None, 'gre_aw': None,
               'student_type': None, 'semester_and_year': None}
    if not row:
        return details
    badges = row.find_all('div', class_=re.compile(r'tw-inline-flex'))
    for badge in badges:
        text = badge.get_text(strip=True)
        if 'GPA' in text:
            match = re.search(r'[\d\.]+', text)
            if match:
                details['gpa'] = float(match.group(0))
        elif 'GRE V' in text:
            match = re.search(r'[\d\.]+', text)
            if match:
                details['gre_v'] = int(match.group(0))
        elif 'GRE AW' in text:
            match = re.search(r'[\d\.]+', text)
            if match:
                details['gre_aw'] = float(match.group(0))
        elif 'GRE' in text:
            match = re.search(r'[\d\.]+', text)
            if match:
                details['gre'] = int(match.group(0))
        elif text in ['International', 'American', 'Other URM']:
            details['student_type'] = text
        elif 'Fall' in text or 'Spring' in text:
            details['semester_and_year'] = text
    return details

# This function underlying is tested but __main__ can't be tested with pytest.
if __name__ == "__main__":  # pragma: no cover
    print("Running scrape_and_clean.py as a standalone script...")
    with psycopg.connect(DB_CONN_STR) as connection:
        main(connection)

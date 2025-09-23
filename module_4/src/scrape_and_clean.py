import urllib3
import json
import re
import psycopg
from datetime import datetime, date
from urllib.robotparser import RobotFileParser
from urllib.parse import urljoin
from bs4 import BeautifulSoup

# --- Constants ---
OUTPUT_FILE = 'new_structured_entries.json'
BASE_URL = 'https://www.thegradcafe.com/'
ROBOTS_URL = urljoin(BASE_URL, 'robots.txt')
TARGET_URL = urljoin(BASE_URL, 'survey/index.php')
USER_AGENT = 'Burch'
DB_CONN_STR = "dbname=grad_cafe user=postgres"

# --- Main Entry Point for the Pipeline ---
def main(conn):
    """ 
    Scrapes the latest data, or starts from Jan 2020.
    """
    print("--- Starting Scrape & Clean Step ---")

    # Check for robots.txt rules.
    rp = RobotFileParser()
    rp.set_url(ROBOTS_URL)
    rp.read()
    if not rp.can_fetch(USER_AGENT, TARGET_URL):
        print(f"Scraping disallowed by robots.txt. Aborting.")
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
    else:
        print("No new entries were found.")
        return 0

def get_latest_day_info(conn):
    """ 
    This was added to get the latest date from the database and all PIDs from that date.
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
            print(f"Most recent entry date: {latest_date}. Found {len(pids_on_date)} existing entries for that day.")
            return latest_date, pids_on_date
            
    except psycopg.Error as e:
        print(f"Database error while fetching latest day info: {e}")
        return None, set()

def scrape_and_clean(latest_db_date=None, pids_on_latest_date=None, page_limit=100):
    """ 
    This function does the scraping and formatting.
    """
    if pids_on_latest_date is None:
        pids_on_latest_date = set()

    http = urllib3.PoolManager()
    new_entries = []
    stop_scraping = False
    
    # Goes out to the page limit, the default is 100.
    for page_num in range(1, page_limit + 1):
        if stop_scraping: break
        
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
            if not date_added_str: continue

            current_entry_date = datetime.strptime(date_added_str, '%Y-%m-%d').date()
            
            if latest_db_date and current_entry_date < latest_db_date:
                stop_scraping = True
                print(f"Found entry from {current_entry_date}, which is older than the cutoff date ({latest_db_date}). Stopping scrape.")
                break
            
            cells = row.find_all('td')
            if len(cells) <= 4: continue

            url_tag = cells[4].find('a', href=re.compile(r'/result/\d+'))
            url = urljoin(BASE_URL, url_tag['href']) if url_tag else ''
            pid_match = re.search(r'/(\d+)$', url)
            if not pid_match: continue
            
            pid = int(pid_match.group(1))

            if current_entry_date == latest_db_date and pid in pids_on_latest_date:
                continue

            detail_row = row.find_next_sibling('tr')
            comment_row = None
            if detail_row and len(detail_row.find_all('td', recursive=False)) == 1:
                comment_row_candidate = detail_row.find_next_sibling('tr')
                if comment_row_candidate and len(comment_row_candidate.find_all('td', recursive=False)) == 1:
                    comment_row = comment_row_candidate

            status, decision_date_str = parse_status_and_date(cells[3])
            ref_year = int(date_added_str.split('-')[0]) if date_added_str else None
            
            program_cell = cells[1]
            degree_spans = program_cell.find_all('span')

            entry = {
                'pid': pid,
                'university': cells[0].get_text(strip=True),
                'program': program_cell.find('span').get_text(strip=True) if program_cell.find('span') else '',
                'degree': degree_spans[-1].get_text(strip=True) if len(degree_spans) > 1 else '',
                'status': status, 
                'date_added': date_added_str,
                'decision_date': format_decision_date(decision_date_str, ref_year),
                'url': url,
                'comments': comment_row.find('p').get_text(strip=True) if comment_row and comment_row.find('p') else None
            }
            entry.update(parse_details_from_badges(detail_row))
            new_entries.append(entry)
    
    return new_entries

def infer_years(date_strings: list[str]) -> list[str]:
    """ I noticed sometimes the date existed without the year.
        This function was added to check entries before and after
        to determine what to put for the year since the entries
        are chronological.
    """
    
    parsed_dates = []
    formats_with_year = ['%d %b %y', '%d %b %Y', '%B %d, %Y']
    format_no_year = '%d %b'
    
    for s in date_strings:
        date_obj = None
        for fmt in formats_with_year:
            try:
                date_obj = datetime.strptime(s, fmt)
                break
            except (ValueError, TypeError):
                continue
        
        if date_obj:
            parsed_dates.append({'date': date_obj, 'inferred': False})
        else:
            try:
                # FIX: Combine the string with a dummy leap year (2000) BEFORE parsing
                full_date_str = f"{s} 2000"
                date_with_dummy_year = datetime.strptime(full_date_str, f"{format_no_year} %Y")
                
                # Now replace the dummy year with the placeholder year for later logic
                no_year_obj = date_with_dummy_year.replace(year=1900)
                parsed_dates.append({'date': no_year_obj, 'inferred': True})
            except (ValueError, TypeError):
                parsed_dates.append({'date': None, 'inferred': False})

    # Forward Pass
    last_known_year = None
    for i in range(len(parsed_dates)):
        if not parsed_dates[i]['date']: continue
        if not parsed_dates[i]['inferred']:
            last_known_year = parsed_dates[i]['date'].year
        elif last_known_year:
            temp_date = parsed_dates[i]['date'].replace(year=last_known_year)
            if i > 0 and parsed_dates[i-1]['date'] and temp_date < parsed_dates[i-1]['date']:
                last_known_year += 1
            parsed_dates[i]['date'] = parsed_dates[i]['date'].replace(year=last_known_year)
            parsed_dates[i]['inferred'] = False
            
    # Backward Pass
    next_known_year = None
    for i in range(len(parsed_dates) - 1, -1, -1):
        if not parsed_dates[i]['date']: continue
        if not parsed_dates[i]['inferred']:
            next_known_year = parsed_dates[i]['date'].year
        elif next_known_year:
            temp_date = parsed_dates[i]['date'].replace(year=next_known_year)
            if i + 1 < len(parsed_dates) and parsed_dates[i+1]['date'] and temp_date > parsed_dates[i+1]['date']:
                next_known_year -= 1
            parsed_dates[i]['date'] = parsed_dates[i]['date'].replace(year=next_known_year)
            parsed_dates[i]['inferred'] = False
            
    # Format results
    results = []
    for item in parsed_dates:
        if item['date']:
            if item['inferred']:
                item['date'] = item['date'].replace(year=datetime.now().year)
            results.append(item['date'].strftime('%Y-%m-%d'))
        else:
            results.append(None)
    return results

def parse_status_and_date(tag):
    """ 
    This function breaks out the decision date and the status.
    """

    text = tag.get_text(strip=True)
    status, decision_date_str = 'Other', None
    if 'Accepted' in text: status = 'Accepted'
    elif 'Rejected' in text: status = 'Rejected'
    elif 'Interview' in text: status = 'Interview'
    elif 'Wait listed' in text: status = 'Wait listed'
    date_match = re.search(r'on\s+(.*)', text, re.IGNORECASE)
    if date_match: decision_date_str = date_match.group(1).strip()
    return status, decision_date_str

def format_decision_date(date_str, reference_year):
    """ This function formats the decision date."""
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

def parse_details_from_badges(row):
    """
    Break the details out of the badges.
    """
    details = {'gpa': None, 'gre': None, 'gre_v': None, 'gre_aw': None, 'student_type': None, 'semester_and_year': None}
    if not row: return details
    badges = row.find_all('div', class_=re.compile(r'tw-inline-flex'))
    for badge in badges:
        text = badge.get_text(strip=True)
        if 'GPA' in text:
            match = re.search(r'[\d\.]+', text)
            if match: details['gpa'] = float(match.group(0))
        elif 'GRE V' in text:
            match = re.search(r'[\d\.]+', text)
            if match: details['gre_v'] = int(match.group(0))
        elif 'GRE AW' in text:
            match = re.search(r'[\d\.]+', text)
            if match: details['gre_aw'] = float(match.group(0))
        elif 'GRE' in text: 
            match = re.search(r'[\d\.]+', text)
            if match: details['gre'] = int(match.group(0))
        elif text in ['International', 'American', 'Other URM']:
            details['student_type'] = text
        elif 'Fall' in text or 'Spring' in text:
            details['semester_and_year'] = text
    return details

# This function underlying is tested but __main__ can't be tested with pytest.
if __name__ == "__main__": # pragma: no cover
    print("Running scrape_and_clean.py as a standalone script...")
    with psycopg.connect(DB_CONN_STR) as connection:
        main(connection)
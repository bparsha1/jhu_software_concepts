import urllib3
import json
import re
from urllib.robotparser import RobotFileParser
from urllib.parse import urljoin

# Constants
BASE_URL = 'https://www.thegradcafe.com/'
TARGET_URL = urljoin(BASE_URL, 'survey/index.php')
USER_AGENT = 'Burch'
TARGET_ENTRIES = 100000 
OUTPUT_FILE = 'raw_html_data.json'

def check_permission(target_url, user_agent):
    """
    Checks the robots.txt file to see if scraping is allowed.
    """
    robots_url = urljoin(target_url, '/robots.txt')
    print(f'Checking permissions at: {robots_url}...')
    
    parser = RobotFileParser()
    parser.set_url(robots_url)
    
    parser.read()
    is_allowed = parser.can_fetch(user_agent, target_url)
    if is_allowed:
        print('Scraping is allowed by robots.txt.')
    else:
        print('Scraping is disallowed by robots.txt.')
    return is_allowed

def scrape_data():
    """ 
    Scrapes raw html data and puts it into a dictionary 
    """
    http = urllib3.PoolManager()
    all_html_data = {}
    entries_completed = 0
    page_num = 1

    print(f'Starting scrape. Target: {TARGET_ENTRIES} entries.')

    while entries_completed < TARGET_ENTRIES:
        # Construct the URL for the current page.
        url = f'{TARGET_URL}?page={page_num}'

        # Make the GET request.
        response = http.request('GET', url, headers={'User-Agent': USER_AGENT})
        
        if response.status == 200:
            html_content = response.data.decode('utf-8', 'ignore')

            # Add a dictionary entry for each page.
            all_html_data[f'page_{page_num}'] = html_content
            
            # Count entries on page based on the regex match so we know how many records we have.
            matches = re.findall(r'https://www.thegradcafe.com/result/', html_content)
            entries_on_page = len(matches)
            entries_completed += entries_on_page

            print(f'Scraped page {page_num}. Found {entries_on_page} entries. Total: {entries_completed}/{TARGET_ENTRIES}')
        else:
            print(f'Failed to scrape page {page_num}. Status code: {response.status}')
            break

        page_num += 1

            
    return all_html_data, entries_completed

def save_data(data, filename):
    """Saves the dictionary data to a JSON file."""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)
    print(f'\nRaw HTML data successfully saved to {filename}')

if __name__ == '__main__':
    if check_permission(TARGET_URL, USER_AGENT):
        raw_data, entry_count = scrape_data()
        
        if raw_data:
            save_data(raw_data, OUTPUT_FILE)
            print(f'Scraping complete. Collected a total of {entry_count} entries.')
        else:
            print('No data was scraped.')
    else:
        print('Exiting program.')

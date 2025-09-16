import json
import re
from bs4 import BeautifulSoup

# Input and output files
RAW_DATA_FILE = 'raw_html_data.json'
CLEAN_DATA_FILE = 'applicant_data.json'

def parse_status_and_date(tag):
    """
    Parses the decision status and date from a table cell tag.
    """
    text = tag.get_text(strip=True)
    status, decision_date = 'Other', None
    
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
        decision_date = date_match.group(1).strip()
        
    return status, decision_date

def parse_details_from_badges(row):
    """
    Parses GPA, GRE scores, student type, and season from the 'badge' divs
    in the second row of an entry.
    """
    details = {
        'gpa': None, 'gre_total': None, 'gre_v': None, 'gre_q': None, 
        'gre_aw': None, 'student_type': None, 'semester_and_year': None
    }
    
    # Using tw-inline-flex found in the div of the badges.
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
            if match: details['gre_total'] = int(match.group(0))
        elif text in ['International', 'American']:
            details['student_type'] = text
        elif 'Fall' in text or 'Spring' in text:
            details['semester_and_year'] = text
            
    return details

def clean_data(raw_html_dict):
    """
    Parses the raw HTML from the JSON file into a structured list of applicant data.
    """
    all_applicants = []
    for page_key, html_content in raw_html_dict.items():
        if not html_content:
            continue
        
        soup = BeautifulSoup(html_content, 'html.parser')

        # The tbody tag has the table entries without the header.
        tbody = soup.find('tbody')
        if not tbody:
            continue

        # Find all top-level rows in the tbody.
        # This is complex because comments and details are also in <tr> tags.
        rows = tbody.find_all('tr', recursive=False)
        
        i = 0
        while i < len(rows):
            # The first row for an applicant doesn't have a colspan attribute.
            first_tds = rows[i].find_all('td')
            if not first_tds or first_tds[0].get('colspan'):
                i += 1
                continue

            # Parse the first row. 
            school = first_tds[0].get_text(strip=True)
            
            program_cell = first_tds[1]
            program_name = program_cell.find('span').get_text(strip=True)
            degree = program_cell.find_all('span')[-1].get_text(strip=True)
            
            date_added = first_tds[2].get_text(strip=True)
            status, decision_date = parse_status_and_date(first_tds[3])
            
            # Finds the url ending for the individual record.
            url_tag = first_tds[4].find('a', href=re.compile(r'/result/\d+'))
            url = 'https://www.thegradcafe.com' + url_tag['href']

            applicant_entry = {
            	'university': school,
                'program': program_name, 
                'Degree': degree,
                'date_added': date_added,
                'status': status,
                'decision_date': decision_date,
                'url': url
            }
            
            # Parse the details row  with the badges.
            i += 1
            details_row = rows[i]
            details = parse_details_from_badges(details_row)
            applicant_entry.update(details)

            # Parse the optional comment row.
            i += 1
            comments = None
            # A comment row has a single td with colspan='100%' and a <p> tag
            if i < len(rows) and rows[i].find('td', {'colspan': '100%'}):
                comment_tag = rows[i].find('p')
                if comment_tag:
                    comments = comment_tag.get_text(strip=True)
                    i += 1 # Increment because we consumed this row
            
            applicant_entry['comments'] = comments
            all_applicants.append(applicant_entry)
                
    return all_applicants

def save_data(data, filename):
    """Saves the cleaned data to a JSON file."""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)
    print(f'Cleaned data successfully saved to {filename}')

def load_data(filename):
    """Loads data from a JSON file."""
    with open(filename, 'r', encoding='utf-8') as f:
        return json.load(f)

if __name__ == '__main__':
    print(f'Loading raw HTML data from {RAW_DATA_FILE}...')
    raw_data = load_data(RAW_DATA_FILE)
    
    if raw_data:
        print('Cleaning and structuring data...')
        cleaned_data = clean_data(raw_data)
        print(f'Processed {len(cleaned_data)} applicant entries.')
        save_data(cleaned_data, CLEAN_DATA_FILE)

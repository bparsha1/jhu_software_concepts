import pytest
import json
import psycopg
import urllib3
from datetime import date
from unittest.mock import MagicMock, patch
from src.scrape_and_clean import main as scrape_main, get_latest_day_info, scrape_and_clean


# This has three realistic html entries from a scrape in order to test.
FAKE_HTML = """
<html><body><table><tbody>
    
    <tr>
        <td class="tw-py-5 tw-pr-3"><div class="tw-font-medium">Test University</div></td>
        <td class="tw-px-3 tw-py-5"><div><span>Software Engineering</span><span class="tw-text-gray-500">MS</span></div></td>
        <td class="tw-px-3 tw-py-5">September 23, 2025</td>
        <td class="tw-px-3 tw-py-5"><div class="tw-bg-green-50">Accepted on 23 Sep</div></td>
        <td class="tw-relative tw-py-5"><a href="/result/103">Note</a></td>
    </tr>
    <tr class="tw-border-none">
        <td colspan="3" class="tw-pt-2 tw-pb-5">
            <div class="tw-gap-2 tw-flex">
                <div class="tw-inline-flex">American</div>
                <div class="tw-inline-flex">GPA 4.00</div>
                <div class="tw-inline-flex">GRE 330</div>
            </div>
        </td>
    </tr>
    <tr class="tw-border-none">
        <td colspan="100%" class="tw-pb-5"><p>This is a new comment.</p></td>
    </tr>

    <tr>
        <td class="tw-py-5 tw-pr-3"><div class="tw-font-medium">Another University</div></td>
        <td class="tw-px-3 tw-py-5"><div><span>Mechanical Engineering</span><span class="tw-text-gray-500">PhD</span></div></td>
        <td class="tw-px-3 tw-py-5">September 22, 2025</td>
        <td class="tw-px-3 tw-py-5"><div class="tw-bg-red-50">Rejected on 22 Sep</div></td>
        <td class="tw-relative tw-py-5"><a href="/result/101">Note</a></td>
    </tr>
    <tr class="tw-border-none"><td colspan="3"><div>Details...</div></td></tr>
    <tr class="tw-border-none"><td colspan="100%"><p>This is a duplicate comment.</p></td></tr>

    <tr>
        <td class="tw-py-5 tw-pr-3"><div class="tw-font-medium">Older University</div></td>
        <td class="tw-px-3 tw-py-5"><div><span>Electrical Engineering</span><span class="tw-text-gray-500">MS</span></div></td>
        <td class="tw-px-3 tw-py-5">September 21, 2025</td>
        <td class="tw-px-3 tw-py-5"><div class="tw-bg-blue-50">Interview</div></td>
        <td class="tw-relative tw-py-5"><a href="/result/100">Note</a></td>
    </tr>
    <tr class="tw-border-none"><td colspan="100%"><p>This comment should not be scraped.</p></td></tr>

</tbody></table></body></html>
"""


# When Scraping is Disallowed by robots.txt
@pytest.mark.web
def test_main_aborts_if_robots_disallows(mocker, capsys):
    """Test that main function exits early when robots.txt prevents scraping.
    
    This test verifies that the scraper respects robots.txt restrictions by
    checking if the main function properly exits when robots.txt disallows
    scraping. It mocks the RobotFileParser to return False for can_fetch
    and ensures the function returns 0 with an appropriate message.
    
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    :param capsys: Pytest fixture for capturing stdout and stderr.
    :type capsys: pytest.CaptureFixture
    """
    # Mock the RobotFileParser to disallow fetching.
    mock_parser = MagicMock()
    mock_parser.can_fetch.return_value = False
    mocker.patch('src.scrape_and_clean.RobotFileParser', return_value=mock_parser)

    # Run the main function
    result = scrape_main(None)

    # Check that it returned 0 and printed the correct message
    assert result == 0
    captured = capsys.readouterr()
    assert "Scraping disallowed by robots.txt" in captured.out


# When New Data is Found (on an empty DB)
@pytest.mark.db
def test_main_scrapes_and_saves_new_data(mocker, tmp_path, db_session):
    """Test that main function scrapes and saves new data when database is empty.
    
    This test verifies the complete workflow when the database is empty and
    new data is found during scraping. It mocks the database query to return
    empty results, mocks the scraper to return fake data, and verifies that
    the data is properly saved to the output file with the correct count returned.
    
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    :param tmp_path: Pytest temporary directory fixture.
    :type tmp_path: pathlib.Path
    :param db_session: Database session fixture providing a clean database connection.
    :type db_session: psycopg.Connection
    """

    # Mock the database call to simulate an empty database.
    mocker.patch('src.scrape_and_clean.get_latest_day_info', return_value=(None, set()))

    # Mock the scraper to return some fake data.
    fake_scraped_data = [{'pid': 1}, {'pid': 2}]
    mock_scraper = mocker.patch('src.scrape_and_clean.scrape_and_clean', return_value=fake_scraped_data)

    # Mock the output file path to use a temporary file.
    fake_output_file = tmp_path / "output.json"
    mocker.patch('src.scrape_and_clean.OUTPUT_FILE', str(fake_output_file))

    # Run the main function.
    result = scrape_main(db_session)

    # Check that the scraper was called with the default start date.
    mock_scraper.assert_called_once_with(
        latest_db_date=date(2020, 1, 1),
        pids_on_latest_date=set()
    )

    # Check that the function returned the correct count.
    assert result == 2

    # Check that the output file was actually written with the correct data.
    assert fake_output_file.exists()
    with open(fake_output_file, 'r') as f:
        saved_data = json.load(f)
        assert saved_data == fake_scraped_data


# When No New Data is Found
@pytest.mark.db
def test_main_handles_no_new_data(mocker, db_session, capsys):
    """Test that main function handles the case where scraper finds no new entries.
    
    This test verifies that when the scraper runs but finds no new data
    (returns empty list), the main function handles this gracefully by
    returning 0 and printing an appropriate message. It mocks a populated
    database state and empty scraper results.
    
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    :param db_session: Database session fixture providing a clean database connection.
    :type db_session: psycopg.Connection
    :param capsys: Pytest fixture for capturing stdout and stderr.
    :type capsys: pytest.CaptureFixture
    """

    # Simulate a database that has data up to a certain date
    latest_date = date(2025, 9, 23)
    mocker.patch('src.scrape_and_clean.get_latest_day_info', return_value=(latest_date, {101, 102}))

    # Mock the scraper to return an empty list (no new data)
    mock_scraper = mocker.patch('src.scrape_and_clean.scrape_and_clean', return_value=[])

    # Run the main function
    result = scrape_main(db_session)

    # Check that the scraper was called with the correct latest date
    mock_scraper.assert_called_once_with(
        latest_db_date=latest_date,
        pids_on_latest_date={101, 102}
    )
    # Check that the function returned 0
    assert result == 0
    # Check that the correct message was printed
    captured = capsys.readouterr()
    assert "No new entries were found" in captured.out


# Check an empty database.
@pytest.mark.db
def test_get_latest_day_info_empty_db(db_session):
    """Test that ``get_latest_day_info`` returns None for an empty database.
    
    This test verifies that when the database has no applicant records,
    the function returns None for the latest date and an empty set for PIDs,
    which represents the initial state before any data has been scraped.
    
    :param db_session: Database session fixture providing a clean database connection.
    :type db_session: psycopg.Connection
    """
    # The db_session fixture provides a clean, empty database.
    conn = db_session

    # Call the function.
    latest_date, pids = get_latest_day_info(conn)

    # Check for the expected empty result.
    assert latest_date is None
    assert pids == set()


# Check a populated database.
@pytest.mark.db
def test_get_latest_day_info_with_data(db_session):
    """Test that ``get_latest_day_info`` returns the newest date and PIDs from populated database.
    
    This test verifies that when the database contains multiple applicant records
    with different dates, the function correctly identifies the most recent date
    and returns all PIDs from that date. This is essential for determining
    where to resume scraping and avoiding duplicate entries.
    
    :param db_session: Database session fixture providing a clean database connection.
    :type db_session: psycopg.Connection
    """
    # Insert records with different dates into the test database.
    conn = db_session
    with conn.cursor() as cur:
        cur.execute("INSERT INTO applicants (pid, date_added) VALUES (%s, %s);", (100, date(2025, 9, 12)))
        cur.execute("INSERT INTO applicants (pid, date_added) VALUES (%s, %s);", (101, date(2025, 9, 20)))
        cur.execute("INSERT INTO applicants (pid, date_added) VALUES (%s, %s);", (102, date(2025, 9, 22)))
        cur.execute("INSERT INTO applicants (pid, date_added) VALUES (%s, %s);", (103, date(2025, 9, 22)))
    conn.commit()

    # Call the function.
    latest_date, pids = get_latest_day_info(conn)

    # Check that it found the correct latest date and the right PIDs.
    assert latest_date == date(2025, 9, 22)
    assert pids == {102, 103}


# Check for database error reaction.
@pytest.mark.db
def test_get_latest_day_info_db_error(mocker):
    """Test that ``get_latest_day_info`` returns default values on database error.
    
    This test verifies that when a database error occurs during the query,
    the function gracefully handles the exception and returns safe default
    values (None, empty set) instead of crashing, ensuring robust error handling.
    
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    """
    # Create a mock connection object that will raise an error when used.
    mock_conn = MagicMock()
    mock_conn.cursor.side_effect = psycopg.OperationalError("DB connection failed")
    
    # Call the function with the failing mock connection.
    latest_date, pids = get_latest_day_info(mock_conn)

    # Check that the 'except' block returned the default empty result.
    assert latest_date is None
    assert pids == set()


# Test for duplicate records parsed out by scrape_and_clean.
@pytest.mark.web
def test_scrape_and_clean_logic(mocker):
    """Test that ``scrape_and_clean`` skips duplicates and stops at older entries.
    
    This test verifies the core logic of the scraper: skipping duplicate records
    that already exist in the database (same PID on the same date) and stopping
    when it encounters entries older than the latest database date. This ensures
    efficient scraping that avoids duplicates and unnecessary processing.
    
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    """
    # Mock the web request to return our fake HTML instead of hitting the internet.
    mock_response = mocker.Mock()
    mock_response.status = 200
    mock_response.data = FAKE_HTML.encode('utf-8')
    mocker.patch('urllib3.PoolManager.request', return_value=mock_response)
    
    # Mock the helper function that determines the year for dates.
    mocker.patch(
        'src.scrape_and_clean.infer_years', 
        return_value=['2025-09-23', '2025-09-22', '2025-09-21']
    )
    
    # Define the "current state" of our database.
    latest_db_date = date(2025, 9, 22)
    pids_on_latest_date = {101} # We already have pid 101 for Sep 22nd.

    # Run the function with our defined database state.
    new_entries = scrape_and_clean(
        latest_db_date=latest_db_date, 
        pids_on_latest_date=pids_on_latest_date
    )

    # We should get exactly ONE new entry back.
    # The duplicate (pid 101) should be skipped.
    # The scrape should have stopped before processing the older entry (pid 100).
    assert len(new_entries) == 1
    
    # Verify the contents of the one new entry that was parsed.
    entry = new_entries[0]
    assert entry['pid'] == 103
    assert entry['university'] == 'Test University'
    assert entry['status'] == 'Accepted'
    assert entry['gpa'] == 4.00
    assert entry['comments'] == 'This is a new comment.'


# Check that scrape_and_clean deals with None pids.
@pytest.mark.web
def test_scrape_and_clean_handles_none_pids(mocker):
    """Test that ``scrape_and_clean`` handles None pids_on_latest_date parameter.
    
    This test verifies that when pids_on_latest_date is None (which can happen
    when there's no existing data for the latest date), the function handles
    this gracefully without crashing and processes entries appropriately.
    
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    """
    # Set up mocks to match the 3 entries in the full FAKE_HTML.
    mock_response = mocker.Mock()
    mock_response.status = 200
    mock_response.data = FAKE_HTML.encode('utf-8')
    mocker.patch('urllib3.PoolManager.request', return_value=mock_response)
    
    mocker.patch(
        'src.scrape_and_clean.infer_years',
        return_value=['2025-09-23', '2025-09-22', '2025-09-21']
    )

    # We set the date to match the second entry in the fake HTML.
    latest_db_date = date(2025, 9, 22)

    # Call the function with pids_on_latest_date=None.
    new_entries = scrape_and_clean(
        latest_db_date=latest_db_date,
        pids_on_latest_date=None # This is the specific condition we are testing
    )

    # The function should process the first (newer) entry and the second (same day)
    # entry, then stop when it sees the third (older) entry.
    # Therefore, we expect 2 new entries.
    assert len(new_entries) == 2
    
    # Verify the PIDs of the returned entries.
    returned_pids = {entry['pid'] for entry in new_entries}
    assert returned_pids == {103, 101}


# Check scrape_and_clean http error handling.
@pytest.mark.web
def test_scrape_and_clean_handles_http_error_status(mocker, capsys):
    """Test that ``scrape_and_clean`` handles HTTP error status codes gracefully.
    
    This test verifies that when the scraper encounters a non-200 HTTP status
    code (like 404 Not Found), it breaks the scraping loop, returns an empty
    list, and prints an appropriate error message rather than continuing
    with invalid responses.
    
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    :param capsys: Pytest fixture for capturing stdout and stderr.
    :type capsys: pytest.CaptureFixture
    """
    # Mock the web request to return a failed status code.
    mock_response = mocker.Mock()
    mock_response.status = 404 # Simulate "Not Found"
    mocker.patch('urllib3.PoolManager.request', return_value=mock_response)

    # Run the function, limiting it to one page for speed.
    result = scrape_and_clean(page_limit=1)

    # The function should return no entries and print an error.
    assert result == []
    captured = capsys.readouterr()
    assert "Failed to fetch page 1. Status: 404" in captured.out


## Check for network error handling.
@pytest.mark.web
def test_scrape_and_clean_handles_network_error(mocker, capsys):
    """Test that ``scrape_and_clean`` handles network errors gracefully.
    
    This test verifies that when a network error occurs during scraping
    (such as connection timeouts or DNS failures), the function catches
    the exception, breaks the scraping loop, and returns an empty list
    with an appropriate error message.
    
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    :param capsys: Pytest fixture for capturing stdout and stderr.
    :type capsys: pytest.CaptureFixture
    """
    # Mock the web request to RAISE a network exception.
    mocker.patch(
        'urllib3.PoolManager.request',
        side_effect=urllib3.exceptions.MaxRetryError(None, '/', "Simulated network error")
    )

    # Run the function.
    result = scrape_and_clean(page_limit=1)

    # The function should return no entries and print an error.
    assert result == []
    captured = capsys.readouterr()
    assert "Network error while fetching page 1" in captured.out


## Handles Missing HTML Content
@pytest.mark.web
def test_scrape_and_clean_handles_missing_tbody(mocker, capsys):
    """Test that ``scrape_and_clean`` handles missing HTML table structure.
    
    This test verifies that when the expected HTML table structure (tbody element)
    is not found on the scraped page, the function handles this gracefully by
    breaking the scraping loop and returning an empty list with an error message.
    
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    :param capsys: Pytest fixture for capturing stdout and stderr.
    :type capsys: pytest.CaptureFixture
    """
    # Mock a successful web request, but with invalid HTML content.
    mock_response = mocker.Mock()
    mock_response.status = 200
    mock_response.data = b"<html><body><p>This is not the page you are looking for.</p></body></html>"
    mocker.patch('urllib3.PoolManager.request', return_value=mock_response)

    # Run the function.
    result = scrape_and_clean(page_limit=1)
    
    # The function should return no entries and print an error.
    assert result == []
    captured = capsys.readouterr()
    assert "Could not find the results table (tbody) on the page" in captured.out


# Test no valid rows in raw HTML.
@pytest.mark.web
def test_scrape_and_clean_stops_on_no_valid_rows(mocker, capsys):
    """Test that ``scrape_and_clean`` stops when no valid data rows are found.
    
    This test verifies that when a page contains the expected table structure
    but no valid multi-column data rows (for example, only colspan rows with
    "no results" messages), the function detects this condition and stops
    scraping with an appropriate message.
    
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    :param capsys: Pytest fixture for capturing stdout and stderr.
    :type capsys: pytest.CaptureFixture
    """
    # Create fake HTML with a tbody that has no valid data rows.
    # This row has only one 'td', so your function will filter it out,
    # resulting in an empty 'main_rows' list.
    fake_html_no_rows = """
    <tbody>
      <tr><td colspan="5">No more results found on this page.</td></tr>
    </tbody>
    """
    mock_response = mocker.Mock()
    mock_response.status = 200
    mock_response.data = fake_html_no_rows.encode('utf-8')
    mocker.patch('urllib3.PoolManager.request', return_value=mock_response)

    # Call the function.
    result = scrape_and_clean(page_limit=1)

    # Check that the function returned an empty list
    # and printed the correct message before breaking the loop.
    assert result == []
    captured = capsys.readouterr()
    assert "No valid entry rows found on this page." in captured.out

@pytest.mark.web
def test_process_table_row_with_no_degree_span():
    """Test process_table_row when there's only one span (no degree).
    
    This test covers line 185 in scrape_and_clean.py where the degree
    is extracted only if there are multiple spans.
    """
    from bs4 import BeautifulSoup
    from src.scrape_and_clean import process_table_row
    
    # Create HTML with only one span (no degree span)
    html = """
    <tr>
        <td>MIT</td>
        <td><span>Computer Science</span></td>
        <td>01 Sep</td>
        <td>Accepted on 15 Mar</td>
        <td><a href="/result/12345">Link</a></td>
    </tr>
    """
    soup = BeautifulSoup(html, 'html.parser')
    row = soup.find('tr')
    cells = row.find_all('td')
    
    # Mock the detail row to return None for find_next_sibling
    row.find_next_sibling = lambda tag: None
    
    # Process the row
    result = process_table_row(
        row, cells, 12345,
        'https://www.thegradcafe.com/result/12345',
        '2025-09-01'
    )
    
    # Verify the degree field is empty when only one span exists
    assert result['degree'] == ''
    assert result['program'] == 'Computer Science'


@pytest.mark.web
def test_process_table_row_with_no_spans():
    """Test process_table_row when there are no spans in program cell.
    
    This test covers line 173 in scrape_and_clean.py where the program
    is extracted, but the cell has no span elements.
    """
    from bs4 import BeautifulSoup
    from src.scrape_and_clean import process_table_row
    
    # Create HTML with no spans in the program cell
    html = """
    <tr>
        <td>MIT</td>
        <td>No Program Info</td>
        <td>01 Sep</td>
        <td>Accepted on 15 Mar</td>
        <td><a href="/result/12345">Link</a></td>
    </tr>
    """
    soup = BeautifulSoup(html, 'html.parser')
    row = soup.find('tr')
    cells = row.find_all('td')
    
    # Mock the detail row
    row.find_next_sibling = lambda tag: None
    
    # Process the row
    result = process_table_row(
        row, cells, 12345,
        'https://www.thegradcafe.com/result/12345',
        '2025-09-01'
    )
    
    # Verify the program field is empty when no span exists
    assert result['program'] == ''
    assert result['degree'] == ''


@pytest.mark.web
def test_process_table_row_with_no_comment_paragraph():
    """Test process_table_row when comment row exists but has no <p> tag.
    
    This test covers line 191 in scrape_and_clean.py where comments
    are extracted only if a <p> tag exists in the comment row.
    """
    from bs4 import BeautifulSoup
    from src.scrape_and_clean import process_table_row
    
    # Create main row HTML
    html = """
    <tr>
        <td>MIT</td>
        <td><span>Computer Science</span><span>PhD</span></td>
        <td>01 Sep</td>
        <td>Accepted on 15 Mar</td>
        <td><a href="/result/12345">Link</a></td>
    </tr>
    """
    soup = BeautifulSoup(html, 'html.parser')
    row = soup.find('tr')
    cells = row.find_all('td')
    
    # Create a detail row (single td)
    detail_html = """<tr><td>Details here</td></tr>"""
    detail_soup = BeautifulSoup(detail_html, 'html.parser')
    detail_row = detail_soup.find('tr')
    
    # Create a comment row without a <p> tag
    comment_html = """<tr><td>Comment without paragraph tag</td></tr>"""
    comment_soup = BeautifulSoup(comment_html, 'html.parser')
    comment_row = comment_soup.find('tr')
    
    # Set up the mock relationships
    row.find_next_sibling = lambda tag: detail_row if tag == 'tr' else None
    detail_row.find_next_sibling = lambda tag: comment_row if tag == 'tr' else None
    
    # Process the row
    result = process_table_row(
        row, cells, 12345,
        'https://www.thegradcafe.com/result/12345',
        '2025-09-01'
    )
    
    # Verify comments is None when no <p> tag exists
    assert result['comments'] is None

@pytest.mark.web
def test_scrape_and_clean_no_pid_in_url():
    """Test when pid_match is None (URL doesn't contain a PID pattern).   
    It creates an entry where the URL doesn't contain the expected /digits pattern,
    causing the regex to return None and triggering the continue statement.
    """
    from src.scrape_and_clean import scrape_and_clean
    
    # Create HTML with a malformed URL that doesn't have /digits pattern
    fake_html = """
    <html><body><table><tbody>
        <tr>
            <td>University A</td>
            <td><span>Computer Science</span><span>MS</span></td>
            <td>15 Sep 2025</td>
            <td>Accepted</td>
            <td><a href="/result/no-pid-here">Bad URL</a></td>
        </tr>
        <tr>
            <td>University B</td>
            <td><span>Biology</span><span>PhD</span></td>
            <td>14 Sep 2025</td>
            <td>Rejected</td>
            <td><a href="/result/12345">Good URL</a></td>
        </tr>
    </tbody></table></body></html>
    """
    
    with patch('urllib3.PoolManager') as mock_pool:
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.data = fake_html.encode('utf-8')
        mock_pool.return_value.request.return_value = mock_response
        
        # Mock infer_years to return valid dates
        with patch('src.scrape_and_clean.infer_years') as mock_infer:
            mock_infer.return_value = ['2025-09-15', '2025-09-14']
            
            # Run scrape_and_clean
            result = scrape_and_clean(
                latest_db_date=date(2020, 1, 1),
                pids_on_latest_date=set(),
                page_limit=1
            )
            
            # Should only get 1 result (the second entry with valid PID)
            # The first entry should be skipped due to no PID match
            assert len(result) == 1
            assert result[0]['pid'] == 12345
            assert result[0]['university'] == 'University B'
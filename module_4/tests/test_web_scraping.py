import pytest
import json
import psycopg
import urllib3
from datetime import date
from unittest.mock import MagicMock
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
    """
    Tests that the main function exits early if robots.txt prevents scraping.
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
    """
    Tests the database is empty, the scraper finds new data,
    and the data is saved to a file.
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
    """
    Tests the case where the scraper runs but finds no new entries.
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
    """
    get_latest_day_info should return None for an empty database.
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
    """
    get_latest_day_info should return the newest date in the fake data.
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
    """
    get_latest_day_info should return a default value on error.
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
    """
    scrape_and_clean should skip duplicate records, and stop before entering an old one.
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
    """
    Tests the `if pids_on_latest_date is None:` line by calling the function
    with that argument set to None, ensuring it doesn't crash.
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
    """
    Tests that the scraper breaks the loop if it gets a non-200 status code.
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
    """
    Tests that the scraper breaks the loop if a network error occurs.
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
    """
    Tests that the scraper breaks the loop if the expected HTML table is not found.
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
    """
    Tests that the scraper correctly stops if a page contains a table
    but no valid multi-column data rows, covering the 'if not main_rows' case.
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

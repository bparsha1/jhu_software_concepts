import pytest
from bs4 import BeautifulSoup
from datetime import datetime
from src import query_data # Import the query objects
from src.scrape_and_clean import (
    infer_years,
    parse_status_and_date,
    format_decision_date,
    parse_details_from_badges
)


@pytest.mark.analysis
def test_analysis_labels_and_rounding(client, mocker, db_session):
    """Test the /analysis endpoint for correct data presentation.

    This test mocks the database query execution to return predefined results.
    It then sends a GET request to the '/analysis' route and checks the
    response to ensure that:
    1.  The page loads successfully.
    2.  Numerical data (like percentages and averages) is correctly rounded
        and formatted in the rendered HTML.
    3.  Labels and text are displayed as expected.

    :param client: The Flask test client fixture.
    :type client: flask.testing.FlaskClient
    :param mocker: The pytest-mock fixture for mocking objects.
    :type mocker: pytest_mock.MockerFixture
    :param db_session: The database session fixture (unused but required for context).
    :type db_session: psycopg.Connection
    """
    mock_results = {
        query_data.q2: (18.756,),
        query_data.q3: (3.51234, 320.987, 160.123, 4.567),
        query_data.q9: [("Test University", 10), ("Another Uni", 5)],
        query_data.q10: [("Accepted", 3.8), ("Rejected", 3.2)]
    }

    def mock_query_side_effect(conn, query, fetch="one"):
        return mock_results.get(query, (0,))

    mocker.patch(
        'src.app.query_data.execute_query',
        side_effect=mock_query_side_effect
    )

    # Verify the expected results.
    response = client.get('/analysis')
    assert response.status_code == 200
    
    soup = BeautifulSoup(response.data, 'html.parser')
    answer_elements = soup.find_all(class_='answer')
    assert any("Answer" in elem.get_text() for elem in answer_elements)

    assert b"18.76%" in response.data
    assert b"Average GPA: 3.51" in response.data
    assert b"Test University: 10 applications" in response.data


@pytest.mark.analysis
def test_parse_details_from_badges():
    """Test parsing of applicant details from HTML badge elements.

    This test verifies that the ``parse_details_from_badges`` function
    can correctly extract structured data (GPA, GRE scores, student type,
    and semester) from a given HTML snippet representing a table row.
    """
    html = """
    <tr><td>
        <div class="tw-inline-flex">A GPA: 4.0</div>
        <div class="tw-inline-flex">GRE: 320</div>
        <div class="tw-inline-flex">GRE V: 160</div>
        <div class="tw-inline-flex">GRE AW: 4.5</div>
        <div class="tw-inline-flex">International</div>
        <div class="tw-inline-flex">Fall 2025</div>
    </td></tr>
    """
    soup = BeautifulSoup(html, 'html.parser')
    row = soup.find('tr')
    details = parse_details_from_badges(row)
    assert details['gpa'] == 4.0
    assert details['gre'] == 320
    assert details['gre_v'] == 160
    assert details['gre_aw'] == 4.5
    assert details['student_type'] == 'International'
    assert details['semester_and_year'] == 'Fall 2025'


@pytest.mark.analysis
@pytest.mark.parametrize("date_str, ref_year, expected", [
    ("15 Apr 24", 2024, "2024-04-15"),
    ("01 May", 2024, "2024-05-01"),
    (None, 2024, None),
    ("Invalid Date", 2024, None),
])
def test_format_decision_date(date_str, ref_year, expected):
    """Test the formatting of decision date strings.

    This parametrized test checks the ``format_decision_date`` function with
    various inputs to ensure it correctly converts them to 'YYYY-MM-DD' format.
    It covers cases with full dates, dates missing a year (which should use
    the reference year), and invalid or None inputs.

    :param date_str: The raw date string to be formatted.
    :type date_str: str or None
    :param ref_year: The reference year to use if the date string lacks one.
    :type ref_year: int
    :param expected: The expected output string in 'YYYY-MM-DD' format, or None.
    :type expected: str or None
    """
    assert format_decision_date(date_str, ref_year) == expected


@pytest.mark.analysis
@pytest.mark.parametrize("html_input, expected_status, expected_date", [
    ("<td>Accepted on 15 Apr 24</td>", "Accepted", "15 Apr 24"),
    ("<td>Rejected via E-mail on 01 May</td>", "Rejected", "01 May"),
    ("<td>Interview via Phone on 20 Feb 2023</td>", "Interview", "20 Feb 2023"),
    ("<td>Wait listed</td>", "Wait listed", None),
    ("<td>Other</td>", "Other", None)
])
def test_parse_status_and_date(html_input, expected_status, expected_date):
    """Test parsing of application status and date from HTML.

    This parametrized test verifies that the ``parse_status_and_date``
    function can correctly extract the decision status and the raw date
    string from various HTML ``<td>`` tag contents.

    :param html_input: The HTML snippet containing the status and date.
    :type html_input: str
    :param expected_status: The expected application status string.
    :type expected_status: str
    :param expected_date: The expected raw date string, or None if not present.
    :type expected_date: str or None
    """
    soup = BeautifulSoup(html_input, 'html.parser')
    tag = soup.find('td')
    status, date_str = parse_status_and_date(tag)
    assert status == expected_status
    assert date_str == expected_date


@pytest.mark.analysis
def test_infer_years_cross_year():
    """Test year inference for dates spanning a new year.

    This test checks that ``infer_years`` correctly assigns the year to
    dates without one when the sequence of dates crosses from one year
    to the next (e.g., from December to January).
    """
    dates = ["30 Dec 23", "02 Jan", "05 Jan 24"]
    expected = ["2023-12-30", "2024-01-02", "2024-01-05"]
    assert infer_years(dates) == expected


@pytest.mark.analysis
def test_infer_years_all_inferred():
    """Test year inference when no date has an explicit year.

    This test ensures that ``infer_years`` defaults to the current year
    when processing a list of dates where none of the strings contain a year.
    """
    dates = ["01 Jan", "02 Jan"]
    # If no year is provided, it should default to the current year
    current_year = datetime.now().year
    expected = [f"{current_year}-01-01", f"{current_year}-01-02"]
    assert infer_years(dates) == expected


@pytest.mark.analysis
@pytest.mark.parametrize("bad_input", [
    "this is not a date",
    "",
    None,
    "99 Zzz 9999"
])
def test_infer_years_handles_unparseable_string(bad_input):
    """Test that `infer_years` handles unparseable date strings gracefully.

    This parametrized test verifies that when the ``infer_years`` function
    encounters a string that cannot be parsed as a date, it returns ``None``
    in that position in the output list instead of raising an error.

    :param bad_input: An unparseable or invalid date string.
    :type bad_input: str or None
    """
    # A list containing a valid date and the bad input.
    input_dates = ["01 Jan 2025", bad_input]

    # Call the function.
    result = infer_years(input_dates)

    # The unparseable string should have become None in the output.
    assert result == ['2025-01-01', None]


@pytest.mark.analysis
def test_infer_years_backward_pass_corrects_year():
    """Test the backward pass logic for year correction in `infer_years`.

    This test specifically targets the "backward pass" feature of the
    ``infer_years`` function. It ensures that a date like '31 Dec' that
    appears chronologically before '01 Jan 2025' is correctly assigned
    to the previous year (2024), even though it was processed first.
    """
    # A list of dates spanning a year change.
    input_dates = ["31 Dec", "01 Jan 2025"]

    # Call the function.
    result = infer_years(input_dates)

    # The backward pass should have corrected the year of the first date.
    expected_output = ['2024-12-31', '2025-01-01']
    assert result == expected_output
import pytest
from bs4 import BeautifulSoup
from src import query_data # Import the query objects
from src.scrape_and_clean import (
    infer_years,
    parse_status_and_date,
    format_decision_date,
    parse_details_from_badges
)


@pytest.mark.analysis
def test_analysis_labels_and_rounding(client, mocker, db_session):
    """
    Tests that analysis is correctly labeled and percentages are formatted.
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
    """ 
    Tests parsing the details from badges.
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
    """
    This tests the date format logic.
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
    """
    This tests the date and status parsing.
    """
    soup = BeautifulSoup(html_input, 'html.parser')
    tag = soup.find('td')
    status, date_str = parse_status_and_date(tag)
    assert status == expected_status
    assert date_str == expected_date


@pytest.mark.analysis
def test_infer_years_cross_year():
    """
    Test to make sure the year is correctly decided when the year
    prior is different than the year after.
    """
    dates = ["30 Dec 23", "02 Jan", "05 Jan 24"]
    expected = ["2023-12-30", "2024-01-02", "2024-01-05"]
    assert infer_years(dates) == expected


@pytest.mark.analysis
def test_infer_years_all_inferred():
    """
    This checks that the date logic is followed.
    """
    dates = ["01 Jan", "02 Jan"]
    # If no year is provided, it should default to the current year
    from datetime import datetime
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
    """
    Tests that an unparseable date string results in None in the final output.
    """
    # A list containing a valid date and the bad input.
    input_dates = ["01 Jan 2025", bad_input]

    # Call the function.
    result = infer_years(input_dates)

    # The unparseable string should have become None in the output.
    assert result == ['2025-01-01', None]


@pytest.mark.analysis
def test_infer_years_backward_pass_corrects_year():
    """
    Tests the "backward pass" logic in infer_years. It should correctly
    infer that a date like '31 Dec' that comes before '01 Jan 2025'
    must belong to the previous year (2024).
    """
    # A list of dates spanning a year change.
    input_dates = ["31 Dec", "01 Jan 2025"]

    # Call the function.
    result = infer_years(input_dates)

    # The backward pass should have corrected the year of the first date.
    expected_output = ['2024-12-31', '2025-01-01']
    assert result == expected_output


import pytest
import re
from bs4 import BeautifulSoup
from src import app

@pytest.fixture
def client():
    app.app.config['TESTING'] = True
    with app.app.test_client() as client:
        yield client

@pytest.mark.analysis
def test_analysis_labels_and_formatting(client, mocker):
    """
    GIVEN mocked analysis data with a floating-point number
    WHEN the main page is rendered
    THEN verify percentages are formatted to two decimal places.
    """
    # Mock the query function to return predictable data.
    # We'll simulate one query returning a value that needs rounding.
    mock_results = {
        "q1": [("Acceptance Rate", 0.3928123)],
        "q2": [("Total Entries", 100)],
        "q3": [], "q4": [], "q5": [], "q6": [], "q7": [], "q8": [],
        "q9": [], "q10": []
    }
    mocker.patch('src.query_data.execute_query', side_effect=lambda q, fetch='one': mock_results[q])

    response = client.get('/')
    assert response.status_code == 200

    page_content = response.get_data(as_text=True)
    
    # 1. Test for "Answer:" labels
    assert "Answer:" in page_content

    # 2. Test for percentage formatting (39.28%)
    # Use regex to find a number formatted as a percentage with two decimal places.
    match = re.search(r'39\.28%', page_content)
    assert match is not None, "Percentage not formatted correctly to two decimal places"
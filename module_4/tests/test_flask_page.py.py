import pytest
from bs4 import BeautifulSoup
from src import app  # Import the app instance

@pytest.fixture
def client():
    """Create a test client for the Flask app."""
    app.app.config['TESTING'] = True
    with app.app.test_client() as client:
        yield client

@pytest.mark.web
def test_index_page_load(client, mocker):
    """
    GIVEN a Flask application
    WHEN the '/' page is requested (GET)
    THEN check that the response is valid and contains the required elements.
    """
    # Mock the database query to prevent errors during this simple page load test
    mocker.patch('src.query_data.execute_query', return_value=[("Test Data", 1)])

    response = client.get('/')
    assert response.status_code == 200

    soup = BeautifulSoup(response.data, 'html.parser')
    
    # Check for page title/header
    assert "Grad Café Analysis" in soup.find('h1').text

    # Check for buttons
    assert soup.find('button', id='pull-data-btn') is not None, "Pull Data button not found"
    
    # Check for at least one analysis answer placeholder
    assert "Answer:" in response.get_data(as_text=True)
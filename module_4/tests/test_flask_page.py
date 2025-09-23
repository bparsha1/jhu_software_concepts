import pytest
import psycopg
from bs4 import BeautifulSoup


@pytest.mark.web
def test_app_factory(client, db_session, mocker):
    """
    Tests that all routes are live and return the expected status codes.
    Uses 'db_session' to ensure database tables exist.
    Uses 'mocker' to prevent the real data pipeline from running.
    """
    # Mock the slow pipeline function so the test runs fast.
    mocker.patch('src.app.run_full_pipeline')

    # Run each and assert a 200 status code.
    response = client.get('/', follow_redirects=True)
    assert response.status_code == 200 # After redirect

    update_response = client.get('/update-analysis')
    assert update_response.status_code == 200

    pull_response = client.post('/pull-data')
    assert pull_response.status_code == 200

    status_response = client.get('/status')
    assert status_response.status_code == 200

    # Check that testing application was created.
    assert client.application is not None
    assert client.application.config['TESTING'] is True


@pytest.mark.web
def test_content(client, db_with_data):
    """
    Tests that the html elements load correctly on the analysis page.
    Uses the 'db_with_data' fixture to ensure the database is populated.
    """
    response = client.get('/analysis')
    assert response.status_code == 200 # First, ensure the page loaded
    soup = BeautifulSoup(response.data, 'html.parser')

    # Find the elements in the page. 
    pull_button = soup.find('button', id='pullDataBtn')
    update_button = soup.find('button', id='updateAnalysisBtn')
    answer_elements = soup.find_all('p', class_='answer')
    has_answer_text = any("Answer" in p.text for p in answer_elements)

    # Run asserts to verify our expectations.
    assert pull_button is not None
    assert "Pull New Data" in pull_button.text
    assert "Update Analysis" in update_button.text
    assert has_answer_text is True
    assert "Grad School Cafe Data Analysis" in soup.find('h1').text


@pytest.mark.web 
@pytest.mark.buttons
def test_update_analysis_handles_db_error(client, mocker):
    """
    Checking that a database error results in a 500 status code.
    """
    # Use mocker to make the database connection fail.
    # Patch 'psycopg.connect' and tell it to raise an exception
    # when it's called, simulating that the database is down.
    mocker.patch(
        'psycopg.connect',
        side_effect=Exception("Simulated database connection error")
    )

    # Call the endpoint that we now expect to fail.
    response = client.get('/update-analysis')

    # Check that the 'except' block correctly returned a 500 error
    # and the expected JSON error message.
    assert response.status_code == 500
    assert response.is_json
    error_data = response.get_json()
    assert error_data['error'] == "Error loading data from the database."


@pytest.mark.web
def test_index_route_db_error(client, mocker):
    """Test that the index route returns 500 on a database connection error."""
    mocker.patch('psycopg.connect', side_effect=psycopg.OperationalError("DB is down"))
    response = client.get('/analysis')
    assert response.status_code == 500
    assert b"Error loading data from the database" in response.data


import pytest
import psycopg
from bs4 import BeautifulSoup


@pytest.mark.web
def test_app_factory(client, db_session, mocker):
    """Test application factory and route availability.
    
    This test verifies that all application routes are properly configured
    and return expected HTTP status codes. It uses database fixtures to
    ensure proper test isolation and mocking to prevent side effects from
    the data pipeline execution.
    
    The test patches ``src.app.run_full_pipeline`` to prevent the actual
    data pipeline from executing, which would slow down the test and
    potentially cause side effects. It verifies the following routes:
    GET /, GET /update-analysis, POST /pull-data, and GET /status.
    
    It also validates that the test application is properly configured
    with TESTING=True.
    
    :param client: Test client fixture for making HTTP requests to the application.
    :type client: flask.testing.FlaskClient
    :param db_session: Database session fixture that ensures database tables exist for the test environment.
    :type db_session: sqlalchemy.orm.Session
    :param mocker: Pytest mocker fixture for patching dependencies and preventing real data pipeline execution.
    :type mocker: pytest_mock.MockerFixture
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
    """Test HTML content rendering on the analysis page.
    
    This test verifies that the analysis page renders correctly with all 
    expected HTML elements present and properly structured. It ensures
    the frontend components are loading as expected when the database
    contains data.
    
    The test parses the HTML response using BeautifulSoup to verify that
    the Pull Data button exists with correct ID and text, the Update Analysis
    button exists with correct ID and text, answer elements are present with
    expected content, and the page title contains "Grad School Cafe Data Analysis".
    
    :param client: Test client fixture for making HTTP requests to the application.
    :type client: flask.testing.FlaskClient
    :param db_with_data: Database session fixture that provides a populated database with test data.
    :type db_with_data: sqlalchemy.orm.Session
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
    """Test error handling for database connection failures in update analysis.
    
    This test verifies that the application properly handles database connection 
    errors during the update analysis operation by returning appropriate HTTP 
    status codes and error messages in JSON format.
    
    The test simulates a database outage by patching ``psycopg.connect`` to
    raise an exception. It verifies that the endpoint returns HTTP 500,
    the response is properly formatted as JSON, and the error message matches
    the expected format. This ensures graceful degradation when database
    connectivity issues occur.
    
    :param client: Test client fixture for making HTTP requests to the application.
    :type client: flask.testing.FlaskClient
    :param mocker: Pytest mocker fixture used to simulate database connection failures.
    :type mocker: pytest_mock.MockerFixture
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
    """Test error handling for database connection failures on the index route.
    
    This test verifies that the index/analysis route properly handles database 
    operational errors by returning an appropriate HTTP 500 status code
    and error message to the user.
    
    The test specifically simulates a ``psycopg.OperationalError`` (rather than
    a generic Exception) to test more realistic database failure scenarios.
    It verifies that the route returns HTTP 500 when database is unavailable
    and the response contains the expected error message in the body.
    
    :param client: Test client fixture for making HTTP requests to the application.
    :type client: flask.testing.FlaskClient
    :param mocker: Pytest mocker fixture used to simulate database operational errors.
    :type mocker: pytest_mock.MockerFixture
    """
    mocker.patch('psycopg.connect', side_effect=psycopg.OperationalError("DB is down"))
    response = client.get('/analysis')
    assert response.status_code == 500
    assert b"Error loading data from the database" in response.data
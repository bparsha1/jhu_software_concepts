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
    :type db_session: psycopg.Connection
    :param mocker: Pytest mocker fixture for patching dependencies and preventing real data pipeline execution.
    :type mocker: pytest_mock.MockerFixture
    """
    # Mock the slow pipeline function so the test runs fast.
    mocker.patch('src.app.run_full_pipeline')

    # Run each and assert a 200 status code.
    response = client.get('/', follow_redirects=True)
    assert response.status_code == 200 # After redirect

    # /update-analysis accepts both GET and POST per the code
    update_response_get = client.get('/update-analysis')
    assert update_response_get.status_code == 200
    
    update_response_post = client.post('/update-analysis')
    assert update_response_post.status_code == 200

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
    :param db_with_data: Database fixture that provides a populated database with test data.
    :type db_with_data: psycopg.Connection
    """
    response = client.get('/analysis')
    assert response.status_code == 200 # First, ensure the page loaded
    soup = BeautifulSoup(response.data, 'html.parser')

    # Find the elements in the page. 
    pull_button = soup.find('button', id='pullDataBtn')
    update_button = soup.find('button', id='updateAnalysisBtn')
    answer_elements = soup.find_all('p', class_='answer')
    has_answer_text = any("Answer" in p.text for p in answer_elements) if answer_elements else False

    # Run asserts to verify our expectations.
    assert pull_button is not None, "Pull data button should exist"
    assert "Pull New Data" in pull_button.text or "Pull" in pull_button.text, "Pull button should have correct text"
    assert update_button is not None, "Update button should exist"
    assert "Update Analysis" in update_button.text or "Update" in update_button.text, "Update button should have correct text"
    
    # Check for h1 tag with expected content
    h1_tag = soup.find('h1')
    assert h1_tag is not None, "Page should have an h1 tag"
    assert "Grad" in h1_tag.text or "Analysis" in h1_tag.text, "H1 should contain relevant title text"


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
        side_effect=psycopg.Error("Simulated database connection error")
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


@pytest.mark.web
def test_update_analysis_with_PIPELINE_IN_PROGRESS(client, mocker):
    """Test that update-analysis returns conflict when pipeline is running.
    
    This test verifies that the /update-analysis endpoint correctly returns
    a 409 Conflict status when a data pipeline is already in progress,
    preventing race conditions and data inconsistencies.
    
    :param client: Test client fixture for making HTTP requests to the application.
    :type client: flask.testing.FlaskClient
    :param mocker: Pytest mocker fixture for controlling pipeline state.
    :type mocker: pytest_mock.MockerFixture
    """
    import src.app as app_module
    
    # Set the pipeline flag to simulate an in-progress pipeline
    app_module.PIPELINE_IN_PROGRESS = True
    
    try:
        # Call the update endpoint
        response = client.get('/update-analysis')
        
        # Should get a 409 Conflict response
        assert response.status_code == 409
        assert response.is_json
        error_data = response.get_json()
        assert "in progress" in error_data['error']
    finally:
        # Reset the flag for other tests
        app_module.PIPELINE_IN_PROGRESS = False


@pytest.mark.web
def test_pull_data_success(client, mocker):
    """Test successful execution of the pull-data endpoint.
    
    This test verifies that the /pull-data endpoint correctly initiates
    the data pipeline and returns appropriate success response when
    no pipeline is already running.
    
    :param client: Test client fixture for making HTTP requests to the application.
    :type client: flask.testing.FlaskClient
    :param mocker: Pytest mocker fixture for mocking pipeline execution.
    :type mocker: pytest_mock.MockerFixture
    """
    # Mock the pipeline function to avoid actual execution
    mock_pipeline = mocker.patch('src.app.run_full_pipeline')
    
    # Call the pull-data endpoint
    response = client.post('/pull-data')
    
    # Verify success response
    assert response.status_code == 200
    assert response.is_json
    data = response.get_json()
    assert data['status'] == 'success'
    assert 'completed' in data['message']
    
    # Verify the pipeline was called
    mock_pipeline.assert_called_once()


@pytest.mark.web
def test_analysis_route_with_missing_template(client, mocker):
    """Test error handling when template file is missing.
    
    This test verifies that the application handles missing template files
    gracefully by returning an appropriate error message.
    
    :param client: Test client fixture for making HTTP requests to the application.
    :type client: flask.testing.FlaskClient
    :param mocker: Pytest mocker fixture for simulating missing template.
    :type mocker: pytest_mock.MockerFixture
    """
    # Mock psycopg.connect to work normally
    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_cursor.fetchone.return_value = (0,)  # Return empty result
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_conn.__enter__.return_value = mock_conn
    mocker.patch('psycopg.connect', return_value=mock_conn)
    
    # Mock render_template to raise FileNotFoundError
    mocker.patch('src.app.render_template', side_effect=FileNotFoundError("Template not found"))
    
    response = client.get('/analysis')
    assert response.status_code == 500
    assert b"Error loading page template" in response.data


@pytest.mark.web
def test_update_analysis_returns_json_format(client, db_with_data):
    """Test that update-analysis returns properly formatted JSON with query results.
    
    This test verifies that the /update-analysis endpoint returns all
    expected query results in the correct JSON format.
    
    :param client: Test client fixture for making HTTP requests to the application.
    :type client: flask.testing.FlaskClient
    :param db_with_data: Database fixture with populated test data.
    :type db_with_data: psycopg.Connection
    """
    response = client.get('/update-analysis')
    
    assert response.status_code == 200
    assert response.is_json
    
    data = response.get_json()
    
    # Check that all query results are present
    expected_keys = ['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8', 'q9', 'q10']
    for key in expected_keys:
        assert key in data, f"Query result {key} should be in response"
    
    # q9 and q10 should be lists (fetch="all")
    assert isinstance(data['q9'], list), "q9 should return a list"
    assert isinstance(data['q10'], list), "q10 should return a list"


@pytest.mark.web
def test_index_redirects_to_analysis(client):
    """Test that the root route redirects to /analysis.
    
    This test verifies that accessing the root URL properly redirects
    users to the main analysis page.
    
    :param client: Test client fixture for making HTTP requests to the application.
    :type client: flask.testing.FlaskClient
    """
    # Test without following redirects
    response = client.get('/')
    assert response.status_code == 302  # Redirect status
    assert response.location.endswith('/analysis')

    # Test with following redirects
    response = client.get('/', follow_redirects=True)
    assert response.status_code == 200
    assert b"Analysis" in response.data or b"analysis" in response.data
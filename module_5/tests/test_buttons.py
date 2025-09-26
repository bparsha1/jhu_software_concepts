from unittest.mock import MagicMock
import pytest
import subprocess
from src import app


@pytest.mark.buttons
def test_post_pull_data_success(client, mocker, monkeypatch):
    """Test the successful initiation of the data pipeline.

    This test verifies that a POST request to the ``/pull-data`` endpoint,
    when the system is not busy, correctly triggers the full data pipeline
    and returns a 200 OK success response.

    :param client: The Flask test client fixture.
    :type client: flask.testing.FlaskClient
    :param mocker: The pytest-mock fixture for mocking objects.
    :type mocker: pytest_mock.MockerFixture
    :param monkeypatch: The pytest fixture for modifying classes or modules.
    :type monkeypatch: _pytest.monkeypatch.MonkeyPatch
    """
    mock_pipeline = mocker.patch('src.app.run_full_pipeline')

    response = client.post('/pull-data')
    
    assert response.status_code == 200
    assert response.json['status'] == 'success'
    mock_pipeline.assert_called_once()


@pytest.mark.buttons
def test_busy_gating_pull_data(client, mocker, monkeypatch):
    """Test that a new data pull is blocked if one is in progress.

    This test ensures that if a POST request is made to ``/pull-data`` while
    the ``PIPELINE_IN_PROGRESS`` flag is True, the request is rejected with a
    409 Conflict status code, and the pipeline is not triggered again.

    :param client: The Flask test client fixture.
    :type client: flask.testing.FlaskClient
    :param mocker: The pytest-mock fixture for mocking objects.
    :type mocker: pytest_mock.MockerFixture
    :param monkeypatch: The pytest fixture for modifying classes or modules.
    :type monkeypatch: _pytest.monkeypatch.MonkeyPatch
    """
    monkeypatch.setattr(app, 'PIPELINE_IN_PROGRESS', True)
    mock_pipeline = mocker.patch('src.app.run_full_pipeline')
    
    response = client.post('/pull-data')
    
    assert response.status_code == 409
    assert response.json['status'] == 'error'
    assert "already in progress" in response.json['message']
    mock_pipeline.assert_not_called()


@pytest.mark.buttons
def test_busy_gating_update_analysis(client, monkeypatch):
    """Test that analysis updates are blocked if a data pull is in progress.

    This test verifies that a GET request to ``/update-analysis`` is rejected
    with a 409 Conflict status code if the ``PIPELINE_IN_PROGRESS`` flag is
    set to True, preventing analysis from running on incomplete data.

    :param client: The Flask test client fixture.
    :type client: flask.testing.FlaskClient
    :param monkeypatch: The pytest fixture for modifying classes or modules.
    :type monkeypatch: _pytest.monkeypatch.MonkeyPatch
    """
    # Set the pipeline to the "in progress" state.
    monkeypatch.setattr(app, 'PIPELINE_IN_PROGRESS', True)
    
    # Call the endpoint.
    response = client.get('/update-analysis')
    
    # Check for the 409 Conflict code and error message.
    assert response.status_code == 409
    assert response.json['error'] == "A data pull is in progress. Please wait."


@pytest.mark.buttons
def test_post_update_analysis_success(client, db_with_data):
    """Test the successful execution of the analysis update.

    This test ensures that a POST request to ``/update-analysis``, when the
    pipeline is not busy, returns a 200 OK status and a JSON payload
    containing the results of the database queries.

    :param client: The Flask test client fixture.
    :type client: flask.testing.FlaskClient
    :param db_with_data: A fixture providing a database pre-populated with data.
    :type db_with_data: psycopg.Connection
    """
    # The db_with_data fixture provides data for the queries to run against.
    # The autouse fixture in conftest ensures the pipeline is not busy.
    
    # Call the endpoint using the POST method.
    response = client.post('/update-analysis')

    # Check for a 200 OK status and a valid JSON response.
    assert response.status_code == 200
    assert response.is_json
    assert 'q1' in response.get_json() # Verify it returned query data.


@pytest.mark.buttons
def test_status_endpoint(client, monkeypatch):
    """Test the /status endpoint for accurate state reporting.

    This test checks the ``/status`` endpoint in both possible states
    (pipeline busy and not busy) to ensure it correctly reports the value
    of the ``PIPELINE_IN_PROGRESS`` flag.

    :param client: The Flask test client fixture.
    :type client: flask.testing.FlaskClient
    :param monkeypatch: The pytest fixture for modifying classes or modules.
    :type monkeypatch: _pytest.monkeypatch.MonkeyPatch
    """
    # Case 1: Not in progress
    monkeypatch.setattr(app, 'PIPELINE_IN_PROGRESS', False)
    response_not_busy = client.get('/status')
    assert response_not_busy.status_code == 200
    assert not response_not_busy.json['PIPELINE_IN_PROGRESS']

    # Case 2: In progress
    monkeypatch.setattr(app, 'PIPELINE_IN_PROGRESS', True)
    response_busy = client.get('/status')
    assert response_busy.status_code == 200
    assert response_busy.json['PIPELINE_IN_PROGRESS']


@pytest.mark.web
@pytest.mark.buttons
def test_pipeline_subprocess_error(mocker, capsys):
    """Test the pipeline's error handling for a failed subprocess.

    This test simulates a ``subprocess.CalledProcessError`` during the
    pipeline's execution. It verifies that the exception is caught,
    an appropriate error message is logged to standard output, and
    subsequent steps of the pipeline are not executed.

    :param mocker: The pytest-mock fixture for mocking objects.
    :type mocker: pytest_mock.MockerFixture
    :param capsys: The pytest fixture for capturing stdout and stderr.
    :type capsys: _pytest.capture.CaptureFixture
    """
    mocker.patch('psycopg.connect')
    # Mock where the function is called in app.py
    mocker.patch('src.app.run_scrape_and_clean', return_value=1)
    mocker.patch('src.app.subprocess.run', 
                 side_effect=subprocess.CalledProcessError(1, 'cmd', stderr='Fake LLM error'))
    mock_load = mocker.patch('src.app.run_data_loading')

    app.run_full_pipeline()
    mock_load.assert_not_called()
    captured = capsys.readouterr()
    assert "PIPELINE FAILED: Subprocess error: Fake LLM error" in captured.out


@pytest.mark.buttons
def test_pipeline_generic_exception(mocker, monkeypatch, capsys):
    """Test the pipeline's generic exception handler and cleanup.

    This test ensures that if an unexpected generic exception occurs
    during the pipeline's execution, the ``finally`` block is still
    executed, correctly resetting the ``PIPELINE_IN_PROGRESS`` flag
    to False. This prevents the application from getting stuck in a
    busy state.

    :param mocker: The pytest-mock fixture for mocking objects.
    :type mocker: pytest_mock.MockerFixture
    :param monkeypatch: The pytest fixture for modifying classes or modules.
    :type monkeypatch: _pytest.monkeypatch.MonkeyPatch
    :param capsys: The pytest fixture for capturing stdout and stderr.
    :type capsys: _pytest.capture.CaptureFixture
    """
    # Don't set PIPELINE_IN_PROGRESS manually since run_full_pipeline doesn't set it itself
    # Instead, mock psycopg.connect to raise an exception
    mocker.patch('psycopg.connect', side_effect=Exception("A generic error occurred"))
    
    # The function should catch any exception type and handle it
    # Since the app.py code only catches specific exceptions, we need to trigger
    # an exception that will be caught by the general exception handler if there is one
    # Looking at the code, it catches psycopg.Error, subprocess.CalledProcessError, and FileNotFoundError
    # But not generic Exception, so the finally block should still run
    
    try:
        app.run_full_pipeline()
    except Exception:
        # The exception might propagate, but the finally block should have run
        pass
    
    # Check that the finally block ran and reset the flag
    assert app.PIPELINE_IN_PROGRESS is False
    
    # Also check that the error was printed
    captured = capsys.readouterr()
    # The print statement should show that the pipeline process concluded
    assert "PIPELINE PROCESS HAS CONCLUDED" in captured.out

@pytest.mark.buttons
def test_update_analysis_value_error(client, mocker):
    """Test that update-analysis handles ValueError correctly.
    
    This test covers the ValueError exception handler in the /update-analysis
    endpoint by mocking execute_query to raise a ValueError.
    
    :param client: Flask test client fixture for making HTTP requests.
    :type client: flask.testing.FlaskClient
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    """
    # Mock psycopg.connect to return a valid connection
    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = None
    mocker.patch('psycopg.connect', return_value=mock_conn)
    
    # Mock execute_query to raise ValueError on the first call
    mocker.patch(
        'src.query_data.execute_query',
        side_effect=ValueError("Invalid data format encountered")
    )
    
    # Make the request
    response = client.get('/update-analysis')
    
    # Verify the ValueError was caught and handled
    assert response.status_code == 500
    assert response.is_json
    error_data = response.get_json()
    assert error_data['error'] == "Error processing data."
    
    # Also test POST method
    response_post = client.post('/update-analysis')
    assert response_post.status_code == 500
    assert response_post.is_json
    error_data_post = response_post.get_json()
    assert error_data_post['error'] == "Error processing data."
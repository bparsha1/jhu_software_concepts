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
    the ``pipeline_in_progress`` flag is True, the request is rejected with a
    409 Conflict status code, and the pipeline is not triggered again.

    :param client: The Flask test client fixture.
    :type client: flask.testing.FlaskClient
    :param mocker: The pytest-mock fixture for mocking objects.
    :type mocker: pytest_mock.MockerFixture
    :param monkeypatch: The pytest fixture for modifying classes or modules.
    :type monkeypatch: _pytest.monkeypatch.MonkeyPatch
    """
    monkeypatch.setattr(app, 'pipeline_in_progress', True)
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
    with a 409 Conflict status code if the ``pipeline_in_progress`` flag is
    set to True, preventing analysis from running on incomplete data.

    :param client: The Flask test client fixture.
    :type client: flask.testing.FlaskClient
    :param monkeypatch: The pytest fixture for modifying classes or modules.
    :type monkeypatch: _pytest.monkeypatch.MonkeyPatch
    """
    # Set the pipeline to the "in progress" state.
    monkeypatch.setattr(app, 'pipeline_in_progress', True)
    
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
    of the ``pipeline_in_progress`` flag.

    :param client: The Flask test client fixture.
    :type client: flask.testing.FlaskClient
    :param monkeypatch: The pytest fixture for modifying classes or modules.
    :type monkeypatch: _pytest.monkeypatch.MonkeyPatch
    """
    # Case 1: Not in progress
    monkeypatch.setattr(app, 'pipeline_in_progress', False)
    response_not_busy = client.get('/status')
    assert response_not_busy.status_code == 200
    assert not response_not_busy.json['pipeline_in_progress']

    # Case 2: In progress
    monkeypatch.setattr(app, 'pipeline_in_progress', True)
    response_busy = client.get('/status')
    assert response_busy.status_code == 200
    assert response_busy.json['pipeline_in_progress']


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
    mocker.patch('src.app.run_scrape_and_clean', return_value=1)
    mocker.patch('subprocess.run', side_effect=subprocess.CalledProcessError(1, 'cmd', stderr='Fake LLM error'))
    mock_load = mocker.patch('src.app.run_data_loading')

    app.run_full_pipeline()
    mock_load.assert_not_called()
    captured = capsys.readouterr()
    assert "PIPELINE FAILED: Subprocess error: Fake LLM error" in captured.out


@pytest.mark.buttons
def test_pipeline_generic_exception(mocker, monkeypatch):
    """Test the pipeline's generic exception handler and cleanup.

    This test ensures that if an unexpected generic exception occurs
    during the pipeline's execution, the ``finally`` block is still
    executed, correctly resetting the ``pipeline_in_progress`` flag
    to False. This prevents the application from getting stuck in a
    busy state.

    :param mocker: The pytest-mock fixture for mocking objects.
    :type mocker: pytest_mock.MockerFixture
    :param monkeypatch: The pytest fixture for modifying classes or modules.
    :type monkeypatch: _pytest.monkeypatch.MonkeyPatch
    """
    monkeypatch.setattr(app, 'pipeline_in_progress', True)
    mocker.patch('psycopg.connect', side_effect=Exception("A generic error occurred"))
    
    # The function should catch the exception and reset the flag.
    app.run_full_pipeline()
    assert app.pipeline_in_progress is False
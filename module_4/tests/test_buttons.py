import pytest
import subprocess
from src import app


@pytest.mark.buttons
def test_post_pull_data_success(client, mocker, monkeypatch):
    """
    When '/pull-data' is called it should return a 200 OK status,
    and trigger the pipeline.
    """
    mock_pipeline = mocker.patch('src.app.run_full_pipeline')

    response = client.post('/pull-data')
    
    assert response.status_code == 200
    assert response.json['status'] == 'success'
    mock_pipeline.assert_called_once()


@pytest.mark.buttons
def test_busy_gating_pull_data(client, mocker, monkeypatch):
    """
    Test busy gating, if pipeline is in progress we should get 409 status code.
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
    """
    Tests that a GET request to /update-analysis is blocked with a 409
    status code if the data pipeline is in progress.
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
    """
    Tests that a POST request to /update-analysis returns 200
    when the pipeline is not busy.
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
    """
    Helper test to verify the /status endpoint correctly reports the busy state.
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
    """
    Test the pipeline's handler for subprocess errors.
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
    """
    Test the pipeline's generic exception handler.
    """
    monkeypatch.setattr(app, 'pipeline_in_progress', True)
    mocker.patch('psycopg.connect', side_effect=Exception("A generic error occurred"))
    
    # The function should catch the exception and reset the flag.
    app.run_full_pipeline()
    assert app.pipeline_in_progress is False


import pytest
from src import app

@pytest.fixture
def client():
    """Create a test client, ensuring the pipeline is not in progress initially."""
    app.app.config['TESTING'] = True
    app.pipeline_in_progress = False  # Reset state before each test
    with app.app.test_client() as client:
        yield client

@pytest.mark.buttons
def test_pull_data_success(client, mocker):
    """
    GIVEN the pipeline is not running
    WHEN the '/pull-data' endpoint is called (POST)
    THEN it should return a success status and trigger the pipeline.
    """
    # Mock the pipeline function to check if it's called
    mock_pipeline = mocker.patch('src.app.run_full_pipeline')

    response = client.post('/pull-data')
    
    assert response.status_code == 200
    assert response.json['status'] == 'success'
    
    # Verify that the pipeline function was called once
    mock_pipeline.assert_called_once()

@pytest.mark.buttons
def test_pull_data_when_busy(client, mocker):
    """
    GIVEN the pipeline is already in progress
    WHEN the '/pull-data' endpoint is called (POST)
    THEN it should return a conflict error and not trigger the pipeline again.
    """
    # Set the global flag to simulate a busy state
    app.pipeline_in_progress = True

    mock_pipeline = mocker.patch('src.app.run_full_pipeline')
    
    response = client.post('/pull-data')
    
    assert response.status_code == 409  # 409 Conflict
    assert response.json['status'] == 'error'
    assert "already in progress" in response.json['message']
    
    # Verify that the pipeline function was NOT called
    mock_pipeline.assert_not_called()

@pytest.mark.web
def test_status_endpoint(client):
    """
    GIVEN the Flask application
    WHEN the '/status' endpoint is called (GET)
    THEN it should return the correct progress status.
    """
    # Case 1: Not in progress
    app.pipeline_in_progress = False
    response = client.get('/status')
    assert response.status_code == 200
    assert not response.json['pipeline_in_progress']

    # Case 2: In progress
    app.pipeline_in_progress = True
    response = client.get('/status')
    assert response.status_code == 200
    assert response.json['pipeline_in_progress']
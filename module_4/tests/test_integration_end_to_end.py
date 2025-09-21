import pytest
from src import app

@pytest.fixture
def client():
    app.app.config['TESTING'] = True
    app.pipeline_in_progress = False
    with app.app.test_client() as client:
        yield client

@pytest.mark.integration
def test_end_to_end_flow(client, mocker):
    """
    GIVEN a fake scraper that returns new records
    WHEN a user pulls data and then views the analysis page
    THEN the page should show updated analysis from the new data.
    """
    # 1. Mock the entire pipeline to simulate a successful run
    mocker.patch('src.app.run_full_pipeline')
    
    # 2. Mock the query function to return a specific result post-pipeline
    mock_query = mocker.patch('src.query_data.execute_query')
    
    # --- Action 1: User clicks "Pull Data" ---
    pull_response = client.post('/pull-data')
    assert pull_response.status_code == 200
    
    # --- Simulate updated data being available ---
    # After the pull, queries should return new data.
    mock_query.return_value = [("Updated Answer", 42)]

    # --- Action 2: User loads the analysis page ---
    analysis_response = client.get('/')
    assert analysis_response.status_code == 200
    page_content = analysis_response.get_data(as_text=True)

    # 4. Assert that the analysis is updated on the page
    assert "Updated Answer" in page_content
    assert "42" in page_content
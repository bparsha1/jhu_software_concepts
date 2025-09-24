import pytest
import json
from bs4 import BeautifulSoup


FAKE_ENTRY_DATA = [
    {
        'pid': 103, 'comments': 'Great experience.', 'date_added': '2025-09-01',
        'url': 'http://example.com/101', 'status': 'Accepted', 'term': 'Fall 2025',
        'us_or_international': 'American', 'gpa': 3.2, 'gre': 319, 'gre_v': 159,
        'gre_aw': 4.1, 'degree': 'PhD', 'llm_generated_program': 'Mechanical Engineering',
        'llm_generated_university': 'Post University'
    },
    {
        'pid': 104, 'comments': 'Number one fun.', 'date_added': '2025-09-01',
        'url': 'http://example.com/101', 'status': 'Accepted', 'term': 'Fall 2025',
        'us_or_international': 'American', 'gpa': 4.0, 'gre': 310, 'gre_v': 150,
        'gre_aw': 4.2, 'degree': 'MS', 'llm_generated_program': 'Computer Science',
        'llm_generated_university': 'Get University'
    }
]


@pytest.mark.integration
def test_end_to_end_flow_pull_update_render(client, db_session, mocker, tmp_path):
    """Test the complete end-to-end workflow from data pull to final rendering.
    
    This integration test verifies the full application workflow: pulling data
    from a source, updating the analysis via the endpoint, and rendering the
    final results on the web page. It ensures that all components work together
    correctly and that the data flows properly through the entire system.
    
    The test creates fake data, mocks external dependencies, executes the pull
    and update operations, and then verifies that the final rendered page
    contains the expected analysis results.
    
    :param client: Flask test client fixture for making HTTP requests to the application.
    :type client: flask.testing.FlaskClient
    :param db_session: Database session fixture providing a clean database connection.
    :type db_session: psycopg.Connection
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    :param tmp_path: Pytest temporary directory fixture.
    :type tmp_path: pathlib.Path
    """
    # --- SETUP & PULL ---
    jsonl_file = tmp_path / "integration_test_data.jsonl"
    with open(jsonl_file, 'w') as f:
        for item in FAKE_ENTRY_DATA:
            f.write(json.dumps(item) + '\n')

    mocker.patch('src.load_new_data.INPUT_FILE', str(jsonl_file))
    mocker.patch('src.app.run_scrape_and_clean', return_value=len(FAKE_ENTRY_DATA))
    mocker.patch('subprocess.run')

    pull_response = client.post('/pull-data')
    assert pull_response.status_code == 200
    with db_session.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        assert cur.fetchone()[0] == 2

    # --- UPDATE ---
    update_response = client.post('/update-analysis')
    assert update_response.status_code == 200
    assert "q1" in update_response.get_json()

    # --- RENDER & VERIFY ---
    analysis_response = client.get('/', follow_redirects=True)
    assert analysis_response.status_code == 200

    # The assertion checks for the text inside the specific answer element
    soup = BeautifulSoup(analysis_response.data, 'html.parser')
    q6_answer_element = soup.find(id='q6-answer')
    
    assert "Average GPA Acceptance: 3.60" in q6_answer_element.get_text()


@pytest.mark.integration
def test_multiple_pulls_are_idempotent(client, db_session, mocker, tmp_path):
    """Test that multiple pulls with identical data do not create duplicate entries.
    
    This integration test verifies the idempotency of the data pull process by
    running the pull operation twice with the exact same data and ensuring that
    no duplicate rows are created in the database. This is critical for ensuring
    data integrity when the pull process might be run multiple times.
    
    The test sets up fake data, performs an initial pull to populate the database,
    then performs a second pull with identical data and verifies that the row
    count remains unchanged.
    
    :param client: Flask test client fixture for making HTTP requests to the application.
    :type client: flask.testing.FlaskClient
    :param db_session: Database session fixture providing a clean database connection.
    :type db_session: psycopg.Connection
    :param mocker: Pytest mocker fixture for mocking external dependencies.
    :type mocker: pytest_mock.MockerFixture
    :param tmp_path: Pytest temporary directory fixture.
    :type tmp_path: pathlib.Path
    """
    # --- FIRST PULL ---
    jsonl_file = tmp_path / "idempotency_test_data.jsonl"
    with open(jsonl_file, 'w') as f:
        for item in FAKE_ENTRY_DATA:
            f.write(json.dumps(item) + '\n')
    
    mocker.patch('src.load_new_data.INPUT_FILE', str(jsonl_file))
    mocker.patch('src.app.run_scrape_and_clean', return_value=len(FAKE_ENTRY_DATA))
    mocker.patch('subprocess.run')
    
    # Run the first pull.
    client.post('/pull-data')
    with db_session.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        assert cur.fetchone()[0] == 2

    # --- SECOND PULL ---
    # Run the pull a second time with the exact same data.
    client.post('/pull-data')
    with db_session.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM applicants;")
        # Assert that the row count has NOT changed
        assert cur.fetchone()[0] == 2
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
    """
    Tests the full flow: pulling data, updating the analysis via the endpoint,
    and then rendering the page to see the final result.
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
    """
    Tests that running the pull process twice with the same data does not
    create duplicate rows in the database.
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
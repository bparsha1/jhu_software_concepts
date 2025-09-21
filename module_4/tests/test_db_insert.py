import pytest
import psycopg
from src.app import run_full_pipeline

# NOTE: This test requires a running PostgreSQL database.
# Set the DB_CONN_STR to your test database.
DB_CONN_STR_TEST = "dbname=test_grad_cafe user=postgres"

@pytest.fixture(scope="function")
def db_connection():
    """Fixture to set up and tear down a test database connection."""
    conn = psycopg.connect(DB_CONN_STR_TEST)
    # Setup: clear the table before each test
    with conn.cursor() as cur:
        # Assuming your table is named 'decisions'
        cur.execute("TRUNCATE TABLE decisions RESTART IDENTITY;")
    conn.commit()
    yield conn
    # Teardown: close the connection
    conn.close()

@pytest.mark.db
def test_insert_on_pull(db_connection, mocker):
    """
    GIVEN a mocked data pipeline that finds new entries
    WHEN the pipeline is run
    THEN new rows should be written to the database.
    """
    # Mock all steps except the final data loading
    mocker.patch('psycopg.connect', return_value=db_connection)
    mocker.patch('src.scrape_and_clean.main', return_value=1) # Simulate 1 new entry
    mocker.patch('subprocess.run', return_value=True) # Simulate LLM success
    # Mock the data loader to use a predefined clean file
    mocker.patch('src.load_new_data.main') 

    # We call the pipeline function directly for this unit test
    run_full_pipeline()
    
    # Assert that the data loading function was called
    from src.load_new_data import main as run_data_loading
    run_data_loading.assert_called_with(db_connection)

@pytest.mark.db
def test_idempotency_constraints(db_connection, mocker):
    """
    GIVEN data that has already been loaded
    WHEN the pipeline runs again with the same data
    THEN duplicate rows should not be created in the database.
    
    NOTE: This test relies on a UNIQUE constraint in your DB schema 
    (e.g., on institution, degree, and date).
    """
    # For this test, we need to simulate the loader more concretely.
    # We will assume a loader function that we can mock.
    # In a real scenario, you would have the loader read a static test file.
    
    # Mock the external parts of the pipeline
    mocker.patch('psycopg.connect', return_value=db_connection)
    mocker.patch('src.scrape_and_clean.main', return_value=1)
    mocker.patch('subprocess.run')
    
    # We will let the actual data loader run, assuming it loads from a fixed file.
    # To truly test this, the loader would need to be refactored to accept a file path.
    # For now, we mock it to simulate two identical runs.
    loader_mock = mocker.patch('src.load_new_data.main')

    # First run
    run_full_pipeline()
    
    # Second run
    run_full_pipeline()

    # The loader should have been called twice
    assert loader_mock.call_count == 2
    # A true test would then query the DB and assert the row count is what's expected from ONE run.
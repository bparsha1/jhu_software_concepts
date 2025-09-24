import pytest
import psycopg
from src.app import app as flask_app
from src.load_data import setup_database

# In conftest.py I set up test database and app creation for use in tests.

@pytest.fixture(scope="session")
def test_db():
    """Create and tear down a test database for the entire session.

    This fixture connects to a default PostgreSQL instance, creates a new
    database specifically for testing, runs the schema setup, and yields
    the connection string. After all tests in the session are complete,
    it tears down the test database.

    :yields: The connection string for the newly created test database.
    :rtype: str
    """
    default_conn_str = "dbname=postgres user=postgres"
    test_db_name = "test_grad_cafe"
    test_conn_str = f"dbname={test_db_name} user=postgres"

    with psycopg.connect(default_conn_str, autocommit=True) as conn:
        print(f"\n--- Dropping old test database '{test_db_name}' (if exists) ---")
        conn.execute(f"DROP DATABASE IF EXISTS {test_db_name} WITH (FORCE)")
        
        print(f"--- Creating new test database '{test_db_name}' ---")
        conn.execute(f"CREATE DATABASE {test_db_name}")

    # Connect to the new test database to set up the schema.
    print("--- Creating tables in test database ---")
    setup_database(test_conn_str)

    # The setup is complete. Yield the connection string for other fixtures to use.
    yield test_conn_str
    # Teardown: runs after all tests are finished.

    with psycopg.connect(default_conn_str, autocommit=True) as conn:
        print(f"\n--- Dropping test database '{test_db_name}' ---")
        conn.execute(f"DROP DATABASE {test_db_name} WITH (FORCE)")


@pytest.fixture(scope="session")
def app(test_db):
    """Create a Flask app instance for the entire test session.

    This fixture configures the Flask application for testing, setting the
    TESTING flag to True and providing the test database URI.

    :param test_db: The connection string from the `test_db` fixture.
    :type test_db: str
    :yields: The configured Flask application instance.
    :rtype: flask.Flask
    """
    flask_app.config.update({
        "TESTING": True,
        "DATABASE_URI": test_db,
    })
    yield flask_app


@pytest.fixture(scope="session")
def client(app):
    """Provide a Flask test client for the application.

    This client can be used to make requests to the application's endpoints
    without running a live server.

    :param app: The Flask application instance from the `app` fixture.
    :type app: flask.Flask
    :return: A Flask test client.
    :rtype: flask.testing.FlaskClient
    """
    return app.test_client()


@pytest.fixture(scope="function")
def db_session(test_db):
    """Provide a clean database session for each test function.

    This fixture establishes a connection to the test database and truncates
    all tables before yielding the connection. This ensures that each test
    runs against a clean slate, preventing data from one test from
    interfering with another. The connection is closed after the test completes.

    :param test_db: The connection string from the `test_db` fixture.
    :type test_db: str
    :yields: A database connection object.
    :rtype: psycopg.Connection
    """
    conn = psycopg.connect(test_db)
    try:
        with conn.cursor() as cur:
            # Using CASCADE ensures that any tables with foreign key relationships
            # are also truncated correctly.
            cur.execute("TRUNCATE TABLE applicants RESTART IDENTITY CASCADE;")
        conn.commit()
        yield conn
    finally:
        conn.close()


@pytest.fixture(scope="function")
def db_with_data(db_session):
    """Provide a database session pre-populated with test data.

    This fixture builds upon `db_session` by inserting a consistent set of
    records into the `applicants` table. This is useful for tests that
    require pre-existing data to operate on.

    :param db_session: A clean database connection from the `db_session` fixture.
    :type db_session: psycopg.Connection
    :yields: A database connection object with pre-loaded data.
    :rtype: psycopg.Connection
    """
    conn = db_session # Receives the clean connection
    
    with conn.cursor() as cur:
        test_data = [
            {
                'pid': 101, 'comments': 'Great experience.', 'date_added': '2025-09-01',
                'url': 'http://example.com/101', 'status': 'Accepted', 'term': 'Fall 2025',
                'us_or_international': 'American', 'gpa': 3.8, 'gre': 320, 'gre_v': 160,
                'gre_aw': 4.5, 'degree': 'MS', 'llm_generated_program': 'Computer Science',
                'llm_generated_university': 'Test University'
            },
            {
                'pid': 102, 'comments': 'Applied late.', 'date_added': '2025-09-02',
                'url': 'http://example.com/102', 'status': 'Rejected', 'term': 'Fall 2025',
                'us_or_international': 'International', 'gpa': 3.5, 'gre': 315, 'gre_v': 155,
                'gre_aw': 4.0, 'degree': 'PhD', 'llm_generated_program': 'Data Science',
                'llm_generated_university': 'Another University'
            }
        ]

        insert_query = """
            INSERT INTO applicants (
                pid, program, comments, date_added, url, status, term,
                us_or_international, gpa, gre, gre_v, gre_aw, degree,
                llm_generated_program, llm_generated_university
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for entry in test_data:
            llm_uni, llm_prog = entry['llm_generated_university'], entry['llm_generated_program']
            record = (
                entry['pid'], f"{llm_uni}, {llm_prog}", entry['comments'], entry['date_added'],
                entry['url'], entry['status'], entry['term'], entry['us_or_international'],
                entry['gpa'], entry['gre'], entry['gre_v'], entry['gre_aw'], entry['degree'],
                llm_prog, llm_uni
            )
            cur.execute(insert_query, record)

    conn.commit()
    yield conn


@pytest.fixture(autouse=True)
def reset_pipeline_flag():
    """Reset the global `pipeline_in_progress` flag before each test.

    This is an `autouse` fixture, meaning it is automatically applied to
    every test function. It ensures that the pipeline status is reset to a
    known state (False) before a test begins, preventing state leakage.
    """
    from src import app
    app.pipeline_in_progress = False
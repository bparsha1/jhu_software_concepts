"""
Module for analyzing graduate school application data.

This module connects to a PostgreSQL database and executes various
SQL queries to analyze applicant statistics including GPA averages,
acceptance rates, and application statistics.
"""
import psycopg
from psycopg import sql

# --- DATABASE CONNECTION ---
DB_CONN_STR = "dbname=grad_cafe user=postgres"

# --- SQL QUERIES WITH COMPOSITION ---

# Query 1: Fall 2025 applicant count
q1_template = sql.SQL("""
    SELECT
        COUNT(*)
    FROM
        {table}
    WHERE
        {term_col} = {term_val}
    LIMIT 1;
""")

q1 = q1_template.format(
    table=sql.Identifier("applicants"),
    term_col=sql.Identifier("term"),
    term_val=sql.Literal("Fall 2025")
)

# Query 2: International student percentage
q2_template = sql.SQL("""
    SELECT
        ROUND(
            (
                SUM(CASE WHEN {intl_col} = {intl_val} THEN 1 ELSE 0 END) * 100.0 /
                NULLIF(COUNT(*), 0)
            ),
        2)
    FROM
        {table}
    LIMIT 1;
""")

q2 = q2_template.format(
    table=sql.Identifier("applicants"),
    intl_col=sql.Identifier("us_or_international"),
    intl_val=sql.Literal("International")
)

# Query 3: Average GPA and GRE scores
q3_template = sql.SQL("""
    SELECT
        AVG({gpa_col}),
        AVG({gre_col}),
        AVG({gre_v_col}),
        AVG({gre_aw_col})
    FROM
        {table}
    LIMIT 1;
""")

q3 = q3_template.format(
    table=sql.Identifier("applicants"),
    gpa_col=sql.Identifier("gpa"),
    gre_col=sql.Identifier("gre"),
    gre_v_col=sql.Identifier("gre_v"),
    gre_aw_col=sql.Identifier("gre_aw")
)

# Query 4: Average GPA for American students Fall 2025
q4_template = sql.SQL("""
    SELECT
        AVG({gpa_col})
    FROM
        {table}
    WHERE
        {intl_col} = {intl_val} AND {term_col} = {term_val}
    LIMIT 1;
""")

q4 = q4_template.format(
    table=sql.Identifier("applicants"),
    gpa_col=sql.Identifier("gpa"),
    intl_col=sql.Identifier("us_or_international"),
    intl_val=sql.Literal("American"),
    term_col=sql.Identifier("term"),
    term_val=sql.Literal("Fall 2025")
)

# Query 5: Acceptance percentage Fall 2025
q5_template = sql.SQL("""
    SELECT
        ROUND(
            (
                SUM(CASE WHEN {status_col} = {status_val} THEN 1 ELSE 0 END) * 100.0 /
                NULLIF(COUNT(*), 0)
            ),
        2)
    FROM
        {table}
    WHERE
        {term_col} = {term_val}
    LIMIT 1;
""")

q5 = q5_template.format(
    table=sql.Identifier("applicants"),
    status_col=sql.Identifier("status"),
    status_val=sql.Literal("Accepted"),
    term_col=sql.Identifier("term"),
    term_val=sql.Literal("Fall 2025")
)

# Query 6: Average GPA for accepted students Fall 2025
q6_template = sql.SQL("""
    SELECT
        AVG({gpa_col})
    FROM
        {table}
    WHERE
        {term_col} = {term_val} AND {status_col} = {status_val}
    LIMIT 1;
""")

q6 = q6_template.format(
    table=sql.Identifier("applicants"),
    gpa_col=sql.Identifier("gpa"),
    term_col=sql.Identifier("term"),
    term_val=sql.Literal("Fall 2025"),
    status_col=sql.Identifier("status"),
    status_val=sql.Literal("Accepted")
)

# Query 7: Johns Hopkins CS Masters applications
q7_template = sql.SQL("""
    SELECT
        COUNT(*)
    FROM
        {table}
    WHERE
        {uni_col} ILIKE {uni_val}
        AND {prog_col} ILIKE {prog_val}
        AND {degree_col} = {degree_val}
    LIMIT 1;
""")

q7 = q7_template.format(
    table=sql.Identifier("applicants"),
    uni_col=sql.Identifier("llm_generated_university"),
    uni_val=sql.Literal("%johns hopkins%"),
    prog_col=sql.Identifier("llm_generated_program"),
    prog_val=sql.Literal("%computer science%"),
    degree_col=sql.Identifier("degree"),
    degree_val=sql.Literal("Masters")
)

# Query 8: Georgetown PhD CS acceptances 2025
q8_template = sql.SQL("""
    SELECT
        COUNT(*)
    FROM
        {table}
    WHERE
        {uni_col} ILIKE {uni_val}
        AND {prog_col} ILIKE {prog_val}
        AND {degree_col} = {degree_val}
        AND {term_col} LIKE {term_pattern}
        AND {status_col} = {status_val}
    LIMIT 1;
""")

q8 = q8_template.format(
    table=sql.Identifier("applicants"),
    uni_col=sql.Identifier("llm_generated_university"),
    uni_val=sql.Literal("%georgetown%"),
    prog_col=sql.Identifier("llm_generated_program"),
    prog_val=sql.Literal("%computer science%"),
    degree_col=sql.Identifier("degree"),
    degree_val=sql.Literal("PhD"),
    term_col=sql.Identifier("term"),
    term_pattern=sql.Literal("%2025%"),
    status_col=sql.Identifier("status"),
    status_val=sql.Literal("Accepted")
)

# Query 9: Top 3 most applied-to universities
q9_template = sql.SQL("""
    SELECT
        {uni_col},
        COUNT(*) as app_count
    FROM
        {table}
    WHERE
        {uni_col} IS NOT NULL
    GROUP BY
        {uni_col}
    ORDER BY
        app_count DESC
    LIMIT {limit_val};
""")

q9 = q9_template.format(
    table=sql.Identifier("applicants"),
    uni_col=sql.Identifier("llm_generated_university"),
    limit_val=sql.Literal(3)
)

# Query 10: Average GPA by status
q10_template = sql.SQL("""
    SELECT
        {status_col},
        AVG({gpa_col})
    FROM
        {table}
    WHERE
        {status_col} = ANY({status_vals})
    GROUP BY
        {status_col}
    LIMIT {limit_val};
""")

q10 = q10_template.format(
    table=sql.Identifier("applicants"),
    status_col=sql.Identifier("status"),
    gpa_col=sql.Identifier("gpa"),
    status_vals=sql.Literal(["Accepted", "Rejected"]),
    limit_val=sql.Literal(10)  # Reasonable limit for grouped results
)


def execute_query(connection, query, fetch="one"):
    """Execute a SQL query on the given database connection and return results.
    
    This function provides a standardized interface for executing SQL queries
    against the database. It handles cursor management and supports both
    single-row and multi-row result fetching based on the fetch parameter.
    
    :param connection: Database connection object for executing the query.
    :type connection: psycopg.Connection
    :param query: SQL query string or sql.Composed object to execute.
    :type query: str or sql.Composed
    :param fetch: Result fetch mode - "one" for single row, "all" for multiple rows.
    :type fetch: str
    :returns: Query result as tuple (for "one") or list of tuples (for "all").
    :rtype: tuple or list[tuple]
    """
    with connection.cursor() as cur:
        cur.execute(query)
        if fetch == "one":
            return cur.fetchone()
        return cur.fetchall()


def run_all_queries_for_console(connection):
    """Execute all predefined analysis queries and print formatted results to console.
    
    This function runs a comprehensive set of graduate school application analysis
    queries and formats the output for console display. It covers various statistics
    including application counts, acceptance rates, GPA averages, international
    student percentages, and university-specific metrics.
    
    The queries analyze data across different dimensions such as application status,
    student demographics, academic metrics (GPA, GRE scores), and specific programs
    or universities of interest.
    
    :param connection: Database connection object for executing queries.
    :type connection: psycopg.Connection
    """
    print("--- Running Grad Cafe Data Analysis Queries ---")

    print(f"1. Applicants for Fall 2025: {execute_query(connection, q1)[0]}")
    print(f"2. Percentage of International Students: {execute_query(connection, q2)[0]}%")

    r3 = execute_query(connection, q3)
    print(f"3. Averages - GPA: {r3[0]:.2f}, GRE: {r3[1]:.0f}, "
          f"GRE V: {r3[2]:.0f}, GRE AW: {r3[2]:.2f}")

    r4 = execute_query(connection, q4)
    print(f"4. Avg GPA for American Students (Fall 2025): {r4[0]:.2f}")

    r5 = execute_query(connection, q5)
    print(f"5. Acceptance Percentage (Fall 2025): {r5[0]}%")

    r6 = execute_query(connection, q6)
    print(f"6. Avg GPA for Accepted Students (Fall 2025): {r6[0]:.2f}")

    print(f"7. JHU Masters in CS Applications (2025): {execute_query(connection, q7)[0]}")
    print(f"8. Georgetown PhD CS Acceptances (2025): {execute_query(connection, q8)[0]}")

    print("9. Top 3 Most Applied-to Universities:")
    for uni, count in execute_query(connection, q9, fetch="all"):
        print(f"   - {uni}: {count} applications")

    print("10. Average GPA Comparison (Accepted vs. Rejected):")
    for status, avg_gpa in execute_query(connection, q10, fetch="all"):
        avg_gpa_str = f"{avg_gpa:.2f}" if avg_gpa is not None else "N/A"
        print(f"   - {status}: {avg_gpa_str}")


# This function underlying is tested but __main__ can't be tested with pytest.
if __name__ == "__main__":  # pragma: no cover
    with psycopg.connect(DB_CONN_STR) as conn:
        run_all_queries_for_console(conn)

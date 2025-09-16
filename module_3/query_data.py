import psycopg

# --- DATABASE CONNECTION ---
DB_CONN_STR = "dbname=grad_cafe user=postgres"

# --- SQL QUERIES ---

q1 = """
    SELECT 
        COUNT(*) 
    FROM applicants 
    WHERE term = 'Fall 2025';
"""

q2 = """
    SELECT 
        ROUND((COUNT(CASE WHEN us_or_international = 'International' THEN 1 END) * 100.0) / COUNT(*), 2) 
    FROM applicants;
"""

q3 = """
    SELECT
        AVG(gpa),
        AVG(gre),
        AVG(gre_v),
        AVG(gre_aw)
    FROM applicants;
"""

q4 = """
    SELECT 
        AVG(gpa)
    FROM applicants
    WHERE us_or_international = 'American'
      AND term = 'Fall 2025';
"""

q5 = """
    SELECT 
        ROUND((COUNT(CASE WHEN status = 'Accepted' THEN 1 END) * 100.0) / NULLIF(COUNT(*), 0), 2) 
    FROM applicants WHERE term = 'Fall 2025';
"""

q6 = """
    SELECT 
        AVG(gpa)
    FROM applicants
    WHERE term = 'Fall 2025'
      AND status = 'Accepted';
"""

q7 = """
    SELECT 
        COUNT(*) 
    FROM applicants 
    WHERE (llm_generated_university ILIKE '%johns hopkins%') 
        AND (llm_generated_program ILIKE '%computer science%') 
        AND degree = 'Masters';
"""

q8 = """
    SELECT 
        COUNT(*) 
    FROM applicants 
    WHERE (llm_generated_university ILIKE '%georgetown%') 
        AND (llm_generated_program ILIKE '%computer science%') 
        AND degree = 'PhD' 
        AND term LIKE '%2025%' AND status = 'Accepted';
"""

q9 = """
    SELECT 
        llm_generated_university, 
        COUNT(*) as app_count
    FROM applicants
    WHERE llm_generated_university IS NOT NULL
    GROUP BY llm_generated_university
    ORDER BY app_count DESC
    LIMIT 3;
"""

q10 = """
    SELECT 
        status, 
        AVG(gpa)
    FROM applicants
    WHERE status IN ('Accepted', 'Rejected')
    GROUP BY status;
"""

def execute_query(query, fetch="one"):
    """Executes a SQL query using psycopg3 and returns the result."""
    with psycopg.connect(DB_CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            if fetch == "one":
                return cur.fetchone()
            else:
                return cur.fetchall()

def run_all_queries_for_console():
    """Runs all assigned queries and prints the results to the console."""
    print("--- Running Grad Cafe Data Analysis Queries ---")

    print(f"1. Applicants for Fall 2025: {execute_query(q1)[0]}")
    print(f"2. Percentage of International Students: {execute_query(q2)[0]}%")

    r3 = execute_query(q3)
    print(f"3. Averages - GPA: {r3[0]:.2f}, GRE: {r3[1]:.0f}, GRE V: {r3[2]:.0f}, GRE AW: {r3[2]:.2f}")

    r4 = execute_query(q4)
    print(f"4. Avg GPA for American Students (Fall 2025): {r4[0]:.2f}")

    r5 = execute_query(q5)
    print(f"5. Acceptance Percentage (Fall 2025): {r5[0]}%")

    r6 = execute_query(q6)
    print(f"6. Avg GPA for Accepted Students (Fall 2025): {r6[0]:.2f}")

    print(f"7. JHU Masters in CS Applications (2025): {execute_query(q7)[0]}")
    print(f"8. Georgetown PhD CS Acceptances (2025): {execute_query(q8)[0]}")

    print("9. Top 3 Most Applied-to Universities:")
    for uni, count in execute_query(q9, fetch="all"):
        print(f"   - {uni}: {count} applications")

    print("10. Average GPA Comparison (Accepted vs. Rejected):")
    for status, avg_gpa in execute_query(q10, fetch="all"):
        avg_gpa_str = f"{avg_gpa:.2f}" if avg_gpa is not None else "N/A"
        print(f"   - {status}: {avg_gpa_str}")

if __name__ == "__main__":
    run_all_queries_for_console()
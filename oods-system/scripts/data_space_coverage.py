import duckdb
def main():
    con = duckdb.connect()
    total_objs = con.execute("SELECT COUNT(DISTINCT object_id) FROM read_csv_auto('../providers/*/observations.csv')").fetchone()[0]
    
    res = con.execute('''
        SELECT object_id, COUNT(DISTINCT regexp_extract(filename, 'providers/([^/]+)', 1)) as p_count
        FROM read_csv_auto('../providers/*/observations.csv', filename=true)
        GROUP BY object_id
    ''').fetchall()
    
    full_cov = sum(1 for r in res if r[1] == 3)    
    partial = total_objs - full_cov
    score = (full_cov / total_objs) * 100.0 if total_objs > 0 else 0.0
    
    print(f"TOTAL OBJECTS: {total_objs}")
    print(f"FULL COVERAGE: {full_cov}")
    print(f"PARTIAL: {partial}")
    print(f"COVERAGE SCORE: {score:.0f}%")

if __name__ == '__main__':
    main()

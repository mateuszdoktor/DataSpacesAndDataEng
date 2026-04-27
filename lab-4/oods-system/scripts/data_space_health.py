import duckdb
def main():
    con = duckdb.connect()
    total_objs = con.execute("SELECT COUNT(DISTINCT object_id) FROM read_csv_auto('../providers/*/observations.csv')").fetchone()[0]
    res_objs = con.execute('''
        SELECT object_id, COUNT(DISTINCT regexp_extract(filename, 'providers/([^/]+)', 1)) as p_count
        FROM read_csv_auto('../providers/*/observations.csv', filename=true)
        GROUP BY object_id
    ''').fetchall()
    
    full_cov = sum(1 for r in res_objs if r[1] == 3)
    cov_ratio = full_cov / total_objs if total_objs > 0 else 0
    
    full = con.execute("SELECT COUNT(*) FROM read_csv_auto('../providers/*/observations.csv')").fetchone()[0]
    fed = con.execute("SELECT COUNT(*) FROM read_csv_auto(['../providers/satellite_A/observations.csv', '../providers/satellite_B/observations.csv'])").fetchone()[0]
    fed_loss = full - fed > 0

    if cov_ratio < 1.0 or fed_loss:
        print("DATA SPACE HEALTH: WARNING")
        print("Reason: incomplete coverage and federated loss detected")
    else:
        print("DATA SPACE HEALTH: GOOD")

if __name__ == '__main__':
    main()

import duckdb
def main():
    con = duckdb.connect()
    total = con.execute("SELECT COUNT(*) FROM read_csv_auto('../providers/*/observations.csv')").fetchone()[0]
    
    res = con.execute('''
        SELECT regexp_extract(filename, 'providers/([^/]+)', 1) as p, COUNT(*) as c
        FROM read_csv_auto('../providers/*/observations.csv', filename=true)
        GROUP BY p
        ORDER BY c DESC
    ''').fetchall()
    
    for r in res:
        pct = (r[1] / total) * 100.0 if total > 0 else 0.0
        print(f"{r[0]}: {r[1]} ({pct:.0f}%)")
        
    if res:
        print(f"DOMINANT PROVIDER: {res[0][0]}")

if __name__ == '__main__':
    main()

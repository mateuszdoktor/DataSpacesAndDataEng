import duckdb
def main():
    con = duckdb.connect()
    res = con.execute('''
        SELECT object_id, 
               list(regexp_extract(filename, 'providers/([^/]+)', 1)) as p_list
        FROM read_csv_auto('../providers/*/observations.csv', filename=true)
        GROUP BY object_id
        ORDER BY object_id
    ''').fetchall()

    all_p = {"satellite_A", "satellite_B", "ground_station"}
    for r in res:
        obj = r[0]
        provs = set(r[1])
        missing = all_p - provs
        if missing:
            print(f"{obj} missing in: {', '.join(sorted(list(missing)))}")

if __name__ == '__main__':
    main()

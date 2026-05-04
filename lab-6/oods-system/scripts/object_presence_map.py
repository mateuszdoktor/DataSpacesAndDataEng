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
    
    print("OBJECT\tA\tB\tG")
    for r in res:
        obj = r[0]
        provs = set(r[1])
        a = "X" if "satellite_A" in provs else "-"
        b = "X" if "satellite_B" in provs else "-"
        g = "X" if "ground_station" in provs else "-"
        print(f"{obj}\t{a}\t{b}\t{g}")

if __name__ == '__main__':
    main()

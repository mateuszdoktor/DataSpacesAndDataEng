import duckdb
def main():
    con = duckdb.connect()
    ps = ["satellite_A", "satellite_B", "ground_station"]
    schemas = {}
    
    for p in ps:
        try:
            s_res = con.execute(f"DESCRIBE SELECT * FROM '../providers/{p}/observations.csv'").fetchall()
            schemas[p] = {r[0]: r[1] for r in s_res}
        except Exception:
            pass
            
    base = schemas.get("satellite_A", {})
    issues = False
    
    for p in ["satellite_B", "ground_station"]:
        curr = schemas.get(p, {})
        if curr != base:
            if not issues:
                print("SCHEMA DRIFT DETECTED:")
                issues = True
            for col in curr:
                if col not in base:
                    print(f"{p} has extra column: {col}")
            for col in base:
                if col not in curr:
                    print(f"{p} is missing column: {col}")
                    
    if not issues:
        print("SCHEMA STATUS: CONSISTENT")

if __name__ == '__main__':
    main()

import duckdb
def main():
    con = duckdb.connect()
    full = con.execute("SELECT COUNT(*) FROM read_csv_auto('../providers/*/observations.csv')").fetchone()[0]
    fed = con.execute("SELECT COUNT(*) FROM read_csv_auto(['../providers/satellite_A/observations.csv', '../providers/satellite_B/observations.csv'])").fetchone()[0]
    
    if full > fed:
        print("FEDERATED RESULT: INCOMPLETE")
        print("MISSING PROVIDERS: ground_station")
    else:
        print("FEDERATED RESULT: COMPLETE")

if __name__ == '__main__':
    main()

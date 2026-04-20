import duckdb
def main():
    con = duckdb.connect()
    full = con.execute("SELECT COUNT(*) FROM read_csv_auto('../providers/*/observations.csv')").fetchone()[0]
    
    fed = con.execute("SELECT COUNT(*) FROM read_csv_auto(['../providers/satellite_A/observations.csv', '../providers/satellite_B/observations.csv'])").fetchone()[0]
    
    missing = full - fed
    loss = (missing / full) * 100.0 if full > 0 else 0.0
    
    print(f"FULL: {full}")
    print(f"FEDERATED: {fed}")
    print(f"MISSING: {missing}")
    print(f"LOSS: {loss:.1f}%")

if __name__ == '__main__':
    main()

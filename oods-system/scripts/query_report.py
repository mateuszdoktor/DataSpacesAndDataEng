import duckdb
import datetime
import sys
import os

def main():
    obj_id = sys.argv[1] if len(sys.argv) > 1 else "OBJ-003"
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file_timestamped = f"../reports/query_report_{timestamp}.txt"
    report_file_main = f"../reports/query_report.txt"
    
    rep_content = []
    rep_content.append("DATA SPACE QUERY REPORT")
    rep_content.append("-----------------------")
    rep_content.append(f"Generated at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    rep_content.append("")
    rep_content.append("[GLOBAL STATISTICS]")
    
    con = duckdb.connect()
    total_obs = con.execute("SELECT COUNT(*) FROM read_csv_auto('../providers/*/observations.csv')").fetchone()[0]
    distinct_objs = con.execute("SELECT COUNT(DISTINCT object_id) FROM read_csv_auto('../providers/*/observations.csv')").fetchone()[0]
    
    rep_content.append(f"Total observations: {total_obs}")
    rep_content.append(f"Distinct objects: {distinct_objs}")
    rep_content.append("")
    
    rep_content.append(f"[OBJECT ANALYSIS: {obj_id}]")
    rep_content.append("Providers containing object:")
    
    provs = con.execute(f'''
        SELECT DISTINCT regexp_extract(filename, 'providers/([^/]+)', 1) as p
        FROM read_csv_auto('../providers/*/observations.csv', filename=true)
        WHERE object_id = '{obj_id}'
    ''').fetchall()
    
    for p in provs:
        rep_content.append(f"- {p[0]}")
        
    obj_obs = con.execute(f"SELECT COUNT(*) FROM read_csv_auto('../providers/*/observations.csv') WHERE object_id = '{obj_id}'").fetchone()[0]
    rep_content.append(f"Total observations: {obj_obs}")
    rep_content.append("")
    
    rep_content.append("[FEDERATED QUERY COMPARISON]")
    fed = con.execute(f"SELECT COUNT(*) FROM read_csv_auto(['../providers/satellite_A/observations.csv', '../providers/satellite_B/observations.csv']) WHERE object_id = '{obj_id}'").fetchone()[0]
    
    rep_content.append(f"FULL RESULT: {obj_obs}")
    rep_content.append(f"FEDERATED RESULT: {fed}")
    if fed == obj_obs:
        rep_content.append("COMPLETE: YES")
    else:
        rep_content.append("COMPLETE: NO")
    rep_content.append("")
    
    rep_content.append("[SCHEMA VALIDATION]")
    sch_status = "CONSISTENT"
    try:
        s_a = con.execute("DESCRIBE SELECT * FROM '../providers/satellite_A/observations.csv'").fetchall()
        s_b = con.execute("DESCRIBE SELECT * FROM '../providers/satellite_B/observations.csv'").fetchall()
        if s_a != s_b:
            sch_status = "INCONSISTENT"
    except Exception:
        sch_status = "INCONSISTENT"
        
    rep_content.append(f"Schema consistency: {sch_status}")
    
    full_text = "\n".join(rep_content) + "\n"
    
    with open(report_file_timestamped, "w") as f:
        f.write(full_text)
    
    with open(report_file_main, "w") as f:
        f.write(full_text)
    print("Report generated:", report_file_timestamped)
    print("Report generated:", report_file_main)
        
if __name__ == '__main__':
    main()

import duckdb
def main():
    con = duckdb.connect()
    res = con.execute('''
        SELECT object_id, 
               max(temperature) - min(temperature) as diff_temp,
               max(velocity) - min(velocity) as diff_vel
        FROM read_csv_auto('../providers/*/observations.csv')
        GROUP BY object_id
        HAVING COUNT(*) > 1
        ORDER BY object_id
    ''').fetchall()
    
    for r in res:
        obj = r[0]
        dt = r[1] if r[1] else 0
        dv = r[2] if r[2] else 0
        if dt > 0.1 or dv > 0.1:
            attr = "temperature" if dt > 0.1 else "velocity"
            if dt > 0.1 and dv > 0.1:
                attr = "temperature/velocity"
                
            print(f"{obj} inconsistency detected ({attr}):")
            
            vals = con.execute(f'''
                SELECT regexp_extract(filename, 'providers/([^/]+)', 1) as p, temperature, velocity
                FROM read_csv_auto('../providers/*/observations.csv', filename=true)
                WHERE object_id = '{obj}'
            ''').fetchall()
            
            visited = set()
            for v in vals:
                p_name, t_val, v_val = v[0], v[1], v[2]
                if p_name not in visited:
                    visited.add(p_name)
                    if "temperature" in attr and "velocity" not in attr:
                        print(f"{p_name}: {t_val}")
                    elif "velocity" in attr and "temperature" not in attr:
                        print(f"{p_name}: {v_val}")
                    else:
                        print(f"{p_name}: {t_val} (temp), {v_val} (vel)")

if __name__ == '__main__':
    main()

#!/usr/bin/env python3
import duckdb
import os
import json

def get_providers():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    registry_path = os.path.join(os.path.dirname(base_dir), 'contracts', 'providers_registry.json')
    try:
        with open(registry_path, 'r') as f:
            return json.load(f).get('providers', {})
    except Exception:
        return {
            "satellite_A": {"url": "http://localhost:8001"},
            "satellite_B": {"url": "http://localhost:8002"},
            "ground_station": {"url": "http://localhost:8003"}
        }

def main():
    providers = get_providers()
    
    queries = []
    for pid, pinfo in providers.items():
        url = f"{pinfo['url']}/observations.csv"
        queries.append(f"SELECT '{pid}' as provider, * FROM read_csv_auto('{url}')")
        
    union_query = " UNION ALL ".join(queries)
    
    conn = duckdb.connect(':memory:')
    
    try:
        total = conn.execute(f"SELECT COUNT(*) FROM ({union_query})").fetchone()[0]
        print(f"TOTAL OBSERVATIONS: {total}")
        
        obj_count = conn.execute(f"SELECT COUNT(*) FROM ({union_query}) WHERE object_id = 'OBJ-003'").fetchone()[0]
        print(f"OBJ-003 OBSERVATIONS: {obj_count}")
        
        provider_counts = conn.execute(f"SELECT provider, COUNT(*) FROM ({union_query}) GROUP BY provider ORDER BY provider").fetchall()
        largest_provider = ("", 0)
        for row in provider_counts:
            print(f"{row[0]}: {row[1]}")
            if row[1] > largest_provider[1]:
                largest_provider = row
                
        print(f"LARGEST PROVIDER: {largest_provider[0]}")
    except Exception as e:
        print(f"Error executing federated DuckDB query: {e}")

if __name__ == '__main__':
    main()

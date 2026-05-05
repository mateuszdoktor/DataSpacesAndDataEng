import json
import os
import requests
import datetime
import duckdb

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    contracts_dir = os.path.join(os.path.dirname(base_dir), 'contracts')
    reports_dir = os.path.join(os.path.dirname(base_dir), 'reports')
    os.makedirs(reports_dir, exist_ok=True)
    
    with open(os.path.join(contracts_dir, 'providers_registry.json'), 'r') as f:
        providers = json.load(f)
        
    with open(os.path.join(contracts_dir, 'observation_schema.json'), 'r') as f:
        schema = json.load(f)
        
    required_fields = set(schema.get('required_fields', []))
    
    provider_status = {}
    contract_status = {}
    rest_data = []
    
    for provider in providers:
        pid = provider['name']
        url = provider['url'] + '/observations'
        try:
            resp = requests.get(url, timeout=2)
            if resp.status_code == 200:
                provider_status[pid] = "AVAILABLE"
                data = resp.json()
                is_ok = True
                for obs in data:
                    rest_data.append({**obs, 'provider': pid})
                    if not required_fields.issubset(obs.keys()):
                        is_ok = False
                contract_status[pid] = "OK" if is_ok else "VIOLATION"
            else:
                provider_status[pid] = "UNAVAILABLE"
                contract_status[pid] = "UNCLASSIFIED"
        except requests.exceptions.RequestException:
            provider_status[pid] = "UNAVAILABLE"
            contract_status[pid] = "UNCLASSIFIED"
            
    total_obs = len(rest_data)
    distinct_objs = set(o.get('object_id') for o in rest_data if o.get('object_id'))
    
    target_object = "OBJ-003"
    obj3_obs = [o for o in rest_data if o.get('object_id') == target_object]
    obj3_providers = set(o['provider'] for o in obj3_obs)
    
    duckdb_count = 0
    try:
        conn = duckdb.connect(':memory:')
        queries = []
        for p in providers:
            if provider_status.get(p['name']) == "AVAILABLE":
                queries.append(f"SELECT * FROM read_csv_auto('{p['url']}/observations.csv')")
        if queries:
            union_query = " UNION ALL ".join(queries)
            duckdb_count = conn.execute(f"SELECT COUNT(*) FROM ({union_query})").fetchone()[0]
    except Exception:
        duckdb_count = 0
        
    graphql_count = 0
    try:
        q = '{"query": "{ observations { objectId } }"}'
        resp = requests.post("http://localhost:9000/graphql", data=q, headers={'Content-Type': 'application/json'}, timeout=2)
        if resp.status_code == 200:
            graphql_count = len(resp.json().get('data', {}).get('observations', []))
    except Exception:
        graphql_count = 0
        
    is_complete = "YES" if all(st == "AVAILABLE" for st in provider_status.values()) and all(cs == "OK" for cs in contract_status.values()) else "NO"
    
    now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = os.path.join(reports_dir, f"federated_access_report_{now_str}.txt")
    
    content = [
        "FEDERATED ACCESS REPORT",
        "----------------------",
        f"Generated at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "\n[REGISTERED PROVIDERS]"
    ]
    for p in providers:
        content.append(f"- {p['name']}")
        
    content.append("\n[PROVIDER STATUS]")
    for p in providers:
        content.append(f"{p['name']}: {provider_status.get(p['name'], 'UNKNOWN')}")
        
    content.append("\n[CONTRACT VALIDATION]")
    for p in providers:
        content.append(f"{p['name']}: {contract_status.get(p['name'], 'UNKNOWN')}")
        
    content.extend([
        "\n[GLOBAL STATISTICS]",
        f"Total observations: {total_obs}",
        f"Distinct objects: {len(distinct_objs)}",
        f"\n[OBJECT ANALYSIS: {target_object}]",
        "Providers containing object:"
    ])
    for p in obj3_providers:
        content.append(f"- {p}")
    content.append(f"Total observations: {len(obj3_obs)}")
    
    content.extend([
        "\n[ACCESS LAYERS]",
        f"REST RESULT: {total_obs}",
        f"DUCKDB RESULT: {duckdb_count}",
        f"GRAPHQL RESULT: {graphql_count}",
        "\n[COMPLETENESS]",
        f"Federation complete: {is_complete}"
    ])
    
    with open(report_file, 'w') as f:
        f.write("\n".join(content) + "\n")
        
    print("\n".join(content))

if __name__ == '__main__':
    main()

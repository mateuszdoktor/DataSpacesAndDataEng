import requests
object_id = "OBJ-003"
response = requests.get(f"http://127.0.0.1:8001/observations/{object_id}")
data = response.json()
print("OBJECT:", object_id)
print("RESULTS:", len(data))
print(data)
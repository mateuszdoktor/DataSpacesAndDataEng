import requests
object_id = "OBJ-003"
def query_satellite(satellite, object):
    if(satellite == "A"):
        sat_id = 1
    elif(satellite == "B"):
        sat_id = 2

    response = requests.get(f"http://127.0.0.1:800{sat_id}/observations")
    data = response.json()
    n_obs = len(data)

    response = requests.get(f"http://127.0.0.1:800{sat_id}/observations/{object_id}")
    data = response.json()
    n_obs_obj = len(data)


    return n_obs, n_obs_obj

n_a, n_obj_a = query_satellite("A", object_id)
n_b, n_obj_b = query_satellite("B", object_id)

print(f"satellite_A: total number of observations={n_a}, number of {object_id} observations ={n_obj_a}")
print(f"satellite_B: total number of observations={n_b}, number of {object_id} observations ={n_obj_b}")

if (n_a > 0 and n_b > 0):
    print(f"{object_id} is present in both providers")
else:
    print(f"{object_id} is NOT present in both providers")

import json
import copy
import uuid
import random
import string

def generate_random_string(length=5):
    letters = string.ascii_lowercase + string.digits
    return ''.join(random.choice(letters) for i in range(length))

with open('basic-cluster-snapshot.json', 'r') as f:
    data = json.load(f)

# Keep the existing node
node = data['Nodes'][0]

# Keep the existing scheduled pod
scheduled_pod = data['Pods'][0]

# Template for pending pod
pending_pod_template = data['Pods'][1]

new_pods = [scheduled_pod]

# Generate 500 pending pods
for i in range(180):
    pod = copy.deepcopy(pending_pod_template)
    pod['UID'] = str(uuid.uuid4())
    pod['Name'] = f"nginx-random-{generate_random_string()}"
    # Vary requests slightly to prevent simple caching/grouping
    # Base is 5662310400 (approx 5.6GB). Vary by +/- 1MB
    variation = random.randint(-1000000, 1000000)
    pod['AggregatedRequests']['memory'] = 5662310400 + variation
    new_pods.append(pod)

data['Pods'] = new_pods

with open('heavy-cluster-snapshot.json', 'w') as f:
    json.dump(data, f, indent=2)

print("Generated heavy-cluster-snapshot.json with 180 pending pods.")

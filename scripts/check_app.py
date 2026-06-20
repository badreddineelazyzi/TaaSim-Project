import json, urllib.request
d = json.loads(urllib.request.urlopen("http://spark-master:8080/json/").read())
apps = d.get("completedapps", [])
if apps:
    a = apps[-1]
    print(f"Last app: {a['id']} duration={a['duration']}ms cores={a.get('cores','?')}")
else:
    print("No completed apps")

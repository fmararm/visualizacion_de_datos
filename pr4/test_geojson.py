import json
with open('data/Municipios-2024.json') as f:
    d = json.load(f)
    print(d['features'][0]['properties'].keys())

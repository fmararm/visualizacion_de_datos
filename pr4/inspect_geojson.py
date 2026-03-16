import json
from collections import Counter

with open('data/Municipios-2024.json') as f:
    d = json.load(f)

# Obtenemos todas las propiedades del primer feature como ejemplo
properties = d['features'][0]['properties']
print(f"Ejemplo de propiedades para un municipio ({properties.get('label', 'N/A')}):\n")
for k, v in properties.items():
    print(f"  - {k}: {v}")

print("\n" + "="*50 + "\n")

# Comprobamos la presencia de las claves en todos los features
all_keys = Counter()
for feature in d['features']:
    all_keys.update(feature['properties'].keys())

print(f"Total de municipios en GeoJSON: {len(d['features'])}")
print("Mapeo de propiedades presentes en los municipios:")
for k, count in all_keys.most_common():
    print(f"  - {k}: presente en {count}/{len(d['features'])} municipios")

import csv
import os

# 16 Arrondissements of Casablanca with realistic geographic adjacency
arrondissements = [
    {"zone_id": 1, "zone_name": "Anfa", "zone_type": "commercial", "population_density": 8000, "centroid_lat": 33.593, "centroid_lon": -7.632},
    {"zone_id": 2, "zone_name": "Maarif", "zone_type": "commercial", "population_density": 15000, "centroid_lat": 33.585, "centroid_lon": -7.640},
    {"zone_id": 3, "zone_name": "Sidi Belyout", "zone_type": "commercial", "population_density": 9500, "centroid_lat": 33.592, "centroid_lon": -7.618},
    {"zone_id": 4, "zone_name": "El Fida", "zone_type": "residential", "population_density": 35000, "centroid_lat": 33.570, "centroid_lon": -7.608},
    {"zone_id": 5, "zone_name": "Mers Sultan", "zone_type": "residential", "population_density": 30000, "centroid_lat": 33.575, "centroid_lon": -7.612},
    {"zone_id": 6, "zone_name": "Ain Sebaa", "zone_type": "industrial", "population_density": 10000, "centroid_lat": 33.606, "centroid_lon": -7.540},
    {"zone_id": 7, "zone_name": "Hay Mohammadi", "zone_type": "residential", "population_density": 40000, "centroid_lat": 33.596, "centroid_lon": -7.570},
    {"zone_id": 8, "zone_name": "Roches Noires", "zone_type": "industrial", "population_density": 12000, "centroid_lat": 33.600, "centroid_lon": -7.585},
    {"zone_id": 9, "zone_name": "Hay Hassani", "zone_type": "residential", "population_density": 22000, "centroid_lat": 33.560, "centroid_lon": -7.675},
    {"zone_id": 10, "zone_name": "Ain Chock", "zone_type": "transit_hub", "population_density": 18000, "centroid_lat": 33.542, "centroid_lon": -7.614},
    {"zone_id": 11, "zone_name": "Sidi Bernoussi", "zone_type": "industrial", "population_density": 16000, "centroid_lat": 33.590, "centroid_lon": -7.490},
    {"zone_id": 12, "zone_name": "Sidi Moumen", "zone_type": "residential", "population_density": 25000, "centroid_lat": 33.575, "centroid_lon": -7.525},
    {"zone_id": 13, "zone_name": "Ben M'Sick", "zone_type": "residential", "population_density": 38000, "centroid_lat": 33.555, "centroid_lon": -7.580},
    {"zone_id": 14, "zone_name": "Sbata", "zone_type": "residential", "population_density": 34000, "centroid_lat": 33.550, "centroid_lon": -7.590},
    {"zone_id": 15, "zone_name": "Moulay Rachid", "zone_type": "residential", "population_density": 28000, "centroid_lat": 33.540, "centroid_lon": -7.550},
    {"zone_id": 16, "zone_name": "Sidi Othmane", "zone_type": "residential", "population_density": 26000, "centroid_lat": 33.545, "centroid_lon": -7.570},
]

# Geographic adjacency based on actual Casablanca arrondissement borders
# Each zone's neighbors determined by geographic proximity of centroids
ADJACENCY = {
    1: [2, 3, 9],           # Anfa: coastal west, neighbors Maarif, Sidi Belyout, Hay Hassani
    2: [1, 5, 9, 14],       # Maarif: borders Anfa, Mers Sultan, Hay Hassani, Sbata
    3: [1, 5, 7, 8],        # Sidi Belyout: downtown, near Mers Sultan, Hay Mohammadi, Roches Noires
    4: [5, 13, 14],         # El Fida: inner city, near Mers Sultan, Ben M'Sick, Sbata
    5: [3, 4, 8, 13],       # Mers Sultan: central, near Sidi Belyout, El Fida, Roches Noires, Ben M'Sick
    6: [7, 11, 12],         # Ain Sebaa: NE industrial, near Hay Mohammadi, Sidi Bernoussi, Sidi Moumen
    7: [3, 6, 8, 12],       # Hay Mohammadi: east-central, near Sidi Belyout, Ain Sebaa, Roches Noires, Sidi Moumen
    8: [3, 5, 7, 13],       # Roches Noires: port area, near Sidi Belyout, Mers Sultan, Hay Mohammadi, Ben M'Sick
    9: [1, 2, 10, 14],      # Hay Hassani: SW, near Anfa, Maarif, Ain Chock, Sbata
    10: [9, 14, 15, 16],    # Ain Chock: south, near Hay Hassani, Sbata, Moulay Rachid, Sidi Othmane
    11: [6, 12],            # Sidi Bernoussi: far NE, near Ain Sebaa, Sidi Moumen
    12: [6, 7, 11, 15, 16], # Sidi Moumen: east, near Ain Sebaa, Hay Mohammadi, Sidi Bernoussi, Moulay Rachid, Sidi Othmane
    13: [4, 5, 8, 14, 16],  # Ben M'Sick: inner south, near El Fida, Mers Sultan, Roches Noires, Sbata, Sidi Othmane
    14: [2, 4, 9, 10, 13],  # Sbata: south-central, near Maarif, El Fida, Hay Hassani, Ain Chock, Ben M'Sick
    15: [10, 12, 16],       # Moulay Rachid: SE, near Ain Chock, Sidi Moumen, Sidi Othmane
    16: [10, 12, 13, 15],   # Sidi Othmane: south-east, near Ain Chock, Sidi Moumen, Ben M'Sick, Moulay Rachid
}

for zone in arrondissements:
    zone["adjacency_list"] = str(ADJACENCY[zone["zone_id"]])

def main():
    output_path = os.path.join(os.path.dirname(__file__), "..", "data", "zone_mapping.csv")

    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ["zone_id", "zone_name", "zone_type", "population_density", "centroid_lat", "centroid_lon", "adjacency_list"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        writer.writeheader()
        for zone in arrondissements:
            writer.writerow(zone)

    print(f"Successfully generated {output_path} with {len(arrondissements)} zones.")

if __name__ == "__main__":
    main()

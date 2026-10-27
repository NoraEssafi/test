from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

villes = ["Rabat", "Salé", "Casablanca", "Fès", "Marrakech","Tanger", "Tetouan","Dakhela"]

produits = {
    "Laptop": 8500,
    "Souris": 150,
    "Clavier": 300,
    "Ecran": 2500,
    "Casque": 600,
    "TV":7000,
    "PC Bureau":9700
}

id_vente = 1

while True:

    produit = random.choice(list(produits.keys()))
    prix = produits[produit]
    quantite = random.randint(1, 5)
    ville = random.choice(villes)

    vente = {
        "id": id_vente,
        "ville": ville,
        "produit": produit,
        "prix": prix,
        "quantite": quantite
    }

    producer.send("ventes", vente)

    print("Vente envoyée :", vente)

    id_vente += 1

    time.sleep(2)
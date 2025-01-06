from confluent_kafka import Consumer, KafkaException, KafkaError
import json

# Configuration du consommateur Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',  # Adresse du serveur Kafka
    'group.id': 'python-consumer',          # ID du groupe de consommateurs
    'auto.offset.reset': 'earliest'         # Consommer les messages depuis le début si le consommateur est nouveau
}

# Créer un consommateur Kafka
consumer = Consumer(conf)

# S'abonner au topic
topic = "fhir_observations"
consumer.subscribe([topic])

# Fonction pour traiter les messages reçus
def process_message(message):
    try:
        # Convertir le message JSON
        data = json.loads(message)
        print("Message reçu :")
        print(json.dumps(data, indent=4))  # Afficher le message formaté
    except json.JSONDecodeError as e:
        print(f"Erreur de décodage JSON : {e}")

# Consommer des messages en boucle
try:
    while True:
        # Lire un message
        msg = consumer.poll(timeout=1.0)  # Temps d'attente pour un message

        if msg is None:
            continue  # Aucun message reçu, continuer la boucle
        elif msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Atteint la fin de la partition {msg.partition} à l'offset {msg.offset}")
            else:
                raise KafkaException(msg.error())
        else:
            # Si un message est reçu, traiter le message
            process_message(msg.value().decode('utf-8'))

except KeyboardInterrupt:
    print("Arrêt du consommateur")

finally:
    # Fermer le consommateur proprement
    consumer.close()

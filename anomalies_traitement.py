from confluent_kafka import Consumer, KafkaException, KafkaError
from elasticsearch import Elasticsearch, exceptions as es_exceptions
import json
import os
import logging
from collections.abc import Mapping  # Compatibilité Python >= 3.10

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration du consommateur Kafka
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'anomaly-detection-group',
    'auto.offset.reset': 'earliest'
}

# Configuration d'Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])
es_index = "fhir_observations_anomalies"

# Vérifier la connexion à Elasticsearch
try:
    es_info = es.info()
    logging.info(f"Connexion à Elasticsearch réussie, cluster : {es_info['cluster_name']}")
except es_exceptions.ConnectionError as e:
    logging.error(f"Erreur de connexion à Elasticsearch : {e}")
    exit(1)
except Exception as e:
    logging.error(f"Erreur inconnue lors de la connexion à Elasticsearch : {e}")
    exit(1)

# Dossier pour sauvegarder les données normales
normal_data_dir = "./normal_data"
os.makedirs(normal_data_dir, exist_ok=True)

# Fonction pour catégoriser la pression artérielle
def categorize_blood_pressure(systolic, diastolic):
    if systolic < 120 and diastolic < 80:
        return "Normal"
    elif 120 <= systolic <= 129 and diastolic < 80:
        return "Elevated"
    elif 130 <= systolic <= 139 or 80 <= diastolic <= 89:
        return "Hypertension Stage 1"
    elif systolic >= 140 or diastolic >= 90:
        return "Hypertension Stage 2"
    elif systolic > 180 or diastolic > 120:
        return "Hypertensive Crisis"
    return "Uncategorized"

# Fonction pour traiter une observation
def process_observation(observation_json):
    try:
        observation = json.loads(observation_json)
        
        # Vérifier qu'il s'agit d'une observation de pression artérielle
        if observation.get("resourceType") == "Observation" and "component" in observation:
            patient_id = observation["subject"]["reference"].split("/")[-1]
            systolic = None
            diastolic = None

            for component in observation["component"]:
                code = component["code"]["coding"][0]["code"]
                value = component["valueQuantity"]["value"]
                if code == "8480-6":  # Systolic blood pressure
                    systolic = value
                elif code == "8462-4":  # Diastolic blood pressure
                    diastolic = value

            if systolic is not None and diastolic is not None:
                category = categorize_blood_pressure(systolic, diastolic)
                
                # Si les données sont normales, les sauvegarder dans un fichier JSON
                if category == "Normal":
                    file_path = os.path.join(normal_data_dir, f"{patient_id}.json")
                    with open(file_path, "a") as f:
                        f.write(json.dumps(observation) + "\n")
                    return None
                
                # Si une anomalie est détectée, retourner l'anomalie
                anomaly_type = category
                return {
                    "patient_id": patient_id,
                    "systolic_pressure": systolic,
                    "diastolic_pressure": diastolic,
                    "anomaly_type": anomaly_type,
                    "observation_id": observation["id"]
                }
    except KeyError as e:
        logging.error(f"Clé manquante dans les données : {e}")
    except json.JSONDecodeError as e:
        logging.error(f"Erreur de décodage JSON : {e}")
    except Exception as e:
        logging.error(f"Erreur de traitement : {e}")
    return None

# Fonction pour envoyer les anomalies à Elasticsearch
def send_to_elasticsearch(data):
    try:
        res = es.index(index=es_index, body=data)
        logging.info(f"Anomalie envoyée à Elasticsearch : {res['result']}")
    except es_exceptions.ElasticsearchException as e:
        logging.error(f"Erreur d'envoi à Elasticsearch : {e}")
    except Exception as e:
        logging.error(f"Erreur inconnue lors de l'envoi à Elasticsearch : {e}")

# Consommateur Kafka pour lire les messages et détecter les anomalies
consumer = Consumer(kafka_conf)
consumer.subscribe(["fhir_observations"])  # Nom du topic Kafka

try:
    logging.info("Démarrage du consommateur Kafka.")
    while True:
        msg = consumer.poll(1.0)  # Attendre un message
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logging.info(f"Fin de la partition : {msg.partition()}")
            else:
                raise KafkaException(msg.error())
        else:
            # Processer le message reçu
            observation_json = msg.value().decode('utf-8')
            anomaly = process_observation(observation_json)
            if anomaly:
                logging.info(f"Anomalie détectée : {json.dumps(anomaly, indent=4)}")
                send_to_elasticsearch(anomaly)

except KeyboardInterrupt:
    logging.info("Arrêt du consommateur Kafka.")
finally:
    consumer.close()
    logging.info("Consommateur Kafka arrêté.")

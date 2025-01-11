from confluent_kafka import Consumer, KafkaException, KafkaError
from elasticsearch import Elasticsearch, exceptions as es_exceptions
import json
import os
import logging

#configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#configuration du consommateur Kafka
kafka_conf = {
    'bootstrap.servers': 'kafka1:29092',  
    'group.id': 'anomaly-detection-group',
    'auto.offset.reset': 'earliest'
}

#configuration d'Elasticsearch
es = Elasticsearch(["http://elasticsearch:9200"])
es_index = "fhir_observations_anomalies"

#vérifier la connexion à Elasticsearch
try:
    es.info()
    logging.info("Connexion à Elasticsearch réussie.")
except es_exceptions.ConnectionError as e:
    logging.error(f"Erreur de connexion à Elasticsearch : {e}")
    exit(1)

#dossier pour sauvegarder les données normales
normal_data_dir = "./normal_data"
os.makedirs(normal_data_dir, exist_ok=True)

#fonction pour catégoriser la pression artérielle
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
    elif systolic > 180 and diastolic > 120:
        return "Hypertensive Crisis"
    return "Uncategorized"

#fonction pour traiter une observation
def process_observation(observation_json):
    try:
        observation = json.loads(observation_json)
        if observation.get("resourceType") == "Observation" and "component" in observation:
            patient_id = observation["subject"]["reference"].split("/")[-1]
            systolic = None
            diastolic = None

            for component in observation["component"]:
                code = component["code"]["coding"][0]["code"]
                value = component["valueQuantity"]["value"]
                if code == "8480-6":  
                    systolic = value
                elif code == "8462-4":  
                    diastolic = value

            if systolic is not None and diastolic is not None:
                category = categorize_blood_pressure(systolic, diastolic)
                if category == "Normal":
                    file_path = os.path.join(normal_data_dir, f"{patient_id}.json")
                    with open(file_path, "a") as f:
                        f.write(json.dumps(observation) + "\n")
                    return None
                return {
                    "patient_id": patient_id,
                    "systolic_pressure": systolic,
                    "diastolic_pressure": diastolic,
                    "anomaly_type": category,
                    "observation_id": observation["id"]
                }
    except Exception as e:
        logging.error(f"Erreur de traitement : {e}")
    return None

#fonction pour envoyer les anomalies à Elasticsearch
def send_to_elasticsearch(data):
    try:
        es.index(index=es_index, body=data)
        logging.info(f"Anomalie envoyée à Elasticsearch : {data}")
    except Exception as e:
        logging.error(f"Erreur d'envoi à Elasticsearch : {e}")

#consommateur Kafka
consumer = Consumer(kafka_conf)
consumer.subscribe(["fhir_observations"])

try:
    logging.info("Démarrage du détecteur d'anomalies.")
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logging.info(f"Fin de la partition : {msg.partition()}")
            else:
                raise KafkaException(msg.error())
        else:
            observation_json = msg.value().decode('utf-8')
            anomaly = process_observation(observation_json)
            if anomaly:
                logging.info(f"Anomalie détectée : {anomaly}")
                send_to_elasticsearch(anomaly)

except KeyboardInterrupt:
    logging.info("Arrêt du détecteur d'anomalies.")
finally:
    consumer.close()
    logging.info("Détecteur d'anomalies arrêté.")
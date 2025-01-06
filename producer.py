from confluent_kafka import Producer
import json
from datetime import datetime, timedelta
import random
import pytz
from fhir.resources.observation import Observation  # Import FHIR Observation class

# Fonction pour créer une observation de pression artérielle (systolique et diastolique)
def create_blood_pressure_observation(patient_id, systolic_pressure, diastolic_pressure, date):
    # Assurez-vous que 'date' est 'timezone aware'
    if date.tzinfo is None:
        date = date.astimezone(pytz.UTC)  # Assurez-vous que la date est UTC ou ajustez selon votre besoin
    
    observation_data = {
        "resourceType": "Observation",
        "id": "blood-pressure",
        "meta": {
            "profile": ["http://hl7.org/fhir/StructureDefinition/vitalsigns"]
        },
        "status": "final",
        "category": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                        "code": "vital-signs",
                        "display": "Signes vitaux"
                    }
                ],
                "text": "Signes vitaux"
            },
            {
                "coding": [
                    {
                        "system": "http://loinc.org",
                        "code": "85354-9",
                        "display": "Blood pressure"
                    }
                ],
                "text": "Blood pressure"
            }
        ],
        "code": {
            "coding": [
                {
                    "system": "http://loinc.org",
                    "code": "85354-9",
                    "display": "Blood pressure"
                }
            ],
            "text": "Blood pressure"
        },
        "subject": {
            "reference": f"Patient/{patient_id}"
        },
        "effectiveDateTime": date.isoformat(),  # Utilisation de la date au format ISO
        "component": [
            {
                "code": {
                    "coding": [
                        {
                            "system": "http://loinc.org",
                            "code": "8480-6",
                            "display": "Systolic blood pressure"
                        }
                    ],
                    "text": "Systolic blood pressure"
                },
                "valueQuantity": {
                    "value": systolic_pressure,
                    "unit": "mmHg",
                    "system": "http://unitsofmeasure.org",
                    "code": "mm[Hg]"
                }
            },
            {
                "code": {
                    "coding": [
                        {
                            "system": "http://loinc.org",
                            "code": "8462-4",
                            "display": "Diastolic blood pressure"
                        }
                    ],
                    "text": "Diastolic blood pressure"
                },
                "valueQuantity": {
                    "value": diastolic_pressure,
                    "unit": "mmHg",
                    "system": "http://unitsofmeasure.org",
                    "code": "mm[Hg]"
                }
            }
        ]
    }

    # Créer l'objet Observation
    observation = Observation(**observation_data)

    # Convertir l'objet en JSON
    return observation.json(indent=4)

# Liste des 200 patients à générer
patients = [{"id": str(i), "name": f"Patient {i}"} for i in range(1, 201)]

# Liste pour stocker toutes les observations
all_observations = []

# Générer des observations pour chaque patient
for patient in patients:
    patient_id = patient["id"]
    start_date = datetime.now(pytz.UTC) - timedelta(days=30)  # Début il y a 30 jours avec timezone UTC
    end_date = datetime.now(pytz.UTC)

    # Générer 30 observations pour chaque patient
    current_date = start_date
    for _ in range(30):  # 30 observations par patient
        # Générer des valeurs réalistes de pression artérielle
        systolic_pressure = random.randint(70, 190)  # Pression systolique entre 70 et 190 mmHg
        diastolic_pressure = random.randint(60, 130)  # Pression diastolique entre 60 et 130 mmHg

        # Créer l'observation JSON pour la pression artérielle
        observation_json = create_blood_pressure_observation(patient_id, systolic_pressure, diastolic_pressure, current_date)
        
        # Ajouter l'observation à la liste
        all_observations.append(observation_json)
        
        # Avancer de 6 heures pour chaque observation
        current_date += timedelta(hours=6)

# Configuration du producteur Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',  # Adresse du serveur Kafka
    'client.id': 'python-producer'
}

# Fonction de callback pour la gestion des erreurs
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Créer un producteur Kafka
producer = Producer(conf)

# Fonction pour publier un message dans Kafka
def publish_message(topic, message):
    # Utilisation de la fonction produce() pour envoyer un message au topic
    producer.produce(topic, key="fhir-observation", value=message, callback=delivery_report)
    producer.flush()  # Attendre que tous les messages soient envoyés

# Publier les observations dans Kafka
topic = "fhir_observations"  # Nom du topic Kafka

# Publier chaque observation générée dans Kafka
for observation_json in all_observations:
    publish_message(topic, observation_json)

# Afficher un message confirmant que tout a été publié
print(f"Tous les messages ont été envoyés au topic '{topic}'.")

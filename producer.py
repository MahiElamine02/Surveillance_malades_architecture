from confluent_kafka import Producer
import json
from datetime import datetime, timedelta
import random
import pytz
from fhir.resources.observation import Observation
import logging

#configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#fonction pour créer une observation de pression artérielle
def create_blood_pressure_observation(patient_id, systolic_pressure, diastolic_pressure, date):
    if date.tzinfo is None:
        date = date.astimezone(pytz.UTC)

    observation_data = {
        "resourceType": "Observation",
        "id": "blood-pressure",
        "meta": {"profile": ["http://hl7.org/fhir/StructureDefinition/vitalsigns"]},
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
        "subject": {"reference": f"Patient/{patient_id}"},
        "effectiveDateTime": date.isoformat(),
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

    observation = Observation(**observation_data)
    return observation.json(indent=4)

#configuration du producteur Kafka
conf = {
    'bootstrap.servers': 'kafka1:29092',  
    'client.id': 'python-producer'
}

#callback pour la gestion des erreurs
def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

#créer un producteur Kafka
producer = Producer(conf)

#fonction pour publier un message dans Kafka
def publish_message(topic, message):
    try:
        producer.produce(topic, key="fhir-observation", value=message, callback=delivery_report)
        producer.flush()
    except Exception as e:
        logging.error(f"Erreur lors de la publication du message : {e}")

#générer des observations pour 200 patients
patients = [{"id": str(i), "name": f"Patient {i}"} for i in range(1, 201)]  
all_observations = []

#log pour vérifier le nombre de patients
logging.info(f"Nombre de patients : {len(patients)}")

for patient in patients:
    patient_id = patient["id"]
    start_date = datetime.now(pytz.UTC) - timedelta(days=30)  
    end_date = datetime.now(pytz.UTC)

    #log pour vérifier les dates de début et de fin
    logging.info(f"Traitement du patient {patient_id} - Date de début : {start_date}, Date de fin : {end_date}")

    current_date = start_date
    for day in range(30):  
        for observation_count in range(4):  
            systolic_pressure = random.randint(70, 190)
            diastolic_pressure = random.randint(60, 130)
            observation_json = create_blood_pressure_observation(patient_id, systolic_pressure, diastolic_pressure, current_date)
            all_observations.append(observation_json)
            current_date += timedelta(hours=6)  

#log pour vérifier le nombre total d'observations générées
logging.info(f"Nombre total d'observations générées : {len(all_observations)}")

#publier les observations dans Kafka
topic = "fhir_observations"
for observation_json in all_observations:
    publish_message(topic, observation_json)

logging.info(f"Tous les messages ont été envoyés au topic '{topic}'.")
from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import logging

#configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#configuration du consommateur Kafka
conf = {
    'bootstrap.servers': 'kafka1:29092',  
    'group.id': 'python-consumer',
    'auto.offset.reset': 'earliest'
}

#créer un consommateur Kafka
consumer = Consumer(conf)

#s'abonner au topic
topic = "fhir_observations"
consumer.subscribe([topic])

#fonction pour traiter les messages reçus
def process_message(message):
    try:
        data = json.loads(message)
        logging.info(f"Message reçu : {json.dumps(data, indent=4)}")
    except json.JSONDecodeError as e:
        logging.error(f"Erreur de décodage JSON : {e}")

#consommer des messages en boucle
try:
    logging.info("Démarrage du consommateur Kafka.")
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logging.info(f"Fin de la partition : {msg.partition()}")
            else:
                raise KafkaException(msg.error())
        else:
            process_message(msg.value().decode('utf-8'))

except KeyboardInterrupt:
    logging.info("Arrêt du consommateur Kafka.")
finally:
    consumer.close()
    logging.info("Consommateur Kafka arrêté.")
#!/bin/bash

# Attendre que Kafka soit prêt
echo "Attente que Kafka soit prêt..."
./wait-for-it.sh kafka1:29092 --timeout=60 --strict -- echo "Kafka est prêt."

# Exécuter producer.py
echo "Démarrage de producer.py..."
python3 ./producer.py

# Exécuter consumer.py
echo "Démarrage de consumer.py..."
python3 ./consumer.py

# Exécuter anomalies_traitement.py
echo "Démarrage de anomalies_traitement.py..."
python3 ./anomalies_traitement.py
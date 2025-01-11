#utiliser une image de base Python officielle
FROM python:3.10-slim
#définir le répertoire de travail dans le conteneur
WORKDIR /app

#copier les fichiers nécessaires dans le conteneur
COPY producer.py consumer.py anomalies_traitement.py requirements.txt ./

#installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt
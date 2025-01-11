# Utiliser une image de base Python officielle
FROM python:3.10-slim
# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Copier les fichiers nécessaires dans le conteneur
COPY producer.py consumer.py anomalies_traitement.py requirements.txt ./

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt
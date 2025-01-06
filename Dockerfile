# Utilisation d'une image de base légère
FROM python:3.10-slim

# Installer Netcat (nc) en tant que root
RUN apt-get update && apt-get install -y netcat && rm -rf /var/lib/apt/lists/*

# Définir un utilisateur non root
RUN useradd -m myuser
WORKDIR /app
USER myuser

# Copier et installer les dépendances
COPY --chown=myuser:myuser requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier les scripts Python et le script shell
COPY --chown=myuser:myuser producer.py .
COPY --chown=myuser:myuser consumer.py .
COPY --chown=myuser:myuser anomalies_traitement.py .
COPY --chown=myuser:myuser start.sh .
COPY --chown=myuser:myuser wait-for-it.sh .

# Rendre les scripts shell exécutables
RUN chmod +x start.sh
RUN chmod +x wait-for-it.sh

# Définir la commande par défaut
CMD ["./start.sh"]
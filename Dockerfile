# Utiliser une image de base Python
FROM python:3.9-slim

# Installer les dépendances Python
RUN pip install pika mysql-connector-python paramiko

# Copier les scripts Python dans le conteneur
COPY script.py /app/script.py
# COPY consume_and_save.py /app/consume_and_save.py

# Définir le répertoire de travail
WORKDIR /app

# Commande par défaut pour exécuter le script
CMD ["python", "script.py"]

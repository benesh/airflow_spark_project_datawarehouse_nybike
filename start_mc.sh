#!/bin/sh

echo "Configuration du client MinIO…"

# Ajouter l’hôte MinIO (si pas déjà fait)
mc config host add minio http://minio:9000 $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY

echo "Création des buckets…"

# Liste des buckets à créer
BUCKETS="raw-data-nybike dw-nybike"

for bucket in $BUCKETS; do
    echo "→ Bucket : $bucket"
    mc mb --ignore-existing minio/$bucket
done



echo "Upload des fichiers…"

# Exemple d’upload
mc cp /data_nybike/2013-citibike-tripdata/* minio/raw-data-nybike/
#mc cp /data/videos/* minio/videos/
#mc cp /data/backups/* minio/backups/

echo "Initialisation terminée."

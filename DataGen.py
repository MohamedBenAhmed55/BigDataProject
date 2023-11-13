from faker import Faker
import random
import csv
from datetime import datetime, timedelta

fake = Faker()

# Liste de sujets de vidéos réels
topics_list = [
    'Science', 'Technology', 'Music', 'Art', 'Cooking',
    'Travel', 'Fitness', 'Education', 'Gaming', 'Comedy'
]

# Génération de l'historique de visualisation avec les sujets
with open('historique_visualisation.csv', 'w', newline='') as csvfile:
    fieldnames = ['UserID', 'VideoID', 'Timestamp', 'Duration', 'Topics']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for _ in range(100000):  # 100 000 lignes d'historique de visualisation
        writer.writerow({
            'UserID': random.randint(1, 1000),
            'VideoID': random.randint(1, 200),
            'Timestamp': fake.date_time_between(start_date='-30d', end_date='now'),
            'Duration': random.randint(5, 60),
            'Topics': random.choice(topics_list)
        })

# Génération des données des vidéos avec les sujets réels
with open('donnees_videos.csv', 'w', newline='') as csvfile:
    fieldnames_videos = ['VideoID', 'Topics']
    writer_videos = csv.DictWriter(csvfile, fieldnames=fieldnames_videos)

    writer_videos.writeheader()
    for video_id in range(1, 201):  # 200 vidéos
        writer_videos.writerow({
            'VideoID': video_id,
            'Topics': random.choice(topics_list)
        })
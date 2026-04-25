import json
import time
import random
from kafka import KafkaProducer

bootstrap_servers = "XXXXXXXXX.centralindia.azure.confluent.cloud:9092"
api_key = "{Your API Key, I have removed it for security reasons}"
api_secret = "{Your API Secret, I have removed it for security reasons}"

topic = "content_dim"

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=api_key,
    sasl_plain_password=api_secret,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

movie_catalog = [
    {"content_id": "MOV100", "title": "Dark Night", "genre": "Action", "duration_sec": 7200, "release_year": 2022},
    {"content_id": "MOV101", "title": "Fast Run", "genre": "Action", "duration_sec": 6800, "release_year": 2021},
    {"content_id": "MOV102", "title": "Love Story", "genre": "Drama", "duration_sec": 5400, "release_year": 2020},
    {"content_id": "MOV103", "title": "The Hacker", "genre": "Thriller", "duration_sec": 6000, "release_year": 2023},
    {"content_id": "MOV104", "title": "Lost World", "genre": "Adventure", "duration_sec": 7200, "release_year": 2022},
    {"content_id": "MOV105", "title": "Comedy Nights", "genre": "Comedy", "duration_sec": 4000, "release_year": 2021},
    {"content_id": "MOV106", "title": "Space War", "genre": "Sci-Fi", "duration_sec": 7500, "release_year": 2023},
    {"content_id": "MOV107", "title": "Mystery Lake", "genre": "Mystery", "duration_sec": 5000, "release_year": 2020},
    {"content_id": "MOV108", "title": "The Last Hero", "genre": "Action", "duration_sec": 7100, "release_year": 2022},
    {"content_id": "MOV109", "title": "Broken City", "genre": "Crime", "duration_sec": 6500, "release_year": 2021},
    {"content_id": "SER110", "title": "Overflow", "genre": "Romance", "duration_sec": 2400, "release_year": 2004},
    {"content_id": "MOV111", "title": "Avengers Endgame", "genre": "Action", "duration_sec": 8000, "release_year": 2019},
    {"content_id": "MOV112", "title": "Shadow Strike", "genre": "Action", "duration_sec": 7300, "release_year": 2023},
    {"content_id": "MOV113", "title": "Silent Whisper", "genre": "Drama", "duration_sec": 5600, "release_year": 2022},
    {"content_id": "MOV114", "title": "Cyber Chase", "genre": "Sci-Fi", "duration_sec": 7100, "release_year": 2024},
    {"content_id": "MOV115", "title": "Golden Heist", "genre": "Crime", "duration_sec": 6400, "release_year": 2021},
    {"content_id": "MOV116", "title": "Jungle Quest", "genre": "Adventure", "duration_sec": 7200, "release_year": 2020},
    {"content_id": "MOV117", "title": "Laugh Riot", "genre": "Comedy", "duration_sec": 3900, "release_year": 2023},
    {"content_id": "MOV118", "title": "Frozen Fear", "genre": "Thriller", "duration_sec": 6100, "release_year": 2022},
    {"content_id": "MOV119", "title": "Galaxy Riders", "genre": "Sci-Fi", "duration_sec": 7800, "release_year": 2024},
    {"content_id": "SER120", "title": "Code Breakers", "genre": "Thriller", "duration_sec": 2700, "release_year": 2023},
    {"content_id": "SER121", "title": "City Hustle", "genre": "Drama", "duration_sec": 2600, "release_year": 2022},
    {"content_id": "SER122", "title": "Love Bytes", "genre": "Romance", "duration_sec": 2500, "release_year": 2021},
    {"content_id": "SER123", "title": "Crime Files", "genre": "Crime", "duration_sec": 2800, "release_year": 2024},
    {"content_id": "SER124", "title": "Startup Life", "genre": "Drama", "duration_sec": 2400, "release_year": 2023},
    {"content_id": "SER125", "title": "The Unknown Cases", "genre": "Mystery", "duration_sec": 2600, "release_year": 2022},
    {"content_id": "ANI126", "title": "Naruto", "genre": "Adventure", "duration_sec": 1500, "release_year": 2023},
    {"content_id": "ANI127", "title": "Shadow Ninja", "genre": "Action", "duration_sec": 1400, "release_year": 2022},
    {"content_id": "ANI128", "title": "Sky Warriors", "genre": "Action", "duration_sec": 1600, "release_year": 2024},
    {"content_id": "ANI129", "title": "Solo Leveling", "genre": "Action", "duration_sec": 1550, "release_year": 2021},
    {"content_id": "ANI130", "title": "Cyber Samurai", "genre": "Action", "duration_sec": 1450, "release_year": 2023},
    {"content_id": "ANI131", "title": "Eternal Quest", "genre": "Adventure", "duration_sec": 1700, "release_year": 2024},
    {"content_id": None, "title": None, "genre": None, "duration_sec": None, "release_year": 1984},
    {"content_id": None, "title": "fkjsengwgn", "genre": None, "duration_sec": 500, "release_year": 1975}
]

def generate_metadata():
    movie = random.choice(movie_catalog)

    return {
        "content_id":movie["content_id"],
        "title": movie["title"],
        "genre": movie["genre"],
        "duration_sec": movie["duration_sec"],
        "release_year": movie["release_year"]
    }

while True:
    data = generate_metadata()

    producer.send(topic, value=data)

    print(f"Sent to {topic}: {data}")

    time.sleep(random.randint(4, 10))
import json
import time
import random
from kafka import KafkaProducer

bootstrap_servers = "XXXXXXXXX.centralindia.azure.confluent.cloud:9092"
api_key = "{Your API Key, I have removed it for security reasons}"
api_secret = "{Your API Secret, I have removed it for security reasons}"

topic = "watch_events"

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=api_key,
    sasl_plain_password=api_secret,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# 🎬 Movie catalog
movie_catalog = [
    {"content_id": "MOV100", "title": "Dark Night"},
    {"content_id": "MOV101", "title": "Fast Run"},
    {"content_id": "MOV102", "title": "Love Story"},
    {"content_id": "MOV103", "title": "The Hacker"},
    {"content_id": "MOV104", "title": "Lost World"},
    {"content_id": "MOV105", "title": "Comedy Nights"},
    {"content_id": "MOV106", "title": "Space War"},
    {"content_id": "MOV107", "title": "Mystery Lake"},
    {"content_id": "MOV108", "title": "The Last Hero"},
    {"content_id": "MOV109", "title": "Broken City"},
    {"content_id": "SER110", "title": "Overflow"},
    {"content_id": "MOV111", "title": "Avengers Endgame"},
    {"content_id": "MOV112", "title": "Shadow Strike"},
    {"content_id": "MOV113", "title": "Silent Whisper"},
    {"content_id": "MOV114", "title": "Cyber Chase"},
    {"content_id": "MOV115", "title": "Golden Heist"},
    {"content_id": "MOV116", "title": "Jungle Quest"},
    {"content_id": "MOV117", "title": "Laugh Riot"},
    {"content_id": "MOV118", "title": "Frozen Fear"},
    {"content_id": "MOV119", "title": "Galaxy Riders"},
    {"content_id": "SER120", "title": "Code Breakers"},
    {"content_id": "SER121", "title": "City Hustle"},
    {"content_id": "SER122", "title": "Love Bytes"},
    {"content_id": "SER123", "title": "Crime Files"},
    {"content_id": "SER124", "title": "Startup Life"},
    {"content_id": "SER125", "title": "The Unknown Cases"},
    {"content_id": "ANI126", "title": "Naruto"},
    {"content_id": "ANI127", "title": "Shadow Ninja"},
    {"content_id": "ANI128", "title": "Sky Warriors"},
    {"content_id": "ANI129", "title": "Solo Leveling"},
    {"content_id": "ANI130", "title": "Cyber Samurai"},
    {"content_id": "ANI131", "title": "Eternal Quest"},
    {"content_id": None, "title": None}
]

# 🔥 skew → popular movies
popular_ids = ["MOV100", "MOV101", "MOV108", "ANI126", "SER121", "ANI129"]

devices = ["mobile", "tv", "tablet", "web", "smarttv", "android", "iphone", "ratbdv", "windows", "mac", "linux", "bla bla", None]
event_types = ["PLAY", "PAUSE", "STOP", "RESUME"]

def generate_event():
    movie = random.choice(
        [m for m in movie_catalog if m["content_id"] in popular_ids]
        if random.random() < 0.6
        else movie_catalog
    )

    return {
        "user_id": random.randint(1, 1000),
        "content_id": movie["content_id"],
        "title": movie["title"],
        "event_type": random.choice(event_types),
        "watch_time_sec": random.randint(-1, 8000),
        "device": random.choice(devices),
        "event_time": random.choice([
            time.time(),
            time.time() - random.randint(60, 300)
        ])
    }

while True:
    data = generate_event()

    if random.random() < 0.1:
        producer.send(topic, value=data)

    producer.send(topic, value=data)

    print(f"Sent to {topic}: {data}")

    time.sleep(random.uniform(0.5, 3))
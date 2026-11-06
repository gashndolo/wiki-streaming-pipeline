import os

from dotenv import load_dotenv

load_dotenv()

STREAM_URL = os.getenv(
    "WIKI_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
USER_AGENT = os.environ["WIKI_USER_AGENT"]   # Wikimedia asks clients to identify themselves
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "wiki.recentchange")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "wiki.recentchange.dlq")
STATS_INTERVAL_S = int(os.getenv("STATS_INTERVAL_S", "10"))
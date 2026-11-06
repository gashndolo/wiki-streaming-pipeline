import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.getenv("DATA_DIR", PROJECT_ROOT / "data"))
CHECKPOINT_DIR = Path(os.getenv("CHECKPOINT_DIR", PROJECT_ROOT / "checkpoints"))

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "wiki.recentchange")

BRONZE_PATH = str(DATA_DIR / "delta" / "bronze" / "wiki_recentchange")
BRONZE_CHECKPOINT = str(CHECKPOINT_DIR / "bronze")

BRONZE_TRIGGER = os.getenv("BRONZE_TRIGGER", "30 seconds")
# Caps how much one micro-batch may read, so a long outage can't create a giant first batch
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "10000"))
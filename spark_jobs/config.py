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

SILVER_PATH = str(DATA_DIR / "delta" / "silver" / "wiki_events")
QUARANTINE_PATH = str(DATA_DIR / "delta" / "quarantine" / "wiki_events")
SILVER_CHECKPOINT = str(CHECKPOINT_DIR / "silver")
QUARANTINE_CHECKPOINT = str(CHECKPOINT_DIR / "quarantine")

SILVER_TRIGGER = os.getenv("SILVER_TRIGGER", "30 seconds")
SILVER_MAX_FILES = int(os.getenv("SILVER_MAX_FILES", "500"))   # caps files read per batch
DEDUPE_WATERMARK = os.getenv("DEDUPE_WATERMARK", "10 minutes")

TEST_WIKI = "pipeline_test"     # injected test events are excluded from gold

GOLD_MINUTE_PATH = str(DATA_DIR / "delta" / "gold" / "edits_per_minute")
GOLD_PAGE_PATH = str(DATA_DIR / "delta" / "gold" / "edits_by_page_5min")
GOLD_MINUTE_CHECKPOINT = str(CHECKPOINT_DIR / "gold_minute")
GOLD_PAGE_CHECKPOINT = str(CHECKPOINT_DIR / "gold_page")

GOLD_TRIGGER = os.getenv("GOLD_TRIGGER", "30 seconds")
GOLD_MAX_FILES = int(os.getenv("GOLD_MAX_FILES", "500"))
GOLD_WATERMARK = os.getenv("GOLD_WATERMARK", "5 minutes")
PIPELINE_MEMORY = os.getenv("PIPELINE_MEMORY", "3g")
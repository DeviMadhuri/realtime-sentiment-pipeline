# Realtime Sentiment Pipeline

A streaming data pipeline that pulls product reviews off a Kafka topic, runs sentiment analysis on each one with a HuggingFace transformer, lands the labeled results in Delta Lake, and shows everything live on a Streamlit dashboard.

I built this as a data engineer's take on an AI/ML project. The data-movement parts (Kafka, Spark, Delta) play to what I already do at work. The model part (transformers, batched inference) is where I'm picking up new ground.

## How it fits together

Four pieces, each running as its own process:

1. **Producer** (`producer/producer.py`) generates synthetic product reviews with a mix of positive, negative, and neutral sentiment, and pushes them to a Kafka topic called `reviews`.
2. **Streaming job** (`streaming/sentiment_streaming.py`) is a PySpark Structured Streaming reader that consumes from Kafka, runs a HuggingFace sentiment pipeline through a `pandas_udf` (so the model loads once per executor and inference is batched), and writes the labeled rows into a Delta table.
3. **Delta Lake** holds the analyzed reviews at `./data/delta/reviews_sentiment/`. ACID writes, schema enforcement, time travel — the usual reasons to pick Delta over plain Parquet.
4. **Dashboard** (`dashboard/app.py`) is a Streamlit app that reads the Delta table on a loop and shows total counts, sentiment percentages, a chart of sentiment over time, and the most recent reviews.

## Stack

- Kafka (via docker-compose) for the event stream
- PySpark Structured Streaming for the consumer
- HuggingFace transformers (`distilbert-base-uncased-finetuned-sst-2-english`) for sentiment
- Delta Lake for the storage layer
- Streamlit + Altair for the dashboard
- Python 3.10+, Java 17

## Setup

You need:

- Docker Desktop (to run Kafka)
- Python 3.10 or newer
- Java 17 — Spark needs it. On Mac: `brew install openjdk@17`. On Windows/Linux: SDKMAN or Adoptium installer.

Start Kafka:

```bash
docker-compose up -d
```

Set up the Python environment:

```bash
python -m venv venv
source venv/bin/activate     # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

First install pulls PyTorch and Spark, so it takes a few minutes.

## Running it

You need three terminals open at once because each piece is its own long-running process. I usually arrange them side by side so I can watch all three.

**Terminal 1 — producer:**

```bash
source venv/bin/activate
python producer/producer.py
```

You'll see reviews printing every second or two.

**Terminal 2 — streaming job:**

```bash
source venv/bin/activate
python streaming/sentiment_streaming.py
```

The first run downloads the HuggingFace model (~250 MB). After that, Spark starts pulling from Kafka and writing labeled rows to Delta.

**Terminal 3 — dashboard:**

```bash
source venv/bin/activate
streamlit run dashboard/app.py
```

Opens at http://localhost:8501. After a minute, metrics start climbing and the chart fills in.

To shut down: Ctrl-C in each terminal, then `docker-compose down`.

## A few things I learned

- Running a transformer inside Spark with `pandas_udf` is way faster than row-by-row inference. The model loads once per executor and inference is batched, so 1000 reviews take roughly the same time as 100. Per-row UDFs were about 50x slower in my testing.
- The first micro-batch is always slow because the model still has to load into memory. After that it settles into a steady rhythm.
- The checkpoint location matters more than I expected. Without it, restarting the stream reprocesses everything from scratch. With it, you pick up exactly where you left off.
- The default DistilBERT only returns POSITIVE or NEGATIVE. If you want a NEUTRAL class, swap in `cardiffnlp/twitter-roberta-base-sentiment`.

## Stuff to add later

- Replace the synthetic producer with real Reddit data via `praw`, or Hacker News stories
- Per-product sentiment trends on the dashboard
- Track model inference latency over time and alert on drift
- Move Delta storage from local disk to S3 with a Glue catalog
- Containerize the streaming job so the whole pipeline starts with one `docker-compose up`

## File structure

```
realtime-sentiment-pipeline/
├── README.md
├── docker-compose.yml          # Kafka + Zookeeper
├── requirements.txt
├── .gitignore
├── producer/
│   └── producer.py             # synthetic reviews → Kafka
├── streaming/
│   └── sentiment_streaming.py  # Spark + HuggingFace → Delta
└── dashboard/
    └── app.py                  # Streamlit reading from Delta
```

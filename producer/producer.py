"""
Synthetic review producer.

Generates fake product reviews with a mix of sentiments and pushes them
to the Kafka topic `reviews` every second or two.
"""

import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


# Templates roughly bucketed by sentiment so the downstream model has
# something interesting to work with.
POSITIVE_TEMPLATES = [
    "Absolutely love this {product}. Best purchase I've made all year.",
    "The {product} exceeded my expectations. Highly recommend it.",
    "Five stars. {product} works perfectly and shipping was lightning fast.",
    "Great quality {product}. Worth every penny.",
    "I've been using this {product} for a month and it's still amazing.",
    "Beautifully designed {product}. The build feels premium.",
]

NEGATIVE_TEMPLATES = [
    "Terrible {product}. Broke after one week of normal use.",
    "Don't waste your money on this {product}. Total junk.",
    "The {product} arrived damaged and customer service was useless.",
    "Worst purchase ever. {product} doesn't work as advertised.",
    "Returning this {product} immediately. Huge disappointment.",
    "Stopped working after a few days. Avoid this {product}.",
]

NEUTRAL_TEMPLATES = [
    "The {product} is okay. Nothing special, nothing terrible.",
    "{product} does what it says but I expected more for the price.",
    "Average {product}. It works.",
    "It's fine. The {product} meets basic expectations.",
]

PRODUCTS = [
    "headphones",
    "blender",
    "laptop",
    "coffee maker",
    "phone case",
    "backpack",
    "speaker",
    "monitor",
    "keyboard",
]


def generate_review() -> dict:
    sentiment = random.choices(
        ["positive", "negative", "neutral"],
        weights=[0.5, 0.3, 0.2],
    )[0]

    if sentiment == "positive":
        template = random.choice(POSITIVE_TEMPLATES)
    elif sentiment == "negative":
        template = random.choice(NEGATIVE_TEMPLATES)
    else:
        template = random.choice(NEUTRAL_TEMPLATES)

    product = random.choice(PRODUCTS)

    return {
        "review_id": f"rev_{int(time.time() * 1000)}_{random.randint(0, 9999)}",
        "product": product,
        "review_text": template.format(product=product),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10,
    )

    print("Producing reviews to Kafka topic 'reviews'. Press Ctrl+C to stop.\n")

    try:
        while True:
            review = generate_review()
            producer.send("reviews", value=review)
            print(f"sent  {review['product']:>12}  |  {review['review_text'][:70]}")
            time.sleep(random.uniform(0.5, 2.0))
    except KeyboardInterrupt:
        print("\nFlushing and shutting down.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()

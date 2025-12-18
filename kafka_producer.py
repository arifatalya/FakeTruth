#!/usr/bin/env python3
"""
Kafka News Producer
===================
"""

import sys
import json
import time
from datetime import datetime

try:
    import feedparser
except ImportError:
    import subprocess
    subprocess.check_call(["sudo", "apt", "install", "-y", "python3-feedparser"])
    import feedparser

try:
    from kafka import KafkaProducer
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "kafka-python", "--break-system-packages"])
    from kafka import KafkaProducer

from bs4 import BeautifulSoup


# =============================================================================
# RSS FEEDS
# =============================================================================
RSS_FEEDS = {
    "politics": [
        "https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml",
        "https://feeds.bbci.co.uk/news/politics/rss.xml",
    ],
    "business": [
        "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
        "https://feeds.bbci.co.uk/news/business/rss.xml",
    ],
    "technology": [
        "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml",
        "https://feeds.bbci.co.uk/news/technology/rss.xml",
    ],
    "world": [
        "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
        "https://feeds.bbci.co.uk/news/world/rss.xml",
    ],
}

KAFKA_TOPIC = "news-stream"
KAFKA_SERVER = "localhost:9092"


def clean_html(text):
    if not text:
        return ""
    soup = BeautifulSoup(text, 'html.parser')
    return soup.get_text(separator=' ', strip=True)


def create_producer():
    """Create Kafka producer"""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    return producer


def scrape_and_send(producer):
    """Scrape news dan kirim ke Kafka"""
    
    sent_count = 0
    seen = set()
    
    print(f"\n🔄 Scraping at {datetime.now().strftime('%H:%M:%S')}")
    
    for subject, urls in RSS_FEEDS.items():
        for url in urls:
            try:
                feed = feedparser.parse(url)
                
                for entry in feed.entries[:10]:
                    title = entry.get('title', '').strip()
                    
                    if not title or title in seen:
                        continue
                    seen.add(title)
                    
                    # Get text
                    text = ""
                    if hasattr(entry, 'summary'):
                        text = clean_html(entry.summary)
                    
                    if len(text) < 30:
                        continue
                    
                    # Parse date
                    date_str = datetime.now().strftime("%B %d, %Y")
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        date_str = time.strftime("%B %d, %Y", entry.published_parsed)
                    
                    # Create message
                    message = {
                        "title": title,
                        "text": text,
                        "subject": subject,
                        "date": date_str,
                        "timestamp": datetime.now().isoformat()
                    }
                    
                    # Send to Kafka
                    producer.send(KAFKA_TOPIC, key=title[:50], value=message)
                    sent_count += 1
                    
            except Exception as e:
                print(f"  ⚠ Error: {e}")
    
    producer.flush()
    print(f"📤 Sent {sent_count} articles to Kafka topic '{KAFKA_TOPIC}'")
    
    return sent_count


def main():
    print("="*50)
    print("KAFKA NEWS PRODUCER")
    print("="*50)
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Server: {KAFKA_SERVER}")
    
    continuous = "--continuous" in sys.argv
    interval = 2  # minutes
    
    # Create producer
    print("\nConnecting to Kafka...")
    producer = create_producer()
    print("✓ Connected!")
    
    try:
        if continuous:
            print(f"\n🔁 Running continuously every {interval} minutes")
            print("Press Ctrl+C to stop\n")
            
            total = 0
            while True:
                count = scrape_and_send(producer)
                total += count
                print(f"📊 Total sent: {total}")
                print(f"⏰ Next batch in {interval} minutes...\n")
                time.sleep(interval * 60)
        else:
            scrape_and_send(producer)
            print("\n✅ Done!")
            
    except KeyboardInterrupt:
        print("\n\n⛔ Stopped by user")
    finally:
        producer.close()
        print("👋 Producer closed")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Simple Kafka consumer that reads trades and writes to Redis.
For testing purposes - no Spark required.
"""

import json
import os
import time
from datetime import datetime
from kafka import KafkaConsumer
import redis

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
TOPIC = 'raw_trades'

def main():
    print(f"Connecting to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Connecting to Redis: {REDIS_HOST}:{REDIS_PORT}")
    
    # Connect to Kafka
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',  # Read from beginning
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=30000,  # 30 seconds timeout
        group_id='simple-consumer-test-v2'  # Use new consumer group
    )
    
    # Connect to Redis
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    print(f"Consuming from topic: {TOPIC}")
    
    # Track prices per symbol
    prices = {}
    count = 0
    
    for message in consumer:
        try:
            data = message.value
            original = data.get('original_data', {})
            symbol = data.get('symbol', original.get('s', 'UNKNOWN'))
            price = float(original.get('p', 0))
            quantity = float(original.get('q', 0))
            
            # Update price tracking
            if symbol not in prices:
                prices[symbol] = {'open': price, 'high': price, 'low': price, 'volume': 0}
            
            prices[symbol]['close'] = price
            prices[symbol]['high'] = max(prices[symbol]['high'], price)
            prices[symbol]['low'] = min(prices[symbol]['low'], price)
            prices[symbol]['volume'] += quantity
            prices[symbol]['timestamp'] = datetime.utcnow().isoformat()
            
            # Write to Redis - use correct key format for API
            key = f"latest_price:{symbol}"
            ts = int(datetime.utcnow().timestamp() * 1000)
            r.hset(key, mapping={
                'price': str(price),
                'volume': str(prices[symbol]['volume']),
                'timestamp': str(ts)
            })
            
            # Write aggregation
            agg_key = f"aggregations:{symbol}:1m"
            r.hset(agg_key, mapping={
                'open': str(prices[symbol]['open']),
                'high': str(prices[symbol]['high']),
                'low': str(prices[symbol]['low']),
                'close': str(prices[symbol]['close']),
                'volume': str(prices[symbol]['volume']),
                'timestamp': str(ts)
            })
            
            count += 1
            if count % 100 == 0:
                print(f"Processed {count} messages. Latest: {symbol} @ ${price:.2f}")
                
        except Exception as e:
            print(f"Error processing message: {e}")
    
    print(f"Consumer finished. Total messages: {count}")
    consumer.close()

if __name__ == '__main__':
    main()

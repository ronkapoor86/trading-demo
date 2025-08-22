import random
import time
import json
from datetime import datetime, date
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BROKER = "localhost:9092"
TRADES_TOPIC = "trades_topic"
PRICES_TOPIC = "prices_topic"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    key_serializer=lambda x: x.encode('utf-8') if x else None
)

# Trading symbols and books
books = [f"BK{i:03d}" for i in range(1, 101)]
symbols = [
    "AAPL","MSFT","AMZN","GOOG","META","TSLA","NVDA","AMD","ORCL","IBM",
    "CSCO","NFLX","INTC","JPM","BAC","WFC","V","MA","KO","PEP",
    "WMT","MCD","GE","CAT","UNH","CVX","XOM","PG","DIS","BA",
    "T","ABT","ADBE","CRM","PYPL","C","GS","NKE","HD","LOW",
    "UPS","F","GM","TGT","SBUX","BK","AXP","BMY","LLY","RTX"
]

# Baseline prices (used as starting market price)
market_prices = {s: random.uniform(50, 200) for s in symbols}

trade_id_counter  = 1
current_order_id_num = 1
max_order_id = 1000000

# Current order state
current_symbol = None
current_side = None
current_book_id = None
fills_remaining = 0
total_fills_for_order = 0
current_fill_count = 0
current_order_id = None

def generate_quantity():
    return random.randint(10, 1000)

print("=" * 80)
print("KAFKA TRADING + PRICE FEEDS WITH MARKET DRIFT")
print("=" * 80)

try:
    while current_order_id_num <= max_order_id:

        # ---- NEW ORDER ----
        if fills_remaining == 0:
            current_order_id = f"O{current_order_id_num:06d}"
            current_order_id_num += 1

            current_symbol   = random.choice(symbols)
            current_side     = random.choice(["BUY","SELL"])
            current_book_id  = random.choice(books)

            fills_remaining       = random.randint(1, 5)
            total_fills_for_order = fills_remaining
            current_fill_count    = 0

            print(f"\nNEW ORDER: {current_order_id} | Symbol:{current_symbol} | Side:{current_side} "
                  f"| Book:{current_book_id} | Expected fills:{total_fills_for_order}")

        # ---- MARKET DRIFT ----
        drift = random.uniform(-0.003, 0.003)   # +/- 0.3%
        market_prices[current_symbol] = round(
            market_prices[current_symbol] * (1 + drift), 4
        )

        # ---- GENERATE A TRADE PRICE ----
        current_fill_count += 1
        fills_remaining    -= 1

        deviation = random.uniform(-0.002, 0.002)  # ~ +/-0.2%
        trade_price = round(
            market_prices[current_symbol] * (1 + deviation), 4
        )

        trade_data = {
            "trade_id": f"T{trade_id_counter:08d}",
            "order_id": current_order_id,
            "book_id": current_book_id,
            "symbol": current_symbol,
            "trade_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "side": current_side,
            "price": trade_price,
            "quantity": generate_quantity(),
            "fill_number": current_fill_count,
            "total_fills": total_fills_for_order,
            "fills_remaining": fills_remaining
        }
        producer.send(TRADES_TOPIC, key=current_order_id, value=trade_data)

        # ---- PUBLISH CURRENT MARKET PRICE ----
        price_data = {
            "symbol": current_symbol,
            "price_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "price": market_prices[current_symbol]
        }
        producer.send(PRICES_TOPIC, key=current_symbol, value=price_data)

        print(f"Trade {trade_id_counter}: "
              f"{current_symbol} {current_side} {trade_data['quantity']}@${trade_price} | "
              f"Market:${market_prices[current_symbol]} | Book:{current_book_id}")

        trade_id_counter += 1
        time.sleep(0.1)

        if fills_remaining == 0:
            print(f"ORDER COMPLETE: {current_order_id} ({total_fills_for_order} fills)")

except KeyboardInterrupt:
    print("\nInterrupted by user")
finally:
    producer.flush()
    producer.close()


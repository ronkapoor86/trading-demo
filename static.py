import os
import random
import string
import pyarrow as pa
from pyiceberg.catalog import load_catalog

# ----------------------------
# CONFIG
# ----------------------------
print("[CONFIG] Setting AWS environment variables...")
os.environ["AWS_REGION"] = ###
os.environ["AWS_DEFAULT_REGION"] = ###

S3_BUCKET = ###
GLUE_DB = ###

NUM_BOOKS = 100
NUM_TRADERS = 100

DESK_TEAM_POOL = [
    "Equities NA", "Equities EMEA", "ETF Desk",
    "Delta One", "Flow Trading", "Equity Derivatives", "Prime Brokerage"
]

ROLES = ["PRIMARY", "SECONDARY"]

# ----------------------------
# Real tickers (top 50)
# ----------------------------
print("[CONFIG] Loaded 50 real ticker symbols...")

REAL_TICKERS = [
    ("AAPL","Apple Inc.","NASDAQ"),
    ("MSFT","Microsoft Corp.","NASDAQ"),
    ("AMZN","Amazon.com Inc.","NASDAQ"),
    ("GOOG","Alphabet Inc.","NASDAQ"),
    ("META","Meta Platforms Inc.","NASDAQ"),
    ("TSLA","Tesla Inc.","NASDAQ"),
    ("NVDA","NVIDIA Corp","NASDAQ"),
    ("AMD","Advanced Micro Devices","NASDAQ"),
    ("ORCL","Oracle Corp","NYSE"),
    ("IBM","International Business Machines","NYSE"),
    ("CSCO","Cisco Systems","NASDAQ"),
    ("NFLX","Netflix Inc.","NASDAQ"),
    ("INTC","Intel Corp.","NASDAQ"),
    ("JPM","JPMorgan Chase & Co.","NYSE"),
    ("BAC","Bank of America Corp.","NYSE"),
    ("WFC","Wells Fargo & Co.","NYSE"),
    ("V","Visa Inc.","NYSE"),
    ("MA","Mastercard Inc.","NYSE"),
    ("KO","Coca-Cola Co.","NYSE"),
    ("PEP","PepsiCo Inc.","NASDAQ"),
    ("WMT","Walmart Inc.","NYSE"),
    ("MCD","McDonald's Corp.","NYSE"),
    ("GE","General Electric Co.","NYSE"),
    ("CAT","Caterpillar Inc.","NYSE"),
    ("UNH","UnitedHealth Group","NYSE"),
    ("CVX","Chevron Corp","NYSE"),
    ("XOM","Exxon Mobil Corp","NYSE"),
    ("PG","Procter & Gamble Co.","NYSE"),
    ("DIS","Walt Disney Co.","NYSE"),
    ("BA","Boeing Co.","NYSE"),
    ("T","AT&T Inc.","NYSE"),
    ("ABT","Abbott Laboratories","NYSE"),
    ("ADBE","Adobe Inc.","NASDAQ"),
    ("CRM","Salesforce Inc.","NYSE"),
    ("PYPL","PayPal Holdings","NASDAQ"),
    ("C","Citigroup Inc.","NYSE"),
    ("GS","Goldman Sachs Group","NYSE"),
    ("NKE","Nike Inc.","NYSE"),
    ("HD","Home Depot Inc.","NYSE"),
    ("LOW","Lowe's Cos Inc.","NYSE"),
    ("UPS","United Parcel Service","NYSE"),
    ("F","Ford Motor Co.","NYSE"),
    ("GM","General Motors Co.","NYSE"),
    ("TGT","Target Corp.","NYSE"),
    ("SBUX","Starbucks Corp.","NASDAQ"),
    ("BK","Bank of New York Mellon","NYSE"),
    ("AXP","American Express Co.","NYSE"),
    ("BMY","Bristol-Myers Squibb","NYSE"),
    ("LLY","Eli Lilly & Co.","NYSE"),
    ("RTX", "Raytheon Corporation", "NYSE")
]

# ----------------------------
# Utility functions
# ----------------------------
def generate_isin(ticker):
    base = ''.join(random.choices(string.ascii_uppercase + string.digits, k=9))
    return f"US{base}1"

def generate_cusip(ticker):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=9))

# ----------------------------
# Prepare symbol lists
# ----------------------------
print("[SYMBOLS] Preparing symbol, name, and exchange lists...")
symbols, names, exchanges = [], [], []
for sym, name, exch in REAL_TICKERS:
    symbols.append(sym)
    names.append(name)
    exchanges.append(exch)

num_rows = len(symbols)
print(f"[SYMBOLS] Prepared {num_rows} symbols.")

# ----------------------------
# CONNECT TO GLUE CATALOG
# ----------------------------
print(f"[GLUE] Connecting to Glue catalog {GLUE_DB} at {S3_BUCKET}...")
catalog = load_catalog(
    "glue_catalog",
    type="glue",
    warehouse=S3_BUCKET,
    region="us-east-1"
)

try:
    catalog.create_namespace(GLUE_DB)
    print(f"[GLUE] Namespace {GLUE_DB} created.")
except Exception:
    print(f"[GLUE] Namespace {GLUE_DB} already exists.")

# ----------------------------
# 1. REF DATA
# ----------------------------
print("[REFDATA] Creating reference data table...")
refdata_table = pa.table({
    "symbol": symbols[:num_rows],
    "isin": [generate_isin(s) for s in symbols[:num_rows]],
    "cusip": [generate_cusip(s) for s in symbols[:num_rows]],
    "description": names[:num_rows],
    "exchange": exchanges[:num_rows],
    "security_type": ["EQUITY"]*num_rows,
    "currency": ["USD"]*num_rows
})
tname = f"{GLUE_DB}.refdata"
try:
    tbl = catalog.create_table(tname, schema=refdata_table.schema)
    print(f"[REFDATA] Created table {tname}.")
except Exception:
    tbl = catalog.load_table(tname)
    print(f"[REFDATA] Loaded existing table {tname}.")
tbl.append(refdata_table)
print(f"[REFDATA] Inserted {num_rows} rows into {tname}.")

# ----------------------------
# 2. BOOKS
# ----------------------------
print("[BOOKS] Creating books table...")
book_ids = [f"BK{i:03d}" for i in range(1, NUM_BOOKS+1)]
books_table = pa.table({
    "book_id": book_ids,
    "book_name": [f"Book {i}" for i in range(1, NUM_BOOKS+1)],
    "desk": [random.choice(DESK_TEAM_POOL) for _ in range(NUM_BOOKS)],
    "currency": ["USD"]*NUM_BOOKS
})
tname = f"{GLUE_DB}.books"
try:
    tbl = catalog.create_table(tname, schema=books_table.schema)
    print(f"[BOOKS] Created table {tname}.")
except Exception:
    tbl = catalog.load_table(tname)
    print(f"[BOOKS] Loaded existing table {tname}.")
tbl.append(books_table)
print(f"[BOOKS] Inserted {NUM_BOOKS} rows into {tname}.")

# ----------------------------
# 3. TRADERS
# ----------------------------
print("[TRADERS] Creating traders table...")
first_names = ["James","Sarah","Michael","Emily","David","Lisa","Robert","Laura","Daniel","Rachel",
               "Andrew","Jessica","Mark","Sophie","Thomas","Amy","Kevin","Olivia","Alex","Jennifer"]
last_names  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Wilson","Martin",
               "Lee","Clark","Walker","Young","Allen","King","Wright","Scott","Green","Baker"]

trader_ids = [f"TR{i:03d}" for i in range(1, NUM_TRADERS+1)]
trader_names = [f"{random.choice(first_names)} {random.choice(last_names)}" for _ in trader_ids]
trader_teams = [random.choice(DESK_TEAM_POOL) for _ in trader_ids]

traders_table = pa.table({
    "trader_id": trader_ids,
    "name": trader_names,
    "team": trader_teams
})
tname = f"{GLUE_DB}.traders"
try:
    tbl = catalog.create_table(tname, schema=traders_table.schema)
    print(f"[TRADERS] Created table {tname}.")
except Exception:
    tbl = catalog.load_table(tname)
    print(f"[TRADERS] Loaded existing table {tname}.")
tbl.append(traders_table)
print(f"[TRADERS] Inserted {NUM_TRADERS} rows into {tname}.")

# ----------------------------
# 4. BOOKTRADERS
# ----------------------------
print("[BOOKTRADERS] Creating book-traders mapping...")
shuffled_books = random.sample(book_ids, NUM_TRADERS)

bt_book_ids, bt_trader_ids, bt_roles = [], [], []

for trader_id, book_id in zip(trader_ids, shuffled_books):
    bt_book_ids.append(book_id)
    bt_trader_ids.append(trader_id)
    bt_roles.append("PRIMARY")  # or random.choice(ROLES)

booktraders_table = pa.table({
    "book_id": bt_book_ids,
    "trader_id": bt_trader_ids,
    "role": bt_roles
})
tname = f"{GLUE_DB}.booktraders"
try:
    tbl = catalog.create_table(tname, schema=booktraders_table.schema)
    print(f"[BOOKTRADERS] Created table {tname}.")
except Exception:
    tbl = catalog.load_table(tname)
    print(f"[BOOKTRADERS] Loaded existing table {tname}.")
tbl.append(booktraders_table)
print(f"[BOOKTRADERS] Inserted {NUM_TRADERS} rows into {tname}.")

print("[DONE] All tables created and populated successfully.")


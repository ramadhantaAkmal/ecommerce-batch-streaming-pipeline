import psycopg2

def connect_to_db(db_config):
    """Create a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
                host=db_config["host"], 
                port=db_config["port"],
                database=db_config["name"], 
                user=db_config["user"], 
                password=db_config["password"])
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def load_to_db(query: str,data: tuple, conn):
    with conn.cursor() as curs:
        curs.execute(query,data)
    conn.commit()
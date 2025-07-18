import psycopg2
from psycopg2.extras import HstoreAdapter, register_hstore

def connection():
    conn_config = {
        'host': 'localhost',
        'database': 'my_database',
        'user': 'admin',
        'pass': 'admin',
        'port': '5432'
    }

def transaction():

    try:
        conn = psycopg2.connect(**conn_config)
        cur = conn.cursor()

        cur.execute('CREATE EXTENSION IF NOT EXISTS hstore;')



    except psycopg2.Error as e:
        print(f"Erro {e}")

    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == '__main__':
    conn = connection()
    if conn:
        transaction(conn)
        conn.close()
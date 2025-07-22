import psycopg2
from psycopg2.extras import register_hstore

def connection():
    conn_config = {
        'host': 'localhost',
        'database': 'postgres',
        'user': 'postgres',
        'password': 'postgres',
        'port': '5432'
    }

    try:
        conn = psycopg2.connect(**conn_config)
        print("Conexão ok")
        return conn
    except psycopg2.Error as e:
        print(f"Erro {e}")

def transaction(conn):
    try:
        cur = conn.cursor()
        cur.execute("CREATE EXTENSION IF NOT EXISTS hstore;")
        register_hstore(conn)

        query = """UPDATE livros
           SET detalhes = detalhes || '"paginas"=>"429"'::hstore
           WHERE detalhes -> 'ISBN' = '1788472292'"""
        cur.execute(query)

        conn.commit()
        print("Transação concluída")
    except psycopg2.Error as e:
        print(f"Erro: {e}")
        conn.rollback()
    finally:
        cur.close()

if __name__ == "__main__":
    conn = connection()
    if conn:
        transaction(conn)
        conn.close()
        print("Conexão encerrada.")

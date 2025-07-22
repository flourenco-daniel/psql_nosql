import psycopg2
from psycopg2.extras import register_hstore

def connection():
    conn_config = {
        'host': 'localhost',
        'database': 'postgres',
        'user': 'postgres',
        'password': 'postgres',  # Era 'pass', agora é 'password'
        'port': '5432'
    }
    
    try:
        conn = psycopg2.connect(**conn_config)
        print("Conexão estabelecida!")
        return conn
    except psycopg2.Error as e:
        print(f"Erro na conexão: {e}")
        return None

def transaction(conn):  # Agora recebe conn como parâmetro
    try:
        cur = conn.cursor()
        
        # Habilitar hstore
        cur.execute("CREATE EXTENSION IF NOT EXISTS hstore;")
        register_hstore(conn)
        
        # Criar tabela
        cur.execute("""CREATE TABLE IF NOT EXISTS livros (
                        nr serial PRIMARY KEY,
                        titulo varchar,
                        detalhes hstore
                    );""")
        
        # INSERT com sintaxe correta do hstore
        cur.execute("""INSERT INTO livros (titulo, detalhes) VALUES (%s, %s);""",
                   ('Mastering PostgreSQL 10', {
                       'paginas': '428',
                       'assunto': 'PostgreSQL',
                       'idioma': 'inglês',
                       'ISBN': '1788472292',
                       'ISBN-13': '978-1788472296',
                       'peso': '798 g'
                   }))
        
        # Consultar dados inseridos
        cur.execute("SELECT * FROM livros;")
        resultados = cur.fetchall()
        
        print("\n--- Livros cadastrados ---")
        for row in resultados:
            print(f"ID: {row[0]} | Título: {row[1]} | Detalhes: {row[2]}")
        
        conn.commit()
        print("Transação concluída!")
        
    except psycopg2.Error as e:
        print(f"Erro: {e}")
        conn.rollback()
    
    finally:
        cur.close()

if __name__ == '__main__':
    conn = connection()
    if conn:
        transaction(conn)
        conn.close()
        print("Conexão fechada.")
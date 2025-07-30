import psycopg2
from psycopg2.extras import Json

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
        print("Conexão estabelecida!")
        return conn
    except psycopg2.Error as e:
        print(f"Erro na conexão: {e}")
        return None

def transaction(conn): 
    try:
        cur = conn.cursor()
        
        
        cur.execute("""CREATE TABLE IF NOT EXISTS livros_json (
                        nr serial PRIMARY KEY,
                        titulo varchar,
                        detalhes json
                    );""")
        
        cur.execute("""INSERT INTO livros_json (titulo, detalhes) VALUES (%s, %s);""",
                   ('Mastering PostgreSQL 10', Json({
                       'paginas': '428',
                       'assunto': 'PostgreSQL',
                       'idioma': 'inglês',
                       'ISBN-13': '978-1788472296',
                       'peso': '798 g'
                   })))
        
        cur.execute("""INSERT INTO livros_json (titulo, detalhes) VALUES (%s, %s);""",
            ('Mastering PostgreSQL 10', Json({
                'paginas': '216',
                'assunto': 'Novatec',
                'idioma': 'português',
                'ISBN': '8575223380',
                'peso': '381 g'
            })))
        
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
import psycopg2
from psycopg2.extras import Json

def connection():
    conn_config = {
        'host': 'localhost',
        'database': 'postgres',
        'user': 'postgres',
        'password': 'postgres',
        'port':'5432'
    }

    try:
        conn = psycopg2.connect(**conn_config)
        print("Conexão estabelecida")
        return conn
    except psycopg2.Error as e:
        print(f"Erro na conexão: {e}")
        return None
    
def transaction(conn):
    try:
        cur = conn.cursor()

        cur.execute("""CREATE TABLE IF NOT EXISTS filmes (
                        ifilme serial NOT NULL,
                        dados jsonb
                    );""")
        
        filmes = [
            {"titulo": "Filme_A", "generos": ["Curta", "Romance", "Terror"], "publicado": "FALSE"},
            {"titulo": "Filme_B", "generos": ["Marketing", "Auto-ajuda", "Psicologia"], "publicado": "TRUE"},
            {"titulo": "Filme_C", "generos": ["Justiça", "Política"], "autores": ["Ana","Nei"], "publicado" : "TRUE"},
            {"titulo": "Filme_D", "generos": ["Produtividade", "Tecnologia"], "publicado" : "TRUE"},
            {"titulo": "Filme_E", "generos": ["Ficção", "Infanto-juvenil"], "publicado" : "TRUE"},
        ]

        for filme in filmes:
            cur.execute("""INSERT INTO filmes (dados) VALUES (%s);""", (Json(filme),))
        
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
        print("Conexão fechada")
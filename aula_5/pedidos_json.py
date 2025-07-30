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

        cur.execute("""CREATE TABLE IF NOT EXISTS pedidos_json (
                        nrped serial PRIMARY KEY,
                        detalhes json
                    );""")
        
        pedidos = [
            {"cliente": "Nei", "produto": "Produto_1", "qtd": 8, "peso": "1 kg"},
            {"cliente": "Rui", "produto": "Produto_2", "qtd": 9, "peso": "10 kg"},
            {"cliente": "Lia", "produto": "Produto_3", "qtd": 15, "peso": "200 gr"},
            {"cliente": "Lia", "produto": "Produto_4", "qtd": 25, "volume": 2, "cor": "azul"},
            {"cliente": "Ana", "produto": "Produto_5", "qtd": 35, "volume": 3, "cor": "verde"},
            {"cliente": "Lia", "produto": "Produto_6", "qtd": 45, "volume": 4, "cor": "branco"},
            {"cliente": "Rui", "produto": "Produto_7", "qtd": 55, "comprimento": "30 cm"},
            {"cliente": "Lia", "produto": "Produto_8", "qtd": 65, "comprimento": "4 m"},
            {"cliente": "Rui", "produto": "Produto_9", "qtd": 75, "comprimento": "50 mm"},
            {"cliente": "Eli", "produto": "Produto_10", "qtd": 85, "caixa_com": 20},
            {"cliente": "Eli", "produto": "Produto_11", "qtd": 95, "caixa_com": 30},
            {"cliente": "Ana", "produto": "Produto_12", "qtd": 105, "caixa_com": 60}
        ]

        for pedido in pedidos:
            cur.execute("""INSERT INTO pedidos_json (detalhes) VALUES (%s);""", (Json(pedido),))
        
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
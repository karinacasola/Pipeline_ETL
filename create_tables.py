import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """Criando o banco sparkifydb."""
    # conectar ao banco de dados padrão criado no postgres
    conn = psycopg2.connect("host=127.0.0.1 dbname=karinadb user=karinateste password=teste")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # codificação UTF8
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # fechar a conexão com o banco de dados padrão
    conn.close()    
    
    # conectar ao banco de dados sparkify
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=karinateste password=teste")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """Elimine todas as tabelas em sparkifydb."""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Criar tabelas"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Crie nosso banco de dados e certifique-se de que nossas tabelas sejam criadas e vazias."""
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
import os
import glob
import psycopg2
import datetime
import pandas as pd
from sql_queries import *

def insert_song_record(cur, df):
    """inserção dos dados revelevantes das musicas do df e inserindo na tabela música"""
    song_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_data = df.loc[0, song_columns].values.tolist()
    song_data[3] = song_data[3].item() # Converta dtype de numpy.int64 para int
    cur.execute(song_table_insert, song_data)
    
def insert_artist_record(cur, df):
    """Selecione os dados de usuário relevantes do DataFrame e insira-os na tabela de artistas."""
    artist_columns = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artist_data = df.loc[0, artist_columns].values.tolist()
    cur.execute(artist_table_insert, artist_data)

def process_song_file(cur, filepath):
    """converter json da musica em um df e extrair os dados da música na nossa tabela"""
    # abrir o arquivo de som
    df = pd.read_json(filepath, lines=True)

    # inserir registros em tabelas de música e artista
    insert_song_record(cur, df)
    insert_artist_record(cur, df)

def insert_time_records(cur, df):
    "" "Construa um DataFrame de dados de tempo e insira suas linhas na tabela de tempo." ""
    t = pd.to_datetime(df['ts'], unit='ms')
    time_data = (df['ts'].values, t.dt.hour.values, t.dt.day.values, t.dt.week.values, t.dt.month.values, t.dt.year.values, t.dt.weekday.values)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    
    # Converta tuplas em um dicionário para que possam ser convertidas em um DataFrame
    time_dict = dict(zip(column_labels, time_data)) 
    time_df = pd.DataFrame(time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        
def insert_user_records(cur, df):
    """Selecione os dados do usuário relevantes no DataFrame e insira-os na tabela de usuários."""
    user_columns = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_df = df.loc[:, user_columns]
    # user_df = user_df.drop_duplicates(subset='userId')

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        
def insert_songplay_records(cur, df):
    """
    Construindo dados de reprodução para inserir estes registros em nossa tabela que irá reproduzir as músicas.
    """
    for index, row in df.iterrows():
        # obter songid e artistid das tabelas de música e artista
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # inserir o registro de música
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

def process_log_file(cur, filepath):
    """
    conversão do json em df para extrair os dados e filtrar por NextSong
    """
    # abrindo o arquivo log
    df = pd.read_json(filepath, lines=True)

    # Filtro
    df = df[df['page'] == 'NextSong']

    # converter a coluna timestamp p/ datetime
    df.ts = df['ts'].apply(lambda ts: datetime.datetime.fromtimestamp(ts/1000.0))
    
    # inserindo registros
    insert_time_records(cur, df)
    insert_user_records(cur, df)
    insert_songplay_records(cur, df)

def process_data(cur, conn, filepath, func):
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # nº total de arquivos
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterar sobre arquivos e processar
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """Execute nosso pipeline de ETL conectando-se ao nosso banco de dados e processando os arquivos de dados."""
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=karinateste password=teste")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
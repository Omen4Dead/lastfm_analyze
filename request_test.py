import requests
import duckdb
import yaml
import time

# Получение переменных из YAML файла
def get_tokens():
    tokens = None
    with open('.project_config.yaml') as f:
        tokens = yaml.load(f, Loader=yaml.FullLoader)[0]
    return tokens

def config_and_request(tokens, limit, page):
    # URL API и параметры запроса
    api_url = "http://ws.audioscrobbler.com/2.0/"
    params = {
      "method": "user.getrecenttracks",
      "user": tokens["user"],
      "api_key": tokens["api_key"],
      "format": "json",
      "limit": limit,
      "page": page
    }
    # Отправка запроса к API
    response = requests.get(api_url, params=params)
    data = response.json()
    return data

def get_total_str(data):
    return data['recenttracks']['@attr']['total']

def write_data_in_file(data):
    with open('data.json', 'w') as f:
        f.write(str(data))

def create_conn(filename):
    # create a connection to a file called 'file.db'
    conn = duckdb.connect(filename)
    return conn

def create_tmp_table(conn):
    
    # create a table and load data into it
    conn.execute('''DROP TABLE IF EXISTS test''')
    conn.execute('''CREATE TABLE test(track_name varchar,
                                      artist varchar,
                                      album varchar,
                                      link varchar,
                                      dt_listen varchar
                                     )''')

def gather_values_and_ins_into_file(conn, data):
    for i in data['recenttracks']['track']:
        artist = i['artist']['#text']
        album = i['album']['#text']
        name = i['name']
        link = i['url']
        try:
            dt = i['date']['#text']
        except KeyError:
            dt = None
        conn.execute('INSERT INTO test VALUES (?, ?, ?, ?, ?)', 
                     [name, artist, album, link, dt])

def show_data_from_table(conn):
    # query the table
    conn.execute('select count(*) from test')
    print(conn.fetchall())

def close_connection(conn):
    conn.close()

def main():
    db_filename = 'file.db'
    batch_size = 200
    tokens = get_tokens()

    first_str_for_total_info = config_and_request(tokens=tokens, limit=1, page=1)
    total_str = int(get_total_str(data=first_str_for_total_info))
    conn = create_conn(filename=db_filename)

    create_tmp_table(conn=conn)

    for i in range(total_str//batch_size):
        print('Page = ', i)
        show_data_from_table(conn=conn)
        time.sleep(0.5)
        data = config_and_request(tokens=tokens, limit=batch_size, page=i+1)
        write_data_in_file(data=data)
        gather_values_and_ins_into_file(conn=conn, data=data)

    close_connection(conn=conn)

if __name__ == '__main__':
    main()
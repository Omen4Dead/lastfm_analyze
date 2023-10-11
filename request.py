import requests
import duckdb
import yaml

# Получение переменных из YAML файла
tokens = None
with open('.project_config.yaml') as f:
    tokens = yaml.load(f, Loader=yaml.FullLoader)[0]


# URL API и параметры запроса
api_url = "http://ws.audioscrobbler.com/2.0/"
params = {
    "method": "user.getrecenttracks",
    "user": tokens["user"],
    "api_key": tokens["api_key"],
    "format": "json",
    "limit": 10
}

# Отправка запроса к API
response = requests.get(api_url, params=params)
data = response.json()

with open('data.json', 'w') as f:
    f.write(str(data))

# print(data['recenttracks']['track'][1]['artist'])
# print(data['recenttracks']['track'][1]['streamable'])
# print(data['recenttracks']['track'][1]['image'])
# print(data['recenttracks']['track'][1]['album'])
# print(data['recenttracks']['track'][1]['name'])
# print(data['recenttracks']['track'][1]['url'])
#print(data['recenttracks']['track'][1]['date'])

# create a connection to a file called 'file.db'
con = duckdb.connect('file.db')
# create a table and load data into it
con.execute('DROP TABLE IF EXISTS test')
con.execute('''CREATE TABLE test(track_name varchar,
                                 artist varchar,
                                 album varchar,
                                 link varchar
                                )''')

for i in data['recenttracks']['track']:
    artist = i['artist']['#text']
    album = i['album']['#text']
    name = i['name']
    link = i['url']
    con.execute('INSERT INTO test VALUES (?, ?, ?, ?)', [name, artist, album, link])
    #dt = i['date']['#text']

#########################
# query the table
con.table('test').show()
# explicitly close the connection
con.close()
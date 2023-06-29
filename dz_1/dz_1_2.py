import json
import psycopg2 as pg
import pathlib
currentDirectory = pathlib.Path('./small/')
for currentFile in currentDirectory.iterdir():
    
    with open( currentFile,'r', encoding="utf-8") as dz_1: 
        data = json.load(dz_1)

    for item in range(len(data)):

        try:
            for i in range(len(data[item]['data']['СвОКВЭД']['СвОКВЭДДоп'])):

                if data[item]['data']['СвОКВЭД']['СвОКВЭДДоп'][i]['КодОКВЭД'] == '61':


                    conn = pg.connect(database='hw_1',
                      host='localhost',
                      user='postgres',
                      password='asd!!123',
                      port=5432                          )
                    print(1)
                    cursor = conn.cursor()

                    sql_query = ( """ INSERT INTO hw_2_2 (okved, inn, full_name, grn, o, ogrn, kod_ofp, kpp) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""")
                    print(2)
                    record_to_insert = (data[item]['data']['СвОКВЭД']['СвОКВЭДДоп'][i]['КодОКВЭД'],
                                        data[item]['inn'],
                                        data[item]['full_name'],
                                        data[item]['data']['СвОКВЭД']['СвОКВЭДДоп'][0]['ГРНДата']['ГРН'],
                                        data[item]['data']['СпрОПФ'],
                                        data[item]['ogrn'],
                                        data[item]['data']['КодОПФ'],
                                        data[item]['kpp'],

                                       )
                    print(3)
                    cursor.execute(sql_query, record_to_insert)
                    print(4)
                    conn.commit()
                    print(5)
                    cursor.close()
                    print(6)
                    conn.close()

        except KeyError:
            pass

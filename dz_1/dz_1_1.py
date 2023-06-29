import json
import psycopg2 as pg

with open('okved_2.json', 'r', encoding="utf-8") as dz_1: 
    data = json.load(dz_1)
        
    for item in range(len(data)):
        
        conn = pg.connect(database='hw_1',
                      host='localhost',
                      user='postgres',
                      password='asd!!123',
                      port=5432
                      )
    
        cursor = conn.cursor()

        sql_query = ( """ INSERT INTO hw_2 (code, parent_code, section, name, comment) VALUES (%s,%s,%s,%s,%s)""")
        record_to_insert = (data[item]['code'], data[item]['parent_code'], data[item]['section'], data[item]['name'], data[item]['comment'])    
        
        cursor.execute(sql_query, record_to_insert)

        conn.commit()    
        
        cursor.close()
        
        conn.close()

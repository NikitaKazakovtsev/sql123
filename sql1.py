import psycopg2

def create_db(conn):
    cur.execute("""
        DROP TABLE phones;
        DROP TABLE test
        """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS test(
        id SERIAL PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        email TEXT)
        """)
    conn.commit()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS phones(
        phones_id SERIAL PRIMARY KEY,
        id INTEGER NOT NULL REFERENCES test(id),
        number INTEGER);
        """)
    conn.commit()



def add_client(conn, first_name, last_name, email, phones=None):
    cur.execute("""
        INSERT INTO test(first_name,last_name,email)
        VALUES (%s,%s,%s)
        RETURNING id, first_name, last_name, email
        """, (first_name,last_name,email))
    p1 = cur.fetchone()
    if phones != '':
        i1 = phones.split(' ')
        for i in i1:
            cur.execute("""
                INSERT INTO phones(number,id)
                VALUES (%s,%s)
                RETURNING phones_id,id,number
                """, (i,p1[0]))
            print(cur.fetchone())
        


def add_phone(conn, client_id, phones):
    cur.execute("""
        INSERT INTO phones(number,id)
        VALUES (%s,%s)
        RETURNING id,number
        """, (phones, client_id))
    print(cur.fetchone())

def change_client(conn, client_id, first_name, last_name, email, phones=None):
    cur.execute("""
        SELECT first_name, last_name, email FROM test
        WHERE id=%s
        """, (client_id,))
    p1 = cur.fetchone()
    q1 = []
    q2 = []
    q3 = []
    if first_name == '' and last_name == '':
        if email == '':
            q1.append(p1[0])
            q2.append(p1[1])
            q3.append(p1[2])
        else:
            q1.append(p1[0])
            q2.append(p1[1])
            q3.append(email)
    elif first_name == '' and email == '':
        q1.append(p1[0])
        q2.append(last_name)
        q3.append(p1[2])
    elif last_name == '' and email == '':
        q1.append(first_name)
        q2.append(p1[1])
        q3.append(p1[2])
    elif first_name == '':
        q1.append(p1[0])
        q2.append(last_name)
        q3.append(email)
    elif last_name == '':
        q1.append(first_name)
        q2.append(p1[1])
        q3.append(email)
    elif email == '':
        q1.append(first_name)
        q2.append(last_name)
        q3.append((p1[2]))
    else:
        q1.append(first_name)
        q2.append(last_name)
        q3.append(email)
    
    cur.execute("""
        UPDATE test SET first_name=%s,last_name=%s, email=%s  WHERE id=%s
        RETURNING id, first_name, last_name, email
        """, (q1[-1], q2[-1], q3[-1], client_id))
    print(cur.fetchone())
    
    if phones != '':
        cur.execute("""
            INSERT INTO phones(number,id)
            VALUES (%s,%s)
            RETURNING id,number
            """, (phones, client_id))
        print(cur.fetchone())

    cur.execute("""
        SELECT id, first_name, last_name, email  FROM test
        """)
    print(cur.fetchall())
    cur.execute("""
        SELECT phones_id, id, number FROM phones
        """)
    print(cur.fetchall())


def delete_phone(conn, client_id, phones):
    cur.execute("""
        DELETE FROM phones WHERE id=%s and number=%s
        """, (client_id, phones))
    conn.commit()

    cur.execute("""
        SELECT id, number FROM phones
        """)
    print(cur.fetchall())



def delete_client(conn, client_id):
    cur.execute("""
    DELETE FROM phones WHERE id=%s
    """, (client_id,))
    cur.execute("""
    DELETE FROM test WHERE id=%s
    """, (client_id,))
    conn.commit()
    cur.execute("""
        SELECT id, number FROM phones
        """)
    print(cur.fetchall())




def find_client(conn, first_name, last_name, email, phones):
    if first_name == '' and last_name == '' and email == '':
        cur.execute("""
            SELECT t.id FROM test t
            JOIN phones p ON p.id = t.id
            WHERE number=%s
            """, (phones,))
        print(cur.fetchall())
    elif first_name == '' and last_name == '' and phones == '':
        cur.execute("""
            SELECT id FROM test
            WHERE email=%s
            """, (email,))
        print(cur.fetchall())
    elif first_name == '' and email == '' and phones == '':
        cur.execute("""
            SELECT id FROM test
            WHERE last_name=%s
            """, (last_name,))
        print(cur.fetchall())
    elif last_name == '' and email == '' and phones == '':
        cur.execute("""
            SELECT id FROM test
            WHERE first_name=%s
            """, (first_name,))
        print(cur.fetchall())
    elif first_name == '' and last_name == '' :
        cur.execute("""
            SELECT t.id FROM test t
            JOIN phones p ON p.id = t.id
            WHERE email=%s and number=%s
            """, (email,phones))
        print(cur.fetchall())
    elif first_name == '' and email == '' :
        cur.execute("""
            SELECT t.id FROM test t
            JOIN phones p ON p.id = t.id
            WHERE last_name=%s and number=%s
            """, (last_name,phones))
        print(cur.fetchall())
    elif last_name == '' and email == '' :
        cur.execute("""
            SELECT t.id FROM test t
            JOIN phones p ON p.id = t.id
            WHERE first_name=%s and number=%s
            """, (first_name,phones))
        print(cur.fetchall())
    elif first_name == '' and phones == '' :
        cur.execute("""
            SELECT id FROM test 
            WHERE last_name=%s and email=%s
            """, (last_name,email))
        print(cur.fetchall())
    elif last_name == '' and phones == '' :
        cur.execute("""
            SELECT id FROM test
            WHERE first_name=%s and email=%s
            """, (first_name,email))
        print(cur.fetchall())
    elif email == '' and phones == '' :
        cur.execute("""
            SELECT id FROM test
            WHERE first_name=%s and last_name=%s
            """, (first_name,last_name))
        print(cur.fetchall())
    elif phones != '':
        cur.execute("""
            SELECT t.id FROM test t
            JOIN phones p ON p.id = t.id
            WHERE first_name=%s AND last_name=%s AND email=%s AND number=%s 
            """, (first_name,last_name,email,phones))
        print(cur.fetchall())
    elif first_name == '':
        cur.execute("""
            SELECT t.id FROM test t
            JOIN phones p ON p.id = t.id
            WHERE last_name=%s AND email=%s
            HAVING number=%s
            """, (last_name,email,phones))
        print(cur.fetchall())
    elif last_name == '':
        cur.execute("""
            SELECT t.id FROM test t
            JOIN phones p ON p.id = t.id
            WHERE first_name=%s AND email=%s
            HAVING number=%s
            """, (first_name,email,phones))
        print(cur.fetchall())
    elif email == '':
        cur.execute("""
            SELECT t.id FROM test t
            JOIN phones p ON p.id = t.id
            WHERE first_name=%s AND last_name=%s
            HAVING number=%s
            """, (first_name,last_name,phones))
        print(cur.fetchall())



    cur.execute("""
        SELECT id, first_name, last_name, email  FROM test
        """)
    print(cur.fetchall())
    cur.execute("""
        SELECT phones_id, id, number FROM phones
        """)
    print(cur.fetchall())










with psycopg2.connect(database="netology_db", user="postgres", password="postgres") as conn:
    with conn.cursor() as cur:    
        while True:
            comand = input('команда')
            if comand == 'db':
                create_db(conn)
            elif comand == 'ac':
                first_name = input("имя")
                last_name = input("фамилия")
                email = input("email")
                phones = input("номера телефонов через пробел ")
                add_client(conn, first_name, last_name, email, phones)
            elif comand == 'ap':
                client_id = input("id клиента")
                phones = input("номер")
                add_phone(conn, client_id, phones)
            elif comand == 'chc':
                client_id = input()
                first_name = input()
                last_name = input()
                email = input()
                phones = input()
                change_client(conn, client_id, first_name, last_name, email, phones)
            elif comand == 'dp':
                client_id = input()
                phones = input()
                delete_phone(conn, client_id, phones)
            elif comand == 'dc':
                client_id = input()
                delete_client(conn, client_id)
            elif comand == '1':
                first_name = input()
                last_name = input()
                email = input()
                phones = input()
                find_client(conn, first_name, last_name, email, phones)
            break
conn.commit()
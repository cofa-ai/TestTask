import sqlite3
from sqlite3 import Error
import json


JSON_FILE_PATH = "test_data.json" # path to json file


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def create_tables(conn):
    sql_create_human = """ CREATE TABLE IF NOT EXISTS humans (
                                        id integer PRIMARY KEY,
                                        email text NOT NULL,
                                        birthday text,
                                        first_name text,
                                        last_name text
                                    ); """
    sql_create_post = """CREATE TABLE IF NOT EXISTS posts (
                                    id integer PRIMARY KEY,
                                    post_name text NOT NULL,
                                    salary integer,
                                    begin_date text NOT NULL,
                                    end_date text NOT NULL,
                                    human_id integer NOT NULL,
                                    FOREIGN KEY (human_id) REFERENCES humans (id)
                                );"""
    sql_create_document = """CREATE TABLE IF NOT EXISTS documents (
                                    id integer PRIMARY KEY,
                                    document_name text NOT NULL,
                                    issue_date text NOT NULL,
                                    end_date text NOT NULL,
                                    human_id integer NOT NULL,
                                    FOREIGN KEY (human_id) REFERENCES humans (id)
                                );"""                            
    if conn is not None:
        create_table(conn, sql_create_human)
        create_table(conn, sql_create_post)
        create_table(conn, sql_create_document)
    else:
        print("Error! cannot create the database connection.")


def load_json_information_from_file(path):
    with open(path, 'r') as f:
        json_data = json.load(f)
        return json_data


def add_human(conn, human):
    sql = ''' INSERT INTO humans(email,birthday,first_name,last_name)
              VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, human)
    conn.commit()
    return cur.lastrowid


def add_post(conn, post):
    sql = ''' INSERT INTO posts(post_name,salary,begin_date,end_date,human_id)
              VALUES(?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, post)
    conn.commit()
    return cur.lastrowid


def add_document(conn, document):
    sql = ''' INSERT INTO documents(document_name,issue_date,end_date,human_id)
              VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, document)
    conn.commit()
    return cur.lastrowid


def create_humans(conn, humans):
    for human in humans:
        human_info = (human['email'], human['birthday'], human['first_name'], human['last_name'])
        human_id = add_human(conn, human_info)
        post = human['post']
        post_info = (post['post_name'], post['salary'], post['begin_date'], post['end_date'], human_id)
        add_post(conn, post_info)
        document = human['document']
        document_info = (document['document_name'], document['issue_date'], document['end_date'], human_id)
        add_document(conn, document_info)
        print(f"Three objects(human, post, document) was created:\n{json.dumps(human, ensure_ascii=False, indent=4)}")


def exec():
    conn = create_connection('test_task.db')
    create_tables(conn)
    humans = load_json_information_from_file(JSON_FILE_PATH)
    create_humans(conn, humans['humans'])


if __name__ == "__main__":
    exec()

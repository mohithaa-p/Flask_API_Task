import sqlite3
DATABASE="users.db"
def get_connection():
    conn=sqlite3.connect(DATABASE)
    conn.row_factory=sqlite3.Row
    return conn
def init_db():
    conn=get_connection()
    cursor=conn.cursor()
    cursor.execute( """
        create table if not exists users( 
            id integer primary key autoincrement,
            first_name text not null,
            last_name text not null,
            company_name text,
            age integer,
            city text,
            state text,
            zip text,
            email text,
            web text
        )""" )
    conn.commit()
    conn.close()
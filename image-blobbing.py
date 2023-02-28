import cx_Oracle

def output_type_handler(cursor, default_type):
    if default_type == cx_Oracle.DB_TYPE_CLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG, arraysize=cursor.arraysize)
    if default_type == cx_Oracle.DB_TYPE_BLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)

def read_image(filename):
    with open(filename, 'rb') as f:
        return f.read()

def write_image(data, filename):
    with open(filename, 'wb') as f:
        f.write(data)

def establish_db_connection(user, password, dsn, client_path):
    cx_Oracle.init_oracle_client(lib_dir=client_path)
    connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)
    connection.outputtypehandler = output_type_handler
    return connection

def insert_image(connection, image_path, table_name, table_column):
    sql = f"insert into {table_name}({table_column}) values(:clob_data)"
    cursor = connection.cursor()
    cursor.execute(sql, clob_data=read_image(image_path))
    connection.commit()
    cursor.close()

def save_image(connection, table_name, table_column, restriction, filename):
    sql = f"select {table_column} from {table_name} where {restriction}"
    cursor = connection.cursor()
    cursor.execute(sql)
    clob_data = cursor.fetchone()
    write_image(clob_data[0], filename)
    cursor.close()

def close_db_connection(connection):
    connection.close()

if __name__ == '__main__':
    pass

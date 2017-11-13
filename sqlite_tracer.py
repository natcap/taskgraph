import sqlite3

def main():
    db_connection = sqlite3.connect('db_test.db')
    db_cursor = db_connection.cursor()
    db_cursor.execute(
        """CREATE TABLE IF NOT EXISTS task_tokens (hash text)""")

if __name__ == '__main__':
    main()

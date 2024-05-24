import sqlite3
import argparse

# Function to create tables
def create_tables(cursor, unique_user_fields):
    # Create Bank table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Bank (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        )
    ''')

    # Create Transaction table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS "Transaction" (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Bank_sender_name TEXT NOT NULL,
            Account_sender_id INTEGER NOT NULL,
            Bank_receiver_name TEXT NOT NULL,
            Account_receiver_id INTEGER NOT NULL,
            Sent_Currency TEXT NOT NULL,
            Sent_Amount REAL NOT NULL,
            Datetime TEXT
        )
    ''')

    # Create User table
    user_table_sql = '''
        CREATE TABLE IF NOT EXISTS User (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Name TEXT NOT NULL,
            Surname TEXT NOT NULL,
            Birth_day TEXT,
            Accounts TEXT NOT NULL
        ''' + (''',
            UNIQUE (Name, Surname)
        ''' if unique_user_fields else '') + '''
        )
    '''
    cursor.execute(user_table_sql)

    # Create Account table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Account (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            User_id INTEGER NOT NULL,
            Type TEXT NOT NULL CHECK(Type IN ('credit', 'debit')),
            Account_Number TEXT NOT NULL UNIQUE,
            Bank_id INTEGER NOT NULL,
            Currency TEXT NOT NULL,
            Amount REAL NOT NULL,
            Status TEXT CHECK(Status IN ('gold', 'silver', 'platinum')),
            FOREIGN KEY(User_id) REFERENCES User(id),
            FOREIGN KEY(Bank_id) REFERENCES Bank(id)
        )
    ''')

# Main script
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Initial DB setup script.")
    parser.add_argument('--unique-user-fields', action='store_true', help='Enable uniqueness constraint on User.Name and User.Surname')
    args = parser.parse_args()

    # Connect to the database
    conn = sqlite3.connect('task3db.db')
    cursor = conn.cursor()

    # Create tables
    create_tables(cursor, args.unique_user_fields)

    # Commit changes and close connection
    conn.commit()
    conn.close()

    print("Database and tables created successfully.")

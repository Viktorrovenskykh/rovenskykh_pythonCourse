import sqlite3
import logging
import csv
from functools import wraps
from datetime import datetime, timedelta
import re
import requests
import random

logging.basicConfig(level=logging.INFO)


# Decorator for database connection
def db_connection(func):
    @wraps(func)
    def with_connection(*args, **kwargs):
        conn = sqlite3.connect('task5db.db')
        try:
            result = func(conn, *args, **kwargs)
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.error(f"Error: {e}")
            result = {"status": "failure", "message": str(e)}
        finally:
            conn.close()
        return result

    return with_connection


# Validation functions
def validate_full_name(full_name):
    clean_name = re.sub(r'[^a-zA-Z\s]', '', full_name)
    if not clean_name:
        raise ValueError("Invalid full name")
    return clean_name.split()


def validate_account_number(account_number):
    account_number = account_number.replace('#', '-').replace('%', '-').replace('_', '-').replace('?', '-').replace('&',
                                                                                                                    '-')
    if len(account_number) != 18:
        raise ValueError("Account number must be 18 characters")
    if not account_number.startswith("ID--"):
        raise ValueError("Account number must start with 'ID--'")
    pattern = r'ID--[a-zA-Z]{1,3}-\d+-'
    if not re.search(pattern, account_number):
        raise ValueError("Account number must match the pattern ID--xxx-xxxx-")
    return account_number


def validate_strict_values(field, value, allowed_values):
    if value not in allowed_values:
        raise ValueError(f"Not allowed value '{value}' for field '{field}'")


def validate_transaction_datetime(dt):
    if not dt:
        return datetime.now().isoformat()
    return dt


# Functions to add data
@db_connection
def add_user(conn, *user_data):
    cursor = conn.cursor()
    user_tuples = user_data if isinstance(user_data[0], tuple) else [user_data]
    for user in user_tuples:
        try:
            name, surname = validate_full_name(user[0])
            cursor.execute("INSERT INTO User (Name, Surname, Birth_day, Accounts) VALUES (?, ?, ?, ?)",
                           (name, surname, user[1], user[2]))
        except Exception as e:
            return {"status": "failure", "message": str(e)}
    return {"status": "success", "message": "Users added successfully"}


@db_connection
def add_bank(conn, *bank_data):
    cursor = conn.cursor()
    bank_tuples = bank_data if isinstance(bank_data[0], tuple) else [bank_data]
    for bank in bank_tuples:
        try:
            cursor.execute("INSERT INTO Bank (name) VALUES (?)", (bank[0],))
        except Exception as e:
            return {"status": "failure", "message": str(e)}
    return {"status": "success", "message": "Banks added successfully"}


@db_connection
def add_account(conn, *account_data):
    cursor = conn.cursor()
    account_tuples = account_data if isinstance(account_data[0], tuple) else [account_data]
    for account in account_tuples:
        try:
            account_number = validate_account_number(account[2])
            validate_strict_values('Type', account[1], ['credit', 'debit'])
            validate_strict_values('Status', account[6], ['gold', 'silver', 'platinum'])
            cursor.execute(
                "INSERT INTO Account (User_id, Type, Account_Number, Bank_id, Currency, Amount, Status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (account[0], account[1], account_number, account[3], account[4], account[5], account[6]))
        except Exception as e:
            return {"status": "failure", "message": str(e)}
    return {"status": "success", "message": "Accounts added successfully"}


# Functions to add data from CSV
@db_connection
def add_users_from_csv(conn, csv_path):
    try:
        with open(csv_path, mode='r') as file:
            reader = csv.DictReader(file)
            cursor = conn.cursor()
            for row in reader:
                name, surname = validate_full_name(row['user_full_name'])
                cursor.execute("INSERT INTO User (Name, Surname, Birth_day, Accounts) VALUES (?, ?, ?, ?)",
                               (name, surname, row['birth_day'], row['accounts']))
        return {"status": "success", "message": "Users added successfully from CSV"}
    except Exception as e:
        return {"status": "failure", "message": str(e)}


# Functions to modify and delete data
@db_connection
def modify_user(conn, user_id, **kwargs):
    cursor = conn.cursor()
    fields = ', '.join(f"{key} = ?" for key in kwargs.keys())
    values = list(kwargs.values()) + [user_id]
    try:
        cursor.execute(f"UPDATE User SET {fields} WHERE id = ?", values)
        return {"status": "success", "message": "User updated successfully"}
    except Exception as e:
        return {"status": "failure", "message": str(e)}


@db_connection
def delete_user(conn, user_id):
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM User WHERE id = ?", (user_id,))
        return {"status": "success", "message": "User deleted successfully"}
    except Exception as e:
        return {"status": "failure", "message": str(e)}


# Money transfer function
def get_exchange_rate(from_currency, to_currency):
    api_key = 'your_api_key_here'
    url = f"https://api.freecurrencyapi.com/v1/latest?apikey={api_key}&base_currency={from_currency}&currencies={to_currency}"
    response = requests.get(url)
    if response.status_code == 200:
        rates = response.json()['data']
        return rates[to_currency]
    else:
        raise Exception("Failed to fetch exchange rate")


@db_connection
def transfer_money(conn, sender_account_number, receiver_account_number, amount):
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT Amount, Currency, User_id FROM Account WHERE Account_Number = ?",
                       (sender_account_number,))
        sender = cursor.fetchone()
        if not sender:
            return {"status": "failure", "message": "Sender account not found"}

        sender_balance, sender_currency, sender_user_id = sender

        if sender_balance < amount:
            return {"status": "failure", "message": "Insufficient balance"}

        cursor.execute("SELECT Amount, Currency FROM Account WHERE Account_Number = ?", (receiver_account_number,))
        receiver = cursor.fetchone()
        if not receiver:
            return {"status": "failure", "message": "Receiver account not found"}

        receiver_balance, receiver_currency = receiver

        if sender_currency != receiver_currency:
            exchange_rate = get_exchange_rate(sender_currency, receiver_currency)
            converted_amount = amount * exchange_rate
        else:
            converted_amount = amount

        cursor.execute("UPDATE Account SET Amount = Amount - ? WHERE Account_Number = ?",
                       (amount, sender_account_number))
        cursor.execute("UPDATE Account SET Amount = Amount + ? WHERE Account_Number = ?",
                       (converted_amount, receiver_account_number))

        cursor.execute("""
            INSERT INTO "Transaction" (Bank_sender_name, Account_sender_id, Bank_receiver_name, Account_receiver_id, Sent_Currency, Sent_Amount, Datetime) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, ("SenderBank", sender_user_id, "ReceiverBank", receiver_account_number, sender_currency, amount,
              datetime.now().isoformat()))

        return {"status": "success", "message": "Transaction completed successfully"}
    except Exception as e:
        return {"status": "failure", "message": str(e)}


# New functionalities

@db_connection
def assign_random_discounts(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM User")
        users = cursor.fetchall()
        selected_users = random.sample(users, k=min(len(users), random.randint(1, 10)))
        discounts = [25, 30, 50]
        user_discounts = {user_id[0]: random.choice(discounts) for user_id in selected_users}

        for user_id, discount in user_discounts.items():
            cursor.execute("UPDATE Account SET Discount = ? WHERE User_id = ?", (discount, user_id))

        return {"status": "success", "message": "Discounts assigned successfully", "data": user_discounts}
    except Exception as e:
        return {"status": "failure", "message": str(e)}


@db_connection
def users_with_debts(conn):
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT Name, Surname FROM User INNER JOIN Account ON User.id = Account.User_id WHERE Account.Amount < 0")
        users = cursor.fetchall()
        full_names = [f"{name} {surname}" for name, surname in users]
        return {"status": "success", "message": "Users with debts fetched successfully", "data": full_names}
    except Exception as e:
        return {"status": "failure", "message": str(e)}


@db_connection
def bank_with_biggest_capital(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT Bank.name, SUM(Account.Amount) as total_capital
            FROM Bank
            INNER JOIN Account ON Bank.id = Account.Bank_id
            GROUP BY Bank.name
            ORDER BY total_capital DESC
            LIMIT 1
        """)
        bank = cursor.fetchone()
        return {"status": "success", "message": "Bank with biggest capital fetched successfully", "data": bank}
    except Exception as e:
        return {"status": "failure", "message": str(e)}


@db_connection
def bank_with_oldest_client(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT Bank.name, MIN(User.Birth_day) as oldest_birth_day
            FROM Bank
            INNER JOIN Account ON Bank.id = Account.Bank_id
            INNER JOIN User ON User.id = Account.User_id
            GROUP BY Bank.name
            ORDER BY oldest_birth_day ASC
            LIMIT 1
        """)
        bank = cursor.fetchone()
        return {"status": "success", "message": "Bank with oldest client fetched successfully", "data": bank}
    except Exception as e:
        return {"status": "failure", "message": str(e)}


@db_connection
def bank_with_most_unique_users_outbound(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT Bank.name, COUNT(DISTINCT Transaction.Account_sender_id) as unique_users
            FROM Bank
            INNER JOIN Account ON Bank.id = Account.Bank_id
            INNER JOIN Transaction ON Account.Account_Number = Transaction.Account_sender_id
            GROUP BY Bank.name
            ORDER BY unique_users DESC
            LIMIT 1
        """)
        bank = cursor.fetchone()
        return {"status": "success", "message": "Bank with most unique users outbound fetched successfully",
                "data": bank}
    except Exception as e:
        return {"status": "failure", "message": str(e)}


@db_connection
def delete_incomplete_users_and_accounts(conn):
    cursor = conn.cursor()
    try:
        cursor.execute(
            "DELETE FROM User WHERE Name IS NULL OR Surname IS NULL OR Birth_day IS NULL OR Accounts IS NULL")
        cursor.execute(
            "DELETE FROM Account WHERE User_id IS NULL OR Type IS NULL OR Account_Number IS NULL OR Bank_id IS NULL OR Currency IS NULL OR Amount IS NULL OR Status IS NULL")
        return {"status": "success", "message": "Incomplete users and accounts deleted successfully"}
    except Exception as e:
        return {"status": "failure", "message": str(e)}


@db_connection
def user_transactions_last_3_months(conn, user_id):
    cursor = conn.cursor()
    try:
        three_months_ago = datetime.now() - timedelta(days=90)
        cursor.execute("""
            SELECT * FROM Transaction 
            WHERE Account_sender_id IN (SELECT Account_Number FROM Account WHERE User_id = ?)
            AND Datetime >= ?
        """, (user_id, three_months_ago.isoformat()))
        transactions = cursor.fetchall()
        return {"status": "success", "message": "User transactions for last 3 months fetched successfully",
                "data": transactions}
    except Exception as e:
        return {"status": "failure", "message": str(e)}

import logging
from task5_1 import (
    add_user, add_bank, add_account, modify_user, delete_user, transfer_money,
    assign_random_discounts, users_with_debts, bank_with_biggest_capital,
    bank_with_oldest_client, bank_with_most_unique_users_outbound,
    delete_incomplete_users_and_accounts, user_transactions_last_3_months
)

logging.basicConfig(level=logging.INFO)

def main():
    try:
        logging.info(assign_random_discounts())
    except Exception as e:
        logging.error(f"Error in assigning discounts: {e}")

    try:
        logging.info(users_with_debts())
    except Exception as e:
        logging.error(f"Error in fetching users with debts: {e}")

    try:
        logging.info(bank_with_biggest_capital())
    except Exception as e:
        logging.error(f"Error in fetching bank with biggest capital: {e}")

    try:
        logging.info(bank_with_oldest_client())
    except Exception as e:
        logging.error(f"Error in fetching bank with oldest client: {e}")

    try:
        logging.info(bank_with_most_unique_users_outbound())
    except Exception as e:
        logging.error(f"Error in fetching bank with most unique users outbound: {e}")

    try:
        logging.info(delete_incomplete_users_and_accounts())
    except Exception as e:
        logging.error(f"Error in deleting incomplete users and accounts: {e}")

    try:
        user_id = 1  # Replace with an actual user ID
        logging.info(user_transactions_last_3_months(user_id))
    except Exception as e:
        logging.error(f"Error in fetching user transactions: {e}")

if __name__ == "__main__":
    main()

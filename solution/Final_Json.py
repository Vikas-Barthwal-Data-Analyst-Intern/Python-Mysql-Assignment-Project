import click
import pandas as pd
import json
from typing import Dict, Set, List
import logging
from os import makedirs

from pre_process_data import get_df, get_sub_dirs, get_transactions, process_transaction

logging.basicConfig(level=logging.INFO)
logger: logging.Logger = logging.getLogger(__name__)


@click.command()
@click.option('--customers_location', required=False, default="\\input_data\\starter\\customers.csv")
@click.option('--products_location', required=False, default="\\input_data\\starter\\products.csv")
@click.option('--transactions_location', required=False, default="\\input_data\\starter\\transactions")
@click.option('--output_location', required=False, default="\\desired_data")
@click.option('--root_dir', required=False, default="C:\\Users\\banti\\Desktop\\Assignment-Project")
def main(root_dir, customers_location: str, products_location: str, transactions_location: str, output_location: str):

    # Get customers as a dataframe from csv
    customer_df: pd.DataFrame = get_df(f"{root_dir}/{customers_location}")

    # Get products as a dataframe from csv
    product_df: pd.DataFrame = get_df(f"{root_dir}/{products_location}")

    output_json = {
        "customers": []
    }
    enc_customers: Set[str] = set()  # A set to keep a track of customers encountered while going through transactions

    transactions_dir_location = f"{root_dir}/{transactions_location}"
    sub_dirs: List[str] = get_sub_dirs(transactions_dir_location)

    for transaction_dir in sub_dirs:
        logger.info(f"Processing the transaction: {transaction_dir}")
        transactions = get_transactions(f"{transactions_dir_location}/{transaction_dir}")
        for transaction in transactions:
            output_json, enc_customers = process_transaction(customer_df, product_df, transaction, output_json, enc_customers)

    makedirs(output_location, exist_ok=True)
    with open(f"{root_dir}/{output_location}/output.json", "w") as f:
        json.dump(output_json, f)


if __name__ == "__main__":
    main()

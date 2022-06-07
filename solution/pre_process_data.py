import json

import pandas as pd
from os import listdir
from typing import Dict, List, Set, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger: logging.Logger = logging.getLogger(__name__)


def get_df(csv_path: str) -> pd.DataFrame:
    """
    Return pandas dataframe from the csv path

    :param csv_path: path to the CSV file
    :return df: pandas dataframe object for the CSV
    """
    df = pd.read_csv(csv_path)
    return df


def get_sub_dirs(main_dir_path: str) -> List[str]:
    """
    Return the subdirectories given a directory path

    :param main_dir_path: path for the main directory
    :return: list of subdirectory names
    """
    return listdir(main_dir_path)


def get_transactions(transaction_dir: str) -> List[Dict]:
    """
    Reads the transactions json file in the directory and returns the list of transactions as dictionaries

    :param transaction_dir: path to the transaction directory which should contain a transactions.json file
    :return transactions: a list with each element as a single transaction dictionary
    """
    transactions: List[Dict] = list()
    try:
        with open(f"{transaction_dir}/transactions.json") as f:
            lines = f.readlines()

        for line in lines:
            transactions.append(json.loads(line))
    except FileNotFoundError:
        logger.info(f"No transaction file found for the directory: {transaction_dir}")

    return transactions


def get_loyalty_score(customer_df: pd.DataFrame, customer_id: str) -> int:
    """
    Returns the loyalty score for the given pandas dataframe and matched customer ID
    :param customer_df: pandas dataframe for customers csv
    :param customer_id: customer ID
    :return loyalty_score: integer value for the loyalty score fetched from pandas query
    """

    loyalty_score = None
    try:
        loyalty_score = int(customer_df[customer_df["customer_id"] == customer_id]["loyalty_score"].values[0])
    except IndexError:
        logger.info(f"Couldn't fetch the loyalty score for customer:{customer_id}")
    return loyalty_score


def get_product_ids(transaction_products: List[Dict]) -> List[str]:
    """
    Returns the product IDs from the products dictionary
    :param transaction_products: List of product dictionaries in a single transaction
    :return product_ids: Product IDs as a list
    """
    product_ids: List[str] = list()

    for product in transaction_products:
        product_ids.append(product["product_id"])

    return product_ids


def get_product_category(product_df: pd.DataFrame, product_id: str) -> str:
    """
    Returns the product category for the given pandas dataframe and matched customer ID
    :param product_df: pandas dataframe for products csv
    :param product_id: product ID for which category needs to be fetched
    :return:
    """
    product_category = ""
    try:
        product_category = product_df[product_df["product_id"] == product_id]["product_category"].values[0]
    except IndexError:
        logger.info(f"Couldn't fetch the product category for product:{product_id}")
    return product_category


def process_product_info(product_df: pd.DataFrame, transaction_products: List[Dict], products: List[Dict]) -> List[Dict]:
    """
    Returns the updated products as list of dictionaries after reading a single transaction
    :param product_df: pandas dataframe for products csv
    :param transaction_products: list of dictionaries for a single transaction
    :param products: products in the existing output json(if any) for the given customer ID
    :return products: Updated products list
    """
    if products:  # If products info already exist then update
        out_product_ids = get_product_ids(products)

        for product in transaction_products:
            if product["product_id"] in out_product_ids:
                index_val = out_product_ids.index(product["product_id"])
                products[index_val]["purchase_count"] +=1
            else:
                product_category: str = get_product_category(product_df, product["product_id"])
                product_json = {
                    "product_id": product["product_id"],
                    "product_category": product_category,
                    "purchase_count": 1
                }
                products.append(product_json)
    else:
        transaction_product_ids: List = get_product_ids(transaction_products)
        for product_id in transaction_product_ids:
            product_category: str = get_product_category(product_df, product_id)
            product_json = {
                "product_id": product_id,
                "product_category": product_category,
                "purchase_count": 0
            }
            products.append(product_json)

    return products


def lookup_products_info(out_json: Dict, customer_id: str) -> List[Dict]:
    """
    Return products for a given customer_id which is already present in the output json generated so far
    since a transaction involving the same customer has been processed before
    :param out_json: output json which has been generated so far
    :param customer_id: customer ID for which products needs to be fetched
    :return products: returns products for the customer ID as a list of dict
    """
    products: List[Dict] = list()

    for customer in out_json["customers"]:
        if customer["customer_id"] == customer_id:
            products = customer["products"]
            break
    return products


def process_transaction(customer_df: pd.DataFrame, product_df: pd.DataFrame, transaction: Dict, out_json: Dict, enc_customers: Set[str]) -> Tuple[Dict, Set[str]]:
    """
    Process a single transaction and update/add info in the output json accordingly
    :param customer_df: pandas dataframe for customers CSV
    :param product_df: pandas dataframe for products CSV
    :param transaction: a single transaction as dictionary which needs to be processed
    :param out_json: output json which has been generated so far
    :param enc_customers: list of customers IDs which have already been discovered so far while processing the transactions one by one
    :return out_json, enc_customers: Returns updated output json as Dict and customers discovered as a list
    """
    customer_json = {
        "customer_id": None,
        "loyalty_score": None,
        "products": []
    }
    products: List[Dict] = list()

    logger.info(f"Processing transaction:{transaction}")

    customer_id: str = transaction["customer_id"]
    if customer_id not in enc_customers:
        customer_json["customer_id"] = customer_id
        loyalty_score: int = get_loyalty_score(customer_df, customer_id)
        customer_json["loyalty_score"] = loyalty_score

        products: List[Dict] = process_product_info(product_df, transaction["basket"], products)
        customer_json["products"].extend(products)

        out_json["customers"].append(customer_json)
        enc_customers.add(customer_id)

    else:  # Customer already present in output json
        products: List[Dict] = lookup_products_info(out_json, customer_id)
        products: List[Dict] = process_product_info(product_df, transaction["basket"], products)
        out_json["products"] = products

    return out_json, enc_customers

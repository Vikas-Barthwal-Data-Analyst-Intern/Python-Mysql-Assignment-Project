import pytest
import pandas as pd
from typing import List, Dict

from pre_process_data import get_df, get_transactions, get_sub_dirs, get_product_ids, get_product_category, \
    get_loyalty_score, process_transaction, process_product_info, lookup_products_info


@pytest.fixture
def example_customers_df() -> pd.DataFrame:
    df_path = "C:\\Users\\banti\\Desktop\\Assignment-Project\\input_data\\starter\\customers.csv"
    return pd.read_csv(df_path)


@pytest.fixture
def example_products_df() -> pd.DataFrame:
    df_path = "C:\\Users\\banti\\Desktop\\Assignment-Project\\input_data\\starter\\products.csv"
    return pd.read_csv(df_path)


@pytest.fixture
def example_transaction_products() -> List[Dict]:
    return [{"product_id": "P31", "price": 1828}, {"product_id": "P27", "price": 835}]


def test_get_df(example_customers_df):
    df_path = "C:\\Users\\banti\\Desktop\\Assignment-Project\\input_data\\starter\\customers.csv"
    assert type(get_df(df_path)) == pd.DataFrame


def test_get_sub_dirs():
    dir_path = "C:\\Users\\banti\\Desktop\\Assignment-Project\\input_data\\starter\\transactions"
    assert (type(get_sub_dirs(dir_path)) == list) and (len(get_sub_dirs(dir_path)) != 0)


def test_get_transactions():
    json_dir_path = "C:\\Users\\banti\\Desktop\\Assignment-Project/input_data/starter/transactions/d=2018-12-01"
    assert (type(get_transactions(json_dir_path)) == list) and (len(get_transactions(json_dir_path)) != 0)


def test_get_loyalty_score(example_customers_df):
    assert get_loyalty_score(example_customers_df, "C1") == 7


def test_get_loyalty_score_neg(example_customers_df):
    assert not get_loyalty_score(example_customers_df, "C1456") == 7


def test_get_product_ids(example_transaction_products):
    assert get_product_ids(example_transaction_products) == ["P31", "P27"]


def test_get_product_category(example_products_df):
    assert get_product_category(example_products_df, "P01") == "house"


def test_get_product_category_neg(example_products_df):
    assert not get_product_category(example_products_df, "P5421") == "house"


def test_process_product_info(example_products_df, example_transaction_products):
    products = [{"product_id": "P31", "product_category": "fruit_veg", "purchase_count": 1}]
    updated_products = process_product_info(example_products_df, example_transaction_products, products)
    assert (type(updated_products) == list) and (updated_products == [
        {
            "product_id": "P31",
            "product_category": "fruit_veg",
            "purchase_count": 2
        },
        {
            "product_id": "P27",
            "product_category": "fruit_veg",
            "purchase_count": 1
        }
    ]
                                                 )


def test_lookup_products_info():
    output_json = {"customers": [{"customer_id": "C6", "loyalty_score": 10, "products": [{"product_id": "P31", "product_category": "fruit_veg", "purchase_count": 2}, {"product_id": "P27", "product_category": "fruit_veg", "purchase_count": 1}]}]}
    products = lookup_products_info(output_json, "C6")
    assert (type(products) == list) and (products == [{"product_id": "P31", "product_category": "fruit_veg", "purchase_count": 2}, {"product_id": "P27", "product_category": "fruit_veg", "purchase_count": 1}])

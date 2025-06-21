# Extracting source tables
from airflow import DAG
import pandas as pd
import datetime
from datetime import datetime, timedelta
import numpy as np
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
import os


def extract():

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    print(BASE_DIR)
    file_path = {
        "cust_info": os.path.join(BASE_DIR, 'datasets/source_crm/cust_info.csv'),
        "prod_info": os.path.join(BASE_DIR, 'datasets/source_crm/prd_info.csv'),
        "sales_info": os.path.join(BASE_DIR, 'datasets/source_crm/sales_details.csv'),
        "cust_az": os.path.join(BASE_DIR, 'datasets/source_erp/CUST_AZ12.csv'),
        "loc_a1": os.path.join(BASE_DIR, 'datasets/source_erp/LOC_A101.csv'),
        "px_cat": os.path.join(BASE_DIR, 'datasets/source_erp/PX_CAT_G1V2.csv')
    }
    for key, path in file_path.items():
        if not os.path.exists(path):
                print(f"{path} not found")
        else:
                print(f"{key} successfully found")
                data = pd.read_csv(path)
                data.to_csv(os.path.join(BASE_DIR, f'datasets/raw/{key}.csv'), index=False)
        



# Tranforming Source tables
Today = pd.to_datetime(datetime.today())


def transform():
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_dir = os.path.join(BASE_DIR, 'datasets/raw')
    clean_dir = os.path.join(BASE_DIR, 'datasets/clean')
    os.makedirs(clean_dir, exist_ok=True)

    cust_info = pd.read_csv(os.path.join(raw_dir, 'cust_info.csv'))
    prod_info = pd.read_csv(os.path.join(raw_dir, 'prd_info.csv'))
    sales_info = pd.read_csv(os.path.join(raw_dir, 'sales_info.csv'))
    cust_az = pd.read_csv(os.path.join(raw_dir, 'cust_az.csv'))
    loc_a1 = pd.read_csv(os.path.join(raw_dir, 'loc_a1.csv'))
    px_cat = pd.read_csv(os.path.join(raw_dir, 'px_cat.csv'))

    cust_info = cust_info.dropna(subset=["cst_id", "cst_key"])
    cust_info = cust_info.drop_duplicates(subset=["cst_id", "cst_key"])
    cust_info["cst_id"] = cust_info["cst_id"].astype("int")
    cust_info["cst_firstname"] = cust_info["cst_firstname"].str.strip()
    cust_info["cst_firstname"] = cust_info["cst_firstname"].fillna("Unknown")
    cust_info["cst_lastname"] = cust_info["cst_lastname"].fillna("Unknown")
    cust_info["cst_marital_status"] = cust_info["cst_marital_status"].fillna(
        "Unknown")
    map = {"M": "Married", "S": "Single"}
    cust_info["cst_marital_status"] = cust_info["cst_marital_status"].replace(
        map)
    cust_info["cst_marital_status"] = cust_info["cst_marital_status"].astype(
        "category")
    cust_info["cst_gndr"] = cust_info["cst_gndr"].fillna("Unknown")
    map1 = {"M": "Male",
            "F": "Female"}
    cust_info["cst_gndr"] = cust_info["cst_gndr"].replace(map1)
    cust_info["cst_gndr"] = cust_info["cst_gndr"].astype("category")
    cust_info["cst_create_date"] = pd.to_datetime(
        cust_info["cst_create_date"], errors="coerce")

    prod_info.columns = prod_info.columns.str.strip()
    prod_info = prod_info.dropna(subset=["prd_id", "prd_key", "prd_start_dt"])
    prod_info["prd_cost"] = prod_info["prd_cost"].fillna(0)
    prod_info.loc[prod_info["prd_cost"] < 0, "prd_cost"] = 0
    prod_info["prd_line"] = prod_info["prd_line"].str.strip()
    map2 = {"R": "Road", "M": "Mountain",
            "S": "Street", "T": "Trail"}
    prod_info["prd_line"] = prod_info["prd_line"].fillna("Unknown")
    prod_info["prd_line"] = prod_info["prd_line"].replace(map2)
    prod_info["prd_line"] = prod_info["prd_line"].astype("category")
    prod_info["prd_start_dt"] = pd.to_datetime(
        prod_info["prd_start_dt"], errors="coerce")
    prod_info["prd_end_dt"] = pd.to_datetime(
        prod_info["prd_end_dt"], errors="coerce")

    sales_info["sls_ship_dt"] = pd.to_datetime(
        sales_info["sls_ship_dt"], errors="coerce")
    sales_info["sls_due_dt"] = pd.to_datetime(
        sales_info["sls_due_dt"], errors="coerce")
    sales_info["sls_order_dt"] = pd.to_datetime(
        sales_info["sls_order_dt"], errors="coerce")
    sales_info.loc[sales_info["sls_ship_dt"] > Today, "sls_ship_dt"] = Today
    sales_info.loc[sales_info["sls_order_dt"] > Today, "sls_order_dt"] = Today
    sales_info.loc[sales_info["sls_sales"] < 0, "sls_sales"] = 0
    sales_info.loc[sales_info["sls_quantity"] < 0, "sls_quantity"] = 0
    sales_info.loc[sales_info["sls_price"] < 0, "sls_price"] = 0

    cust_az.columns = cust_az.columns.str.lower()
    cust_az["bdate"] = pd.to_datetime(cust_az["bdate"], errors="coerce")
    cust_az["gen"] = cust_az["gen"].str.strip()
    cust_az["gen"] = cust_az["gen"].str.upper()
    cust_az["gen"] = cust_az["gen"].fillna("Unknown")
    map3 = {"MALE": "M",
            "FEMALE": "F"}
    cust_az["gen"] = cust_az["gen"].replace(map3)
    map4 = {"M": "Male", "F": "Female"}
    cust_az["gen"] = cust_az["gen"].replace(map4)
    cust_az["gen"] = cust_az["gen"].astype("category")

    loc_a1.columns = loc_a1.columns.str.lower().str.strip()
    loc_a1["cntry"] = loc_a1["cntry"].str.lower().str.strip()
    map5 = {"australia": "Australia",
            "us": "United States",
            "united states": "United States",
            "usa": "United States",
            "uk": "United Kingdom",
            "united kingdom": "United Kingdom",
            "fra": "France",
            "france": "France",
            "can": "Canada",
            "canada": "Canada",
            "germany": "Germany",
            "de": "Denmark",
            "denmark": "Denmark"}
    loc_a1["cntry"] = loc_a1["cntry"].replace(map5)
    loc_a1["cntry"] = loc_a1["cntry"].fillna("Others")
    loc_a1["cntry"] = loc_a1["cntry"].astype("category")

    px_cat.columns = px_cat.columns.str.strip().str.lower()
    px_cat["cat"] = px_cat["cat"].astype("category")
    px_cat["subcat"] = px_cat["subcat"].astype("category")
    px_cat["maintenance"] = px_cat["maintenance"].astype("category")

    cust_info.to_csv(os.path.join(clean_dir, 'cust_info_clean.csv'), index=False)
    prod_info.to_csv(os.path.join(clean_dir, 'prod_info_clean.csv'), index=False)
    sales_info.to_csv(os.path.join(clean_dir, 'sales_info_clean.csv'), index=False)
    cust_az.to_csv(os.path.join(clean_dir, 'cust_az_clean.csv'), index=False)
    loc_a1.to_csv(os.path.join(clean_dir, 'loc_a1_clean.csv'), index=False)
    px_cat.to_csv(os.path.join(clean_dir, 'px_cat_clean.csv'), index=False)

    print("Succesfully transformed and cleaned the data")

def load():
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        clean_dir = os.path.join(BASE_DIR, 'datasets/clean')

        output_dir = os.path.join(BASE_DIR, 'datasets/final')
        os.makedirs(output_dir, exist_ok=True)

        cust_info = pd.read_csv(os.path.join(clean_dir, 'cust_info_clean.csv'))
        prod_info = pd.read_csv(os.path.join(clean_dir, 'prod_info_clean.csv'))
        sales_info = pd.read_csv(os.path.join(clean_dir, 'sales_info_clean.csv'))
        cust_az = pd.read_csv(os.path.join(clean_dir, 'cust_az_clean.csv'))
        loc_a1 = pd.read_csv(os.path.join(clean_dir, 'loc_a1_clean.csv'))
        px_cat = pd.read_csv(os.path.join(clean_dir, 'px_cat_clean.csv'))

        cust_info.to_csv(os.path.join(output_dir, 'cust_info_clean.csv'), index=False)
        prod_info.to_csv(os.path.join(output_dir, 'prod_info_clean.csv'), index=False)
        sales_info.to_csv(os.path.join(output_dir, 'sales_info_clean.csv'), index=False)
        cust_az.to_csv(os.path.join(output_dir, 'cust_az_clean.csv'), index=False)
        loc_a1.to_csv(os.path.join(output_dir, 'loc_a1_clean.csv'), index=False)
        px_cat.to_csv(os.path.join(output_dir, 'px_cat_clean.csv'), index=False)

        return 'Data saved to CSV files'


with DAG(dag_id="crm-erp",
         start_date=datetime(2025, 6, 17),
         schedule="@daily",
         catchup=False) as dag:
    task_1 = BashOperator(task_id="task_1",
                          bash_command="echo 'Extracting data from source tables'")
    task_2 = PythonOperator(task_id="task_2",
                            python_callable=extract)
    task_3 = PythonOperator(task_id="task_3",
                            python_callable=transform)
    task_4 = PythonOperator(task_id = "task_4",
                            python_callable = load)
    task_1 >> task_2 >> task_3 >> task_4

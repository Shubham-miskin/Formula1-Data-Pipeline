# Databricks notebook source
bronze_folder_path = "abfss://bronze@datalakework1.dfs.core.windows.net"
silver_folder_path = "abfss://silver@datalakework1.dfs.core.windows.net"
gold_folder_path = "abfss://gold@datalakework1.dfs.core.windows.net"
storage_account_name = "datalakework1"

storage_account_access_key = dbutils.secrets.get(scope="storageaccount", key="storageaccountkey")
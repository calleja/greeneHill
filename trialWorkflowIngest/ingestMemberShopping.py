from trialWorkflowIngest import shop_log
import pandas as pd

shop = pd.read_csv(shop_log)

shop.columns = [i.replace(' ','_') for i in shop.columns]

shop['Activity_Date'] = pd.to_datetime(shop['Activity_Date'], format = "%Y-%m-%d %H:%M")

shop['ingest_date'] = datetime.datetime.today()

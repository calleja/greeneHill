#from trialWorkflowIngest import shop_log
import pandas as pd
import datetime

# "Constituent Summary" <- CIVI report name
def process_contacts(member_file):
    members = pd.read_csv(member_file)
    members.columns = [i.replace(' ','_').lower() for i in members.columns]

    return(members)


# "Shopping Log - Activity Report" <- CIVI report name
def process_shoplog(shop_log_file):
    shop = pd.read_csv(shop_log_file)
    shop.columns = [i.replace(' ','_') for i in shop.columns]
    shop['Activity_Date'] = pd.to_datetime(shop['Activity_Date'], format = "%Y-%m-%d %H:%M")
    shop['ingest_date'] = datetime.datetime.today()

    return(shop)
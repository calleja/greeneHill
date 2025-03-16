#!/usr/bin/env python
# coding: utf-8

# CIVI Report location: https://plum.greenehillfood.coop/civicrm/report/instance/101?reset=1&output=criteria <br> Select Activity Details - Membership Related

'''
name collision concern: You can avoid name collisions in your programs by properly using the local Python scope. This also makes functions more self-contained and creates maintainable program units. Additionally, since you can’t change local names from remote places in your code, your programs will be easier to debug, read, and modify.

You can inspect the names and parameters of a function using .__code__, which is an attribute that holds information on the function’s internal code. 
'''


import os
import pandas as pd
import numpy as np
import re
import datetime

#module-wide variables


def remove_controller(activityReport_version):
    #funct intake_file() creates a smaller df from the larger one that subsets for all cases where there are multiple records having the same set of ['Target_Email_act','Activity_Date_DT_act','Activity_Type_act']
    df_grouped_output = intake_file(activityReport_version)
    if df_grouped_output is None:
        print('the result from intake_file function is empty')
        return(activityReport_version) #sending back original file and killing any further processes
    else:
        #apply_rules filters for the rows of the DF to keep
        inclusion_df = apply_rules(df_grouped_output)

    if inclusion_df is None:
        print('inclusion_df - a variable in the apply_rules funct is empty, and I cannot proceed')
    else:
        activityReport_scrubbed = apply_exclusion(inclusion_df,df_grouped_output,activityReport_version)

    if activityReport_scrubbed is None:
        print('final df - the output of apply_exclusion function is empty')
    else:
        return activityReport_scrubbed


def intake_file(activityReport):

    #I removed some data munging here, as this should be done in another module or the calling ipynb; munging removed denoted w/ "*"

    #activityReport.columns = [i.replace(' ','_')+'_act' for i in list(activityReport.columns)]*

    #trim all string fields
    #strip all whitespace from each cell
    #activityReport = activityReport.map(lambda x: x.strip() if isinstance(x,str) else x) *

    #NOTE: Activity_Date_act field DOES NOT provide seconds
    #activityReport = activityReport.assign(Activity_Date_DT_act = pd.to_datetime(activityReport['Activity_Date_act'], format = '%Y-%m-%d %H:%M')) *

    try:
        activityReport = activityReport.drop_duplicates(ignore_index = True, subset = ['Activity_Type_act', 'Subject_act', 'Activity_Date_act', 'Activity_Status_act', 'Activity_Date_DT_act','Target_Email_act'])

        # ### SELECT BEST TRANSACTION LOGIC
        #NOTE: real dupes have multiple entries on fields: 'Target_Email_act','Activity_Date_DT_act','Activity_Type_act'
        #NOTE 'Activity_Type_act' = specifies if change is for status or type
        # looks like the index is preserved on a groupby
        df_grouped = activityReport.groupby(['Target_Email_act','Activity_Date_DT_act','Activity_Type_act']).filter(lambda x: len(x) > 1)
        

    except ValueError as e:
        raise ValueError(f'fields in this version of the dataframe do not coincide with the code in civiActivityReport_selectBestTrans.py: {e}')


    #insert a circuit breaker if there are no cases caught in df_grouped; if df_grouped length = 0 then this will break the program of civiActivityReport
    if len(df_grouped) == 0:
        print('pitching back the original dataframe') #accomplish this by introducing None in the control flow
        return None
    else:
        pass

    #assign a row value to ea group member: used later for selection
    df_grouped['count'] = df_grouped.groupby(['Target_Email_act','Activity_Date_DT_act','Activity_Type_act']).cumcount()+1


    # Issues to handle:
    # - multiple records for different versions of Target_Name_act (yet Target_Email_act is the same) <- will need to check that this doesn't delete Family account members
    # - records made by different Source_Email_act ie systematic records made by CIVI

    df_grouped['from'] = df_grouped['Subject_act'].str.extract(r'from\s(\w+)') #str.extract(r'from\s(\w+)')
    df_grouped['to'] = df_grouped['Subject_act'].str.extract(r'to\s(\w+)') #str.extract(r'to\s(\w+)')


    # The record to keep is that where the "From" = "To" of the companion line of the group. There are only at most two entries with the same Start_dt, so I only need to worry about passing back the derived "To" and "From" fields twice. Either of row = 1 or row = 2 can be the best one to keep. Essentially I'm searching for the best determinite record of the status going forward, ignoring the journey (implying that accurate status is more important than accurate/comprehensive journey).
    # 
    # Will need to offset forwards and backwards. Ea row will then have a pair of offset values (from the row above and the one beneath). Depending on the "count" value (ie 1,2), only one from the pair will be relevant and tested to determine the "best" and "final" value.

    df_grouped.sort_values(['Target_Email_act','Activity_Type_act','Activity_Date_DT_act'],inplace= True)
    # -1 = the row below (lead); +1 = the row above (lagged)
    df_grouped[['from_-1','from_1']] = df_grouped['from'].shift(periods = [-1,1])
    df_grouped[['to_-1','to_1']] = df_grouped['to'].shift(periods = [-1,1])

    #do the same for Subject_act: this will help me sift through cases where Trial expirations are conflicting with new trial or membership starts (some kind of rollover - ex. taylor.m.posey@outlook.com)
    df_grouped[['Subject_act_-1','Subject_act_1']] = df_grouped['Subject_act'].shift(periods = [-1,1])


    # Handle cases where the -1 or +1 shift are irrelevant: have to do with different email addresses, as detectable by the 'count' field (ie 1,2)
    # This will actually be handled by a filter

    df_grouped.reset_index(names = 'index', inplace= True)
    return df_grouped

    #ideally just delete the irrelevant records: will need to identify the 'keep' and 'delete' records, specifically relying on index
    #CASE WHERE ROW NUMBER =1 THEN WE DON'T CARE ABOUT LAGGED DATA (ie only pertinent data is suffix = -1)
    #CASE WHERE ROW NUMBER =2 THEN WE DON'T CARE ABOUT LEAD DATA (ie only pertinent data is suffix = 1)
    #REVERSING TRANSACTIONS: DELETE/DROP BOTH
    #an example
    #df_grouped.loc[df_grouped['Activity_Type_act'] != 'Membership Signup',['Target_Email_act','Activity_Date_DT_act','Subject_act','Subject_act_-1','Subject_act_1','count','from', 'to', 'from_-1', 'from_1','to_-1', 'to_1']].head(5)
    # CASE WHERE ROW NUMBER = 1 AND to_-1 from -1


    # Select logic in words:
    # - if row = 1 THEN real case: from_-1 <> to AND to_-1 = from (choose this row and discard where row = 2)
    # - if row = 2 THEN real case: to_1 = from AND from_1 <> to (choose this row and discard where row = 1)
    # - case where two status updates are made at the same time that don't conform to the above two logic statements: choose whichever row DOES NOT contain "Expired"
    # - if Activity_Type_act = 'Membership Signup' THEN choose the row where Subject_act does not contain "Expired"

#this model records/selects the records to "keep"
def apply_rules(df_grouped):
    #make a copy of the DataFrame schema to serve as the cumulative DF to store either "+" or +"-" records (keep/delete)
    concat_grouped_df = df_grouped.iloc[:0,:].copy()

    #review the impact of removing records having "expired" in Subject_act field
    #each subset will be concatenated to a cumulative DF; the most important field is the index

    #case where the companion record contains the word "Expired" and the "count = 1" record does not
    concat_grouped_df = pd.concat([concat_grouped_df,df_grouped.loc[(df_grouped['Activity_Type_act'] == 'Membership Signup') & (df_grouped['count'] == 1) & (df_grouped['Subject_act_-1'].str.contains('Expired')) & (df_grouped['Subject_act'].str.contains('Expired') == False),:]])
    #this returns 17 records, and so should be included
    concat_grouped_df = pd.concat([concat_grouped_df,df_grouped.loc[(df_grouped['Activity_Type_act'] == 'Membership Signup') & (df_grouped['count'] == 2) & (df_grouped['Subject_act_1'].str.contains('Expired')) & (df_grouped['Subject_act'].str.contains('Expired') == False),:]])

    #returns 0
    concat_grouped_df = pd.concat([concat_grouped_df,df_grouped.loc[(df_grouped['Activity_Type_act'] == 'Membership Signup') & (df_grouped['count'] == 1) & (df_grouped['Subject_act_-1'].str.contains('Trial')) & (df_grouped['Subject_act'].str.contains('Trial') == False),:]])

    #returns 40, so should be included
    concat_grouped_df = pd.concat([concat_grouped_df,df_grouped.loc[(df_grouped['Activity_Type_act'] == 'Membership Signup') & (df_grouped['count'] == 2) & (df_grouped['Subject_act_1'].str.contains('Trial')) & (df_grouped['Subject_act'].str.contains('Trial') == False),:]])

    #returns 6 values, so should be used
    concat_grouped_df = pd.concat([concat_grouped_df,df_grouped.loc[(df_grouped['Activity_Type_act'] != 'Membership Signup') & (df_grouped['count'] == 2) & (df_grouped['to_1'] == df_grouped['from']) & (df_grouped['from_1'] != df_grouped['to']),:]])

    #else grab the first row and pray to god
    #row to keep (second row)
    concat_grouped_df = pd.concat([concat_grouped_df, df_grouped.loc[~(df_grouped['Target_Email_act'].isin(concat_grouped_df['Target_Email_act'])) & (df_grouped['count'] == 2),:]])

    #in order to grab the remaining records will reference the email account (and optionally the timestamp)
    concat_grouped_ser = concat_grouped_df['Target_Email_act'].drop_duplicates()

    #a un-duped series of the email addresses and most importantly the indexes to keep
    return concat_grouped_ser


def apply_exclusion(inclusion_ser,df_grouped,activityReport):
    #inclusion_df = concat_grouped_ser from apply_rules()
    #exclusion_list = a list - all indices of df_grouped OUTSIDE of those we want to keep (ie contact_grouped_df <- the cumulative DF)

    #check that all expected columns exist
    if ('index' in df_grouped.columns):
        exclusion_list = df_grouped.loc[~df_grouped['index'].isin(inclusion_ser),'index'].to_list()
        #isolate the records to remove by negative indexing on concat_grouped_df

        #TODO inverse index activityReport by selecting indices NOT IN exclusion_list
        #inspection
        #df_grouped.loc[~df_grouped['index'].isin(concat_grouped_df['index']),:].sort_values(['Target_Email_act','Activity_Date_DT_act']).to_csv('/home/mofongo/Documents/ghfc/membershipReportsCIVI/records_to_remove.csv', index = False)
        #test = activityReport.loc[~df_grouped.loc[~df_grouped['index'].isin(concat_grouped_df['index']),'index'],:]
        test = activityReport.index.difference(exclusion_list)
        filtered_df = activityReport.loc[test,:]
        return filtered_df.drop_duplicates(ignore_index = True)
    else:
        raise ValueError(f"either of the two dfs in apply_exclusion() do not include the required 'index' column")
    



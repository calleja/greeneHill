import pandas as pd
import numpy as np
import os
import re
import datetime
import sys
import sqlalchemy
#edit to the path of the `container_credentials` module
sys.path.append(('/home/mofongo/Documents/ghfc/membershipReportsCIVI/greeneHill'))
from container_credentials import return_credentials

#func to build a Series or other data container of the final day of the month to serve as the measurement data of the 'curr' and 'prev'
def last_day_of_month(any_day):
    # The day 28 exists in every month. 4 days later, it's always next month
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    # subtracting the number of the current day brings us back one month
    return next_month - datetime.timedelta(days=next_month.day)


#function that takes in a dataframe of a month's scores (frequency table) along with activity_calc fields, which then combines the TWO activity_calc fields (prev and curr) into a tuple of the status combination. Extracts the unq.email field (int) and converts that into the scores Series with index corresponding to the combo tuple
#can theoretically apply this iteratively to ea DataFrame in the result set dict
def package_scores_vector(df: pd.DataFrame):
    #in order to avoid issues with "None" values in the "member status" combinations, convert None to "None" (string)
    df = df.assign(prev_activity_calc = df['prev_activity_calc'].fillna('None'), curr_activity_calc = df['curr_activity_calc'].fillna('None'))

    #make the 'combo' tuple
    test_df = df.assign(combo = [tuple(i.values()) for i in df[['curr_activity_calc','prev_activity_calc']].to_dict('records')])

    #scores_series_vector = the vector that I ultimately multiply by the 'category' matrix (odf) AFTER I compile the index intersection
    scores_series_vector = pd.Series(data = test_df['unq_email'].values, index = test_df['combo'])

    return scores_series_vector


#function to accomplish all processes in this segment: a) import ODS; b) parse and reunify the status tuple that serves as the combo index; c) fillna() properly
# funct argument is filepath to the statusCombinationMatrix_acctFlows.ods file
def import_treat_cat_df(filepath: str):
    #NOTE: the combo tuple will be messed up, and needs to be re-formated and ultimately recognized as a tuple in python
    cats_df = pd.read_excel(filepath, engine="odf")
    cats_df.columns = [i.replace(" ","_").lower() for i in cats_df.columns]

    #use regex to remove the artifacts from the tuple encoding when the .ods file was created from the df copy (done manually)
    pattern = re.compile("[a-zA-Z0-9_ ]+")

    cats_df = cats_df.assign(curr_activity_calc_norm = cats_df['curr_activity_calc'].apply(lambda x: list(re.findall(pattern, x))[-1].strip()), prev_activity_calc_norm = cats_df['prev_activity_calc'].apply(lambda x: list(re.findall(pattern, x))[-1].strip()))

    #create the final tuple column
    cats_df['tuple_index'] = tuple(zip(cats_df['curr_activity_calc_norm'], cats_df['prev_activity_calc_norm']))

    #select relevant fields
    cats_df = cats_df.loc[:,[i for i in list(cats_df.columns) if 'activity' not in i]]

    #set the index
    cats_df.set_index('tuple_index', inplace = True)

    #converts the matrix to a 1,0
    cats_df = cats_df.fillna(0)

    return cats_df


#function to ingest the matrix and vector, compile the intersection and return the equal sized objects (ready to multiply) in a tuple
# the cat_matrix is the ods file as processed above, terminating in the fillna(0) operation
def sync_objects(scores_vector:pd.Series, cat_df:pd.DataFrame):
    cats_df2_index= list(cat_df.index)
    scores_index = list(scores_vector.index)

    # quantify the # of columns in the scores/contingency vector and NOT in the categorical matrix
    #outer = list(set() - set(cats_df2_index))
    outer = list(set(scores_index).difference(cats_df2_index))
    #main_list = list(set(list_2) - set(list_1))

    #TODO an intersection leaves out potentially relevant combinations in the scores index for the month; I should either throw an error or be made to enter those missing values in the categorical DataFrame
    index_intersection = list(set(cats_df2_index) & set(scores_index))

    cats_df2 = cat_df.loc[index_intersection,:].sort_index()

    scores_series_vector_prod = scores_vector[index_intersection].sort_index()


    return scores_series_vector_prod, cats_df2, outer


#function to convert "categories" df to matrix, then execute the multiplcation
#requires the vector and matrix to be "right-sized"
def apply_multiply(scores_vec:pd.Series, cat_df:pd.DataFrame):
    if scores_vec.shape[0] == cat_df.shape[0]:
        mat = cat_df.to_numpy()
#scores_series_vector = db resultset of status combinations and counts
        new_mat = np.matmul(np.transpose(scores_vec.array), mat)

        #returns a Series with the category balances
        return pd.Series(new_mat, index = cat_df.columns)
    
    else:
        raise TypeError("objects are not of appropriate size")
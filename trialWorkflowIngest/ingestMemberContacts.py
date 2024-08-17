# %% [markdown]
# CIVI Constituent Summary or Constituent Detail: https://plum.greenehillfood.coop/civicrm/report/instance/1?reset=1&output=criteria
# <br> This report is preferable because this is most useful when contacting prospective members (email & phone included)

# %%
import os
import pandas as pd
import re
import datetime
import itertools
import pickle
import sqlalchemy

class ingestMemberContacts:

    def __init__(self,filepath: str):
        #os.chdir('/home/candela/Documents/greeneHill/membershipReportsCIVI/membershipReportingLogicSampleReports')
        if 'csv' in filepath & os.path.exists(filepath):
            self.members = pd.read_csv(filepath)
        else:
            print('filepath does not exist')


#rename the columns and remove blank spaces
    def treat_df(self):
         self.members.columns = [i.replace(' ','_').lower() for i in self.members.columns]



    # DEFINE THE DATABASE CREDENTIALS
    def connect_db(self):
        user = 'root'
        password = 'baeldung'
        host = '172.17.0.2'
        port = 3306
        database = 'membership'

        def get_connection():
	        return sqlalchemy.create_engine(url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
			user, password, host, port, database))
        
        return get_connection()
	

if __name__ == '__main__':

	try:
	
		# GET THE CONNECTION OBJECT (ENGINE) FOR THE DATABASE
		engine = get_connection()
		print(
			f"Connection to the {host} for user {user} created successfully.")
	except Exception as ex:
		print("Connection could not be made due to the following error: \n", ex)


# %%
try:
    frame = members.to_sql('member_directory2', con=engine, if_exists='replace', index=False)
except ValueError as vx:
    print(vx)
except Exception as ex:   
    print(ex)
else:
    print("Table %s created successfully."%'mem_status');   
finally:
    engine.dispose()



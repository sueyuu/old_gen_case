#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
import pandas as pd
from os.path import join
from os import listdir


# In[35]:


Base = declarative_base()

one_year=timedelta(days=455)

# CREATE THE TABLE MODEL TO USE IT FOR QUERYING
class Cases(Base):
  
  __tablename__ = 'CaseList'
  
  CL_PatientID = sa.Column(sa.String, primary_key=True)
  CL_CASE_DATE = sa.Column(sa.String)
  CL_CASE_TYPE = sa.Column(sa.String)
  CL_Timestamp = sa.Column(sa.TIMESTAMP)  

def GetCase(weekfile):
    
# Create engine
  engine = sa.create_engine('{dialect}+{driver}://{user}:{password}@{host}:{port}/{dbname}'
                            .format(dialect='mysql', driver='mysqlconnector',
                            user='root', password='sql@r00t', host='192.168.0.16', port='3306', dbname='dm1'))

# CREATE A SESSION OBJECT TO INITIATE QUERY IN DATABASE
  Session = sessionmaker(bind = engine)
  session = Session()

#grouped
  df_case=pd.read_csv(weekfile, usecols=['個案身分證號','預約日期'], parse_dates = ['預約日期'], infer_datetime_format=True)

  grouped=df_case['個案身分證號'].groupby(df_case['預約日期'])

  for exportdate, groupdf in grouped:    
    one_year_ago=exportdate-one_year
    ptlist = groupdf.to_list()
    
#open new csv and write header
    exp_date = exportdate.strftime('%Y%m%d')
    pd.DataFrame(columns=['id','time','case']).to_csv(f'casefolder/{exp_date}.csv', index=False)

# SQLAlCHEMY ORM QUERY TO FETCH ALL RECORDS
    pd.read_sql(
    sql = session.query(Cases.CL_PatientID, Cases.CL_CASE_DATE, 
          Cases.CL_CASE_TYPE).filter(
          Cases.CL_PatientID.in_(ptlist),
          Cases.CL_Timestamp.between(one_year_ago.strftime('%Y-%m-%d'), 
          exportdate.strftime('%Y-%m-%d'))).order_by(Cases.CL_PatientID, 
          Cases.CL_CASE_DATE).statement,
          con = engine).to_csv(f'casefolder/{exp_date}.csv', header=None, index=False, mode='a')
# Close connection


# In[ ]:





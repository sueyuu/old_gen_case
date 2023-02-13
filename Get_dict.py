#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd

from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from datetime import timedelta, datetime

from functools import partial, reduce
import re
from os import listdir
from os.path import join
import glob
import traceback


# In[6]:


ad_year=relativedelta(years=1911)

#抓結案病人
#抓IGT

def GetDoneDict(*args):

  result = []

  for arg in args:
    
    df = pd.read_excel(glob.glob(arg)[0], header=None)
#找身分證和日期欄位
    _rw, cl_dict=ColumnsSearch(df, ['看診日期','身份證字號'])

    done_df = df.iloc[_rw+1:,[cl_dict['身份證字號'],cl_dict['看診日期']]]
    
    done=done_df.groupby(cl_dict['身份證字號']).max()
   
    done=done.applymap(lambda x: parse(x)+ad_year)
    
#轉成dict(key: id, value: datetime)
    result.append({key: done.at[key, cl_dict['看診日期']] for key in done.index})
  
  return tuple(result)


#對耀聖方案，有錯者另外拉成newdf並修改case csv
def CasePair():

#用有is的group改df_case和add to newdf
  def _casepair():
#dict for inner_casecompair
    pair_dict={'is1408CKD':'p4302c.xls', 'is1409CKD':'p4302c.xls', 
               'is4302CKD':'p4301c.xls', 'is4301CKD':'p4301c.xls', 
               'is1408':'p1408c.xls', 'is1409': 'p1409c.xls', 'is1407':'p1407c.xls',
               'not1408':'n1408c.xls', 'not1409': 'n1409c.xls', 'not1407':'n1407c.xls',
               'is7001':'p7001c.xls', 'is7002': 'p7002c.xls'}
    
    def inner_compair(chart=[]):
#rv_dict雙案第一個元素單案第二個元素除了P4302C
      nonlocal newdf, df_case
    
      _rv=[{'p4302c.xls':['is1408CKD','is1409CKD'],'p4301c.xls':['is4302CKD','is4301CKD']}, 
        {'p1408c.xls':['is1408CKD', 'is4302CKD', 'is1408'], 'p1409c.xls':['is1409CKD', 'is1409'], 'p1407c.xls':['is1407']},
        {'p7001c.xls':['is7001'], 'p7002c.xls':['is7002']},
        {'n1407c.xls':['not1407'], 'n1408c.xls':['not1408'], 'n1409c.xls':['not1409']}]
        
      rv_dict = reduce(lambda d, current: d.update(current) or d, _rv, {})
      
      caselist=[]
    
#比對charts
      def _inner(cts):
        
#get_k for找key's index(一定有，不會出現找不到的情形)
        get_k = lambda ct, _rvk, i: i if ct in _rvk[i] else get_k(ct, _rvk, i+1)
    
        six_day=timedelta(days=6)
        nonlocal newdf, df_case, g, _rv

        for ct in cts:          
          try:
            
            for i in range(rw+1, len(chartdict[ct])-1):
              
              if chartdict[ct].iat[i, cl_dict['身份證字號']] == _id:

                chart_tm = parse(chartdict[ct].iat[i, cl_dict['看診日期']]) + ad_year

                if tm-chart_tm<six_day and tm>=chart_tm:
#如果時間有差，要改成chart_tm(會變成到在一周內較早的時間)
                  if tm != chart_tm:
                    df_case.loc[(df_case.id == _id) & (df_case.time == tm), 'time'] = chart_tm  
                  caselist.append(ct)

#以下是ct有比對到的狀況
 
#if _rv空的 or 找到not/is7(=n/p7), return
                  if not _rv or re.match('n|p7', ct):
                    return
        
#因找到not/is7都會return，剩下就是is4/1，比對對方就好(get_k找到key後pop掉該組，_rv[0]就是剩下另一組)，_rv先歸零
                  else:
                    del _rv[get_k(ct, [d.keys() for d in _rv], 0)]
                    another_k = list(_rv[0].keys())
                    _rv=[]
                    
#another_k非n/is7開頭再跑剩下的
                    if not re.match('n|p7', another_k[0]):
                      _inner(another_k)
    
#在時間範圍外找到(找比tm大的就好)，先插入重整後，之後輪到會再比對一次(剛好在要找的chart裡才找得到)
                if chart_tm>tm and chart_tm not in g['time'].to_list():
                  
                  newdf=newdf.append({'id':_id,'time':chart_tm,'oldcase':'','newcase':ct}, ignore_index=True)
                  d=pd.DataFrame({'id':[_id],'time':[chart_tm], 'case':[ct]})
                  
                  df_case = df_case.append(d, ignore_index=True)
                  df_case = df_case.sort_values(['id', 'time'])
                  df_case = df_case.reset_index(drop=True)
                  
                  g = g.append(d, ignore_index=True)
                  g=g.sort_values(['id', 'time'])
                  g=g.reset_index(drop=True)
                    
          except Exception as err:
            print(traceback.format_exc())

#if沒找到，_rv未空，先取出目前的key剩下的value或輪下一個key。_rv空了，return。
        try:
          now_k = get_k(list(cts)[0], [d.keys() for d in _rv], 0)

#now_k = 目前的_rv[cts[0]]，跑剩下的keys，並且_rv[now_k]keys/values要重設成這些剩餘的。else not(now_k)(if value是空的)+dc now_k
          if set(_rv[now_k].keys())-set(cts):
            left_v = set(_rv[now_k].keys())-set(cts)
            _rv[now_k] = {k:_rv[now_k][k] for k in left_v}

          else:
            left_v = _rv[int(not(now_k))].keys()
            del _rv[now_k]

          _inner(left_v)
        except:
          return
#inner_compair start...            
#優先比對相符的chart
      _inner(chart)
      
#用len區分單案雙案
#有找到not一律是單案
      try:
        if len(caselist)==1:
#P4302C如果是單案跳'err', df_case先不改
          if caselist[0]=='p4302c.xls':
            newdf=newdf.append({'id':_id,'time':tm,'oldcase':case,'newcase':'err'}, ignore_index=True)
          elif case != rv_dict[caselist[0]][-1]:
            df_case.at[index,'case']=rv_dict[caselist[0]][-1]
            newdf=newdf.append({'id':_id,'time':tm,'oldcase':case,'newcase':df_case.at[index,'case']}, ignore_index=True)
#P1407C ~ N開頭如果是雙案跳'err', df_case先不改
        elif len(caselist)==2:
#if case是後4個key的其中一個=>P1407C or N開頭
          if case in list(rv_dict.keys())[4:]:
            newdf=newdf.append({'id':_id,'time':tm,'oldcase':case,'newcase':'err'}, ignore_index=True)
          elif case not in set(rv_dict[caselist[0]]) & set(rv_dict[caselist[1]]):
            df_case.at[index,'case']=(set(rv_dict[caselist[0]]) & set(rv_dict[caselist[1]])).pop()            
            newdf=newdf.append({'id':_id,'time':tm,'oldcase':case,'newcase':df_case.at[index,'case']}, ignore_index=True)
#if caselist在2022/03/01後是null，is/not都改成general(一年半後可以取消)
#if之前，is先都改成not1408, 其他不動
        else:
          if tm>datetime(2022, 3, 1) and re.match('^[^g]',case):
            df_case.at[index,'case']='general1408'
            newdf=newdf.append({'id':_id,'time':tm,'oldcase':case,'newcase':''}, ignore_index=True)
          elif re.match('is',case):
            df_case.at[index,'case']='not1408'
            newdf=newdf.append({'id':_id,'time':tm,'oldcase':case,'newcase':''}, ignore_index=True)
      except Exception as err:
        print(traceback.format_exc())
#casepair start...    
    for index, _id, tm, case in zip(g.index, g['id'], g['time'], g['case']):
      try:
        if tm<StDate or tm>EnDate:
          continue
      except Exception as err:
        print(traceback.format_exc())

      try:
#is/not
        if case in list(pair_dict.keys()):
          inner_compair([pair_dict[case]])
          continue
#general，先比對not
        elif re.match('general',case):
          inner_compair(['n1408c.xls'])
#特殊碼特殊案不動

      except Exception as err:
        print(traceback.format_exc())
        
#CasePair start...            
  casefolder='casefolder'
#open new casepair.csv
  newdf=pd.DataFrame(columns=['id','time','oldcase','newcase'])
  newdf.to_csv('casepair.csv', index=False)

#先把chartfolder裡檔案都打開，add to chartdict
  chartdict={}

#抓日期區間
  _strfmt='\d{3}.\d{2}.\d{2}'
    
  try:
#拉IGT診斷碼xls，存ID，最後日期(key: id, value: datetime)
#**IGT未轉小寫
    igt_dict, =GetDoneDict('IGT*')
#檔名通通轉小寫
    for _f in glob.glob('[np][0-9][0-9][0-9][0-9]c*'):
      chartdict[_f.lower()]=pd.read_excel(_f)
    
  except Exception as err:
    print(traceback.format_exc())

  StDate, EnDate=tuple(re.findall(_strfmt, chartdict['p1407c.xls'].columns[0]))
#抓StDate, EnDate
  StDate, EnDate = parse(StDate)+ad_year, parse(EnDate)+ad_year
    
#抓看診日期和身分證字號row and cl index
  rw, cl_dict=ColumnsSearch(chartdict['p1407c.xls'], ['看診日期','身份證字號'])
            
#df_case存case的csv檔
  for case in listdir(casefolder):
    try:
      
      df_case=pd.read_csv(join(casefolder, case))
      date1 = pd.to_datetime(df_case['time'], errors='coerce', format='%Y%m%d')
      date2 = pd.to_datetime(df_case['time'], errors='coerce', infer_datetime_format=True)
      df_case['time'] = date1.fillna(date2)
    except Exception as err:
      print(traceback.format_exc())
      continue
        
#df_case_copy(for drop igt不進入方案比對)
    df_case_copy=df_case.copy()
    
#先對igt_dict裡的id
    for k, v in igt_dict.items():
      try:
        
        df_case.loc[df_case['id'] == k, 'case']=df_case.loc[df_case['id'] == k].apply(lambda arrLike, y, z: 
        'general1408' if arrLike[y]<=v and arrLike[z]!='general1408' else arrLike[z], 
        axis=1, args=('time', 'case'))
        
#set df_case_copy and drop igt cases(if id == k and time<=v)
        f1=df_case_copy.id == k
        f2=df_case_copy.time <=v
        
        df_case_copy.drop(range(df_case_copy[f1&f2].index[0], df_case_copy[f1&f2].index[-1]+1), inplace=True)
    
      except Exception as err:
        pass
    
#比對全部id and groupby    
    for _, g in df_case_copy.groupby('id'):
      _casepair()
#drop duplicated, 一律丟棄first
    df_case.drop_duplicates(subset=['id', 'time'], keep='last', inplace=True)
    df_case.to_csv(join(casefolder, case), index=False)

  newdf.to_csv('casepair.csv', index=False, header=None, mode='a')    

#找欄位，傳入dataframe和想找的欄位list
def ColumnsSearch(df, columnlist):
  row_num=0
  column_dict={}
  for cl in columnlist:
    for row in range(0, len(df)):
      for column in range(0, len(df.columns)):
        if df.iat[row, column]==cl:
          row_num=row
          column_dict[cl]=column
  return row_num, column_dict


# In[ ]:





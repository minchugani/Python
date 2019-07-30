# -*- coding: utf-8 -*-
"""
Created on Tue Jul 30 09:48:24 2019

@author: srh001e
"""

import mysql.connector
import pandas as pd 
import json
import numpy as np  
from flask import Flask, jsonify, request
def getdata(item_num , quan):
    mydb = mysql.connector.connect(
    host='localhost',
    user='sunil',
    passwd='Sowj@nya94',
    database = 'rebates'
)
    cursor = mydb.cursor()
   # sql = "SELECT poamount FROM item_sales  where item_number = ? order by period desc"
    sql =  "SELECT poamount FROM item_sales  where item_number = "+item_num+" order by period desc"
    #cursor = mydb.cursor(prepared=True)
    #cursor.execute(sql ,(item_number,))
    cursor.execute(sql)
    data = cursor.fetchall ()
    x = map(list, list(data))
    x = sum(x, [])                            # flatten
    series = pd.Series(x)
    result = series.pct_change()
    series = pd.Series(result)
    absval = series.abs()
    series = pd.Series(absval)
    percentage_increase =  series.mean()
    curr_exp_inc = 100*(percentage_increase  + 0.25*(percentage_increase))
    expec_reb_temp = (np.asarray(x[0])*curr_exp_inc)/100
    expec_reb_amt = expec_reb_temp + x[0]
    #sql_quantity = "SELECT poquantity FROM item_sales  where item_number = ? order by period desc"
    sql_quantity=  "SELECT poquantity FROM item_sales  where item_number = "+item_num+" order by period desc"
    #cursor = mydb.cursor(prepared=True)
    cursor.execute(sql_quantity)
    data_quan = cursor.fetchall ()
    x1 = map(list, list(data_quan))
    x1 = sum(x1, [])    
    series = pd.Series(x1)
    result_quan = series.pct_change()
    series= pd.Series(result_quan)
    absval = series.abs()
    series= pd.Series(absval)
    cur_quan_inc_per = (series.mean())*100
    expec_quan_temp = (x1[0]*cur_quan_inc_per)/100
    expec_quan = x1[0] + expec_quan_temp 
    expec_rate = round((expec_reb_amt/expec_quan)*100 , 2)
    #sql_tierdata  = "SELECT from_value , to_value, tier_rate  FROM tier_results  where item_number =  ? " # dummy selct query 
    sql_tierdata  = "SELECT from_value , to_value, tier_rate  FROM tier_results  where item_number =  "+item_num+" " # dummy selct query 
    cursor.execute(sql_tierdata)
    data_tier = cursor.fetchall ()
    fields = map(lambda x:x[0], cursor.description)
    for row in data_tier:
       Dictionary = dict(zip(fields,row)) 
       dictList=zip(Dictionary.values())
       value1 = dictList[2][0]
       value2 = dictList[1][0]
       value3 = dictList[0][0]
       tier = []
       tier.append(value1)
       tier.append(value2)
       tiermed = np.median(tier) 
       if expec_quan > tiermed:
          rate_inc = expec_rate - value3
          if rate_inc > 0:
              newrate = round(value3 - rate_inc , 2 )
              newfrom  = value1 
              newtoval = tiermed
              new_tier = []
              new_tier.append(newfrom)
              new_tier.append(newtoval)
              new_tier.append(newrate)
              new_tier.append(newtoval + 1)
              new_tier.append(value2)
              new_tier.append(expec_rate)
       if value1 >(newtoval + 1) :             
           new_tier.append(value1)
           new_tier.append(value2)
           new_tier.append(value3)
    pred_tier = map(None,*[iter(new_tier)]*3)
    res = []
    for i  in pred_tier: 
     dictval = (dict(zip(fields,i)) )
     from_val = dictval.get('from_value' , i)
     to_val  = dictval.get('to_value' , i)
     rate = dictval.get('tier_rate',   i )
     quan1 = int(quan)
     if quan1 > from_val and quan1 < to_val:
        cal = quan1*rate
        dictval.__setitem__('calval' , cal)
     res.append(dictval)
    return res
app = Flask(__name__)
@app.route('/result', methods=['GET'])
def getpredtier():
        item_no =request.args.get('item_no', None)
        quan =request.args.get('quan',None)
        pred_res = getdata(item_no ,quan)
        return jsonify(pred_res)
        return item_no
if __name__ == '__main__':
   app.run(debug=True)

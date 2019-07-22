# -*- coding: utf-8 -*-
"""
Created on Mon Jul 15 21:37:12 2019

@author: srh001e
"""

import mysql.connector
import pandas as pd 
import json
import numpy as np  
from flask import Flask, jsonify, request
def getdata(item_num):
    mydb = mysql.connector.connect(
    host='rebatescoe.mysql.database.azure.com',
    user='ganesha@rebatescoe',
    passwd='Sowj@nya94',
    database = 'rebates'
)   
    #item_number = 133791
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
    expec_rate = (expec_reb_amt/expec_quan)*100 
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
              newrate = value3 - rate_inc
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
     res.append(dictval)
    return res
app = Flask(__name__)
@app.route('/result', methods=['GET'])
def getpredtier():
        item_no =request.args.get('item_no', None)
        pred_res = getdata(item_no)
        return jsonify(pred_res)
        return item_no
if __name__ == '__main__':
   app.run(debug=True)
    

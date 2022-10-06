
# coding: utf-8

# In[ ]:


from flask import Flask, request, redirect, url_for, flash, jsonify
import numpy as np
import xgboost as xgb
import pandas as pd
import json
from operator import attrgetter
app = Flask(__name__)


@app.route('/postDataJson',methods = ['POST'])
def success():
  #check methos
   if request.method == 'POST':
      dataJson = request.json
      # request.json để lấy request.body
      return jsonify(dataJson)
    

@app.route('/')
def query_example():

    GENDER = request.args.get('GENDER')
    AGE = request.args.get('AGE')
    TOTAL_PAID_INST_MAX = request.args.get('TOTAL_PAID_INST_MAX')
    DAYS_PAYMENT_DELAY_MAX = request.args.get('DAYS_PAYMENT_DELAY_MAX')
    REMAIN_AMT_PCT = request.args.get('REMAIN_AMT_PCT')
    INTEREST_RATE_MIN = request.args.get('INTEREST_RATE_MIN')
    NUMBEROFREQUESTED = request.args.get('NUMBEROFREQUESTED')
    NUMBEROFREFUSED = request.args.get('NUMBEROFREFUSED')  
    NUMBEROFTERMINATED = request.args.get('NUMBEROFTERMINATED')
    NUMBEROFLIVING = request.args.get('NUMBEROFLIVING')
    WORSTRECENTSTATUS = request.args.get('WORSTRECENTSTATUS')
    NUMBER_TCTD_GR = request.args.get('NUMBER_TCTD_GR')
    NB_6_VER2 = request.args.get('NB_6_VER2')
    REMAIN_AMT_PCT_CARDS = request.args.get('REMAIN_AMT_PCT_CARDS')
    REMAINING_AMOUNT_CARDS = request.args.get('REMAINING_AMOUNT_CARDS')
    MAXNROFDAYSOFPAYMENTDELAY_CARDS = request.args.get('MAXNROFDAYSOFPAYMENTDELAY_CARDS')
    STATUS1_CONS_6M_CARDS = request.args.get('STATUS1_CONS_6M_CARDS')
    STATUS1_CONS_36M_CARDS = request.args.get('STATUS1_CONS_36M_CARDS')
    DEFAULT_CONS_3M_CARDS = request.args.get('DEFAULT_CONS_3M_CARDS')
    DEFAULT_CONS_36M_CARDS = request.args.get('DEFAULT_CONS_36M_CARDS')
    RMN_NO_MIN = request.args.get('RMN_NO_MIN')
    STATUS1_CONS_36M_INSTALLMENT = request.args.get('STATUS1_CONS_36M_INSTALLMENT')
    DEFAULT_CONS_18M_INSTALLMENT = request.args.get('DEFAULT_CONS_18M_INSTALLMENT')
    NO_MONTH_MAX_DEFAULT_MIN = request.args.get('NO_MONTH_MAX_DEFAULT_MIN')
    MAX_LV_DEFAULT_MIN = request.args.get('MAX_LV_DEFAULT_MIN')
    DATE_WORST_TO_ST_AVG = request.args.get('DATE_WORST_TO_ST_AVG')
    RMN_NO_MIN = request.args.get('RMN_NO_MIN')
    GUA_AMT_SUM = request.args.get('GUA_AMT_SUM')
    
 
    
    

    data={  'GENDER':float(GENDER)
            ,'AGE':float(AGE)
            ,'TOTAL_PAID_INST_MAX':float(TOTAL_PAID_INST_MAX)
            ,'DAYS_PAYMENT_DELAY_MAX':float(DAYS_PAYMENT_DELAY_MAX)
            ,'REMAIN_AMT_PCT':float(REMAIN_AMT_PCT)
            ,'INTEREST_RATE_MIN':float(INTEREST_RATE_MIN)
            ,'NUMBEROFREQUESTED':float(NUMBEROFREQUESTED)
            ,'NUMBEROFREFUSED':float(NUMBEROFREFUSED) 
            ,'NUMBEROFTERMINATED':float(NUMBEROFTERMINATED)
            ,'NUMBEROFLIVING':float(NUMBEROFLIVING)
            ,'WORSTRECENTSTATUS':float(WORSTRECENTSTATUS)
            ,'NUMBER_TCTD_GR':float(NUMBER_TCTD_GR)
            ,'NB_6_VER2':float(NB_6_VER2)
            ,'REMAIN_AMT_PCT_CARDS':float(REMAIN_AMT_PCT_CARDS)
            ,'REMAINING_AMOUNT_CARDS':float(REMAINING_AMOUNT_CARDS)
            ,'MAXNROFDAYSOFPAYMENTDELAY_CARDS':float(MAXNROFDAYSOFPAYMENTDELAY_CARDS)
            ,'STATUS1_CONS_6M_CARDS':float(STATUS1_CONS_6M_CARDS)
            ,'STATUS1_CONS_36M_CARDS':float(STATUS1_CONS_36M_CARDS)
            ,'DEFAULT_CONS_3M_CARDS':float(DEFAULT_CONS_3M_CARDS)
            ,'DEFAULT_CONS_36M_CARDS' :float(DEFAULT_CONS_36M_CARDS)
            ,'RMN_NO_MIN ':float(RMN_NO_MIN)
            ,'STATUS1_CONS_36M_INSTALLMENT':float(STATUS1_CONS_36M_INSTALLMENT)
            ,'DEFAULT_CONS_18M_INSTALLMENT':float(DEFAULT_CONS_18M_INSTALLMENT)
            ,'NO_MONTH_MAX_DEFAULT_MIN':float(NO_MONTH_MAX_DEFAULT_MIN)
            ,'MAX_LV_DEFAULT_MIN':float(MAX_LV_DEFAULT_MIN)
            ,'DATE_WORST_TO_ST_AVG':float(DATE_WORST_TO_ST_AVG)
            ,'RMN_NO_MIN':float(RMN_NO_MIN)
            ,'GUA_AMT_SUM':float(GUA_AMT_SUM)
}

    data = pd.DataFrame(data,index=[0])
    data_score=xgb.DMatrix(data)
    pre_prod = bst.predict(data_score)
    data['PD'] = pd.Series(pre_prod[0])
    a= 427.928058181122
    b = 75.41697308638469
    data['SCORE'] = np.round((a+b* np.log((1-pre_prod)/pre_prod)),0)
    
    # print('Hello world',pd.Series(data['SCORE']))
    output=data['SCORE'].item()
    return jsonify(output)

if __name__ == '__main__':
    bst = xgb.Booster()
    bst.load_model('pcb_ec_v15.model')
    app.run(debug=False, host='0.0.0.0')


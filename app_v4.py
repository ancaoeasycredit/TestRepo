
# coding: utf-8

# In[ ]:
from google.cloud import bigquery
from google.oauth2 import service_account

from flask import Flask, request, redirect, url_for, flash, jsonify
import numpy as np
import xgboost as xgb
import pandas as pd
import json
import datetime as time
from operator import attrgetter
import pandas_gbq
from flask_cors import CORS
app = Flask(__name__)
CORS(app,resources={r"/pcbmodel/*": {"origins": "*"}})


@app.route('/postData',methods = ['POST'])
def success():
  #check methos
   if request.method == 'POST':
      dataJson = request.json
      # request.json để lấy request.body
      return jsonify(dataJson)
    

@app.route('/pcbmodel',methods = ['POST'])
def query_example():
     if request.method == 'POST':
        dataJson = request.json
        data_full = pd.io.json.json_normalize(dataJson)
        col_name=data_full.columns
        contract_number =  data_full['contract_number'][0]
        executive_date= data_full['executive_date'][0]
        executive_date= pd.to_datetime(executive_date,format='%Y-%m-%d %H:%M:%S')
        
        dt = str(dataJson)
        error = '"Error"'

        DATA_RESULT ={}
        DATA_RESULT['CONTRACT_NUMBER'] = contract_number
        DATA_RESULT['EXECUTIVE_DATE'] = executive_date
        DATA_RESULT['GENDER'] = -999
        DATA_RESULT['AGE']=-999
        DATA_RESULT['TOTAL_PAID_INST_MAX']=-999
        DATA_RESULT['DAYS_PAYMENT_DELAY_MAX']=-999
        DATA_RESULT['REMAIN_AMT_PCT']=-999
        DATA_RESULT['INTEREST_RATE_MIN']=-999
        DATA_RESULT['NUMBEROFREQUESTED']=-999
        DATA_RESULT['NUMBEROFREFUSED']=-999
        DATA_RESULT['NUMBEROFTERMINATED']=-999
        DATA_RESULT['NUMBEROFLIVING']=-999
        DATA_RESULT['WORSTRECENTSTATUS']=-999
        DATA_RESULT['NUMBER_TCTD_GR']=-999
        DATA_RESULT['NB_6_VER2']=-999
        DATA_RESULT['REMAIN_AMT_PCT_CARDS']=-999
        DATA_RESULT['REMAINING_AMOUNT_CARDS']=-999
        DATA_RESULT['MAXNROFDAYSOFPAYMENTDELAY_CARDS']=-999
        DATA_RESULT['STATUS1_CONS_6M_CARDS']=-999
        DATA_RESULT['STATUS1_CONS_36M_CARDS']=-999
        DATA_RESULT['DEFAULT_CONS_3M_CARDS']=-999
        DATA_RESULT['DEFAULT_CONS_36M_CARDS']=-999
        DATA_RESULT['STATUS1_CONS_36M_INSTALLMENT']=-999
        DATA_RESULT['DEFAULT_CONS_18M_INSTALLMENT']=-999
        DATA_RESULT['NO_MONTH_MAX_DEFAULT_MIN']=-999
        DATA_RESULT['MAX_LV_DEFAULT_MIN']=-999
        DATA_RESULT['NEXT_DUE_AMT_MIN']=-999
        DATA_RESULT['DATE_WORST_TO_ST_AVG']=-999
        DATA_RESULT['RMN_NO_MIN']=-999
        DATA_RESULT['GUA_AMT_SUM']=-999


        if error.find(dt) != -1 :
            pass
        else : 
            DATA_RESULT['NOTES'] = 'ERROR'

        d1 = {"CommonData.CBContractCode":[None]}
        d2 = {"CommonData.CBContractCode":[None]}

        
        #Check
        GENDER_col = 'Person.Gender'
        DOB_col= 'Matched.Person.DateOfBirth'
        Request_col = 'Contract.Instalments.Summary.NumberOfRequested'
        Refused_col = 'Contract.Instalments.Summary.NumberOfRefused'

        WorstRecentStatus_col = 'CreditHistory.GeneralData.WorstRecentStatus'
        DateWorstStatus_col = 'CreditHistory.GeneralData.WorstRecentStatus'

        inst_living_col = 'Contract.Instalments.Summary.NumberOfLiving'
        inst_terminated_col = 'Contract.Instalments.Summary.NumberOfTerminated'
        noinst_living_col = 'Contract.NonInstalments.Summary.NumberOfLiving'
        noinst_terminated_col = 'Contract.NonInstalments.Summary.NumberOfTerminated'
        card_living_col = 'Contract.Cards.Summary.NumberOfLiving'
        card_terminated_col = 'Contract.Cards.Summary.NumberOfTerminated'


        c_gender = 0
        c_dob = 0
        c_request = 0
        c_living = 0
        c_refused = 0
        c_terminated = 0
        c_worstrecentstatus = 0
        c_noinst_living = 0
        c_noinst_terminated = 0
        c_card_living = 0
        c_card_terminated = 0
        for i in col_name:
            if i.find(GENDER_col)!=-1:
                c_gender = c_gender + 1
                gender_col_name = i 
            if i.find(DOB_col)!=-1:
                c_dob = c_dob + 1
                dob_col_name = i 
            if i.find(Request_col)!=-1:
                c_request = c_request + 1
                request_col_name = i 
            if i.find(inst_living_col)!=-1:
                c_living = c_living + 1
                living_col_name = i 
            if i.find(Refused_col)!=-1:
                c_refused = c_refused + 1
                refused_col_name = i 
            if i.find(inst_terminated_col)!=-1:
                c_terminated = c_terminated + 1
                terminated_col_name = i 
            if i.find(WorstRecentStatus_col)!=-1:
                c_worstrecentstatus = c_worstrecentstatus + 1
                worstrecentstatus_col_name = i  
            if i.find(noinst_living_col)!=-1:
                c_noinst_living = c_noinst_living + 1
                nonins_living_col_name = i   
            if i.find(noinst_terminated_col)!=-1:
                c_noinst_terminated = c_noinst_terminated + 1
                noinst_terminated_col_name = i 
            if i.find(card_living_col)!=-1:
                c_card_living = c_card_living + 1
                card_living_col_name = i 
            if i.find(card_terminated_col)!=-1:
                c_card_terminated = c_card_terminated + 1
                card_terminated_col_name = i 
        #PCB_FLAG
        a1 = data_full[living_col_name].apply(lambda x : 0 if x == '' else x).astype(float)
        a2 = data_full[terminated_col_name].apply(lambda x : 0 if x == '' else x).astype(float)
        a3 = data_full[nonins_living_col_name].apply(lambda x : 0 if x == '' else x).astype(float)
        a4 = data_full[noinst_terminated_col_name].apply(lambda x : 0 if x == '' else x).astype(float)
        a5 = data_full[card_living_col_name].apply(lambda x : 0 if x == '' else x).astype(float)
        a6 = data_full[card_terminated_col_name].apply(lambda x : 0 if x == '' else x).astype(float)
        s = a1[0]+a2[0]+a3[0]+a4[0]+a5[0]+a6[0] 
        if s > 0 :
            PCB_FLAG_HIT_FULL = 1
        else :
            PCB_FLAG_HIT_FULL = 0


        if PCB_FLAG_HIT_FULL == 0 : 
            DATA_RESULT['NOTES'] = 'NON_HITPCB'

        else: 
            inst_granted = "Contract.Instalments.GrantedContract"
            card_granted = "Contract.Cards.GrantedContract"
            c1=0
            c2=0
            for i in col_name:
                if i.find(inst_granted)!=-1:
                    c1=c1+1
                    inst_granted_name = i 
                if i.find(card_granted)!=-1:
                    c2=c2+1
                    card_granted_name = i
            if (c1 == 1) : data_inst_granted=pd.json_normalize(data_full[inst_granted_name][0]) 
            else : 
                data_inst_granted =pd.DataFrame(data=d1)

            if (c2 == 1) : data_card_granted=pd.json_normalize(data_full[card_granted_name][0]) 
            else : data_card_granted =pd.DataFrame(data=d2)

            data_inst_granted=data_inst_granted.rename(columns = {'CommonData.CBContractCode':'CBContractCode'})
            data_card_granted=data_card_granted.rename(columns = {'CommonData.CBContractCode':'CBContractCode'})

            data_profile_inst=pd.DataFrame()

            if(c1 == 1):
                for row in data_inst_granted.itertuples():
                    df1=pd.json_normalize(row.Profiles)
                    df1['CBContractCode'] = row.CBContractCode
                    try:
                        data_profile_inst = pd.concat([data_profile_inst,df1],axis = 0,ignore_index =True)
                    except:
                        pass
            data_profile_card=pd.DataFrame()
            if(c2 == 1) :
                for row in data_card_granted.itertuples():
                    df2=pd.json_normalize(row.Profiles)
                    df2['CBContractCode'] = row.CBContractCode
                    try:
                        data_profile_card = pd.concat([data_profile_card,df2],axis = 0,ignore_index =True)
                    except:
                        pass
            def interest_rate(p_principal, p_monthly_payment, p_term):
                p_precision = 0.00000001
                if (p_monthly_payment == None or p_principal == None or p_term == None) :
                    return None;
                else :
                    z = p_monthly_payment/p_principal;
                    u = 1/(p_term*z);
                    v = 0;
                    delta = 0;
                    for i in range(1,1000) :
                        v     = pow(u, p_term);
                        delta = ( z * u * ( v - 1) - u + 1 )/(z * (p_term + 1) * v - z - 1 ) ;
                        u     = u - delta;
                        i = i+1
                        if (abs(delta) < p_precision) : break;
                return  12*(1/u-1);  
            def month_between(a,b):
                return 12*(b.year - a.year) + b.month - a.month 


            #1. GENDER

            if (data_full[gender_col_name][0]=='F'):
                GENDER = 1    
            else :
                GENDER = 0

            #2. AGE

            string = data_full[dob_col_name][0]
            DOB = time.date(int(string[4:8]),int(string[2:4]),int(string[0:2]))
            AGE = int(month_between(DOB,executive_date)/12)+1

            #3. TOTAL_PAID_INST_MAX
            if(c1==1):
                data_inst_granted['TotalNumberOfInstalments'] = data_inst_granted['TotalNumberOfInstalments'].apply(lambda x : 0 if x == '' else x).astype(float)
                data_inst_granted['RemainingInstalmentsNumber'] = data_inst_granted['RemainingInstalmentsNumber'].apply(lambda x : 0 if x == '' else x).astype(float)
                data_inst_granted['TOTAL_PAID_INST'] = data_inst_granted['TotalNumberOfInstalments']-data_inst_granted['RemainingInstalmentsNumber']
                TOTAL_PAID_INST_MAX = data_inst_granted['TOTAL_PAID_INST'].max()
            else:TOTAL_PAID_INST_MAX=-999

            #4. DAYS_PAYMENT_DELAY_MAX
            data_inst_granted['DAYS_PAYMENT_DELAY_MAX']= data_inst_granted['MaxNrOfDaysOfPaymentDelay']
            DAYS_PAYMENT_DELAY_MAX = data_inst_granted['DAYS_PAYMENT_DELAY_MAX'].max()

            #5. REMAIN_AMT_PCT
            if(c1==1):
                data_inst_granted['RemainingInstalmentsAmount']=data_inst_granted['RemainingInstalmentsAmount'].apply(lambda x : 0 if x == '' else x).astype(float)
                data_inst_granted['UnpaidDueInstalmentsAmount']=data_inst_granted['UnpaidDueInstalmentsAmount'].apply(lambda x : 0 if x == '' else x).astype(float)
                data_inst_granted['TotalAmount'] = data_inst_granted['TotalAmount'].apply(lambda x : 0 if x == '' else x).astype(float)
                data_inst_granted['REMAIN_AMT_PCT'] = (data_inst_granted['RemainingInstalmentsAmount']+data_inst_granted['UnpaidDueInstalmentsAmount'])
                REMAIN_AMT_PCT = data_inst_granted['REMAIN_AMT_PCT'].sum()/data_inst_granted['TotalAmount'].sum()
            else:REMAIN_AMT_PCT=-999

            #6. INTEREST_RATE_MIN
            if(c1==1):
                data_inst_granted['MonthlyInstalmentAmount']=data_inst_granted['MonthlyInstalmentAmount'].apply(lambda x : 0 if x == '' else x).astype(float)
                data_inst_granted['TotalNumberOfInstalments']=data_inst_granted['TotalNumberOfInstalments'].apply(lambda x : 0 if x == '' else x).astype(float)
                data_inst_granted['interest_rate']=None
                for i in range(0,len(data_inst_granted)):
                    if (data_inst_granted['CommonData.Role'][i]=='A' and data_inst_granted['PaymentsPeriodicity'][i]=='M' 
                            and data_inst_granted['MonthlyInstalmentAmount'][i]>0 and data_inst_granted['TotalAmount'][i]>0 
                            and data_inst_granted['TotalNumberOfInstalments'][i]>0)  :
                        data_inst_granted['interest_rate'][i]=interest_rate(data_inst_granted['TotalAmount'][i],data_inst_granted['MonthlyInstalmentAmount'].astype(float)[i],data_inst_granted['TotalNumberOfInstalments'].astype(float)[i])
                    else : data_inst_granted['interest_rate'][i] = None
                INTEREST_RATE_MIN = data_inst_granted['interest_rate'].min()
            else:INTEREST_RATE_MIN=-999

            #7. NUMBEROFREQUESTED
            if(data_full[request_col_name][0]) == '' : 
                NUMBEROFREQUESTED = -999
            else : NUMBEROFREQUESTED = data_full[request_col_name][0].astype(float)
            #8. NUMBEROFREFUSED
            if(data_full[refused_col_name][0]) == '' : 
                NUMBEROFREFUSED = -999
            else : NUMBEROFREFUSED = data_full[refused_col_name][0].astype(float)

            #9. NUMBEROFTERMINATED 
            if(data_full[terminated_col_name][0]) == '' : 
                NUMBEROFTERMINATED = -999
            else : NUMBEROFTERMINATED = data_full[terminated_col_name][0].astype(float)

            #10. NUMBEROFLIVING 
            if(data_full[living_col_name][0]) == '' : 
                NUMBEROFLIVING = -999
            else : NUMBEROFLIVING = data_full[living_col_name][0].astype(float)

            #11. WORSTRECENTSTATUS 
            if(data_full[worstrecentstatus_col_name][0]) == '' : 
                WORSTRECENTSTATUS = -999
            else : WORSTRECENTSTATUS = data_full[worstrecentstatus_col_name][0]

            #12.NUMBER_TCTD_GR
            if(c1==1):
                a=data_inst_granted['CommonData.EncryptedFICode'].unique()
                NUMBER_TCTD_GR = len(a)
            else: NUMBER_TCTD_GR = -999
            #13.NB_6_VER2
            if(c1==1):
                data_inst_granted['NB_6_VER2_flag'] = 0
                for i in range(0,len(data_inst_granted)):
                    t1 = data_inst_granted['CommonData.StartingDate'][i]
                    StartingDate = time.date(int(t1[4:8]),int(t1[2:4]),int(t1[0:2]))
                    t2 = month_between(StartingDate,executive_date)
                    if (t2 <=6 ) :
                        data_inst_granted['NB_6_VER2_flag'][i] = 1 
                    else : data_inst_granted['NB_6_VER2_flag'][i] = 0 

                data_temp = data_inst_granted.loc[data_inst_granted['NB_6_VER2_flag']==1,'CommonData.EncryptedFICode']
                NB_6_VER2 = len(data_temp.unique())
            else: NB_6_VER2 = -999
            #14.REMAIN_AMT_PCT_CARDS
            if (c2==1):
                data_card_granted['ResidualAmount']=data_card_granted['ResidualAmount'].apply(lambda x : 0 if x == '' else x).astype(float)
                data_card_granted['UnpaidDueInstalmentsAmount']=data_card_granted['UnpaidDueInstalmentsAmount'].apply(lambda x : 0 if x == '' else x).astype(float)
                data_card_granted['MaxResidualAmount']=data_card_granted['MaxResidualAmount'].apply(lambda x : 0 if x == '' else x).astype(float)
                data_card_granted['REMAIN_AMT_PCT_CARDS'] = (data_card_granted['ResidualAmount'] + data_card_granted['UnpaidDueInstalmentsAmount'])/data_card_granted['MaxResidualAmount']
                REMAIN_AMT_PCT_CARDS = data_card_granted['REMAIN_AMT_PCT_CARDS'].max()
            else : REMAIN_AMT_PCT_CARDS = -999
            #15.REMAINING_AMOUNT_CARDS
            if (c2==1):
                data_card_granted['REMAINING_AMOUNT_CARDS'] = data_card_granted['ResidualAmount'] + data_card_granted['UnpaidDueInstalmentsAmount']
                REMAINING_AMOUNT_CARDS = data_card_granted['REMAINING_AMOUNT_CARDS'].max()
            else : REMAINING_AMOUNT_CARDS = -999
            #16.MAXNROFDAYSOFPAYMENTDELAY_CARDS
            if (c2==1):
                data_card_granted['MAXNROFDAYSOFPAYMENTDELAY_CARDS']=data_card_granted['MaxNrOfDaysOfPaymentDelay'].apply(lambda x : -999 if x == '' else x).astype(float)
                MAXNROFDAYSOFPAYMENTDELAY_CARDS = data_card_granted['MAXNROFDAYSOFPAYMENTDELAY_CARDS'].max()
            else:MAXNROFDAYSOFPAYMENTDELAY_CARDS=-999
            #17.STATUS1_CONS_6M_CARDS
            if (c2==1):
                data_profile_card['DATE'] = 0
                for i in range(0,len(data_profile_card)):
                    Date = time.date(int(data_profile_card['ReferenceYear'][i]),int(data_profile_card['ReferenceMonth'][i]),1)
                    data_profile_card['DATE'][i] = month_between(Date,executive_date)
                data_temp1 = data_profile_card.loc[(data_profile_card['DATE']<=6)&(data_profile_card['Status']=='1'),'Status']
                STATUS1_CONS_6M_CARDS = data_temp1.count()
            else : STATUS1_CONS_6M_CARDS = -999
            #18.STATUS1_CONS_36M_CARDS
            if (c2==1):
                data_temp2 = data_profile_card.loc[(data_profile_card['DATE']<=36)&(data_profile_card['Status']=='1'),'Status']
                STATUS1_CONS_36M_CARDS = data_temp2.count()
            else: STATUS1_CONS_36M_CARDS = -999

            #19.DEFAULT_CONS_3M_CARDS
            if (c2==1):
                data_temp3 = data_profile_card.loc[(data_profile_card['DATE']<=3)&(data_profile_card['Default']>'0'),'Default']
                DEFAULT_CONS_3M_CARDS = data_temp3.count()
            else : DEFAULT_CONS_3M_CARDS = -999
            #20.DEFAULT_CONS_36M_CARDS
            if(c2==1):
                data_temp3 = data_profile_card.loc[(data_profile_card['DATE']<=36)&(data_profile_card['Default']>'0'),'Default']
                DEFAULT_CONS_36M_CARDS = data_temp3.count()
            else: DEFAULT_CONS_36M_CARDS = -999
            #21.STATUS1_CONS_36M_INSTALLMENT
            if(c1==1): 
                data_profile_inst['DATE'] = data_profile_inst['ReferenceYear'].astype(int)
                for i in range(0,len(data_profile_inst)):
                    Date = time.date(int(data_profile_inst['ReferenceYear'][i]),int(data_profile_inst['ReferenceMonth'][i]),1)
                    data_profile_inst['DATE'][i] = month_between(Date,executive_date)
                data_temp4 = data_profile_inst.loc[(data_profile_inst['DATE']<=36)&(data_profile_inst['Status']=='1'),'Status']
                STATUS1_CONS_36M_INSTALLMENT = data_temp4.count()
            else: STATUS1_CONS_36M_INSTALLMENT = -999

            #22.DEFAULT_CONS_18M_INSTALLMENT
            if(c1==1):
                data_temp5 = data_profile_inst.loc[(data_profile_inst['DATE']<=18)&(data_profile_inst['Default']>'0'),'Default']
                DEFAULT_CONS_18M_INSTALLMENT = data_temp5.count()
            else:DEFAULT_CONS_18M_INSTALLMENT = -999

            #23.NO_MONTH_MAX_DEFAULT_MIN
            if(c1==1):
                def min_(df1,df2):
                    lst = []
                    for s in range(0,len(df2)):
                        for i in range(0,len(df1)):
                            if df2['key'][s].find(df1['key'][i]) != - 1:
                                lst.append(df1['ReferenceYear'][i])
                    return min(lst)
                df1 = data_profile_inst.groupby(['CBContractCode','Default']).count().reset_index()
                df2 = pd.DataFrame(df1.groupby(['CBContractCode'])['Default'].max().reset_index())
                df1['key'] = df1['CBContractCode']+df1['Default']
                df2['key'] = df2['CBContractCode']+df2['Default']
                NO_MONTH_MAX_DEFAULT_MIN = min_(df1,df2)
            else : NO_MONTH_MAX_DEFAULT_MIN = -999
            #24.MAX_LV_DEFAULT_MIN
            if(c1==1):
                MAX_LV_DEFAULT_MIN = df2['Default'].min()
            else:MAX_LV_DEFAULT_MIN = -999
            #25.NEXT_DUE_AMT_MIN
            if(c1==1):
                data_inst_granted['NextDueInstalmentAmount']=data_inst_granted['NextDueInstalmentAmount'].apply(lambda x : '0' if x == '' else x).astype(float)
                NEXT_DUE_AMT_MIN = data_inst_granted['NextDueInstalmentAmount'].min()
            else : NEXT_DUE_AMT_MIN = -999

            #26.DATE_WORST_TO_ST_AVG
            if(c1==1):
                mtem =[]
                for i in range (0,len(data_inst_granted)):
                    string = data_inst_granted['DateWorstStatus'][i]
                    DateWorstStatus = time.date(int(string[4:8]),int(string[2:4]),int(string[0:2]))
                    mtem.append(month_between(DateWorstStatus,executive_date))
                DATE_WORST_TO_ST_AVG = sum(mtem)/len(mtem)
            else : DATE_WORST_TO_ST_AVG =-999
            #27.RMN_NO_MIN
            if(c1==1):
                RMN_NO_MIN = data_inst_granted['RemainingInstalmentsNumber'].min()
            else : RMN_NO_MIN = -999
            #28.GUA_AMT_SUM
            if(c1==1):
                data_inst_granted['PersonalGuaranteeAmount']=data_inst_granted['PersonalGuaranteeAmount'].apply(lambda x : 0 if x == '' else x).astype(float)
                GUA_AMT_SUM = data_inst_granted.loc[data_inst_granted['PersonalGuaranteeAmount'] > 0,'PersonalGuaranteeAmount'].count()
            else : GUA_AMT_SUM = -999
            DATA_RESULT['GENDER'] = GENDER
            DATA_RESULT['AGE']=AGE
            DATA_RESULT['TOTAL_PAID_INST_MAX']=TOTAL_PAID_INST_MAX
            DATA_RESULT['DAYS_PAYMENT_DELAY_MAX']=DAYS_PAYMENT_DELAY_MAX
            DATA_RESULT['REMAIN_AMT_PCT']=REMAIN_AMT_PCT
            DATA_RESULT['INTEREST_RATE_MIN']=INTEREST_RATE_MIN
            DATA_RESULT['NUMBEROFREQUESTED']=NUMBEROFREQUESTED
            DATA_RESULT['NUMBEROFREFUSED']=NUMBEROFREFUSED
            DATA_RESULT['NUMBEROFTERMINATED']=NUMBEROFTERMINATED
            DATA_RESULT['NUMBEROFLIVING']=NUMBEROFLIVING
            DATA_RESULT['WORSTRECENTSTATUS']=WORSTRECENTSTATUS
            DATA_RESULT['NUMBER_TCTD_GR']=NUMBER_TCTD_GR
            DATA_RESULT['NB_6_VER2']=NB_6_VER2
            DATA_RESULT['REMAIN_AMT_PCT_CARDS']=REMAIN_AMT_PCT_CARDS
            DATA_RESULT['REMAINING_AMOUNT_CARDS']=REMAINING_AMOUNT_CARDS
            DATA_RESULT['MAXNROFDAYSOFPAYMENTDELAY_CARDS']=MAXNROFDAYSOFPAYMENTDELAY_CARDS
            DATA_RESULT['STATUS1_CONS_6M_CARDS']=STATUS1_CONS_6M_CARDS
            DATA_RESULT['STATUS1_CONS_36M_CARDS']=STATUS1_CONS_36M_CARDS
            DATA_RESULT['DEFAULT_CONS_3M_CARDS']=DEFAULT_CONS_3M_CARDS
            DATA_RESULT['DEFAULT_CONS_36M_CARDS']=DEFAULT_CONS_36M_CARDS
            DATA_RESULT['STATUS1_CONS_36M_INSTALLMENT']=STATUS1_CONS_36M_INSTALLMENT
            DATA_RESULT['DEFAULT_CONS_18M_INSTALLMENT']=DEFAULT_CONS_18M_INSTALLMENT
            DATA_RESULT['NO_MONTH_MAX_DEFAULT_MIN']=NO_MONTH_MAX_DEFAULT_MIN
            DATA_RESULT['MAX_LV_DEFAULT_MIN']=MAX_LV_DEFAULT_MIN
            DATA_RESULT['NEXT_DUE_AMT_MIN']=NEXT_DUE_AMT_MIN
            DATA_RESULT['DATE_WORST_TO_ST_AVG']=DATE_WORST_TO_ST_AVG
            DATA_RESULT['RMN_NO_MIN']=RMN_NO_MIN
            DATA_RESULT['GUA_AMT_SUM']=GUA_AMT_SUM
            DATA_RESULT['NOTES'] = 'HIT_PCB'
        result = pd.DataFrame([DATA_RESULT],columns=DATA_RESULT.keys()) 
        
    
        
        

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
        output=data['SCORE'].item()
        result['EVENT_DATE'] = time.datetime.now() 
        result['SCORE']=output
        #conectdatabase
        
        credentials = service_account.Credentials.from_service_account_file("bu_risk_serviceaccount.json")
        
        table_id='bu_risk.por_nda_result_score_pcb_model'
        project_id = 'evnfc-bigdata'
        result.to_gbq(table_id, project_id=project_id, if_exists='append', progress_bar=True, credentials=credentials)
        
        
        # print('Hello world',pd.Series(data['SCORE']))
        
        return jsonify({"output" : "OK"})

if __name__ == '__main__':
    bst = xgb.Booster()
    bst.load_model('pcb_ec_v15.model')
    app.run(debug=False, host='0.0.0.0')


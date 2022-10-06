
# coding: utf-8

# In[ ]:
from flask import Flask, request, redirect, url_for, flash, jsonify
import numpy as np
import xgboost as xgb
import pandas as pd
import json
import datetime as time
from operator import attrgetter
import pandas_gbq
# from flask_cors import CORS
import os

app = Flask(__name__)
# CORS(app,resources={r"/pcbmodel/*": {"origins": "*"}})


@app.route('/')
def success():
      return '<h1>Hello PCB Model Score</h1>'
    

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
        error = "Error"
        
        DATA_RESULT ={}
        
        CONTRACT_NUMBER = contract_number
        EXECUTIVE_DATE = executive_date
        GENDER = -999
        AGE =-999
        TOTAL_PAID_INST_MAX =-999
        DAYS_PAYMENT_DELAY_MAX=-999
        REMAIN_AMT_PCT=-999
        INTEREST_RATE_MIN=-999
        NUMBEROFREQUESTED=-999
        NUMBEROFREFUSED=-999
        NUMBEROFTERMINATED=-999
        NUMBEROFLIVING=-999
        WORSTRECENTSTATUS=-999
        NUMBER_TCTD_GR=-999
        NB_6_VER2=-999
        REMAIN_AMT_PCT_CARDS=-999
        REMAINING_AMOUNT_CARDS=-999
        MAXNROFDAYSOFPAYMENTDELAY_CARDS=-999
        STATUS1_CONS_6M_CARDS=-999
        STATUS1_CONS_36M_CARDS=-999
        DEFAULT_CONS_3M_CARDS=-999
        DEFAULT_CONS_36M_CARDS=-999
        STATUS1_CONS_36M_INSTALLMENT=-999
        DEFAULT_CONS_18M_INSTALLMENT=-999
        NO_MONTH_MAX_DEFAULT_MIN=-999
        MAX_LV_DEFAULT_MIN=-999
        NEXT_DUE_AMT_MIN=-999
        DATE_WORST_TO_ST_AVG=-999
        RMN_NO_MIN=-999
        GUA_AMT_SUM=-999
        print(dt.find(error))
        #Nếu không error
        if dt.find(error) == -1 :
            print('OUTPUT OUTPUT1')
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
            t1 = t2 = t3 = t4 = t5 = t6 = t7 = t8 = t9 = t10 = t11 = 0
            for i in col_name:
                if i.find(GENDER_col)!=-1:
                    gender_col_name = i 
                    t1 = t1 + 1
                if i.find(DOB_col)!=-1:
                    dob_col_name = i
                    t2 = t2 + 1
                if i.find(Request_col)!=-1:
                    request_col_name = i
                    t3 = t3 + 1
                if i.find(inst_living_col)!=-1:
                    living_col_name = i
                    t4 = t4 + 1
                if i.find(Refused_col)!=-1:
                    refused_col_name = i 
                    t5 = t5 + 1
                if i.find(inst_terminated_col)!=-1:
                    terminated_col_name = i 
                    t6 = t6 + 1
                if i.find(WorstRecentStatus_col)!=-1:
                    worstrecentstatus_col_name = i
                    t7 = t7 + 1
                if i.find(noinst_living_col)!=-1:
                    nonins_living_col_name = i
                    t8 = t8 + 1
                if i.find(noinst_terminated_col)!=-1:
                    noinst_terminated_col_name = i 
                    t9 = t9 + 1
                if i.find(card_living_col)!=-1:
                    card_living_col_name = i
                    t10 = t10 + 1
                if i.find(card_terminated_col)!=-1:
                    card_terminated_col_name = i 
                    t11 = t11 + 1
            #PCB_FLAG
            a1=a2=a3=a4=a5=a6=0
            if (t4 == 1) :a1 = data_full[living_col_name].apply(lambda x : 0 if x == '' else x).astype(float)[0]
            if (t6 == 1) :a2 = data_full[terminated_col_name].apply(lambda x : 0 if x == '' else x).astype(float)[0]    
            if (t8 == 1) :a3 = data_full[nonins_living_col_name].apply(lambda x : 0 if x == '' else x).astype(float)[0]
            if (t9 == 1) :a4 = data_full[noinst_terminated_col_name].apply(lambda x : 0 if x == '' else x).astype(float)[0]
            if (t10 == 1) :a5 = data_full[card_living_col_name].apply(lambda x : 0 if x == '' else x).astype(float)[0]
            if (t11 == 1) :a6 = data_full[card_terminated_col_name].apply(lambda x : 0 if x == '' else x).astype(float)[0]
            s = a1+a2+a3+a4+a5+a6 
            if s > 0 :
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
                if(t1 == 1):
                    if (data_full[gender_col_name][0]=='F'):
                        GENDER = 1    
                    else :
                        GENDER = 0
                if(t2 == 1):
                    string = data_full[dob_col_name][0]
                    DOB = time.date(int(string[4:8]),int(string[2:4]),int(string[0:2]))
                    AGE = int(month_between(DOB,executive_date)/12)+1

                if(c1==1):
                    data_inst_granted['TotalNumberOfInstalments'] = data_inst_granted['TotalNumberOfInstalments'].apply(lambda x : 0 if x == '' else x).astype(float)
                    data_inst_granted['RemainingInstalmentsNumber'] = data_inst_granted['RemainingInstalmentsNumber'].apply(lambda x : 0 if x == '' else x).astype(float)
                    data_inst_granted['TOTAL_PAID_INST'] = data_inst_granted['TotalNumberOfInstalments']-data_inst_granted['RemainingInstalmentsNumber']
                    TOTAL_PAID_INST_MAX = data_inst_granted['TOTAL_PAID_INST'].max()

                    data_inst_granted['DAYS_PAYMENT_DELAY_MAX']= data_inst_granted['MaxNrOfDaysOfPaymentDelay']
                    DAYS_PAYMENT_DELAY_MAX = data_inst_granted['DAYS_PAYMENT_DELAY_MAX'].max()

                    data_inst_granted['RemainingInstalmentsAmount']=data_inst_granted['RemainingInstalmentsAmount'].apply(lambda x : 0 if x == '' else x).astype(float)
                    data_inst_granted['UnpaidDueInstalmentsAmount']=data_inst_granted['UnpaidDueInstalmentsAmount'].apply(lambda x : 0 if x == '' else x).astype(float)
                    data_inst_granted['TotalAmount'] = data_inst_granted['TotalAmount'].apply(lambda x : 0 if x == '' else x).astype(float)
                    data_inst_granted['REMAIN_AMT_PCT'] = (data_inst_granted['RemainingInstalmentsAmount']+data_inst_granted['UnpaidDueInstalmentsAmount'])
                    REMAIN_AMT_PCT = data_inst_granted['REMAIN_AMT_PCT'].sum()/data_inst_granted['TotalAmount'].sum()


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

                    a=data_inst_granted['CommonData.EncryptedFICode'].unique()
                    NUMBER_TCTD_GR = len(a)
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

                    data_profile_inst['DATE'] = data_profile_inst['ReferenceYear'].astype(int)
                    for i in range(0,len(data_profile_inst)):
                        Date = time.date(int(data_profile_inst['ReferenceYear'][i]),int(data_profile_inst['ReferenceMonth'][i]),1)
                        data_profile_inst['DATE'][i] = month_between(Date,executive_date)
                    data_temp4 = data_profile_inst.loc[(data_profile_inst['DATE']<=36)&(data_profile_inst['Status']=='1'),'Status']
                    STATUS1_CONS_36M_INSTALLMENT = data_temp4.count()

                    data_temp5 = data_profile_inst.loc[(data_profile_inst['DATE']<=18)&(data_profile_inst['Default']>'0'),'Default']
                    DEFAULT_CONS_18M_INSTALLMENT = data_temp5.count()

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
                    MAX_LV_DEFAULT_MIN = df2['Default'].min()
                    data_inst_granted['NextDueInstalmentAmount']=data_inst_granted['NextDueInstalmentAmount'].apply(lambda x : '0' if x == '' else x).astype(float)
                    NEXT_DUE_AMT_MIN = data_inst_granted['NextDueInstalmentAmount'].min()
                    mtem =[]
                    for i in range (0,len(data_inst_granted)):
                        string = data_inst_granted['DateWorstStatus'][i]
                        DateWorstStatus = time.date(int(string[4:8]),int(string[2:4]),int(string[0:2]))
                        mtem.append(month_between(DateWorstStatus,executive_date))
                    DATE_WORST_TO_ST_AVG = sum(mtem)/len(mtem)
     
           
                    RMN_NO_MIN = data_inst_granted['RemainingInstalmentsNumber'].min()
               
       
       
                    data_inst_granted['PersonalGuaranteeAmount']=data_inst_granted['PersonalGuaranteeAmount'].apply(lambda x : 0 if x == '' else x).astype(float)
                    GUA_AMT_SUM = data_inst_granted.loc[data_inst_granted['PersonalGuaranteeAmount'] > 0,'PersonalGuaranteeAmount'].count()
             
                if(t3 == 1):
                    data_full[request_col_name] = data_full[request_col_name].apply(lambda x : -999 if x == '' else x).astype(float)
                    NUMBEROFREQUESTED = data_full[request_col_name][0]
                if(t5 == 1):   
                    data_full[refused_col_name] = data_full[refused_col_name].apply(lambda x : -999 if x == '' else x).astype(float)
                    NUMBEROFREFUSED = data_full[refused_col_name][0]
                if(t6 == 1):
                    data_full[terminated_col_name] = data_full[terminated_col_name].apply(lambda x : -999 if x == '' else x).astype(float)
                    NUMBEROFTERMINATED = data_full[terminated_col_name][0]
                if(t4 == 1):
                    data_full[living_col_name] = data_full[living_col_name].apply(lambda x : -999 if x == '' else x).astype(float)
                    NUMBEROFLIVING = data_full[living_col_name][0]
                if(t7 == 1):
                    data_full[worstrecentstatus_col_name] = data_full[worstrecentstatus_col_name].apply(lambda x : -999 if x == '' else x).astype(float)
                    WORSTRECENTSTATUS = data_full[worstrecentstatus_col_name][0]

                if (c2==1):
                    data_card_granted['ResidualAmount']=data_card_granted['ResidualAmount'].apply(lambda x : 0 if x == '' else x).astype(float)
                    data_card_granted['UnpaidDueInstalmentsAmount']=data_card_granted['UnpaidDueInstalmentsAmount'].apply(lambda x : 0 if x == '' else x).astype(float)
                    data_card_granted['MaxResidualAmount']=data_card_granted['MaxResidualAmount'].apply(lambda x : 0 if x == '' else x).astype(float)
                    data_card_granted['REMAIN_AMT_PCT_CARDS'] = (data_card_granted['ResidualAmount'] + data_card_granted['UnpaidDueInstalmentsAmount'])/data_card_granted['MaxResidualAmount']
                    REMAIN_AMT_PCT_CARDS = data_card_granted['REMAIN_AMT_PCT_CARDS'].max()
                    data_card_granted['REMAINING_AMOUNT_CARDS'] = data_card_granted['ResidualAmount'] + data_card_granted['UnpaidDueInstalmentsAmount']
                    REMAINING_AMOUNT_CARDS = data_card_granted['REMAINING_AMOUNT_CARDS'].max()
                    data_card_granted['MAXNROFDAYSOFPAYMENTDELAY_CARDS']=data_card_granted['MaxNrOfDaysOfPaymentDelay'].apply(lambda x : -999 if x == '' else x).astype(float)
                    MAXNROFDAYSOFPAYMENTDELAY_CARDS = data_card_granted['MAXNROFDAYSOFPAYMENTDELAY_CARDS'].max()
                    data_profile_card['DATE'] = 0
                    for i in range(0,len(data_profile_card)):
                        Date = time.date(int(data_profile_card['ReferenceYear'][i]),int(data_profile_card['ReferenceMonth'][i]),1)
                        data_profile_card['DATE'][i] = month_between(Date,executive_date)
                    data_temp1 = data_profile_card.loc[(data_profile_card['DATE']<=6)&(data_profile_card['Status']=='1'),'Status']
                    STATUS1_CONS_6M_CARDS = data_temp1.count()
                    data_temp2 = data_profile_card.loc[(data_profile_card['DATE']<=36)&(data_profile_card['Status']=='1'),'Status']
                    STATUS1_CONS_36M_CARDS = data_temp2.count()

                    data_temp3 = data_profile_card.loc[(data_profile_card['DATE']<=3)&(data_profile_card['Default']>'0'),'Default']
                    DEFAULT_CONS_3M_CARDS = data_temp3.count()
                    data_temp3 = data_profile_card.loc[(data_profile_card['DATE']<=36)&(data_profile_card['Default']>'0'),'Default']
                    DEFAULT_CONS_36M_CARDS = data_temp3.count()
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
                    ,'GUA_AMT_SUM':float(GUA_AMT_SUM)}
                data = pd.DataFrame(data,index=[0])
                data_score=xgb.DMatrix(data)
                pre_prod = bst.predict(data_score)
                data['PD'] = pd.Series(pre_prod[0])
                a= 427.928058181122
                b = 75.41697308638469
                data['SCORE'] = np.round((a+b* np.log((1-pre_prod)/pre_prod)),0)
                output=data['SCORE'].item()
                note = 'HITPCB'
                print('OUTPUT OUTPUT',output)
                #conectdatabase
            else : 
                print('OUTPUT OUTPUT2')
                note = 'NON_HITPCB'
                output = None
        else : 
            print('OUTPUT OUTPUT3')
            note='ERROR'
            output = None

        


        
        DATA_RESULT ={}
        
        DATA_RESULT['CONTRACT_NUMBER'] = CONTRACT_NUMBER
        DATA_RESULT['EXECUTIVE_DATE'] = EXECUTIVE_DATE
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
        DATA_RESULT['NOTES'] = note
        DATA_RESULT['SCORE'] = output
        DATA_RESULT['EVENT_DATE'] = time.datetime.now()
        result = pd.DataFrame([DATA_RESULT],columns=DATA_RESULT.keys())
        result = result.astype(str) 
        cols = ['TOTAL_PAID_INST_MAX', 'DAYS_PAYMENT_DELAY_MAX', 'REMAIN_AMT_PCT',
       'INTEREST_RATE_MIN', 'NUMBEROFREQUESTED', 'NUMBEROFREFUSED',
       'NUMBEROFTERMINATED', 'NUMBEROFLIVING', 'WORSTRECENTSTATUS',
       'NUMBER_TCTD_GR', 'NB_6_VER2', 'REMAIN_AMT_PCT_CARDS',
       'REMAINING_AMOUNT_CARDS', 'MAXNROFDAYSOFPAYMENTDELAY_CARDS',
       'STATUS1_CONS_6M_CARDS', 'STATUS1_CONS_36M_CARDS',
       'DEFAULT_CONS_3M_CARDS', 'DEFAULT_CONS_36M_CARDS',
       'STATUS1_CONS_36M_INSTALLMENT', 'DEFAULT_CONS_18M_INSTALLMENT',
       'NO_MONTH_MAX_DEFAULT_MIN', 'MAX_LV_DEFAULT_MIN', 'NEXT_DUE_AMT_MIN',
       'DATE_WORST_TO_ST_AVG', 'RMN_NO_MIN', 'GUA_AMT_SUM', 'GENDER', 'AGE']
        
        to_date = DATA_RESULT['EVENT_DATE']
        if(to_date.month)<10 :month = '0'+ str(to_date.month)
        else : month = str(to_date.month)
        if(to_date.day)  <10 : day ='0' + str(to_date.day)
        else :  day = str(to_date.day)
        file = 'result_'+str(to_date.year)+str(month)+str(day)


        list_of_files=os.listdir()
        # list_of_files.append('files.csv')
        df =pd.DataFrame({'files':list_of_files})
        c = 0
        for i in df['files'].unique():
            if(i.find(file) != -1) : 
                c=c+1    
        if(c==1) : result.to_csv("./" + str(file)+str('.csv'), sep=',',mode = 'a',index=False,header=False)
        else: result.to_csv("./" + str(file)+str('.csv'), sep=',',mode = 'a',index=False,header=True)
        result_dict = result.to_dict('r')
        # df = DATA_RESULT[cols]
        # print(df)
        # json_str = json.dumps(DATA_RESULT)
    
        # return jsonify(output = result_dict[0])
        return jsonify({"contract_number" : CONTRACT_NUMBER,"Score" : output , "Type" : note,"FeaturesJson" : result_dict[0]})

if __name__ == '__main__':
    bst = xgb.Booster()
    bst.load_model('pcb_ec_v15.model')
    app.run(debug=False, host='0.0.0.0')


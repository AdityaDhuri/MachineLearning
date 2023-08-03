# go for inv_name then list all the scheme and use single schme at a time

from dbfread import DBF
import pandas as pd
import numpy as np
from datetime import datetime
import glob
import mysql.connector as connection
import mysql.connector as connection
from datetime import date, datetime, timedelta
import mysql.connector

from sqlalchemy import create_engine

cnx = mysql.connector.connect(
    host="localhost",
    user="root",
    password="12345",
    database="sapient"
)
engine = create_engine('mysql+pymysql://root:12345@localhost/sapient')

####################### ADDITION
Purchase_txn_master_qry = (
    "select TRDESC,TREATMENT_TO_UNITS from txn_type_master where rta = 'KFIN'  and treatment_to_units in ('Addition');")
# print(Purchase_txn_master_qry)
P_df = pd.read_sql(Purchase_txn_master_qry, cnx)
P_TRDESC_list = P_df['TRDESC'].tolist()
# print(P_TRDESC_list)

####################### SUBTRACTION
Sale_txn_master_qry = (
    "select TRDESC,TREATMENT_TO_UNITS from txn_type_master where rta = 'KFIN'  and treatment_to_units in ('Subtraction');")
# print(Sale_txn_master_qry)
s_df = pd.read_sql(Sale_txn_master_qry, cnx)
S_TRDESC_list = s_df['TRDESC'].tolist()
# print(S_TRDESC_list)
# print(len(S_TRDESC_list))

######################## SQL QUERY
sql_txn_master = "select TRDESC,TREATMENT_TO_UNITS,SAP_TRXN_TYPE from txn_type_master where rta = 'KFIN'  and treatment_to_units in ('Addition','Subtraction');"
sql_txn_master_df = pd.read_sql(sql_txn_master, cnx)
sql_txn_master_df.rename(columns={'TRDESC': 'RTA_TRXN_TYPE'}, inplace=True)
sql_txn_master_list = sql_txn_master_df['RTA_TRXN_TYPE'].tolist()
# print(sql_txn_master_df)
######################## READING CSV
files = glob.glob("E:\\Sapient Files\\KARVY-StampDuty\\Karvy 01.01.03 To 31.12.03.dbf")
frame = []

for file in files:
    dbf1 = DBF(file, encoding='ISO-8859-1')
    frame.append(pd.DataFrame(iter(dbf1)))
daf = pd.concat(frame)
# print(daf.head())
daf.rename(columns = {'INVNAME':'INV_NAME','PAN1':'PAN','TD_FUND':'AMC_CODE','FMCODE':'SCH_CODE','FUNDDESC':'RTA_SCHEME',
                      'TD_ACNO':'FOLIO_NO','TD_TRTYPE':'TR_TYPE','TRNMODE':'TRXN_MODE','TD_PURRED':'TD_PURRED','TRFLAG':'TRFLAG',
                      'TRDESC':'RTA_TRXN_TYPE','TD_TRNO':'TRXN_NO','TD_TRDT':'TRADE_DATE','TD_PRDT':'POST_DT','LOAD1':'LOAD_VAL',
                      'TD_POP':'NAV','TD_UNITS':'UNITS','TD_AMT':'RTA_AMT','STT':'STT',
                      'TDSAMOUNT':'TDS','NCTREMARKS':'TRXN_DETAIL','CITYCATEG5':'CITY_CAT','STATUS':'TAX_STATUS','DPID':'DP_ID',
                      'CLIENTID':'CLIENT_ID','PRCODE1':'SRC_TRGT_SCHEME','SFUNDDT':'SRC_TRGT_SCHEME_DT',
                      'TD_PTRNO':'SRC_TRGT_SCHEME_TRXN','PURDATE':'SRC_TRGT_SCHEME_PURDT','PURUNITS':'SRC_TRGT_SCHEME_PURUNIT',
                      'PURAMT':'SRC_TRGT_SCHEME_PURAMT','SIPREGDT':'SIP_REGN_DT','DIVOPT':'DIV_OPTION','EUIN':'EUIN',
                      'SUBARNCODE':'ARN_ASSO','TD_BROKER':'ASSO_CODE','BRANCHCODE':'BRANCHCODE','INWARDNUM0':'USRTRXNO','STAMPDUTY':'STAMP',
                      'TD_AGENT':'ARN_MAIN','CRDATE':'FILE_DATE'
                }, inplace = True)
# extra column
# print(daf.head())
#######################  DROP DUPLICATES
df = daf.drop_duplicates()

df_normal = df[df.RTA_TRXN_TYPE.isin(sql_txn_master_list)]

ongoing_df = df_normal[df_normal['UNITS'] != 0.0]

##################### MERGE
result_df = pd.merge(ongoing_df, sql_txn_master_df, on='RTA_TRXN_TYPE', how='inner')

result_df_txn_type = result_df['RTA_TRXN_TYPE']
result_df_txn_type = pd.DataFrame(result_df_txn_type)
result_df_txn_type2 = result_df['UNITS']
result_df_txn_type2 = pd.DataFrame(result_df_txn_type2)
result_df_txn_type.rename(columns={'RTA_TRXN_TYPE': 'RTA_TRXN_TYPE_'}, inplace=True)
result_df_txn_type2.rename(columns={'UNITS': 'UNIT'}, inplace=True)
result_df = pd.concat([result_df, result_df_txn_type, result_df_txn_type2], axis=1)

print("len of result_df", len(result_df))
########################
result_df['RTA_TRXN_TYPE_'] = np.where(result_df.RTA_TRXN_TYPE_.isin(P_TRDESC_list), 'Purchase', result_df.RTA_TRXN_TYPE_)

result_df['RTA_TRXN_TYPE_'] = np.where(result_df.RTA_TRXN_TYPE_.isin(S_TRDESC_list), 'Sales', result_df.RTA_TRXN_TYPE_)


result_df = result_df[result_df['UNITS']!= 0.0]
# print(result_df[result_df['UNITS']!=0])

######################3 CAPITAL GAIN ###############################

buy_list = pd.DataFrame()
sell_list = pd.DataFrame()
print(result_df.info())

result_df = pd.DataFrame(result_df)

buy_list= buy_list.append(result_df[result_df['RTA_TRXN_TYPE_'] == 'Purchase'])
sell_list= sell_list.append(result_df[result_df['RTA_TRXN_TYPE_'] == 'Sales'])
buy_list = pd.DataFrame(buy_list)
sell_list = pd.DataFrame(sell_list)
# for x in range(0, len(result_df)):
#     if (result_df[x][84] == 'Purchase'):
#         buy_list.append(result_df[x])
#     else:
#         sell_list.append(result_df[x])
#
# print('BUY LIST',buy_list)

print(sell_list.head())
print(buy_list.head())


# units = 0
avg_sell_price = 0
amount = 0
types = ['sell']

types = pd.DataFrame(types)
amount = []
units = []
units = sell_list['UNITS'].sum()
amount = sell_list['RTA_AMT'].sum()

# units = pd.DataFrame(units)
# amount = pd.DataFrame(amount)
print('AVERAGE UNITS : ',units)
print('AVERAGE AMOUNT : ',amount)
# for x in sell_list:
#     units += x[17]
#     amount += x[18]
#
# print(units)
# print(amount)

avg_sell_price = amount / units
print(''' ##################################
############################
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    ''')
print(buy_list.info())
print(sell_list.info())
print('avg_sell_price',avg_sell_price)
print('types',types)
print('units',units)
print('amount',amount)
final_sell_list = pd.concat([units,avg_sell_price,amount,types],axis=1)
# final_sell_list.append(avg_sell_price)
# final_sell_list.append(amount)
# final_sell_list.append(types)
#
# print(final_sell_list)
# final_sell_list = pd.DataFrame(final_sell_list)
# print(final_sell_list.info())
##############3done
pur_amount = 0
while units < 0:
    if (buy_list['UNITS'] > -units):
        buy_list['UNITS'] = buy_list['UNITS'] + units
        buy_list['RTA_AMT'] = buy_list['UNITS'] * buy_list['NAV']
        pur_amount += -units * buy_list['NAV']
        print(-units)
        units = 0
    else:
        pur_amount += buy_list['NAV']
        units += buy_list['UNITS']
        print(buy_list[0][2])
        buy_list.pop(0)
print(pur_amount)
# while (final_sell_list[0] < 0):
#     if (buy_list[0][0] > -final_sell_list[0]):
#
#         buy_list[0][0] = buy_list[0][0] + final_sell_list[0]
#         buy_list[0][2] = buy_list[0][0] * buy_list[0][1]
#         pur_amount += -final_sell_list[0] * buy_list[0][1]
#         print(-final_sell_list[0])
#         print(buy_list[0][1])
#         print(-final_sell_list[0] * buy_list[0][1])
#         final_sell_list[0] = 0
#
#     else:
#         pur_amount += buy_list[0][2]
#         final_sell_list[0] += buy_list[0][0]
#         print(buy_list[0][2])
#         buy_list.pop(0)
# print(pur_amount)

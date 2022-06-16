from ast import parse
import pandas as pd
import pymysql
import datetime

db = pymysql.connect(host='', port=, user='', passwd='', db='', charset='')
cur = db.cursor()

path = ''
dfhid = pd.read_excel(path, parse_dates=['orin','orout','opdate'], sheet_name='sheet1')

path = ''
dfcrrt = pd.read_excel(path, skiprows=1, parse_dates=['[간호기록]기록작성일시'])

dfresult = dfhid.copy()
for idx, row in dfhid.iterrows():
    hid = row['hid']
    opdate = row['opdate']
    orin = row['orin']
    orout = row['orout']
    
    # preop_cr
    sql = f'SELECT val FROM labs WHERE hid = {hid} AND parid IN (1725, 303, 462, 560, 671, 1009) AND (dt BETWEEN "{orin}" - INTERVAL 3 MONTH AND "{orin}" ) ORDER BY dt DESC LIMIT 1'
    cur.execute(sql)
    data = cur.fetchone()
    try :
        dfresult.loc[dfresult.index==idx, 'preop_cr'] = data[0]
    except TypeError:
        continue
    
    # preop_aki
    # 수술 전 3개월 이내의 값 중에서, 여러 차례 측정이 이루어진 경우, 2주 interval 이내에 값이 0.3 이상 증가하거나, 1.5배 이상 증가한 경우 1로 표시, 그렇지 않으면 0으로 표시
    sql = f'SELECT dt, val FROM labs WHERE hid = {hid} AND parid IN (1725, 303, 462, 560, 671, 1009) AND (dt BETWEEN "{orin}" - INTERVAL 3 MONTH AND "{orin}" ) ORDER BY dt'
    cur.execute(sql)
    data = cur.fetchall()
    if len(data) > 1:
        for num in range(len(data)-1, 1, -1):
            try:
                if (data[num][0] - data[num-1][0] <= datetime.timedelta(weeks=2)) and ((float(data[num][1]) >= float(data[num-1][1]) + 0.3) or (float(data[num][1]) >= float(data[num-1][1])*1.5)):
                    dfresult.loc[dfresult.index==idx, 'preop_aki'] = 1
            except ValueError:
                continue
        
    # postop_cr
    for term in [2,7]:
        cur = db.cursor()
        sql = f'SELECT MAX(val) FROM labs WHERE hid = {hid} AND parid IN (1725, 303, 462, 560, 671, 1009) AND (dt BETWEEN "{orout}" AND "{orout}" + INTERVAL {term} DAY)'
        cur.execute(sql)
        data = cur.fetchone()
        try :
            dfresult.loc[dfresult.index==idx, f'postop_{term}d_max'] = data[0]
        except TypeError:
            sql = f'SELECT MAX(val) FROM labs WHERE hid = {hid} AND parid IN (1725, 303, 462, 560, 671, 1009) AND (dt BETWEEN "{opdate}" AND "{opdate}" + INTERVAL {term} DAY)'
            cur.execute(sql)
            data = cur.fetchone()
            try: 
                dfresult.loc[dfresult.index==idx, f'postop_{term}d_max'] = data[0]
            except TypeError:
                continue

    # preop_crrt
    data = dfcrrt.loc[(dfcrrt['환자번호']==hid) & (orin-datetime.timedelta(days=90)<=dfcrrt['[간호기록]기록작성일시']) & (dfcrrt['[간호기록]기록작성일시']<=orin)]
    if len(data) >= 2 :
        dfresult.loc[dfresult.index==idx, 'preop_crrt_3months'] = 1
    
    # postop_crrt        
    data = dfcrrt.loc[(dfcrrt['환자번호']==hid) & (orout<=dfcrrt['[간호기록]기록작성일시']) & (dfcrrt['[간호기록]기록작성일시']<=orout+datetime.timedelta(days=7))]
    if len(data) >= 2 :
        dfresult.loc[dfresult.index==idx, 'postop_crrt_7days'] = 1

dfresult.to_excel('', index=False)
import pandas as pd
from datetime import datetime
import os
import shutil
import warnings
warnings.filterwarnings('ignore')

os.chdir('C:/Users/SNUH/Desktop/cpr/')

dfcpr = pd.read_excel('cpr_list.xlsx')
dfcpr.columns = ['환자번호','진료과','CPR 발생일','심정지시간','발생장소']
dfcpr['CPR 발생일'] = pd.to_datetime(dfcpr['CPR 발생일'])

dficu = pd.read_excel('icu_demo_cpr_result_total.xlsx', parse_dates=['bedin', 'bedout'])

dfvf = pd.DataFrame()
for dir, _, files in os.walk('S:/SNUH_Data/Matched/ICU_1hr/'):
    print(dir)
    for file in files:
        if not file.split('.')[1] == 'vital':
            continue
        dt = datetime.strptime(file.split('.')[0][-13:], '%y%m%d_%H%M%S')
        dfvf = dfvf.append({'filename':file, 'filepath':os.path.join(dir,file), 'dt':dt}, ignore_index=True)
    
dfvf.to_csv('dfvf_dataframe.csv', index=False, encoding='utf-8-sig')
dfvf = pd.read_csv('dfvf_dataframe.csv', parse_dates=['dt'])

dfresult = dficu.copy()
for col in dfcpr[['환자번호','CPR 발생일','심정지시간','발생장소']].values: 
    hid = col[0]
    cprdate = col[1]
    cprtime = col[2]
    icu = col[3]

    if icu == 'SICU':
        icu = 'SICU1'

    data = dficu.loc[(dficu['hid']==hid) & (dficu['bedin']<=cprdate) & (cprdate<=dficu['bedout'])]
    
    if len(data) == 1:
        print(hid, data)
        icuinfo = data.values[0][2]
        bedin = data.values[0][5]
        bedout = data.values[0][6]

        results = dfvf.loc[(dfvf['filename'].str.contains(icuinfo)) & (bedin <= dfvf['dt']) & (dfvf['dt'] <= bedout)]
        for result in results.values:
            filename = result[0]
            filepath = result[1]
            # print(icuinfo, result)
            savepath = 'C:/Users/SNUH/Desktop/cpr/vitalfiles/'+filename
            shutil.copyfile(filepath, savepath)

            dt = str(cprdate.date()) + ' ' + str(cprtime)
            dfresult.loc[(dfresult['icuinfo']==icuinfo) & (dfresult['bedin']==bedin), ['cpr', 'cprtime']] = [1, dt]

        # print(dfresult.loc[(dfresult['icuinfo']==icuinfo) & (dfresult['bedin']==bedin), ['cpr', 'cprtime']])

dfresult.to_csv('cpr_result.csv', index=False, encoding='utf-8-sig')
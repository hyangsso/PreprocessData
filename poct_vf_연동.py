from re import L
import vitaldb
import pandas as pd
import os
from joblib import Parallel, delayed
import csv

os.chdir('C:/Users/vitalDB/Desktop/project/')
filelist = ['abga_data_1.xlsx','abga_data_2.xlsx']
POCT_FILENAME = 'poct_data.csv.xz'
POCT_RESULT_NAME = 'poct_result.csv'

USE_MULTIPROCESS = True

def dctime(orin, recorddate, orout):
    if (orin<=recorddate) & (recorddate<=orout):
        return 1
    else:
        return 0 

def dctime_search(mgdata, caseidx):
    mgdata = mgdata.loc[caseidx]
    mgdata['ordata'] = mgdata.apply(lambda x: dctime(x['입실시간'], x['검사시행일'], x['퇴실시간']), axis=1)
    result = mgdata[mgdata['ordata'] == 1]
    
    for row in result.values:
        with open(POCT_RESULT_NAME, 'a', newline='', encoding='utf-8-sig') as f:
            wr = csv.writer(f)
            wr.writerow(row)

def translate_lab(labname):
    return dictlab[labname]

dictlab = {'Lactic acid[POCT, ABGA]': 'Lactic acid',
            'HCO3- [POCT]': 'HCO3-',
            'Hct[POCT, ABGA]': 'Hct',
            'pH[POCT]': 'pH', 
            'pO₂[POCT]': 'pO₂',
            'pCO₂[POCT]': 'pCO₂',
            'O₂SAT[POCT]': 'O₂SAT',
            'Sodium(serum)[POCT, ABGA]': 'Sodium',
            'Potassium(serum)[POCT, ABGA]': 'Potassium',
            'Calcium, ionized [POCT, ABGA]': 'Calcium, ionized',
            'Glucose [POCT, ABGA]': 'Glucose',
            'BE[POCT]': 'BE',
            'CO2, total(serum)[POCT, ABGA]': 'CO2, total',
            'Hb[POCT, ABGA]': 'Hb',
            '[POCT, ABGA]Hct' : 'Hct', 
            '[POCT, ABGA]Sodium': 'Sodium', 
            '[POCT, ABGA]Potassium': 'Potassium',
            '[POCT, ABGA]CO2, total': 'CO2, total',
            '[POCT, ABGA]Calcium, ionized': 'Calcium, ionized',
            '[POCT, ABGA]Glucose': 'Glucose', 
            '[POCT, ABGA]Lactic acid': 'Lactic acid',
            '[POCT, ABGA]Hb': 'Hb',
            'pCO₂[POCT] ( = pCO2[POCT])': 'pCO₂',
            'O₂SAT[POCT] ( = O2SAT[POCT])': 'O₂SAT',
            'pO₂[POCT] ( = pO2[POCT])': 'pO₂',
            'Chloride(serum)[POCT, ABGA]': 'Chloride',
            '[POCT, ABGA]Chloride': 'Chloride'
            }

if os.path.exists(POCT_FILENAME):
    dfdata = pd.read_csv(POCT_FILENAME,  compression='xz', encoding='utf-8-sig', parse_dates=['검사시행일'])
    print(f'using poct file: {POCT_FILENAME}')
else:
    dfdata = pd.DataFrame()
    for filename in filelist:
        dfxl = pd.read_excel(filename, skiprows=1, sheet_name=None)
        dfxl = pd.concat([value.assign(sheet_source=key) for key,value in dfxl.items()], ignore_index=True)
        
        dfdata = dfdata.append(dfxl, ignore_index=True)
    
    dfdata = dfdata[['환자번호','검사시행일','검사코드','검사세부항목명','검사결과']]
    dfdata.to_csv(POCT_FILENAME, index=False, encoding='utf-8-sig')

if not os.path.exists(POCT_RESULT_NAME):
    print('reading file...')
    dfor = pd.read_excel('POCT_20221012.xlsx', sheet_name='vital list_202209', usecols=['index','환자번호','수술일자','입실시간','퇴실시간','파일명'], parse_dates=['입실시간','퇴실시간']).drop_duplicates(['환자번호','파일명'])

    mgdata = pd.merge(dfor, dfdata, how='left', on='환자번호')

    listidx = list(mgdata.index)
    n = len(mgdata)//10000 
    idxes = [listidx[i * n:(i + 1) * n] for i in range((len(listidx) + n - 1) // n )]
        
    print('merging data...')
    with open(POCT_RESULT_NAME, 'a', newline='', encoding='utf-8-sig') as f:
        wr = csv.writer(f)
        wr.writerow(mgdata.columns)
    if USE_MULTIPROCESS:
        Parallel(os.cpu_count()-5)(delayed(dctime_search)(mgdata, idx) for idx in idxes)
    else:
        for idx in idxes: dctime_search(idx)
     
result = pd.read_csv(POCT_RESULT_NAME)
result.columns = ['수술일자', '환자번호', '입실시간', '퇴실시간', '파일명', '검사시행일', '검사코드','검사세부항목명','검사결과','True']
result['index'] = result.index
result['labname'] = result['검사세부항목명'].apply(translate_lab)
print(result)

dfresult = pd.DataFrame(columns=['case_index','lab_index','환자번호','수술일자','파일명','Sampling time']+list({name for name in dictlab.values()}))
filelist = result[['수술일자','환자번호','파일명','index']].drop_duplicates().values
count = 0
try:
    for row in filelist:
        opdate = row[0]
        hid = row[1]
        filename = row[2]
        case_idx = row[3]
        data = result.loc[result['파일명']==filename].sort_values(by='검사시행일')
        timelist = list(data['검사시행일'].unique())
        for lab_idx, time in enumerate(timelist):
            resultlist = data.loc[data['검사시행일']==time][['labname','검사결과']].drop_duplicates().transpose()
            resultlist.columns = resultlist.iloc[0]
            resultlist = resultlist.iloc[1].append(pd.Series({'case_index':case_idx, 'lab_index':lab_idx+1, '환자번호':hid,'수술일자':opdate,'파일명':filename, 'Sampling time':time}))
            dfresult = dfresult.append(resultlist.T, ignore_index=True)
        count += 1
        if count%1000 == 0 :
            print(f'{count}/{len(filelist)}...')
except:
    print(data)
    print(resultlist)
    dfresult.to_csv('except_test.csv', index=False, encoding='utf-8-sig') 
dfresult.to_csv('poct_preprocessing_result.csv', index=False, encoding='utf-8-sig')
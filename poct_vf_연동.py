import vitaldb
import pandas as pd
import os
from joblib import Parallel, delayed
import csv
import warnings
warnings.filterwarnings(action='ignore')

os.chdir('C:/Users/vitalDB/Desktop/project/')
filelist = ['abga_data_por.xlsx'] # POCT data files
POCT_FILENAME = 'poct_data_por.csv.xz' # merged with POCT data files
PREPROCESSED_FILENAME = 'poct_data_final.csv.xz' # POCT preprocessed filename 
POCT_RESULT_FILENAME = 'poct_preprocessing_result_total.csv' # POCT result filename
HID_FILENAME = 'POCT_20221020.xlsx' # hid information filename
FINAL_RESULT_FILENAME = 'poct_vf_add_result_221101.csv' # final result filename

USE_MULTIPROCESS = True # if you want to multiprocess, True
MAKE_POCTFILE = False # if you make new poct file, True 
PREPROCESS = False # if there is no preprocessed file, True 
RPOCEED_FILE = False # if you want to continue with the interrupted file, True

root_dir = 'E:/SynologyDrive/OR_matched/'

# find the path of the file
def print_files_in_dir(root_dir, filename):
    for root, _, file in os.walk(root_dir):
        if filename in file:
            return os.path.join(root, filename).replace('\\','/')

# orin <= recorddate and recorddate <= orout
def dctime(orin, recorddate, orout):
    if (orin<=recorddate) & (recorddate<=orout):
        return 1
    else:
        return 0

# add data to .csv 
def dctime_search(mgdata, caseidx):
    mgdata = mgdata.loc[caseidx]
    mgdata['ordata'] = mgdata.apply(lambda x: dctime(x['입실시간'], x['검사시행일'], x['퇴실시간']), axis=1)
    result = mgdata[mgdata['ordata'] == 1]
    
    for row in result.values:
        with open(PREPROCESSED_FILENAME, 'a', newline='', encoding='utf-8-sig') as f:
            wr = csv.writer(f)
            wr.writerow(row)

# change the test lab name to fit the format
def translate_lab(labname):
    return dictlab[labname]

# add the track value of the vitalfile at the same time as 'Sampling time'
def add_vital_track(dfresult, filename):
    filepath = print_files_in_dir(root_dir, filename)
        
    try:
        vf = vitaldb.VitalFile(filepath)
    except:
        print(f'error file:{filepath}')
        pass
    else:
        vf = vf.to_pandas(track_names=trklist, interval=1, return_datetime=True).fillna(method='ffill', limit=300)
        
        filedata = dfresult.loc[dfresult['파일명']==filename, ['case_index', 'lab_index', 'Sampling time']].values
        for row in filedata:
            case_idx = row[0]
            lab_idx = row[1]
            dt = row[2]
            
            vfresult = pd.DataFrame(columns=cols)
            
            vf['Time'] = pd.to_datetime(vf['Time'].astype(str).str.split('.').str[0])
            result = vf.loc[vf['Time']==dt].drop('Time', axis=1)
            if not result.empty:
                result['case_index'] = case_idx
                result['lab_index'] = lab_idx
                result['Sampling time'] = dt
                    
                vfresult = vfresult.append(result, ignore_index=True)
                
                with open(FINAL_RESULT_FILENAME, 'a', newline='', encoding='utf-8-sig') as f:
                    wr = csv.writer(f)
                    wr.writerow(vfresult.values[0])

# lab dictionary
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

if MAKE_POCTFILE:
    # merged POCT raw files
    if not os.path.exists(POCT_FILENAME):
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
    # 
    if not os.path.exists(PREPROCESSED_FILENAME):
        print('reading file...')
        dfor = pd.read_excel(HID_FILENAME, sheet_name='vital list_202209', usecols=['index','환자번호','수술일자','입실시간','퇴실시간','파일명'], parse_dates=['입실시간','퇴실시간']).drop_duplicates(['환자번호','파일명'])

        mgdata = pd.merge(dfor, dfdata, how='left', on='환자번호')

        listidx = list(mgdata.index)
        n = len(mgdata)//10000 
        idxes = [listidx[i * n:(i + 1) * n] for i in range((len(listidx) + n - 1) // n )]
            
        print('merging data...')
        with open(PREPROCESSED_FILENAME, 'a', newline='', encoding='utf-8-sig') as f:
            wr = csv.writer(f)
            wr.writerow(mgdata.columns)
        if USE_MULTIPROCESS:
            Parallel(os.cpu_count()-5)(delayed(dctime_search)(mgdata, idx) for idx in idxes)
        else:
            for idx in idxes: dctime_search(mgdata,idx)

if PREPROCESS:
    print(f'start preprocessing... using file: {PREPROCESSED_FILENAME}')
    result = pd.read_csv(PREPROCESSED_FILENAME, dtype={'검사결과':object})
    result['index'] = result['Unnamed: 0'] 
    # result.columns = ['수술일자', '환자번호', '입실시간', '퇴실시간', '파일명', '검사시행일', '검사코드','검사세부항목명','검사결과','True']
    # result['index'] = result.index
    result['labname'] = result['검사세부항목명'].apply(translate_lab)
    result['검사결과'] = pd.to_numeric(result['검사결과'], errors='coerce')
    result = result[result['검사결과'].notnull()]
    print(result)

    filelist = result[['수술일자','환자번호','파일명','index']].drop_duplicates().values
    # save result file
    if RPOCEED_FILE:
        dfresult = pd.read_csv('except_test.csv')
        exfilelist = list(set(dfresult['파일명']))
        filelist = [x for x in filelist if x[2] not in exfilelist]
        print(f'start filename: {filelist[0][2]}')
    else: 
        print('new result start...')
        dfresult = pd.DataFrame(columns=['case_index','lab_index','환자번호','수술일자','파일명','Sampling time']+list({name for name in dictlab.values()}))

    count = 0
    try:
        print(f'total cases: {len(filelist)}')
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
        # delete error vitalfile row 
        delidx = dfresult[dfresult['파일명']==filename].index
        dfresult = dfresult.drop(delidx)
        dfresult.to_csv('except_test.csv', index=False, encoding='utf-8-sig') 

    else:
        dfresult.to_csv(POCT_RESULT_FILENAME, index=False, encoding='utf-8-sig')
        quit()
        
vitaldb.api.login('','','')

dfresult = pd.read_csv(POCT_RESULT_FILENAME)
dfresult['Sampling time'] = dfresult['Sampling time'].astype('datetime64[s]').apply(lambda x: x.replace(microsecond=0))

trklist = ['Bx50/AGENT_ET',
        'Bx50/AGENT_FI',
        'Bx50/AGENT_IN',
        'Bx50/AGENT_NAME',
        'Bx50/AMB_PRES',
        'Bx50/ART_DBP',
        'Bx50/ART_MBP',
        'Bx50/ART_SBP',
        'Bx50/AWP',
        'Bx50/BT1',
        'Bx50/CO2',
        'Bx50/ETCO2',
        'Bx50/ETCO2_MMHG',
        'Bx50/FEO2',
        'Bx50/FIO2',
        'Bx50/HR',
        'Bx50/INCO2',
        'Bx50/INCO2_MMHG',
        'Bx50/MV',
        'Bx50/PEEP',
        'Bx50/PLETH_SPO2',
        'Bx50/PPEAK',
        'Bx50/PPV',
        'Bx50/RR',
        'Bx50/RR_CO2',
        'Bx50/TV_EXP',
        'Bx50/TV_INSP',
        'Datex-Ohmeda/AGENT1_NAME',
        'Datex-Ohmeda/AGENT2_NAME',
        'Datex-Ohmeda/AWP',
        'Datex-Ohmeda/CO2',
        'Datex-Ohmeda/COMPLIANCE',
        'Datex-Ohmeda/ET_AGENT1',
        'Datex-Ohmeda/ET_AGENT2',
        'Datex-Ohmeda/ETCO2',
        'Datex-Ohmeda/ETCO2_MMHG',
        'Datex-Ohmeda/ETO2',
        'Datex-Ohmeda/FICO2',
        'Datex-Ohmeda/FICO2_MMHG',
        'Datex-Ohmeda/FIO2',
        'Datex-Ohmeda/FIO2_ETO2',
        'Datex-Ohmeda/Flow',
        'Datex-Ohmeda/FLOW_AIR',
        'Datex-Ohmeda/FLOW_O2',
        'Datex-Ohmeda/IE_RATIO',
        'Datex-Ohmeda/MAWP',
        'Datex-Ohmeda/MV_EXP',
        'Datex-Ohmeda/MV_EXP_SPONT',
        'Datex-Ohmeda/MV_INSP',
        'Datex-Ohmeda/PAMB',
        'Datex-Ohmeda/PEEP_E',
        'Datex-Ohmeda/PEEP_I',
        'Datex-Ohmeda/PEEP_I_TIME',
        'Datex-Ohmeda/PEEP_TOTAL',
        'Datex-Ohmeda/PIP',
        'Datex-Ohmeda/PMIN',
        'Datex-Ohmeda/PPLAT',
        'Datex-Ohmeda/RR_SPONT',
        'Datex-Ohmeda/RR_TOTAL',
        'Datex-Ohmeda/RRCO2',
        'Datex-Ohmeda/SET_FIO2',
        'Datex-Ohmeda/SET_IE',
        'Datex-Ohmeda/SET_IE_E',
        'Datex-Ohmeda/SET_IE_I',
        'Datex-Ohmeda/SET_INSP_PAUSE',
        'Datex-Ohmeda/SET_MODE',
        'Datex-Ohmeda/SET_PEEP',
        'Datex-Ohmeda/SET_PINSP',
        'Datex-Ohmeda/SET_PLIMIT',
        'Datex-Ohmeda/SET_PMAX',
        'Datex-Ohmeda/SET_PSUPP',
        'Datex-Ohmeda/SET_RR',
        'Datex-Ohmeda/SET_TV',
        'Datex-Ohmeda/T_EXP',
        'Datex-Ohmeda/T_INSP',
        'Datex-Ohmeda/TV_EXP',
        'Datex-Ohmeda/TV_EXP_SPONT',
        'Datex-Ohmeda/TV_INSP',
        'Intellivue/ABP_DIA',
        'Intellivue/ABP_HR',
        'Intellivue/ABP_MEAN',
        'Intellivue/ABP_SYS',
        'Intellivue/AGENT_ET',
        'Intellivue/AGENT_INSP',
        'Intellivue/AGENT2_ET',
        'Intellivue/AGENT2_INSP',
        'Intellivue/ART_DIA',
        'Intellivue/ART_MEAN',
        'Intellivue/ART_SYS',
        'Intellivue/AWAY_CO2_ET',
        'Intellivue/AWAY_CO2_ET_PERC',
        'Intellivue/AWAY_CO2_INSP_MIN',
        'Intellivue/AWAY_CO2_INSP_MIN_PERC',
        'Intellivue/COMPLIANCE',
        'Intellivue/DES_ET_PERC',
        'Intellivue/DES_INSP_PERC',
        'Intellivue/ECG_HR',
        'Intellivue/HR',
        'Intellivue/MV',
        'Intellivue/MV_EXP',
        'Intellivue/MV_INSP',
        'Intellivue/O2_ET_PERC',
        'Intellivue/O2_INSP_PERC',
        'Intellivue/PEEP',
        'Intellivue/PEEP_CMH2O',
        'Intellivue/PIP',
        'Intellivue/PIP_CMH2O',
        'Intellivue/PLETH_HR',
        'Intellivue/PLETH_SAT_O2',
        'Intellivue/PPV',
        'Intellivue/RR',
        'Intellivue/SET_RR',
        'Intellivue/SET_TV',
        'Intellivue/SEVO_ET_PERC',
        'Intellivue/SEVO_INSP_PERC',
        'Intellivue/TEMP',
        'Intellivue/TV',
        'Intellivue/TV_EXP',
        'Intellivue/TV_INSP',
        'Intellivue/VENT_RR',
        'Intellivue/VENT_RR_SPONT',
        'Intellivue/VENT_SET_RR',
        'Intellivue/VENT_VOL_LEAK',
        'Primus/COMPLIANCE',
        'Primus/ETCO2',
        'Primus/ETCO2_KPA',
        'Primus/ETCO2_PERCENT',
        'Primus/EXP_AGENT1',
        'Primus/EXP_AGENT2',
        'Primus/FIO2',
        'Primus/FLOW_AIR',
        'Primus/FLOW_O2',
        'Primus/GAS1_AGENT',
        'Primus/GAS1_EXPIRED',
        'Primus/GAS2_AGENT',
        'Primus/GAS2_EXPIRED',
        'Primus/INCO2',
        'Primus/INCO2_KPA',
        'Primus/INCO2_PERCENT',
        'Primus/INSP_AGENT1',
        'Primus/INSP_AGENT2',
        'Primus/MAWP_MBAR',
        'Primus/MV',
        'Primus/MV_MANDATORY',
        'Primus/MV_SPONT',
        'Primus/PAMB_MBAR',
        'Primus/PEEP_MBAR',
        'Primus/PIP_MBAR',
        'Primus/PPLAT_MBAR',
        'Primus/RESISTANCE',
        'Primus/RR',
        'Primus/RR_CO2',
        'Primus/RR_MANDATORY',
        'Primus/RR_SPONT',
        'Primus/SET_FIO2',
        'Primus/SET_IE_I',
        'Primus/SET_INSP_PAUSE',
        'Primus/SET_INSP_PRES',
        'Primus/SET_INSP_TM',
        'Primus/SET_INTER_PEEP',
        'Primus/SET_PEEP',
        'Primus/SET_PIP',
        'Primus/SET_RR_IPPV',
        'Primus/SET_SUPP_PRES',
        'Primus/SET_TV_L',
        'Primus/TV',
        'Primus/TV_MANDATORY',
        'Primus/VENT_LEAK',
        'Solar8000/ART_DBP',
        'Solar8000/ART_MBP',
        'Solar8000/ART_SBP',
        'Solar8000/ART1_DBP',
        'Solar8000/ART1_MBP',
        'Solar8000/ART1_SBP',
        'Solar8000/BT1',
        'Solar8000/BT2',
        'Solar8000/ETCO2',
        'Solar8000/FEO2',
        'Solar8000/FIO2',
        'Solar8000/GAS2_AGENT',
        'Solar8000/GAS2_EXPIRED',
        'Solar8000/GAS2_INSPIRED',
        'Solar8000/GAS3_AGENT',
        'Solar8000/GAS3_EXPIRED',
        'Solar8000/GAS3_INSPIRED',
        'Solar8000/HR',
        'Solar8000/INCO2',
        'Solar8000/PLETH_HR',
        'Solar8000/PLETH_SPO2',
        'Solar8000/RR',
        'Solar8000/RR_CO2',
        'Solar8000/VENT_AUTO_PEEP',
        'Solar8000/VENT_COMPL',
        'Solar8000/VENT_DYN_COMPL',
        'Solar8000/VENT_DYN_RESIS',
        'Solar8000/VENT_IE_E',
        'Solar8000/VENT_IN_HLD',
        'Solar8000/VENT_INSP_MEAS',
        'Solar8000/VENT_INSP_PC',
        'Solar8000/VENT_INSP_TM',
        'Solar8000/VENT_INSP_TV',
        'Solar8000/VENT_INTRIN_PEEP',
        'Solar8000/VENT_MAWP',
        'Solar8000/VENT_MEAS_PEEP',
        'Solar8000/VENT_MV',
        'Solar8000/VENT_PEEP',
        'Solar8000/VENT_PIP',
        'Solar8000/VENT_PPLAT',
        'Solar8000/VENT_PR_SUP',
        'Solar8000/VENT_RESIS',
        'Solar8000/VENT_RR',
        'Solar8000/VENT_SENS',
        'Solar8000/VENT_SET_FIO2',
        'Solar8000/VENT_SET_IE_E',
        'Solar8000/VENT_SET_PCP',
        'Solar8000/VENT_SET_TV',
        'Solar8000/VENT_SPONT_MV',
        'Solar8000/VENT_SPONT_RR',
        'Solar8000/VENT_STAT_COMPL',
        'Solar8000/VENT_STAT_RESIS',
        'Solar8000/VENT_TOTAL_PEEP',
        'Solar8000/VENT_TV',
        'Solar8000/VENT_VNT_RR',
        'Rad-97/SPO2',
        'Root/SPO2',
        'ROOT/SPO2'
        ]
cols = ['case_index', 'lab_index', 'Sampling time'] + trklist

filelist = dfresult['파일명'].drop_duplicates().values

with open(FINAL_RESULT_FILENAME, 'a', newline='', encoding='utf-8-sig') as f:
    wr = csv.writer(f)
    wr.writerow(cols)

if USE_MULTIPROCESS:
    Parallel(os.cpu_count()-3)(delayed(add_vital_track)(dfresult, filename) for filename in filelist)
else:
    for filename in filelist: add_vital_track(dfresult, filename)
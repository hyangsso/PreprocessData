import vitaldb
import pandas as pd
import os

os.chdir('C:/Users/vitalDB/Desktop/project/')
filelist = ['abga_data_1.xlsx','abga_data_2.xlsx']
POCT_FILENAME = 'poct_data.csv.xz'

if os.path.exists(POCT_FILENAME):
    dfdata = pd.read_csv(POCT_FILENAME,  compression='xz', encoding='utf-8-sig')
    print(f'using...{POCT_FILENAME}')
else:
    dfdata = pd.DataFrame()
    for filename in filelist:
        dfxl = pd.read_excel(filename, skiprows=1, sheet_name=None)
        dfxl = pd.concat([value.assign(sheet_source=key) for key,value in dfxl.items()], ignore_index=True)
        
        dfdata = dfdata.append(dfxl, ignore_index=True)
    
    dfdata = dfdata[['환자번호','검사시행일','검사코드','검사세부항목명','검사결과']]
    dfdata.to_csv(POCT_FILENAME, index=False, encoding='utf-8-sig')
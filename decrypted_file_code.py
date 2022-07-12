import pandas as pd
import os
import msoffcrypto
import pymysql
import numpy as np
import openpyxl

# savepath 
savepath = ''

# rawpath
rawpath = ''

EXCEL_COUNTS = 0

dfresult = pd.DataFrame()
for filename in os.scandir(rawpath):
    if not filename.is_file():
        continue
    if filename.name[0] == '~':
        continue
    if os.path.splitext(filename.name)[1] != '.xlsx':
        continue
    
    # decrypting...
    print('decrypting {}...'.format(filename.name), end='', flush=True)
    f = msoffcrypto.OfficeFile(open(filename.path, "rb"))
    f.load_key(password='turb**8282')
    print('saving...', end='', flush=True)
    f.decrypt(open(savepath + "/" + filename.name, "wb"))
    print('done')
    
    # two or more sheet
    print('excel roading...', end='', flush=True)
    dfxl = pd.read_excel(savepath+"/"+filename.name, sheet_name=None, skiprows=1)
    dfxl = pd.concat([value.assign(sheet_source=key) for key,value in dfxl.items()], ignore_index=True)

    # save 1 file at a time or not
    if EXCEL_COUNTS == 1:    
        print('csv saving...')
        dfxl.to_csv(savepath+"/"+os.path.splitext(filename.name)[0]+'.csv.xz', index=False, encoding='utf-8-sig')
    elif EXCEL_COUNTS >= 2:
        dfresult = dfresult.append(dfxl, ignore_index=True)

# save as 1 file at a time 
if EXCEL_COUNTS >= 2:
    dfxl.to_csv(savepath+"/"+os.path.splitext(filename.name)[0]+'.csv.xz', index=False, encoding='utf-8-sig')
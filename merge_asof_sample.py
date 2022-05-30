import pandas as pd
import numpy as np
import os

os.chdir('')


path = ''
dfsu = pd.read_excel(path, skiprows=1, usecols=['원무접수ID','환자번호','[진술문]기록작성일시','간호진술문명','Value'])
dfsu['[진술문]기록작성일시'] = pd.to_datetime(dfsu['[진술문]기록작성일시'])

path = ''
dfop = pd.read_excel(path)
dfop['수술일자'] = pd.to_datetime(dfop['수술일자'])
# dfop = dfop[dfop['원무접수ID'].notnull()]

# dfsu = dfsu[(dfsu['수신진료과']=='신경과')|(dfsu['수신진료과']=='중환자진료(신경과)')|(dfsu['수신진료과']=='중환자의료(신경과)')|(dfsu['수신진료과']=='정신건강의학과')]
dfsu['환자번호'] = dfsu['환자번호'].astype(np.int64)

df = pd.merge_asof(
    left=dfop.sort_values(by='수술일자'),
    right=dfsu.sort_values(by='[진술문]기록작성일시'),
    left_on='수술일자',
    right_on='[진술문]기록작성일시',
    left_by='환자번호',
    right_by='환자번호',
    direction='forward',
    tolerance=pd.Timedelta('7 day')
)

df.to_excel('', index=False)
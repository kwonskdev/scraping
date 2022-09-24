import os
import time
import datetime
from dateutil.relativedelta import relativedelta
import warnings
from itertools import product
import json
from pytz import timezone

import pandas as pd
import numpy as np
import requests
from google.cloud import storage
from flask import render_template_string

#TODO: 빈구장 확인하는거 버튼이든 뭐든 그럴듯 한걸로 수정
TEMPLATE = '''
<!DOCTYPE html>
<body>
  <p>빈구장 찾기 완료</p>
  <a href='https://asia-northeast3-ys-futsal.cloudfunctions.net/show_ys_reservation_info'> 찾은 빈구장 확인 </a>
</body>
'''

def get_info(request):
    now = datetime.datetime.now(timezone('Asia/Seoul'))
    year, month, day, hour, minute = now.year, now.month, now.day, now.hour, now.minute
    info = get_only_reservationable_stadium()
    client = storage.Client.from_service_account_json('credentials.json', project='ys-futsal')
    bucket = client.get_bucket('ys_futsal_info')
    blob = bucket.blob(f'ys_futsal_info_{year}{month:02d}{day:02d}_{hour:02d}{minute:02d}.csv')
    blob.upload_from_string(info.to_csv(index=False), 'text/csv')
    
    return render_template_string(TEMPLATE)


def get_stadium_reservation_info(stadium_code:str, date:datetime.date):
    response = requests.get(URL.format(stadium_code=stadium_code, date=str(date)))
    print(URL.format(stadium_code=stadium_code, date=str(date)))
    text = response.text
    
    #TODO: 모든 에러에 대해서 할 수 있게 코드 / regex로 가능한가?
    if 'Too Many Requests' in text:
        return 429
    else:
        info_start_index = text.index('[')
        info_end_index = text.index(']')
        info = text[info_start_index:info_end_index+1]
        info = eval(info.replace('null', 'None'))

        return info

def get_available_last_day(today):
    result = today + relativedelta(months=2)
    result = result.replace(day=1)
    result = result - relativedelta(days=1)
    return str(result)

def get_all_reservation_info(stadium_codes: str=None,start_date: str=None, end_date:str=None):
    #FIXME: n_request 제한 넘어가면 _info가 error code로 나와서 concat할 때 에러나는 거 처리\
    start_date, end_date = set_date(start_date, end_date)

    dates = pd.date_range(start_date, end_date).date
    if stadium_codes is None:
        stadium_codes = STADIUM.stadium_code.values.tolist()

    info = pd.DataFrame()
    for stadium_code, date in product(stadium_codes, dates):
        n_repeat = 0
        while (n_repeat := n_repeat + 1) <= 5:
            _info = get_stadium_reservation_info(stadium_code, date)
            if isinstance(_info, list):
                _info = pd.DataFrame(_info)
                _info.loc[:, 'szStadium'] = stadium_code
                time.sleep(.5)
                break
            else:
                time.sleep(15)

        info = pd.concat([info, _info], axis=0)

    info = pd.DataFrame(info)
    if 'ssdate' not in info.columns:
        info.loc[:, 'ssdate'] = None
    if 'szDDate' not in info.columns:
        info.loc[:, 'szDDate'] = None

    info.loc[:, 'date'] = info['ssdate'].fillna('') + info['szDDate'].fillna('')
    info.loc[:, 'date'] = pd.to_datetime(info['date'])
    info.loc[:, 'reservation'] = np.select(
        [info['szState']=='N',
         info['szState']=='Z',
         info['szState']==''],
        ['예약완료', '입금대기', '예약가능'])
    info = info[['date', 'strtime', 'szStadium', 'reservation']]
    info.columns = ['date', 'time', 'stadium_code', 'reservation']
    info.loc[:, 'dow'] = info['date'].dt.day_name()
    info.loc[:, 'date'] = info['date'].dt.date
    info.loc[:, 'time'] = info['time'].str.strip()
    info = info.merge(STADIUM, on=['stadium_code'], how='left')

    return info


def set_date(start_date, end_date):
    today = datetime.date.today()
    getable_last_day = get_available_last_day(today)

    if start_date is None:
        start_date = str(today + relativedelta(days=1))

    if start_date < str(today):
        start_date = str(today + relativedelta(days=1))

    if end_date is None:
        end_date = getable_last_day

    if end_date > getable_last_day:
        end_date = getable_last_day

    return start_date, end_date

def get_only_reservationable_stadium(stadium_codes: str=None, start_date: str=None, end_date: str=None):
    start_date, end_date = set_date(start_date, end_date)

    info = get_all_reservation_info(stadium_codes, start_date, end_date)
    #TODO: 공휴일 정보 추가
    #TODO: stadium_codes는 'ABC', 'ACD' 등의 형태로
    weekday = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    weekday_time = ['19:00 ~ 21:00', '20:00 ~ 22:00', '21:00 ~ 23:00', '22:00 ~ 24:00']
    weekend = ['Saturday', 'Sunday']
    weekend_not_time = ['23:00 ~ 01:00', '00:00 ~ 02:00', '01:00 ~ 03:00', '02:00 ~ 04:00',
                        '03:00 ~ 05:00', '04:00 ~ 06:00', '05:00 ~ 07:00', '06:00 ~ 08:00',
                        '07:00 ~ 09:00', '08:00 ~ 09:00']
    today = datetime.date.today()
    
    query = '''
    (
        (
            (dow.isin(@weekday)) and
            (time.isin(@weekday_time)) and
            (reservation=='예약가능')
        ) or
        (
            (dow.isin(@weekend)) and
            (not time.isin(@weekend_not_time)) and
            (reservation=='예약가능')
        )
    ) and
    (date > @today)
    '''
    query = query.replace('\n', '')
    info = info.query(query).sort_values(['date', 'time', 'stadium_name'], ascending=True)

    return info[['date', 'time', 'stadium_name', 'size', 'dow']]
    
URL = 'http://www.futsalbase.com/api/reservation/allList?stadium={stadium_code}&date={date}'
STADIUM = pd.DataFrame({
    'stadium_code': ['A', 'B', 'C', 'H', 'I', 'E', 'D'],
    'stadium_name': ['1구장', '2구장', '3구장', '4구장', '5구장', '6구장', '7구장'],
    'size': ['40x20', '40x20', '40x20', '34x20', '31x20', '40x19', '40x19']
})
ERROR = {
    429: 'Too Many Requests'
}
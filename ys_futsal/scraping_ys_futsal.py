#TODO: pyinstaller 만들기
from pandas import DataFrame
from pandas import to_datetime as pd_to_datetime
from pandas import concat as pd_concat
import numpy as np
import requests
import time
import datetime
import calendar
from itertools import product
import fire
import logging

def set_logger(path: str):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

def get_stadium_reservation_info(stadium_code:str, date:datetime.date):
    response = requests.get(URL.format(stadium_code=stadium_code, date=str(date)))
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

def get_month_calendar(year: int, month: int):
    num_days = calendar.monthrange(year, month)[1]
    days = [datetime.date(year, month, day) for day in range(1, num_days+1)]

    return days

def get_filename(stadium_code: str, month: int, day: int, ver: str):
    today = datetime.date.today()
    current_year = today.year

    if stadium_code is None:
        filename = f'all_{current_year}_{month}'
    else:
        filename = f'{stadium_code}_{current_year}_{month}'

    if day is None:
        filename += f'_{today}'
    else:
        filename += f'_{day}_{today}'

    filename += f'_{ver}ver.csv'

    return filename

def get_all_reservation_info(stadium_code: str=None, month: int=None, day: int=None, save: bool=True):
    #TODO: 시작/끝 날짜 지정, stadium_code list로
    today = datetime.date.today()
    current_year, current_month = today.year, today.month
    if month > current_month + 1:
        raise ValueError(f'{current_month+1}월 까지 정보만 얻을 수 있습니다.')

    if stadium_code is None:
        stadium_codes = STADIUM.stadium_code
    else:
        stadium_codes = [stadium_code]

    if day is None:
        days = get_month_calendar(current_year, month)

    else:
        days = [datetime.date(current_year, month, day)]

    if month is None:
        month = current_month

    info = DataFrame()
    for stadium_code, date in product(stadium_codes, days):
        LOGGER.info(f'{stadium_code=}, {date=}')
        #TODO: repeat 5로 제한, walrus로 가능한가?
        while True:
            _info = get_stadium_reservation_info(stadium_code, date)
            if isinstance(_info, list):
                _info = DataFrame(_info)
                _info.loc[:, 'szStadium'] = stadium_code
                time.sleep(1)
                break
            else:
                LOGGER.info(f'{_info} error : {ERROR[_info]}')
                time.sleep(10)

        info = pd_concat([info, _info], axis=0)

    info = DataFrame(info)
    info.loc[:, 'date'] = info['ssdate'].fillna('') + info['szDDate'].fillna('')
    info.loc[:, 'date'] = pd_to_datetime(info['date'])
    info.loc[:, 'reservation'] = np.select(
        [info['szState']=='N',
         info['szState']=='Z',
         info['szState']==''],
        ['예약완료', '입금대기', '예약가능'])
    info = info[['date', 'strtime', 'szStadium', 'reservation']]
    info.columns = ['date', 'time', 'stadium_code', 'reservation']
    info.loc[:, 'dow'] = info['date'].dt.day_name()
    info.loc[:, 'time'] = info['time'].str.strip()
    info = info.merge(STADIUM, on=['stadium_code'], how='left')

    if save:
        filename = get_filename(stadium_code, month, day, ver='all')
        info.to_csv(filename, index=False, encoding='utf-8-sig')

    return info

def get_only_reservationable_stadium(stadium_code: str=None, month: int=None, day: int=None):
    info = get_all_reservation_info(stadium_code, month, day)
    #TODO: 공휴일 정보 추가
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

    filename = get_filename(stadium_code, month, day, ver='only')
    info.to_csv(filename, index=False)

    
LOGGER = set_logger('log.log')
URL = 'http://www.futsalbase.com/api/reservation/allList?stadium={stadium_code}&date={date}'
{'A': '1구장', 'B': '2구장', 'C': '3구장', 'D': '7구장', 'E': '6구장', 'H': '4구장', 'I': '5구장'}
STADIUM = DataFrame({
    'stadium_code': ['A', 'B', 'C', 'H', 'I', 'E', 'D'],
    'stadium_name': ['1구장', '2구장', '3구장', '4구장', '5구장', '6구장', '7구장'],
    'size': ['40x20', '40x20', '40x20', '34x20', '31x20', '40x19', '40x19']
})
ERROR = {
    429: 'Too Many Requests'
}
    
if __name__ == '__main__':
    fire.Fire({
        'all': get_all_reservation_info,
        'only': get_only_reservationable_stadium
        })


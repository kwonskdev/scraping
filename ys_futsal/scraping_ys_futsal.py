from pandas import DataFrame
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


def get_all_reservation_info(stadium_code: str=None, month: int=None, day: int=None):
    today = datetime.date.today()
    current_year, current_month = today.year, today.month
    if month > current_month + 1:
        raise ValueError(f'{current_month+1}월 까지 정보만 얻을 수 있습니다.')

    if day is None:
        days = get_month_calendar(current_year, month)
        filename = f'{current_year}_{month}_{today}.csv'

    else:
        days = [datetime.date(current_year, month, day)]
        filename = f'{current_year}_{month}_{day}_{today}'

    if stadium_code is None:
        stadium_codes = STADIUM.code
    else:
        stadium_codes = [stadium_code]

    info = []
    for stadium_code, date in product(stadium_codes, days):
        LOGGER.info(f'{stadium_code=}, {date=}')
        #TODO: repeat 5로 제한, walrus로 가능한가?
        while True:
            _info = get_stadium_reservation_info(stadium_code, date)
            time.sleep(1)
            if isinstance(_info, list):
                break
            else:
                LOGGER.info(f'{_info} error : {ERROR[_info]}')
        info.extend(_info)

    info = DataFrame(info)

    info.to_csv(filename, index=False)

    
LOGGER = set_logger('log.log')
URL = 'http://www.futsalbase.com/api/reservation/allList?stadium={stadium_code}&date={date}'
{'A': '1구장', 'B': '2구장', 'C': '3구장', 'D': '7구장', 'E': '6구장', 'H': '4구장', 'I': '5구장'}
STADIUM = DataFrame({
    'code': ['A', 'B', 'C', 'H', 'I', 'E', 'D'],
    'stadium': ['1구장', '2구장', '3구장', '4구장', '5구장', '6구장', '7구장'],
    'size': ['40x20', '40x20', '40x20', '34x20', '31x20', '40x19', '40x19']
})
ERROR = {
    429: 'Too Many Requests'
}
    
if __name__ == '__main__':
    fire.Fire(get_all_reservation_info)

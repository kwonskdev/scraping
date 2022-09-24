from dateutil.relativedelta import relativedelta
import datetime
from operator import itemgetter
import io

import pandas as pd
from google.cloud import storage
from flask import render_template_string


TEMPLATE = '''
<!DOCTYPE html>
<body>
  <p> 기준 시간 : {{ date }} / 전체 조회 날짜 : {{ start_date }} ~ {{end_date}} </p>
  <p> <a href='https://asia-northeast3-ys-futsal.cloudfunctions.net/get_ys_reservation_info'> 업데이트(10분 정도 소요) </a> </p>
  <table>
    <th>날짜</th>
    <th>시간</th>
    <th>구장</th>
    <th>크기</th>
    <th>요일</th>

    {% for row in info %}
    <tr>
    {% for elem in row %}
      <td>{{ elem }}</td>
    {% endfor %}
    </tr>
    {% endfor %}

  </table>
</body>
'''

def show_info(request):
  client = storage.Client.from_service_account_json('credentials.json', project='ys-futsal')
  bucket = client.get_bucket('ys_futsal_info')
  
  #FIXME: 파일이 너무 많아질 때 대비해서 수정 필요
  files = []
  for blob in bucket.list_blobs():
    files.append([blob.name, blob.time_created])

  file_name = sorted(files, key=itemgetter(1))[-1][0]
  date = datetime.datetime.strptime(file_name[15:28], '%Y%m%d_%H%M')
  start_date = date.date()+relativedelta(days=1)
  end_date = start_date + relativedelta(months=2)
  end_date = end_date.replace(day=1)
  end_date = end_date - relativedelta(days=1)

  info = pd.read_csv('gs://ys_futsal_info/ys_futsal_info.csv', storage_options={'token': 'credentials.json'})
  
  return render_template_string(TEMPLATE, info=info.values, date=str(date), start_date=str(start_date), end_date=str(end_date))
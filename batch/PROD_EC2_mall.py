'''
작성자 : Brandon_이승오
일자 : 2021-05-12
설명 :
    2021-05-12
    기존 3개의 사이트 수집기를 하나로 통일

    -- 영구이사 : mcygclean
    -- 이사몰 : 2424
'''


# from utils.db_conn import PG_CONN
import requests
import bs4
from datetime import datetime, timedelta
import pandas as pd
import time

def _check_status(html):
    if html.status_code == 200:
        return True
    return False


def _mcygclean():
    '''
    수집하는데 크게 2가지 조건이 있다.
    1. 출잘지 정보가 없는경우
        : 해당 이슈를 해결하기위해 출발지를 None으로 잡은 뒤 >> 글씨 기준 split하여 해결.
    2. 배치 작업으로 중복된 데이터 막기 위한 작업
        : regtime으로 배치 시간계산하여 제거 하는 로직
        배치 주기 : 5분, 추후 파라미터로 입력하여 바꿀수 있음.
        00시 05분에 수집할시 00시 00분 ~ 00시 04분 수집
    '''
    url = 'https://www.mcygclean.com/service/call_order.jsp'

    html = requests.get(url)

    if not _check_status(html):
        return 0

    html.encoding = 'utf-8'
    print(html.text)
    soup = bs4.BeautifulSoup(html.text, 'html.parser')
    result = []
    now = str(datetime.now())[:19]
    for row in soup.select('tr'):
        arrive = None
        # regtime = row.select('td')[1].get_text()[:-3]
        regtime = row.select('td')[1].get_text()[:5]

        start_time = datetime.strptime(now[-8:-3], "%H:%M") - timedelta(minutes=5)
        end_time = datetime.strptime(now[-8:-3], "%H:%M") - timedelta(minutes=1)
        comparetime = datetime.strptime(regtime, "%H:%M")

        print(f"now: {now}")
        print(f"start_time: {start_time}")
        print(f"end_time: {end_time}")
        print(f"comparetime: {comparetime}")

        if not (start_time <= comparetime <= end_time):
            continue

        kind = row.select('td')[2].get_text()
        movekind_detail = row.select('td')[3].get_text()
        cname = row.select('td')[4].get_text()[:1]
        move = row.select('td')[5].get_text()
        moving_date = row.select('td')[6].get_text()
        if moving_date == '-':
            moving_date = None
        depart = move.strip()
        if '>>' in move:
            arrive = move.split('>>')[0].strip()
            depart = move.split('>>')[1].strip()

        movekind = kind.split()[0].strip()
        movekind = movekind.replace("서비스", "")
        # movekind_detail = kind.split()[1].strip()[1:-1]

        page = {
            'regtime': regtime,
            'cname': cname,
            'seq': None,
            'arrive': arrive,
            'depart': depart,
            'movekind': movekind,
            'movekind_detail': movekind_detail,
            'submitdate': now,
            'crawl_site': 'mcygclean',
            'moving_date': moving_date
        }
        result.append(page)
        # print(page)
        time.sleep(1.5)
    df = pd.DataFrame(result)

    # PG_CONN().save_df('crawler_24', df)
    print(df)






def _boi():
    url = 'http://m.1566-0935.com/state/rt_estimate.asp'
    html = requests.post(url)

    if not _check_status(html):
        return 0

    html.encoding = 'euc-kr'
    soup = bs4.BeautifulSoup(html.text, 'html.parser')
    now = str(datetime.now())[:19]
    result = []
    for row in soup.select('li'):
        print(row)
        regtime = row.select('dd')[0].get_text()
        cname = row.select('dd')[1].get_text()[0]
        arrive = None
        move = row.select('dd')[2].get_text()

        start_time = datetime.strptime(now[-8:-3], "%H:%M") - timedelta(minutes=5)
        end_time = datetime.strptime(now[-8:-3], "%H:%M") - timedelta(minutes=1)
        comparetime = datetime.strptime(regtime, "%H:%M")

        if not (start_time <= comparetime and comparetime <= end_time):
            continue
        depart = move.strip()
        if '>' in move:
            arrive = move.split('>')[0].strip()
            depart = move.split('>')[1].strip()

        movekind = row.select_one('dt').get_text()[-2:]
        movekind_detail = row.select_one('dt').get_text()

        page = {
            'regtime': regtime,
            'cname': cname,
            'seq': None,
            'arrive': arrive,
            'depart': depart,
            'movekind': movekind,
            'movekind_detail': movekind_detail,
            'submitdate': now,
            'crawl_site': 'boi',
            'moving_date': None
        }
        result.append(page)
    if len(result) >= 1:
        time.sleep(1.5)
        df = pd.DataFrame(result)
        # PG_CONN().save_df('crawler_24', df)
        print(df)


def _boi_company():
    url = 'http://m.1566-0935.com/state/rt_estimate.asp'
    data = {
        'page': 1,
        'etype': 'office'
    }
    html = requests.post(url, data=data)
    if not _check_status(html):
        return 0

    html.encoding = 'euc-kr'
    soup = bs4.BeautifulSoup(html.text, 'html.parser')
    now = str(datetime.now())[:19]
    result = []
    for row in soup.select('li'):
        print(row)
        regtime = row.select('dd')[0].get_text()
        cname = row.select('dd')[1].get_text()[0]
        arrive = None
        move = row.select('dd')[2].get_text()

        start_time = datetime.strptime(now[-8:-3], "%H:%M") - timedelta(minutes=5)
        end_time = datetime.strptime(now[-8:-3], "%H:%M") - timedelta(minutes=1)
        comparetime = datetime.strptime(regtime, "%H:%M")

        if not (start_time <= comparetime and comparetime <= end_time):
            continue
        depart = move.strip()
        if '>' in move:
            arrive = move.split('>')[0].strip()
            depart = move.split('>')[1].strip()

        movekind = row.select_one('dt').get_text()[-2:]
        movekind_detail = row.select_one('dt').get_text()

        page = {
            'regtime': regtime,
            'cname': cname,
            'seq': None,
            'arrive': arrive,
            'depart': depart,
            'movekind': movekind,
            'movekind_detail': movekind_detail,
            'submitdate': now,
            'crawl_site': 'boi',
            'moving_date': None
        }
        result.append(page)
    if len(result) >= 1:
        time.sleep(1.5)
        df = pd.DataFrame(result)
        # PG_CONN().save_df('crawler_24', df)
        print(df)



# def _24mall():
#     # 0 0 * * *
#     query = '''
#     select max(seq) from crawler_24
#     '''
#     seq = PG_CONN().read_query(query)
#     seq = seq['max'][0]
#     result = []
#     now = str(datetime.now())[:19]
#     for i in range(50):
#         seq+=1
#         url = f'http://www.2424.net/popup/order_list_popup.php?seq={seq}'
#         headers = {
#             'Host': 'www.2424.net'
#         }
#         html = requests.get(url, headers=headers)
#         if _check_status(html):
#             html.encoding = 'euc-kr'
#             soup = bs4.BeautifulSoup(html.text, 'html.parser')
#             soup.select('tr > td')

#             img = soup.select_one('tr:nth-child(4) > td > img')['src'][-5]
#             if img == '_':
#                 continue
#             elif img == '1':
#                 movekind_detail = '포장이사'
#             elif img == '2':
#                 movekind_detail = '안심이사'
#             elif img == '3':
#                 movekind_detail = '고급포장'
#             elif img == '4':
#                 movekind_detail = '원룸이사'
#             elif img == '5':
#                 movekind_detail = '보관이사'
#             elif img == '6':
#                 movekind_detail = '일반이사'
#             elif img == '7':
#                 movekind_detail = '기업이사'
#             elif img == '8':
#                 movekind_detail = '해외이사'
#             else:
#                 movekind_detail = f'새로운이사유형{img}'


#             regtime = soup.select('tr > td')[0].get_text()[0:5]
#             cname = soup.select('tr > td')[2].get_text().strip()[0]

#             arrive = soup.select('tr > td')[6].get_text().replace('..', '').strip()
#             depart = soup.select('tr > td')[7].get_text().replace('..', '').strip()

#             moving_date = soup.select('tr > td')[4].get_text()

#             page = {
#                 'regtime': regtime,
#                 'cname': cname,
#                 'seq': seq,
#                 'arrive': arrive,
#                 'depart': depart,
#                 'movekind': '이사',
#                 'movekind_detail': movekind_detail,
#                 'submitdate': now,
#                 'crawl_site': '2424',
#                 'moving_date': moving_date
#             }
#             result.append(page)
#             time.sleep(0.5)
            
#     df = pd.DataFrame(result)
#     # PG_CONN().save_df('crawler_24', df)
#     print(df)


if __name__ == '__main__':
    # pass
    _mcygclean()
    # _boi_company()
    # _boi()
    # _24mall()
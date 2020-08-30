import itertools
import time
from json import JSONDecodeError
import concurrent.futures
import requests
import json
import os

PATH = os.path.dirname(os.path.abspath(__file__))
ENCODING = 'utf-8'


class EgrRegNums:
    @staticmethod
    def get_regnums():
        if os.path.isfile(f'{os.path.join(PATH, "ip.json")}') and os.path.isfile(f'{os.path.join(PATH, "jur.json")}'):
            with open(f'{os.path.join(PATH, "ip.json")}') as f:
                ip_regnums = [regnum['NM'] for regnum in json.load(f) if regnum['NM'] is not None]
            with open(f'{os.path.join(PATH, "jur.json")}') as f:
                jur_regnums = [regnum['NM'] for regnum in json.load(f) if regnum['NM'] is not None]
        else:
            ip_url = 'http://egr.gov.by/egrn/API.jsp?TP=1&MASK=01000000000000000'
            # EgrParser.server_check(ip_url)
            print('Download registration numbers.')
            ip = requests.get(ip_url).json()
            ip_regnums = [regnum['NM'] for regnum in ip if regnum['NM'] is not None]
            with open(f'{os.path.join(PATH, "ip.json")}', 'w', encoding=ENCODING) as f:
                json.dump(ip, f, ensure_ascii=False)
            print('ip regnums downloaded')
            jur = requests.get("http://egr.gov.by/egrn/API.jsp?TP=2&MASK=01000000000000000").json()
            jur_regnums = [regnum['NM'] for regnum in jur if regnum['NM'] is not None]
            with open(f'{os.path.join(PATH, "jur.json")}', 'w', encoding=ENCODING) as f:
                json.dump(jur, f, ensure_ascii=False)
            print('jur regnums downloaded')
        return ip_regnums, jur_regnums


class EgrParser:
    def __init__(self, ip_regnums, jur_regnums):
        self.ip_regnums = list(ip_regnums)
        self.jur_regnums = list(jur_regnums)
        self.all_urls = []
        self.main_list = {}
        self.common_methods = ['getAllAddressByRegNum', 'getAllVEDByRegNum', 'getBaseInfoByRegNum',
                               'getEventsByRegNum', 'getShortInfoByRegNum']
        self.ip_methods = ['getAllIPFIOByRegNum']
        self.jur_methods = ['getAllJurNamesByRegNum']

    @staticmethod
    def load_url(url):
        resp = requests.get(url)
        return resp, url

    @staticmethod
    def server_check(url='http://egr.gov.by/api/v2/egr/getAllAddressByRegNum/100059271'):
        resp, _ = EgrParser.load_url(url)
        resp.raise_for_status()

    def parse_json(self, resp, url):
        try:
            data = resp.json()
        except JSONDecodeError:
            self.all_urls.remove(url)
            return
        regnum = int(url.split('/')[-1])
        if not self.main_list.get(regnum):
            self.main_list[regnum] = []
        self.main_list[regnum] += data if isinstance(data, list) else [data, ]
        self.all_urls.remove(url)

    def get_jsons(self, urls):
        with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
            future_to_url = (executor.submit(self.load_url, url) for url in urls)
            for future in concurrent.futures.as_completed(future_to_url):
                try:
                    resp, url = future.result()
                except requests.exceptions.ConnectionError:
                    continue
                else:
                    self.parse_json(resp, url)

    def create_urls(self, regnums, separate_method):
        for regnum in regnums:
            for method in self.common_methods + separate_method:
                self.all_urls.append(f'http://egr.gov.by/api/v2/egr/{method}/{regnum}')

    def get_urls(self):
        if os.path.isfile(f'{os.path.join(PATH, "urls.json")}'):
            with open(f'{os.path.join(PATH, "urls.json")}', 'r', encoding=ENCODING) as f:
                self.all_urls = json.load(f)
        else:
            print('Create urls.')
            self.create_urls(self.ip_regnums, self.ip_methods)
            self.create_urls(self.jur_regnums, self.jur_methods)
            with open(f'{os.path.join(PATH, "urls.json")}', 'w', encoding=ENCODING) as f:
                json.dump(self.all_urls, f, ensure_ascii=False)

    def get_data(self):
        self.server_check()
        self.get_urls()
        self.all_urls = self.all_urls[:40000]
        time1 = time.time()
        while len(self.all_urls) > 0:
            for i in range(0, len(self.all_urls), 500):
                if i % 1000 == 0:
                    print(f'{len(self.all_urls)} urls left.')
                self.get_jsons(self.all_urls[i:i + 500])
        time2 = time.time()
        print(f'Took {time2 - time1:.2f} s')
        print('main list -', len(self.main_list), 'remain -', len(self.all_urls))
        return self.main_list

    def save_data(self):
        if not os.path.exists(os.path.join(PATH, 'jsons')):
            os.mkdir(os.path.join(PATH, 'jsons'))
        for i in range(0, len(self.main_list), 500):
            with open(os.path.join(PATH, 'jsons', f'data{int(i / 500 + 1)}.json'), 'w', encoding=ENCODING) as f:
                json.dump(dict(itertools.islice(self.main_list.items(), i, i + 500)), f, ensure_ascii=False)


def main():
    ip_regnums, jur_regnums = EgrRegNums.get_regnums()
    egr = EgrParser(ip_regnums, jur_regnums)
    time1 = time.time()
    try:
        egr.get_data()
        egr.save_data()
    except KeyboardInterrupt:
        print(len(egr.main_list))
        egr.save_data()
    time2 = time.time()
    print(f'Took {time2 - time1:.2f} s')


if __name__ == '__main__':
    main()

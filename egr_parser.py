import codecs
import time
from json import JSONDecodeError
import concurrent.futures
import requests
import json
import os


class EgrParser:
    def __init__(self):
        self.ip_regnums = []
        self.jur_regnums = []
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

    def get_jsons(self, urls):
        with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
            future_to_url = (executor.submit(self.load_url, url) for url in urls)
            for future in concurrent.futures.as_completed(future_to_url):
                try:
                    resp, url = future.result()
                    data = resp.json()
                    regnum = int(url.split('/')[-1])
                    if not self.main_list.get(regnum):
                        self.main_list[regnum] = []
                    self.main_list[regnum] += data if isinstance(data, list) else [data, ]
                    self.all_urls.remove(url)
                except JSONDecodeError:
                    self.all_urls.remove(url)
                except Exception as e:
                    pass
                    # print(f"Unable to get url due to {e.__class__}.")

    def get_regnums(self):
        try:
            with open(f'{os.path.dirname(os.path.abspath(__file__))}\\ip.json') as f:
                self.ip_regnums = [regnum['NM'] for regnum in json.load(f)]
            with open(f'{os.path.dirname(os.path.abspath(__file__))}\\jur.json') as f:
                self.jur_regnums = [regnum['NM'] for regnum in json.load(f)]
        except IOError:
            print('Download registration numbers.')
            ip = requests.get("http://egr.gov.by/egrn/API.jsp?TP=1MASK=01000000000000000").json()
            self.ip_regnums = [regnum['NM'] for regnum in ip]
            with open(f'{os.path.dirname(os.path.abspath(__file__))}\\ip.json', 'w', encoding="utf-8") as f:
                json.dump(ip, f, ensure_ascii=False)
            print('ip regnums downloaded')
            jur = requests.get("http://egr.gov.by/egrn/API.jsp?TP=2&MASK=01000000000000000").json()
            self.jur_regnums = [regnum['NM'] for regnum in jur]
            with open(f'{os.path.dirname(os.path.abspath(__file__))}\\jur.json', 'w', encoding="utf-8") as f:
                json.dump(jur, f, ensure_ascii=False)

    def get_urls(self):
        self.get_regnums()
        try:
            with open(f'{os.path.dirname(os.path.abspath(__file__))}\\urls.json', 'r', encoding='utf-8') as f:
                self.all_urls = json.load(f)
        except IOError:
            print('Create urls.')
            for regnum in self.ip_regnums:
                if regnum is not None:
                    for method in self.common_methods + self.ip_methods:
                        self.all_urls.append(f'http://egr.gov.by/api/v2/egr/{method}/{regnum}')
            for regnum in self.jur_regnums:
                if regnum is not None:
                    for method in self.common_methods + self.jur_methods:
                        self.all_urls.append(f'http://egr.gov.by/api/v2/egr/{method}/{regnum}')
            with open(f'{os.path.dirname(os.path.abspath(__file__))}\\urls.json', 'w', encoding="utf-8") as f:
                json.dump(self.all_urls, f, ensure_ascii=False)

    def get_data(self):
        self.get_urls()
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
        for i in range(0, len(self.main_list), 20000):
            with codecs.open(f'{os.path.dirname(os.path.abspath(__file__))}\\jsons\\data{int(i / 20000 + 1)}.json', 'w', 'utf-8') as f:
                json.dump(self.main_list[i:i+20000], f, ensure_ascii=False)


def main():
    egr = EgrParser()
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

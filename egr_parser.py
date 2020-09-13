import itertools
import time
from json import JSONDecodeError
import requests
import json
import os
from os.path import join, dirname, abspath, isfile, exists

import concurrent.futures

PATH = dirname(abspath(__file__))
ENCODING = 'utf-8'


class EgrRegNums:
    def __init__(self):
        self.ip_regnums = []
        self.jur_regnums = []
        self.ip_path = join(PATH, "ip.json")
        self.jur_path = join(PATH, "jur.json")

    def get_regnums_from_file(self):
        with open(self.ip_path) as f:
            self.ip_regnums = [regnum['NM'] for regnum in json.load(f)]
        with open(self.jur_path) as f:
            self.jur_regnums = [regnum['NM'] for regnum in json.load(f)]

    def save_regnums(self, filename, regnums_json):
        with open(join(PATH, filename), 'w', encoding=ENCODING) as f:
            json.dump(regnums_json, f, ensure_ascii=False)

    def get_regnums_from_api(self):
        print('Download registration numbers.')
        ip = requests.get('http://egr.gov.by/egrn/API.jsp?TP=2&MASK=01000000000000000').json()
        self.ip_regnums = [regnum['NM'] for regnum in ip]
        self.save_regnums('ip.json', ip)
        print('ip regnums downloaded')
        jur = requests.get("http://egr.gov.by/egrn/API.jsp?TP=1&MASK=01000000000000000").json()
        self.jur_regnums = [regnum['NM'] for regnum in jur]
        self.save_regnums('jur.json', jur)
        print('jur regnums downloaded')

    def get_regnums(self):
        if isfile(join(PATH, "ip.json")) and isfile(join(PATH, "jur.json")):
            self.get_regnums_from_file()
        else:
            self.get_regnums_from_api()
        return self.ip_regnums, self.jur_regnums


class EgrUrls:
    COMMON_METHODS = ['getAllAddressByRegNum', 'getAllVEDByRegNum', 'getBaseInfoByRegNum',
                      'getEventsByRegNum', 'getShortInfoByRegNum']
    IP_METHODS = ['getAllIPFIOByRegNum']
    JUR_METHODS = ['getAllJurNamesByRegNum']

    def __init__(self, ip_regnums, jur_regnums):
        self.urls = []
        self.ip_regnums = list(ip_regnums)
        self.jur_regnums = list(jur_regnums)

    def create_urls(self, regnums, separate_method):
        for regnum in regnums:
            for method in self.COMMON_METHODS + separate_method:
                url = f'http://egr.gov.by/api/v2/egr/{method}/{regnum}'
                self.urls.append(url)

    def get_urls(self):
        print('Create urls.')
        self.create_urls(self.ip_regnums, self.IP_METHODS)
        self.create_urls(self.jur_regnums, self.JUR_METHODS)
        return self.urls


class EgrSave:
    @staticmethod
    def save_data(jsons):
        json_path = join(PATH, 'jsons')
        if not exists(json_path):
            os.mkdir(json_path)
        for current_row in range(0, len(jsons), 20000):
            with open(join(json_path, f'data{int(current_row / 20000 + 1)}.json'), 'w', encoding=ENCODING) as f:
                json.dump(dict(itertools.islice(jsons.items(), current_row, current_row + 20000)), f,
                          ensure_ascii=False)


class EgrRequests:
    @staticmethod
    def load_url(url):
        resp = requests.get(url)
        return resp, url

    @staticmethod
    def server_check(url='http://egr.gov.by/api/v2/egr/getAllAddressByRegNum/100059271'):
        resp, _ = EgrRequests.load_url(url)
        resp.raise_for_status()

    @staticmethod
    def get_resps(urls):
        resps_and_urls = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
            future_to_url = (executor.submit(EgrRequests.load_url, url) for url in urls)
            for future in concurrent.futures.as_completed(future_to_url):
                try:
                    resp, url = future.result()
                except requests.exceptions.ConnectionError:
                    continue
                else:
                    resps_and_urls.append((resp, url))
        return resps_and_urls


class EgrParser:
    def __init__(self, urls):
        self.all_urls = urls
        self.main_list = {}

    def parse_json(self, resps_and_urls):
        for resp, url in resps_and_urls:
            try:
                data = resp.json()
            except JSONDecodeError:
                self.all_urls.remove(url)
                continue
            regnum = url.split('/')[-1]
            if not self.main_list.get(regnum):
                self.main_list[regnum] = []
            self.main_list[regnum] += data if isinstance(data, list) else [data, ]  # сохранять метод для регнама
            self.all_urls.remove(url)

    def get_data(self):
        EgrRequests.server_check()
        self.all_urls = self.all_urls[:20_000]
        while len(self.all_urls) > 0:
            for i in range(0, len(self.all_urls), 500):
                if i % 1000 == 0:
                    print(f'{len(self.all_urls)} urls left. downloaded regnums - {len(self.main_list)}')
                resps_and_urls = EgrRequests.get_resps(self.all_urls[i:i + 500])
                self.parse_json(resps_and_urls)
        print('main list -', len(self.main_list), 'remain -', len(self.all_urls))
        return self.main_list


def main():
    ip_regnums, jur_regnums = EgrRegNums().get_regnums()
    urls = EgrUrls(ip_regnums, jur_regnums).get_urls()
    egr = EgrParser(urls)
    time1 = time.time()
    try:
        egr.get_data()
        EgrSave.save_data(egr.main_list)
    except KeyboardInterrupt:
        print(len(egr.main_list))
        EgrSave.save_data(egr.main_list)
    time2 = time.time()
    print(f'Took {time2 - time1:.2f} s')


if __name__ == '__main__':
    main()

# скорость методов протестить(каждый из 7), в базу уложить
# регнам метод1 ответ1
# регнам метод1 ответ1

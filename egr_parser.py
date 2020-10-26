import time
import requests
import json
from os.path import join, dirname, abspath, isfile
from json import JSONDecodeError

import concurrent.futures

from egr_save import EgrSave

PATH = dirname(abspath(__file__))
ENCODING = 'utf-8'


class EgrRegNums:
    def __init__(self):
        self.ip_reg_nums = []
        self.jur_reg_nums = []
        self.ip_path = join(PATH, "ip.json")
        self.jur_path = join(PATH, "jur.json")

    def get_reg_nums_from_file(self):
        with open(self.ip_path) as f:
            self.ip_reg_nums = [reg_num['NM'] for reg_num in json.load(f)]
        with open(self.jur_path) as f:
            self.jur_reg_nums = [reg_num['NM'] for reg_num in json.load(f)]

    def save_reg_nums(self, filename, reg_nums_json):
        with open(join(PATH, filename), 'w', encoding=ENCODING) as f:
            json.dump(reg_nums_json, f, ensure_ascii=False)

    def get_reg_nums_from_api(self):
        print('Download registration numbers.')
        ip = requests.get('http://egr.gov.by/egrn/API.jsp?TP=2&MASK=01000000000000000').json()
        self.ip_reg_nums = [reg_num['NM'] for reg_num in ip]
        self.save_reg_nums('ip.json', ip)
        print('ip reg nums downloaded')
        jur = requests.get("http://egr.gov.by/egrn/API.jsp?TP=1&MASK=01000000000000000").json()
        self.jur_reg_nums = [reg_num['NM'] for reg_num in jur]
        self.save_reg_nums('jur.json', jur)
        print('jur reg nums downloaded')

    def remove_none_values(self):
        self.ip_reg_nums = list(filter(None, self.ip_reg_nums))
        self.jur_reg_nums = list(filter(None, self.jur_reg_nums))

    def get_reg_nums(self):
        if isfile(join(PATH, "ip.json")) and isfile(join(PATH, "jur.json")):
            self.get_reg_nums_from_file()
        else:
            self.get_reg_nums_from_api()
        self.remove_none_values()
        return self.ip_reg_nums, self.jur_reg_nums


class EgrUrls:
    COMMON_METHODS = ['getAllAddressByRegNum', 'getAllVEDByRegNum', 'getBaseInfoByRegNum',
                      'getEventsByRegNum', 'getShortInfoByRegNum']
    IP_METHODS = ['getAllIPFIOByRegNum']
    JUR_METHODS = ['getAllJurNamesByRegNum']

    def __init__(self, ip_reg_nums, jur_reg_nums):
        self.urls = []
        self.ip_reg_nums = list(ip_reg_nums)
        self.jur_reg_nums = list(jur_reg_nums)

    def create_urls(self, reg_nums, separate_method):
        for reg_num in reg_nums:
            for method in self.COMMON_METHODS + separate_method:
                url = f'http://egr.gov.by/api/v2/egr/{method}/{reg_num}'
                self.urls.append(url)

    def get_urls(self):
        print('Create urls.')
        self.create_urls(self.ip_reg_nums, self.IP_METHODS)  # 1108588
        self.create_urls(self.jur_reg_nums, self.JUR_METHODS)
        return self.urls


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
                    if resp.status_code == 500:
                        continue
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
            reg_num = url.split('/')[-1]
            method = url.split('/')[-2]
            if not self.main_list.get(reg_num):
                self.main_list[reg_num] = {}
            self.main_list[reg_num][method] = data if isinstance(data, list) else [data, ]
            self.all_urls.remove(url)

    def get_data(self):
        EgrRequests.server_check()
        self.all_urls = self.all_urls[:10000]
        while len(self.all_urls) > 0:
            for i in range(0, len(self.all_urls), 500):
                if i % 1000 == 0:
                    print(f'{len(self.all_urls)} urls left. downloaded reg_nums - {len(self.main_list)}')
                resps_and_urls = EgrRequests.get_resps(self.all_urls[i:i + 500])
                self.parse_json(resps_and_urls)
        print('main list -', len(self.main_list), 'remain -', len(self.all_urls))
        return self.main_list


def main():
    ip_reg_nums, jur_reg_nums = EgrRegNums().get_reg_nums()
    urls = EgrUrls(ip_reg_nums, jur_reg_nums).get_urls()
    egr = EgrParser(urls)
    egr_saver = EgrSave()
    egr_saver.clear_db()
    time1 = time.time()
    try:
        egr.get_data()
        egr_saver.save_data(egr.main_list)
    except KeyboardInterrupt:
        print(len(egr.main_list))
        egr_saver.save_data(egr.main_list)
    time2 = time.time()
    print(f'Took {time2 - time1:.2f} s')


if __name__ == '__main__':
    main()

import csv

import requests
import json
import os


# response = requests.get('http://egr.gov.by/api/v2/egr/getAddressByRegNum/100059271')
# tp1 = requests.get("http://egr.gov.by/egrn/API.jsp?TP=1&MASK=01000000000000000")
# data = json.loads(response.text)
#
# print(tp1.text)


class EgrParser:
    def __init__(self):
        with open(f'{os.path.dirname(os.path.abspath(__file__))}\\ip.json') as f:
            self.ip_regnums = [param['NM'] for param in json.load(f)]
        with open(f'{os.path.dirname(os.path.abspath(__file__))}\\jur.json') as f:
            self.jur_regnums = [param['NM'] for param in json.load(f)]
        self.common_methods = ['getAllAddressByRegNum', 'getAllVEDByRegNum', 'getBaseInfoByRegNum',
                               'getEventsByRegNum', 'getShortInfoByRegNum']
        self.ip_methods = ['getAllIPFIOByRegNum']
        self.jur_methods = ['getAllJurNamesByRegNum']

    def get_urls(self):
        urls = []
        for regnum in self.ip_regnums:
            for method in self.common_methods + self.ip_methods:
                urls.append(f'http://egr.gov.by/api/v2/egr/{method}/{regnum}')
        for regnum in self.jur_regnums:
            for method in self.common_methods + self.jur_methods:
                urls.append(f'http://egr.gov.by/api/v2/egr/{method}/{regnum}')
        with open(f'{os.path.dirname(os.path.abspath(__file__))}\\urls.json', 'w', encoding="utf-8") as f:
            json.dump(urls, f, ensure_ascii=False)

    def get_ip_jsons_by_regnum(self, regnum):
        jsons = {}
        for method in self.common_methods + self.ip_methods:
            method_json = requests.get(f'http://egr.gov.by/api/v2/egr/{method}/{regnum}').json()
            # with open(f'{os.path.dirname(os.path.abspath(__file__))}\\{method}.json', 'w', encoding="utf-8") as f:
            #     json.dump(method_json, f, indent=2, ensure_ascii=False)
            jsons += method_json if isinstance(method, list) else [method_json, ]
        return jsons

    def get_jur_jsons_by_regnum(self, regnum):
        jsons = []
        for method in self.common_methods + self.jur_methods:
            print(f'http://egr.gov.by/api/v2/egr/{method}/{regnum}')
            method_json = requests.get(f'http://egr.gov.by/api/v2/egr/{method}/{regnum}').json()
            # with open(f'{os.path.dirname(os.path.abspath(__file__))}\\{method}.json', 'w', encoding="utf-8") as f:
            #     json.dump(method_json, f, indent=2, ensure_ascii=False)
            jsons += method_json if isinstance(method, list) else [method_json, ]
        return jsons
        # final_list = []
        # for elem in jsons:
        #     if elem not in final_list:
        #         final_list.append(elem)
        # return final_list

    def get_jsons(self):
        jsons = []
        for regnum in self.ip_regnums:
            json_by_regnum = self.get_ip_jsons_by_regnum(regnum)
            jsons.append({regnum: json_by_regnum})
        for regnum in self.jur_regnums:
            json_by_regnum = self.get_jur_jsons_by_regnum(regnum)
            jsons.append(json_by_regnum)
        return jsons

def main():
    # egr = EgrParser([700008856, 100059271, 100098469, 190835473], [192201275])
    egr = EgrParser()
    egr.get_urls()
    # jsons = egr.get_jsons()
    # with open(f'{os.path.dirname(os.path.abspath(__file__))}\\1.json', 'w', encoding="utf-8") as f:
    #     json.dump(jsons, f, indent=2, ensure_ascii=False)


if __name__ == '__main__':
    main()

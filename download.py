import requests
import json

if __name__ == '__main__':
    urls = ['http://egr.gov.by/api/v2/egr/getAllAddressByRegNum/100059271',
            'http://egr.gov.by/api/v2/egr/getBaseInfoByRegNum/100059271',
            'http://egr.gov.by/api/v2/egr/getAllVEDByRegNum/100059271']
    with open('jur1.json', 'w', encoding="utf-8") as f:
        json.dump(urls, f, ensure_ascii=False)
    with open('jur1.json', 'r', encoding="utf-8") as f:
        data = json.load(f)
        print(data)
    # response1 = requests.get(f'http://egr.gov.by/api/v2/egr/getAllAddressByRegNum/700008856').json()
    # print(type(response1))
    # response2 = requests.get(f'http://egr.gov.by/api/v2/egr/getAllVEDByRegNum/700008856').json()
    # with open('jur1.json', 'w', encoding="utf-8") as f:
    #     json.dump(response2, f, indent=2, ensure_ascii=False)
    # with open('all_jur.json', 'w', encoding="utf-8") as f:
    #     json.dump(response1, f, indent=2, ensure_ascii=False)

    # with open('all1_jur.json', 'w', encoding="utf-8") as f:
    #     json.dump(response1 + response2, f, indent=2, ensure_ascii=False)
    # ip = requests.get("http://egr.gov.by/egrn/API.jsp?TP=1&MASK=01000000000000000").json()
    # jur = requests.get("http://egr.gov.by/egrn/API.jsp?TP=2&MASK=01000000000000000").json()
    # with open('ip.json', 'w', encoding="utf-8") as f:
    #     json.dump(ip, f, indent=2, ensure_ascii=False)
    # with open('jur.json', 'w', encoding="utf-8") as f:
    #     json.dump(jur, f, indent=2, ensure_ascii=False)

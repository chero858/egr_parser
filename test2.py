import codecs
import json
from json import JSONDecodeError
import concurrent.futures
import requests
import time

CONNECTIONS = 100

with open('urls.json', 'r', encoding="utf-8") as f:
    all_urls = json.load(f)

all_urls = all_urls[:100_000]
main_list = {}


def load_url(url):
    resp = requests.get(url)
    return resp, url


def get(urls):
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
        future_to_url = (executor.submit(load_url, url) for url in urls)
        # time1 = time.time()
        for future in concurrent.futures.as_completed(future_to_url):
            try:
                resp, url = future.result()
                data = resp.json()
                regnum = int(url.split('/')[-1])
                if not main_list.get(regnum):
                    main_list[regnum] = []
                main_list[regnum] += data if isinstance(data, list) else [data, ]
                # global all_urls
                all_urls.remove(url)
            except JSONDecodeError:
                all_urls.remove(url)
            except Exception as e:
                pass
                # print(f"Unable to get url due to {e.__class__}.")
        # time2 = time.time()

    # print(f'Took {time2 - time1:.2f} s')


if __name__ == '__main__':
    time1 = time.time()

    while len(all_urls) > 0:
        get(all_urls)
        print(len(all_urls))
        # prev = 0
        # for i in range(500, len(all_urls), 500):
        #     get(all_urls[prev:i])
        #     prev = i
        #     if i % 1000 == 0:
        #         print(len(all_urls))
    # get(all_urls)
    time2 = time.time()
    print(f'Took {time2 - time1:.2f} s')
    print('main list -', len(main_list), 'remain -', len(all_urls))

    with codecs.open('data1.json', 'w', 'utf-8') as f:
        json.dump(main_list, f, ensure_ascii=False)

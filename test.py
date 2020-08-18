import asyncio
import aiohttp
import time
import json
import codecs

with open('urls.json', 'r', encoding="utf-8") as f:
    urls = json.load(f)


main_list = {}
count = 0
suc = 0


async def get(url):
    regnum = int(url.split('/')[-1])
    if not main_list.get(regnum):
        main_list[regnum] = []
    try:
        connector = aiohttp.TCPConnector(limit=50)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(url=url, headers={'accept': '*/*'}) as response:
                resp = await response.json()
                main_list[regnum] += resp if isinstance(resp, list) else [resp, ]
                global urls, suc
                suc += 1
                urls.remove(url)
    except Exception as e:
        print("Unable to get url {} due to {}.".format(url, e.__class__))
        global count
        count += 1


async def main(urls):
    ret = await asyncio.gather(*[get(url) for url in urls])
    # print("Finalized all. ret is a list of len {} outputs.".format(len(ret)))


if __name__ == '__main__':
    urls = urls[:1000]
    amount = len(urls)
    print(amount)
    prev = 0
    start1 = time.time()
    # for j in range(10):
    print(len(urls))
    for i in range(500, len(urls), 500):
        # start = time.time()
        asyncio.run(main(urls[prev:i]))
        # end = time.time()
        prev = i
        if i % 1000 == 0:
            # print(j, '', i)
            print(f"total - {suc}, unable to get - {count}")
            # print(f"Took {end - start} seconds. total - {len(main_list)}, unable to get - {count}")
    # asyncio.run(main(urls[prev:]))
    end1 = time.time()
    print(f"Took {end1 - start1} seconds to pull 100k websites.")
    print(f'total - {len(main_list)}, unable to get - {count}')
    # start = time.time()
    # asyncio.run(main(urls))
    # end = time.time()
    # print("Took {} seconds to pull {} websites.".format(end - start, len(main_list)))

    with codecs.open('data1.json', 'w', 'utf-8') as f:
        json.dump(main_list, f, ensure_ascii=False)

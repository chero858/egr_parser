import asyncio
import aiohttp
import time
import json
import codecs

with open('urls.json', 'r', encoding="utf-8") as f:
    urls = json.load(f)


main_list = {}


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
    except Exception as e:
        print("Unable to get url {} due to {}.".format(url, e.__class__))


async def main(urls):
    ret = await asyncio.gather(*[get(url) for url in urls])
    print("Finalized all. ret is a list of len {} outputs.".format(len(ret)))


if __name__ == '__main__':
    amount = len(urls)
    prev = 0
    for i in range(500, amount+1, 500):
        start = time.time()
        asyncio.run(main(urls[prev:i]))
        end = time.time()
        prev = i
        print("Took {} seconds to pull {} websites.".format(end - start, len(main_list)))
    asyncio.run(main(urls[prev:]))
    # start = time.time()
    # asyncio.run(main(urls))
    # end = time.time()
    # print("Took {} seconds to pull {} websites.".format(end - start, len(main_list)))

    with codecs.open('data1.json', 'w', 'utf-8') as f:
        json.dump(main_list, f, ensure_ascii=False)

#!/usr/bin/env python3
"""Тестирование фильтра описаний на списке eBay URL."""

import asyncio
import aiohttp
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
import sys

# Загружаем .env
load_dotenv()

# Импортируем наш фильтр
from blocked_description_filter import check_description, blocked_words

URLS = """
https://www.ebay.com/itm/388880765474
https://www.ebay.com/itm/146742439144
https://www.ebay.com/itm/187667142985
https://www.ebay.com/itm/306559868085
https://www.ebay.com/itm/157411778293
https://www.ebay.com/itm/205601718595
https://www.ebay.com/itm/127458430192
https://www.ebay.com/itm/317157525986
https://www.ebay.com/itm/389129470480
https://www.ebay.com/itm/197793516919
https://www.ebay.com/itm/376539238451
https://www.ebay.com/itm/226930008753
https://www.ebay.com/itm/167865641240
https://www.ebay.com/itm/226905187605
https://www.ebay.com/itm/389104864419
https://www.ebay.com/itm/177497182078
https://www.ebay.com/itm/187463236968
https://www.ebay.com/itm/205814426251
https://www.ebay.com/itm/389155899536
https://www.ebay.com/itm/277299334652
https://www.ebay.com/itm/197602573927
https://www.ebay.com/itm/197602573934
https://www.ebay.com/itm/317449826733
https://www.ebay.com/itm/146912677808
https://www.ebay.com/itm/376512962164
https://www.ebay.com/itm/406332200533
https://www.ebay.com/itm/236087597629
https://www.ebay.com/itm/396959507373
https://www.ebay.com/itm/205814594744
https://www.ebay.com/itm/236447914977
https://www.ebay.com/itm/157272660864
https://www.ebay.com/itm/388886561800
https://www.ebay.com/itm/396327138837
https://www.ebay.com/itm/286920318369
https://www.ebay.com/itm/357287355630
https://www.ebay.com/itm/405983062334
https://www.ebay.com/itm/405996401325
https://www.ebay.com/itm/357432870067
https://www.ebay.com/itm/356677254319
https://www.ebay.com/itm/205580239691
https://www.ebay.com/itm/146824094487
https://www.ebay.com/itm/357432876843
https://www.ebay.com/itm/357883813343
https://www.ebay.com/itm/396099987425
https://www.ebay.com/itm/177423839163
https://www.ebay.com/itm/388634539794
https://www.ebay.com/itm/156857372394
https://www.ebay.com/itm/294157499448
https://www.ebay.com/itm/116551768856
https://www.ebay.com/itm/255922568404
https://www.ebay.com/itm/144896345297
https://www.ebay.com/itm/195760027919
https://www.ebay.com/itm/175544351256
https://www.ebay.com/itm/406361620125
https://www.ebay.com/itm/195924444754
https://www.ebay.com/itm/195924443560
https://www.ebay.com/itm/204193815077
https://www.ebay.com/itm/165846836051
https://www.ebay.com/itm/204212468168
https://www.ebay.com/itm/386510018836
https://www.ebay.com/itm/335166595392
https://www.ebay.com/itm/397298281204
https://www.ebay.com/itm/314383518141
https://www.ebay.com/itm/316818326926
https://www.ebay.com/itm/196235670713
https://www.ebay.com/itm/167862322403
https://www.ebay.com/itm/317414482735
https://www.ebay.com/itm/286812814127
https://www.ebay.com/itm/277486586192
https://www.ebay.com/itm/156843818255
https://www.ebay.com/itm/356727035818
https://www.ebay.com/itm/405713689521
https://www.ebay.com/itm/256877762002
https://www.ebay.com/itm/356727034315
https://www.ebay.com/itm/286327205838
https://www.ebay.com/itm/177546779174
https://www.ebay.com/itm/167903933515
https://www.ebay.com/itm/406349434159
https://www.ebay.com/itm/297734979286
https://www.ebay.com/itm/146955429110
https://www.ebay.com/itm/187730555347
https://www.ebay.com/itm/376628605676
https://www.ebay.com/itm/357747606074
https://www.ebay.com/itm/187649295168
https://www.ebay.com/itm/197828961347
https://www.ebay.com/itm/376612754867
https://www.ebay.com/itm/136610126944
https://www.ebay.com/itm/277330870990
https://www.ebay.com/itm/336310656449
https://www.ebay.com/itm/396889251418
https://www.ebay.com/itm/306581733974
https://www.ebay.com/itm/396889251674
https://www.ebay.com/itm/167249772798
https://www.ebay.com/itm/176739624584
https://www.ebay.com/itm/126877353727
https://www.ebay.com/itm/277489490438
https://www.ebay.com/itm/297778230328
https://www.ebay.com/itm/127029291140
https://www.ebay.com/itm/167515896974
https://www.ebay.com/itm/326448775330
https://www.ebay.com/itm/156756606505
https://www.ebay.com/itm/286386872724
https://www.ebay.com/itm/335816696910
https://www.ebay.com/itm/187596148012
https://www.ebay.com/itm/336197551825
https://www.ebay.com/itm/177438432727
https://www.ebay.com/itm/267118554450
https://www.ebay.com/itm/389100941816
https://www.ebay.com/itm/406114456196
https://www.ebay.com/itm/356445859380
https://www.ebay.com/itm/256941831152
https://www.ebay.com/itm/146976196714
https://www.ebay.com/itm/187472249152
https://www.ebay.com/itm/286283010960
https://www.ebay.com/itm/257200298914
https://www.ebay.com/itm/296615559647
https://www.ebay.com/itm/146739953899
https://www.ebay.com/itm/257200299927
https://www.ebay.com/itm/127481731432
https://www.ebay.com/itm/317563001372
https://www.ebay.com/itm/187759118050
https://www.ebay.com/itm/396915157175
https://www.ebay.com/itm/396614887393
https://www.ebay.com/itm/177304013287
https://www.ebay.com/itm/234656437197
https://www.ebay.com/itm/295243432880
https://www.ebay.com/itm/374516265671
https://www.ebay.com/itm/187779286177
https://www.ebay.com/itm/394561153854
https://www.ebay.com/itm/285253548533
https://www.ebay.com/itm/387486733052
https://www.ebay.com/itm/335387926218
https://www.ebay.com/itm/126560357504
https://www.ebay.com/itm/285854157142
https://www.ebay.com/itm/166883645234
https://www.ebay.com/itm/176371825520
https://www.ebay.com/itm/406232818044
https://www.ebay.com/itm/197722589305
https://www.ebay.com/itm/197722589410
https://www.ebay.com/itm/397042885169
https://www.ebay.com/itm/286812894781
https://www.ebay.com/itm/397042884913
https://www.ebay.com/itm/205718194987
https://www.ebay.com/itm/235863831892
https://www.ebay.com/itm/236444228422
https://www.ebay.com/itm/387819100123
https://www.ebay.com/itm/389179949102
https://www.ebay.com/itm/167908833082
https://www.ebay.com/itm/257218652741
https://www.ebay.com/itm/357926637138
https://www.ebay.com/itm/187716188987
https://www.ebay.com/itm/357660063871
https://www.ebay.com/itm/317496677392
https://www.ebay.com/itm/376442750724
https://www.ebay.com/itm/177302154618
https://www.ebay.com/itm/376442755894
https://www.ebay.com/itm/197790889815
https://www.ebay.com/itm/227085965205
https://www.ebay.com/itm/317558422989
https://www.ebay.com/itm/187449350739
https://www.ebay.com/itm/277524971689
https://www.ebay.com/itm/336305978703
https://www.ebay.com/itm/376442782958
https://www.ebay.com/itm/396777735485
https://www.ebay.com/itm/187595813002
https://www.ebay.com/itm/156969784023
https://www.ebay.com/itm/226893862652
https://www.ebay.com/itm/389197508615
https://www.ebay.com/itm/389019993823
https://www.ebay.com/itm/376385421071
https://www.ebay.com/itm/257123423707
""".strip().split('\n')

# Прокси из proxies.txt
def load_proxies():
    try:
        with open('proxies.txt', 'r') as f:
            lines = [l.strip() for l in f if l.strip()]
            if lines:
                parts = lines[0].split(':')
                if len(parts) == 4:
                    return f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
    except:
        pass
    return None

PROXY = load_proxies()

async def fetch_description(session, url, semaphore):
    """Загружает страницу и извлекает описание."""
    item_id = url.split('/')[-1]
    async with semaphore:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    return item_id, None, f"HTTP {resp.status}"
                html = await resp.text()
                soup = BeautifulSoup(html, 'lxml')
                meta = soup.find("meta", {"name": "description"})
                if meta and meta.get("content"):
                    return item_id, meta["content"], None
                return item_id, None, "No description meta"
        except asyncio.TimeoutError:
            return item_id, None, "Timeout"
        except Exception as e:
            return item_id, None, str(e)[:50]

async def main():
    print(f"=== Тест фильтра описаний ===")
    print(f"Заблокированные слова: {blocked_words()}")
    print(f"Всего URL: {len(URLS)}")
    print(f"Прокси: {'Да' if PROXY else 'Нет'}")
    print()

    blocked = []
    passed = []
    errors = []

    connector = aiohttp.TCPConnector(limit=10, ssl=False)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }

    semaphore = asyncio.Semaphore(5)

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        tasks = [fetch_description(session, url, semaphore) for url in URLS]

        for i, coro in enumerate(asyncio.as_completed(tasks)):
            item_id, description, error = await coro

            if error:
                errors.append((item_id, error))
                print(f"[{i+1}/{len(URLS)}] {item_id}: ERROR - {error}")
                continue

            is_blocked, word = check_description(description)

            if is_blocked:
                blocked.append((item_id, word, description[:100] if description else ""))
                print(f"[{i+1}/{len(URLS)}] {item_id}: BLOCKED by '{word}'")
            else:
                passed.append(item_id)
                print(f"[{i+1}/{len(URLS)}] {item_id}: OK")

    # Итоговая сводка
    print("\n" + "="*60)
    print("=== СВОДКА ===")
    print("="*60)

    print(f"\n✅ ПРОШЛИ ФИЛЬТР: {len(passed)}")

    print(f"\n❌ ЗАБЛОКИРОВАНЫ: {len(blocked)}")
    if blocked:
        print("-" * 40)
        # Группируем по причине
        by_reason = {}
        for item_id, word, _ in blocked:
            by_reason.setdefault(word, []).append(item_id)

        for word, items in sorted(by_reason.items(), key=lambda x: -len(x[1])):
            print(f"  '{word}': {len(items)} шт.")
            for item_id in items:
                print(f"    - https://www.ebay.com/itm/{item_id}")

    print(f"\n⚠️  ОШИБКИ: {len(errors)}")
    if errors:
        print("-" * 40)
        for item_id, err in errors[:10]:
            print(f"  {item_id}: {err}")
        if len(errors) > 10:
            print(f"  ... и ещё {len(errors)-10}")

if __name__ == "__main__":
    asyncio.run(main())

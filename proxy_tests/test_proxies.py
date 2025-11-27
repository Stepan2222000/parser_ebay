#!/usr/bin/env python3
"""
Независимый тест прокси для предоставления доказательств поддержке.
Тестирует все прокси из proxies.txt через несколько тестовых сайтов
"""

import asyncio
import aiohttp
import json
import time
from datetime import datetime
from pathlib import Path
from collections import defaultdict

# Список тестовых URL
TEST_URLS = [
    "https://api.ipify.org",
    "https://ident.me",
    "http://checkip.amazonaws.com",
    "http://icanhazip.com",
    "https://ifconfig.me/ip",
    "http://eth0.me",
]

TIMEOUT = 10  # секунд
MAX_CONCURRENT = 20  # параллельных запросов


def load_proxies():
    """Загружает прокси из файла"""
    proxies_file = Path(__file__).parent.parent / "proxies.txt"
    proxies = []

    with open(proxies_file) as f:
        for line in f:
            line = line.strip()
            if line:
                parts = line.split(":")
                if len(parts) == 4:
                    host, port, username, password = parts
                    proxies.append({
                        "host": host,
                        "port": port,
                        "username": username,
                        "password": password,
                        "proxy_url": f"http://{username}:{password}@{host}:{port}"
                    })
    return proxies


async def test_proxy_url(session, proxy, test_url, semaphore):
    """Тестирует один прокси на одном URL"""
    async with semaphore:
        result = {
            "proxy": f"{proxy['host']}:{proxy['port']}",
            "test_url": test_url,
            "status": None,
            "error": None,
            "error_type": None,
            "response_ip": None,
            "response_time_ms": None,
        }

        start = time.time()

        try:
            async with session.get(
                test_url,
                proxy=proxy["proxy_url"],
                timeout=aiohttp.ClientTimeout(total=TIMEOUT)
            ) as response:
                elapsed = (time.time() - start) * 1000
                result["response_time_ms"] = round(elapsed, 2)
                result["status"] = response.status

                if response.status == 200:
                    try:
                        text = await response.text()
                        result["response_ip"] = text.strip()
                        result["error_type"] = "OK"
                    except:
                        result["error_type"] = "OK (no text)"
                else:
                    result["error_type"] = f"HTTP {response.status}"
                    result["error"] = f"Status code: {response.status}"

        except aiohttp.ClientProxyConnectionError as e:
            result["error_type"] = "ProxyConnectionError"
            result["error"] = str(e)[:100]
        except aiohttp.ClientHttpProxyError as e:
            result["error_type"] = f"ProxyError ({e.status})"
            result["error"] = str(e)[:100]
        except aiohttp.ClientResponseError as e:
            result["error_type"] = f"ClientResponseError ({e.status})"
            result["error"] = str(e)[:100]
        except asyncio.TimeoutError:
            result["error_type"] = "Timeout"
            result["error"] = f"Timeout after {TIMEOUT}s"
        except aiohttp.ClientConnectorError as e:
            result["error_type"] = "ConnectionError"
            result["error"] = str(e)[:100]
        except Exception as e:
            result["error_type"] = type(e).__name__
            result["error"] = str(e)[:100]

        return result


async def get_server_ip():
    """Получает IP сервера без прокси"""
    try:
        async with aiohttp.ClientSession() as session:
            for url in ["https://api.ipify.org", "https://ifconfig.me", "http://ip-api.com/json"]:
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            text = await resp.text()
                            if url == "http://ip-api.com/json":
                                data = json.loads(text)
                                return data.get("query", text)
                            return text.strip()
                except:
                    continue
    except:
        pass
    return "unknown"


def get_url_short_name(url):
    """Получает короткое имя для URL"""
    domain = url.replace("https://", "").replace("http://", "").split("/")[0]
    return domain


async def main():
    print("=" * 80)
    print("ТЕСТ ПРОКСИ - МУЛЬТИСАЙТОВАЯ ПРОВЕРКА")
    print("=" * 80)

    # Информация о тесте
    server_ip = await get_server_ip()
    test_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"\nДата: {test_time}")
    print(f"IP сервера: {server_ip}")
    print(f"Тестовые URL ({len(TEST_URLS)}):")
    for url in TEST_URLS:
        print(f"  - {url}")
    print(f"Таймаут: {TIMEOUT} сек")

    # Загрузка прокси
    proxies = load_proxies()
    print(f"\nЗагружено прокси: {len(proxies)}")
    total_tests = len(proxies) * len(TEST_URLS)
    print(f"Всего будет выполнено тестов: {total_tests}")
    print("\nТестирование...")
    print("-" * 80)

    # Тестирование всех комбинаций прокси x URL
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for proxy in proxies:
            for test_url in TEST_URLS:
                tasks.append(test_proxy_url(session, proxy, test_url, semaphore))

        # Показываем прогресс
        results = []
        completed = 0
        for coro in asyncio.as_completed(tasks):
            result = await coro
            results.append(result)
            completed += 1

            # Вывод прогресса каждые 50 тестов
            if completed % 50 == 0 or completed == total_tests:
                print(f"  Прогресс: {completed}/{total_tests} ({round(completed/total_tests*100, 1)}%)")

    print("-" * 80)

    # Организация результатов в матрицу: proxy -> {url: result}
    proxy_results = defaultdict(dict)
    for result in results:
        proxy_results[result["proxy"]][result["test_url"]] = result

    # Анализ результатов
    proxy_stats = {}
    for proxy, url_results in proxy_results.items():
        working_urls = sum(1 for r in url_results.values() if r["error_type"] and r["error_type"].startswith("OK"))
        total_urls = len(url_results)
        proxy_stats[proxy] = {
            "working": working_urls,
            "total": total_urls,
            "percent": round(working_urls / total_urls * 100, 1) if total_urls > 0 else 0,
            "url_results": url_results
        }

    # Сортируем прокси по количеству работающих URL (лучшие первыми)
    sorted_proxies = sorted(proxy_stats.items(), key=lambda x: (-x[1]["working"], x[0]))

    # Статистика по URL
    url_stats = {}
    for url in TEST_URLS:
        working_proxies = sum(1 for proxy_res in proxy_results.values()
                             if proxy_res[url]["error_type"] and proxy_res[url]["error_type"].startswith("OK"))
        url_stats[url] = {
            "working": working_proxies,
            "total": len(proxies),
            "percent": round(working_proxies / len(proxies) * 100, 1) if proxies else 0
        }

    # Общая статистика
    fully_working = sum(1 for stats in proxy_stats.values() if stats["working"] == len(TEST_URLS))
    partially_working = sum(1 for stats in proxy_stats.values() if 0 < stats["working"] < len(TEST_URLS))
    not_working = sum(1 for stats in proxy_stats.values() if stats["working"] == 0)

    print(f"\nОБЩАЯ СТАТИСТИКА:")
    print(f"  Полностью работающие (все {len(TEST_URLS)} сайтов):  {fully_working} из {len(proxies)} ({round(fully_working/len(proxies)*100, 1)}%)")
    print(f"  Частично работающие:  {partially_working} из {len(proxies)} ({round(partially_working/len(proxies)*100, 1)}%)")
    print(f"  Не работающие:        {not_working} из {len(proxies)} ({round(not_working/len(proxies)*100, 1)}%)")

    print(f"\nСТАТИСТИКА ПО САЙТАМ:")
    for url in TEST_URLS:
        stats = url_stats[url]
        print(f"  {get_url_short_name(url):25} {stats['working']:3}/{stats['total']} ({stats['percent']:5.1f}%)")

    # Сохранение отчёта
    report_dir = Path(__file__).parent

    # Текстовый отчёт с матрицей
    report_txt = f"""{'=' * 80}
ТЕСТ ПРОКСИ - МУЛЬТИСАЙТОВАЯ ПРОВЕРКА - ОТЧЁТ ДЛЯ ПОДДЕРЖКИ
{'=' * 80}

Дата теста: {test_time}
IP сервера: {server_ip}
Таймаут: {TIMEOUT} сек

Тестовые URL ({len(TEST_URLS)}):
"""
    for url in TEST_URLS:
        report_txt += f"  - {url}\n"

    report_txt += f"""
{'=' * 80}
ОБЩАЯ СТАТИСТИКА:
{'=' * 80}
Всего прокси: {len(proxies)}
Полностью работающие (все {len(TEST_URLS)} сайтов): {fully_working} ({round(fully_working/len(proxies)*100, 1)}%)
Частично работающие: {partially_working} ({round(partially_working/len(proxies)*100, 1)}%)
Не работающие: {not_working} ({round(not_working/len(proxies)*100, 1)}%)

{'=' * 80}
СТАТИСТИКА ПО САЙТАМ:
{'=' * 80}
"""
    for url in TEST_URLS:
        stats = url_stats[url]
        report_txt += f"{get_url_short_name(url):25} {stats['working']:3}/{stats['total']} ({stats['percent']:5.1f}%)\n"

    report_txt += f"""
{'=' * 80}
ДЕТАЛИ ПО КАЖДОМУ ПРОКСИ (отсортировано по количеству работающих сайтов):
{'=' * 80}
"""

    # Матрица результатов
    url_short_names = [get_url_short_name(url) for url in TEST_URLS]
    header = f"{'Прокси':<22} | {'Успех':>5} | " + " | ".join(f"{name[:12]:^12}" for name in url_short_names)
    report_txt += "\n" + header + "\n"
    report_txt += "-" * len(header) + "\n"

    for proxy, stats in sorted_proxies:
        row = f"{proxy:<22} | {stats['working']:>2}/{stats['total']:<2} | "
        cells = []
        for url in TEST_URLS:
            result = stats["url_results"][url]
            if result["error_type"] and result["error_type"].startswith("OK"):
                ms = result["response_time_ms"]
                cell = f"✓ {ms:>4.0f}ms"
            else:
                error = result["error_type"] if result["error_type"] else "Error"
                if "503" in str(error):
                    cell = "✗ 503"
                elif "Timeout" in str(error):
                    cell = "✗ Timeout"
                elif "Connection" in str(error):
                    cell = "✗ Conn"
                elif "Proxy" in str(error):
                    cell = "✗ Proxy"
                else:
                    cell = f"✗ {error[:8]}"
                cell = cell[:12]
            cells.append(f"{cell:^12}")

        row += " | ".join(cells)
        report_txt += row + "\n"

    # Детальная информация
    report_txt += f"""

{'=' * 80}
ДЕТАЛЬНАЯ ИНФОРМАЦИЯ ПО РАБОТАЮЩИМ ПРОКСИ:
{'=' * 80}
"""

    for proxy, stats in sorted_proxies:
        if stats["working"] > 0:
            report_txt += f"\n{proxy} - работает {stats['working']}/{stats['total']} сайтов:\n"
            for url in TEST_URLS:
                result = stats["url_results"][url]
                if result["error_type"] and result["error_type"].startswith("OK"):
                    ip = result["response_ip"]
                    ms = result["response_time_ms"]
                    report_txt += f"  ✓ {get_url_short_name(url):25} IP: {ip:15} ({ms:6.1f}ms)\n"
                else:
                    error = result["error_type"] if result["error_type"] else "Unknown"
                    report_txt += f"  ✗ {get_url_short_name(url):25} {error}\n"

    # Сохраняем текстовый отчёт
    with open(report_dir / "report.txt", "w", encoding="utf-8") as f:
        f.write(report_txt)

    # JSON отчёт
    report_json = {
        "test_info": {
            "date": test_time,
            "server_ip": server_ip,
            "test_urls": TEST_URLS,
            "timeout_sec": TIMEOUT,
        },
        "summary": {
            "total_proxies": len(proxies),
            "total_urls": len(TEST_URLS),
            "total_tests": total_tests,
            "fully_working": fully_working,
            "partially_working": partially_working,
            "not_working": not_working,
        },
        "url_stats": {url: stats for url, stats in url_stats.items()},
        "proxy_results": {
            proxy: {
                "working": stats["working"],
                "total": stats["total"],
                "percent": stats["percent"],
                "url_results": {url: {
                    "status": r["status"],
                    "error_type": r["error_type"],
                    "error": r["error"],
                    "response_ip": r["response_ip"],
                    "response_time_ms": r["response_time_ms"],
                } for url, r in stats["url_results"].items()}
            } for proxy, stats in proxy_stats.items()
        },
        "raw_results": results,
    }

    with open(report_dir / "report.json", "w", encoding="utf-8") as f:
        json.dump(report_json, f, indent=2, ensure_ascii=False)

    print(f"\nОтчёты сохранены:")
    print(f"  {report_dir / 'report.txt'}")
    print(f"  {report_dir / 'report.json'}")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())

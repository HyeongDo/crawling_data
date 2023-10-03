from bs4 import BeautifulSoup
import os
import csv
import logging
import asyncio
import aiohttp


LOGIN_INFO = {
    'username': 'username',
    'password': 'password'
}

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 Edge/16.16299'
}


file_name = 'file_name'

if os.path.isfile(f'{file_name}.csv'):
    for i in range(0, 1000):
        if not os.path.isfile(f'{file_name}_{i}.csv'):
            file_name = 'untappd' + '_' + str(i)
            break

log_file = f'{file_name}.log'
logging.basicConfig(level=logging.INFO)
file_handler = logging.FileHandler(log_file)
logging.getLogger('').addHandler(file_handler)

semaphore = asyncio.Semaphore(10)

header = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']

login_url = ''
scrape_url = ''

with open(f'{file_name}.csv', mode='w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=header)
    w.writeheader()


async def fetch_and_scrape_chunk(session, chunk):
    for page_number in chunk:
        url = f"{scrape_url}/{page_number}"

        try:
            async with semaphore:
                try:
                    async with session.get(url, headers=headers, timeout=10) as response:
                        try:
                            redirected_url = str(response.url)
                            response_text = await response.text()
                            soup = BeautifulSoup(response_text, "html.parser")

                            if redirected_url.split('/')[-1] != url.split('/')[-1]:
                                url = redirected_url
                                async with session.get(url, headers=headers, timeout=10) as response:
                                    try:
                                        response_text = await response.text()
                                        soup = BeautifulSoup(response_text, "html.parser")
                                    except asyncio.TimeoutError as e:
                                        logging.error(f'{page_number}, timeout. {e}')
                                        continue
                        except asyncio.TimeoutError as e:
                            logging.error(f'{page_number}, timeout. {e}')

                        try:
                            sub = soup.find('div', class_='c').text
                            if 'Expected Word' not in sub:
                                continue

                            data = {}

                            a = soup.find('div', class_='a').find("p").get_text(strip=True)
                            b = soup.find('p', class_='b').get_text(strip=True)
                            c = soup.find('div', class_='c').get_text(strip=True)
                            d = soup.find('p', class_='d').get_text(strip=True)
                            e = soup.find('p', class_='e').get_text(strip=True)
                            f = soup.find('p', class_='f').get_text(strip=True)
                            g = soup.find('span', class_='g').get_text(strip=True)
                            h = soup.find('p', class_='h').get_text(strip=True)

                            data['a'] = a
                            data['b'] = b
                            data['c'] = c
                            data['d'] = d
                            data['e'] = e
                            data['f'] = f
                            data['g'] = g
                            data['h'] = h

                            data = [data]
                            with open(f'{file_name}.csv', mode='a', newline='', encoding='utf-8') as f:
                                w = csv.DictWriter(f, fieldnames=header)
                                for r in data:
                                    w.writerow(r)
                            logging.info(f'{page_number} saved.')
                        except:
                            logging.error(f'{page_number}, no wanted.')
                except asyncio.TimeoutError as e:
                    logging.error(f'{page_number}, timeout. {e}')

        except aiohttp.ClientError as e:
            logging.error(f'{page_number}, timeout. {e}')

        finally:
            with open(f'{file_name}_page_numbers.txt', 'a') as page_numbers_file:
                page_numbers_file.write(f'{page_number}\n')


async def main():
    try:
        async with aiohttp.ClientSession() as session:
            login_request = await session.post(login_url, data=LOGIN_INFO, headers=headers)
            if login_request.status != 200:
                logging.error('login fail.')
                return

            tasks = []
            total_pages = 5548766
            chunk_size = 100

            # 제외할 파일들 등록
            except_numbers_files = ['file1', 'file2', 'file3']

            if except_numbers_files:
                all_saved_page_numbers = set()

                for except_numbers in except_numbers_files:
                    if except_numbers:
                        with open(f'{except_numbers}.txt', 'r') as page_numbers_file:
                            saved_page_numbers = set(int(line.strip()) for line in page_numbers_file)
                        all_saved_page_numbers.update(saved_page_numbers)

                unsaved_page_numbers = [page for page in range(1, total_pages + 1) if
                                        page not in all_saved_page_numbers]

                for chunk_start in range(0, len(unsaved_page_numbers), chunk_size):
                    chunk_end = min(chunk_start + chunk_size, len(unsaved_page_numbers))
                    chunk = unsaved_page_numbers[chunk_start:chunk_end]
                    try:
                        task = fetch_and_scrape_chunk(session, chunk)
                        tasks.append(task)
                    except Exception as e:
                        logging.error(f'chunk {chunk_start} {chunk_end} error. {e}')

            else:

                for chunk_start in range(1, total_pages + 1, chunk_size):
                    chunk_end = min(chunk_start + chunk_size, total_pages + 1)
                    chunk = list(range(chunk_start, chunk_end))
                    try:
                        task = fetch_and_scrape_chunk(session, chunk)
                        tasks.append(task)
                    except Exception as e:
                        logging.error(f'chunk {chunk_start} {chunk_end} error. {e}')

            await asyncio.gather(*tasks)
    except Exception as e:
        logging.error(f'login session error, {e}')


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

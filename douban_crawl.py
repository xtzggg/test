import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
from datetime import datetime
from Helper import MySqlHelper


def crawl_douban_top100():
    """
    爬取豆瓣电影 Top 100
    返回：列表，每个元素为字典 {'movie_rank': int, 'title': str, 'director': str, 'year': int, 'rating': float, 'reviews_count': int, 'genre': str, 'country': str, 'description': str}
    """
    base_url = 'https://movie.douban.com/top250'
    options = Options()
    options.add_argument('--headless')
    options.add_argument(
        '--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36')

    service = Service('/usr/local/bin/chromedriver')

    try:
        driver = webdriver.Chrome(service=service, options=options)
        movie_list = []

        for start in range(0, 100, 25):  # Top 100: 0, 25, 50, 75
            url = f"{base_url}?start={start}"
            print(f"爬取第 {start // 25 + 1} 页: {url}")
            driver.get(url)
            time.sleep(2)  # 延时避免反爬

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            items = soup.select('.item')

            for idx, item in enumerate(items):
                try:
                    movie_rank = start + idx + 1
                    title_elem = item.select_one('.title')
                    title = title_elem.text.strip() if title_elem else None

                    info_elem = item.select_one('.info .bd p:first-child')
                    info_text = info_elem.text.strip() if info_elem else ''
                    director = info_text.split('导演: ')[1].split('\n')[0].strip() if '导演: ' in info_text else None
                    parts = info_text.split('/')
                    country = parts[-2].strip() if len(parts) > 2 else None
                    genre = parts[-3].strip() if len(parts) > 2 else None
                    year = int(parts[-1].strip()) if parts[-1].strip().isdigit() else None

                    rating_elem = item.select_one('.rating_num')
                    rating = float(rating_elem.text.strip()) if rating_elem else None

                    reviews_elem = item.select_one('.star span:last-child')
                    reviews_text = reviews_elem.text.strip() if reviews_elem else ''
                    reviews_count = int(''.join(
                        filter(str.isdigit, reviews_text))) if reviews_text and '人评价' in reviews_text else None

                    quote_elem = item.select_one('.quote .inq')
                    description = quote_elem.text.strip() if quote_elem else None

                    movie_list.append({
                        'movie_rank': movie_rank,
                        'title': title,
                        'director': director,
                        'year': year,
                        'rating': rating,
                        'reviews_count': reviews_count,
                        'genre': genre,
                        'country': country,
                        'description': description
                    })

                except Exception as e:
                    print(f"解析第 {movie_rank} 部电影失败: {e}")
                    continue

        driver.quit()
        print(f"爬取成功！总计 {len(movie_list)} 部电影，时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return movie_list

    except Exception as e:
        print(f"爬取失败：{e}")
        if 'driver' in locals():
            driver.quit()
        return []


# 主程序
if __name__ == "__main__":
    # 1. 爬取数据
    top100 = crawl_douban_top100()

    # 打印前 5 条以调试
    if top100:
        print("爬取的电影数据（前 5 条）：")
        for movie in top100[:5]:
            print(
                f"{movie['movie_rank']}. {movie['title']} ({movie['year']}) - 评分: {movie['rating']} - 导演: {movie['director'][:20] if movie['director'] else 'N/A'}... - 类型: {movie['genre']} - 国家: {movie['country']}")
    else:
        print("无数据爬取")

    # 2. 初始化 MySqlHelper
    try:
        db = MySqlHelper(
            host='localhost',
            user='root',
            password='12345678',
            database='test'
        )
    except Exception as e:
        print(f"Failed to initialize MySqlHelper: {e}")
        exit(1)

    # 3. 批量插入数据库
    if top100:
        inserted = db.insert_movies(top100)
        print(f"共插入 {inserted} 条电影记录")
    else:
        print("无数据插入")
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from Helper import MySqlHelper  # 修正导入
import time


def crawl_baidu_hot_top10_selenium():
    """
    爬取百度热搜 Top 10 关键词及其热度
    返回：列表，每个元素为字典 {'rank': int, 'keyword': str, 'heat': int}
    """
    url = 'https://top.baidu.com/board?tab=realtime'
    options = Options()
    options.add_argument('--headless')
    options.add_argument(
        '--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36')

    try:
        driver = webdriver.Chrome(service=Service('/usr/local/bin/chromedriver'), options=options)
        time.sleep(3)
        driver.get('https://top.baidu.com/board?tab=realtime')
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        print(soup.select('.title-content-title')[:10])
        print(soup.select('.hot-index_1Bl1a')[:10])
        print(soup.prettify()[:2000])
        items = soup.select('.title-content-title')[:10]  # 关键词选择器
        heat_elements = soup.select('.hot-index_1Bl1a')[:10]  # 热度选择器
        # print(soup.select('.c-single-text-ellipsis')[:10])
        # print(soup.select('.hot-index_1Bl1a')[:10])
        driver.quit()

        hot_list = []
        # 更新选择器，基于百度热搜页面结构
        items = soup.select('.c-single-text-ellipsis')[:10]
        for idx, item in enumerate(items, 1):
            keyword = item.text.strip() if item else f"关键词 {idx}"
            heat_elem = item.find_parent().select_one('.hot-index_1Bl1a')
            heat = int(heat_elem.text.strip().replace(',', '')) if heat_elem else 0
            hot_list.append({'rank': idx, 'keyword': keyword, 'heat': heat})

        print(f"爬取成功！时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return hot_list

    except Exception as e:
        print(f"爬取失败：{e}")
        return []


# 主程序
if __name__ == "__main__":
    # 1. 爬取数据
    top10 = crawl_baidu_hot_top10_selenium()

    # 打印爬取结果以调试
    if top10:
        print("爬取的热搜数据：")
        for item in top10:
            print(f"{item['rank']}. {item['keyword']} (热度: {item['heat']})")
    else:
        print("无数据爬取")

    # 2. 初始化 MySqlHelper
    try:
        db = MySqlHelper(
            host='localhost',
            user='root',
            password='12345678',  # 确认密码正确
            database='test'  # 确认数据库存在
        )
    except Exception as e:
        print(f"Failed to initialize MySqlHelper: {e}")
        exit(1)

    # 3. 插入数据库
    if top10:
        inserted = db.insert_hot_searches(top10)
        print(f"共插入 {inserted} 条记录")
    else:
        print("无数据插入")



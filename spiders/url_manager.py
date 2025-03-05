# spiders/url_manager.py
from kafka import KafkaProducer
import time

class URLManager:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: str(v).encode('utf-8')
        )
    
    def add_seed_urls(self, urls):
        for url in urls:
            self.producer.send('url_seeds', value=url)
            print(f"Sent: {url}")
            time.sleep(0.5)

if __name__ == "__main__":
    manager = URLManager()
    manager.add_seed_urls([
        'https://news.sina.com.cn',     # 新浪新闻
        'https://www.163.com',          # 网易新闻
        'https://www.sohu.com'          # 搜狐新闻
    ])

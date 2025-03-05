# spiders/crawler_worker.py
import os
import random
import time
import multiprocessing
import logging
from kafka import KafkaConsumer, KafkaProducer
import requests
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
import socks
import socket

# 配置日志格式
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

class CrawlerWorker:
    def __init__(self):
        self.seen_urls = set()  # 本地URL去重集合
        self.user_agent = "Mozilla/5.0 (compatible; MyCrawler/1.0; +http://example.com/bot)"
        self.proxy_pool = self._init_proxy_pool()  # 初始化代理池
        self.kafka_producer = self._init_kafka_producer()
        self.kafka_consumer = self._init_kafka_consumer()
        
        # 配置SOCKS支持（如果使用）
        self._setup_socks_support()

    def _init_proxy_pool(self):
        """初始化代理池"""
        proxy_file = os.path.abspath(
            os.path.join(os.path.dirname(__file__), '..', 'proxies.txt')
        )
        valid_proxy_file = os.path.abspath(
            os.path.join(os.path.dirname(__file__), '..', 'valid_proxies.txt')
        )

        # 优先加载已验证代理
        if os.path.exists(valid_proxy_file):
            return self._load_proxies(valid_proxy_file)
            
        # 测试并保存有效代理
        raw_proxies = self._load_proxies(proxy_file)
        valid_proxies = self._test_proxies(raw_proxies)
        self._save_valid_proxies(valid_proxies, valid_proxy_file)
        return valid_proxies

    def _load_proxies(self, file_path):
        """从文件加载代理"""
        proxies = []
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        protocol, addr = line.split('://')
                        ip, port = addr.split(':')
                        proxies.append({
                            'protocol': protocol,
                            'ip': ip,
                            'port': int(port)
                        })
            logging.info(f"成功加载 {len(proxies)} 个代理 from {os.path.basename(file_path)}")
        except Exception as e:
            logging.error(f"加载代理文件失败: {str(e)}")
        return proxies

    def _test_proxies(self, proxies, test_url='http://httpbin.org/ip', timeout=5):
        """测试代理有效性"""
        valid_proxies = []
        logging.info(f"开始测试 {len(proxies)} 个代理...")
        
        for proxy in proxies:
            try:
                start = time.time()
                response = requests.get(
                    test_url,
                    proxies=self._format_proxy(proxy),
                    timeout=timeout
                )
                if response.status_code == 200:
                    speed = time.time() - start
                    logging.info(f"有效代理: {proxy['protocol']}://{proxy['ip']}:{proxy['port']} 响应时间: {speed:.2f}s")
                    valid_proxies.append(proxy)
            except Exception as e:
                continue
                
        logging.info(f"有效代理数量: {len(valid_proxies)}/{len(proxies)}")
        return valid_proxies

    def _save_valid_proxies(self, proxies, file_path):
        """保存有效代理列表"""
        try:
            with open(file_path, 'w') as f:
                for p in proxies:
                    f.write(f"{p['protocol']}://{p['ip']}:{p['port']}\n")
            logging.info(f"成功保存 {len(proxies)} 个有效代理")
        except Exception as e:
            logging.error(f"保存代理文件失败: {str(e)}")

    def _format_proxy(self, proxy):
        """格式化代理为requests需要的格式"""
        proxy_url = f"{proxy['protocol']}://{proxy['ip']}:{proxy['port']}"
        return {
            'http': proxy_url,
            'https': proxy_url
        }

    def _setup_socks_support(self):
        """配置SOCKS代理支持"""
        try:
            socks.set_default_proxy()
            socket.socket = socks.socksocket
            logging.info("SOCKS代理支持已启用")
        except Exception as e:
            logging.error(f"SOCKS支持配置失败: {str(e)}")

    def _init_kafka_producer(self):
        """初始化Kafka生产者"""
        return KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: str(v).encode('utf-8'),
            retries=5,
            request_timeout_ms=30000
        )

    def _init_kafka_consumer(self):
        """初始化Kafka消费者"""
        return KafkaConsumer(
            'url_seeds',
            bootstrap_servers='localhost:9092',
            group_id='crawler-cluster',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            max_poll_records=100
        )

    def _check_robots(self, base_url):
        """检查robots.txt"""
        rp = RobotFileParser()
        try:
            rp.set_url(f"{base_url}/robots.txt")
            rp.read()
            return rp
        except Exception as e:
            logging.warning(f"无法获取robots.txt: {str(e)}")
            return None

    def _check_meta_robots(self, soup):
        """检查网页meta robots标签"""
        meta_robots = soup.find('meta', attrs={'name': 'robots'})
        if meta_robots:
            directives = meta_robots.get('content', '').lower().split(',')
            if 'noindex' in directives:
                return False
        return True

    def _process_page(self, url):
        """处理单个页面"""
        if url in self.seen_urls:
            return
        self.seen_urls.add(url)

        # 解析基础URL
        base_url = '/'.join(url.split('/')[:3])
        robots = self._check_robots(base_url)

        # 遵守robots协议
        if robots and not robots.can_fetch(self.user_agent, url):
            logging.info(f"Blocked by robots.txt: {url}")
            return

        try:
            # 随机选择代理
            proxy = random.choice(self.proxy_pool) if self.proxy_pool else None
            proxy_config = self._format_proxy(proxy) if proxy else None
            logging.info(f"正在爬取: {url} [使用代理: {proxy['ip'] if proxy else '无'}]")

            # 遵守crawl-delay
            delay = random.uniform(1, 3)
            if robots and robots.crawl_delay(self.user_agent):
                delay = max(delay, robots.crawl_delay(self.user_agent))
            time.sleep(delay)

            # 发送请求
            response = requests.get(
                url,
                headers={'User-Agent': self.user_agent},
                proxies=proxy_config,
                timeout=15
            )

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'lxml')
                
                # 检查meta robots
                if not self._check_meta_robots(soup):
                    logging.info(f"Blocked by meta tag: {url}")
                    return

                # 存储到Kafka
                self.kafka_producer.send('processed_urls', value=url)
                
                # 提取新链接
                for link in soup.find_all('a', href=True):
                    new_url = link['href'].strip()
                    if new_url.startswith('http'):
                        self.kafka_producer.send('url_seeds', value=new_url)

        except Exception as e:
            logging.error(f"请求失败: {url} - {str(e)}")
            # 移除失效代理
            if proxy and proxy in self.proxy_pool:
                self.proxy_pool.remove(proxy)
                logging.info(f"移除失效代理: {proxy['ip']}:{proxy['port']}")

    def run(self):
        """主运行循环"""
        for message in self.kafka_consumer:
            url = message.value.decode('utf-8')
            self._process_page(url)

if __name__ == "__main__":
    # 依赖检查
    try:
        import socks
    except ImportError:
        logging.critical("缺少依赖库: 请执行 pip install requests[socks]")
        exit(1)

    # 启动多进程
    workers = []
    for _ in range(4):
        p = multiprocessing.Process(target=CrawlerWorker().run)
        p.start()
        workers.append(p)
        logging.info(f"启动爬虫进程 PID: {p.pid}")

    try:
        for p in workers:
            p.join()
    except KeyboardInterrupt:
        logging.info("接收到中断信号，停止爬虫...")
        for p in workers:
            p.terminate()

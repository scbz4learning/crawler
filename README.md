# 简易分布式爬虫系统

基于Kafka的分布式网页爬虫，遵守Robots协议，支持代理池与分布式部署。

## 技术栈

- **消息队列**: Kafka (Docker部署)
- **爬虫框架**: Python + Requests + BeautifulSoup
- **代理管理**: 自动验证代理池
- **存储**: Kafka Topic持久化
- **部署**: Docker Compose

## 核心功能

✅ 分布式URL调度  
✅ 智能代理轮换 (HTTP/SOCKS)  
✅ Robots协议遵守  
✅ 网页内容自动解析  
✅ Kafka消息持久化  
✅ 多进程并发爬取  

## 项目结构

```
.
├── docker-compose.yml    # Kafka集群配置
├── proxies.txt          # 代理服务器列表
├── spiders/             # 爬虫核心代码
│   ├── crawler_worker.py # 爬虫工作节点
│   └── url_manager.py    # URL管理器
└── requirements.txt     # Python依赖库
```

## 快速开始

### 1. 环境准备

```bash
# 安装依赖
sudo apt install docker.io docker-compose python3-pip
pip install -r requirements.txt
```

### 2. 启动Kafka集群

```bash
docker-compose up -d
# 等待60秒初始化
sleep 60
```

### 3. 配置代理池

编辑`proxies.txt`文件:

```
http://12.34.56.78:8080
socks4://87.65.43.21:1080
```

### 4. 运行爬虫系统

```bash
# 启动URL管理器
python3 spiders/url_manager.py

# 启动爬虫工作节点（多个终端执行）
python3 spiders/crawler_worker.py
```

## 配置说明

### 代理配置

- 格式: `协议://IP:端口`
- 支持协议: http/https/socks
- 自动验证: 首次运行生成`valid_proxies.txt`

### Kafka主题

| 主题名称         | 说明                |
|------------------|--------------------|
| url_seeds       | 初始种子URL         |
| processed_urls  | 已处理URL记录       |

## 监控与调试

```bash
# 查看已处理URL
docker-compose exec kafka kafka-console-consumer \
  --topic processed_urls \
  --bootstrap-server localhost:9092

# 查看爬虫日志
tail -f crawler.log
```



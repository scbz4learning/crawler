from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('test_topic', b'test_message')
print("Connection successful!")

from kafka import KafkaConsumer

consumidor = KafkaConsumer('quickstart-events', bootstrap_servers=['localhost:9092'])

for message in consumidor:
        print(message.value.decode("utf-8"))


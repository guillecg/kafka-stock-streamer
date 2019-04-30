


if __name__ == '__main__':
    consumer = KafkaConsumer(
        TOPIC_NAME, group_id='group1', bootstrap_servers=BOOTSTRAP_SERVERS
    )

import pandas as pd

from datetime import datetime

from streaming.iex_producer import IEXProducer


BOOTSTRAP_SERVERS = ['localhost:9092', 'localhost:9093', 'localhost:9094']
TOPIC_NAME = 'iexfinance'

# IDEAS:
# Train with get_historical_data (one year), predict with Kafka streaming (one month)
# Visualization: simple dash with temporal series plot
# Seasonality/trend: advise wheter it's better to buy/sell/wait
# TEST: launch zookeeper + one broker to test reliability of the system

# Memoria Kafka:
# 1.  detalles de configuración de Kafka
# 2.  qué hace el código
# 3.  detalles de ejecución
# 4.  pruebas de funcionamiento

# TODO
# - More than one broker (server-1.properties, server-2.properties, etc.)
#       SEE: https://kafka.apache.org/quickstart#quickstart_multibroker
#       SEE: https://github.com/zoidbergwill/docker-compose-kafka/blob/master/docker-compose.yml
#       SEE (OK): https://gree2.github.io/docker/2016/05/29/kafka-docker
#       SEE: https://nathancatania.com/posts/deploying-a-multi-node-kafka-cluster-with-docker
#       SEE: http://blog.sysco.no/devops/integration/scale-kafka-containers/
# - More than one topic (one per stock), both produce and consume
# - Topic partitions: https://stackoverflow.com/questions/35432326/how-to-get-latest-offset-for-a-partition-for-a-kafka-topic
# - Tests: broker failure, wrong data input

# FOR ERRORS:
# Confluent Kafka: https://github.com/CloudKarafka/python-kafka-example/blob/master/producer.py
# PyKafka: https://pykafka.readthedocs.io/en/latest/
#       SEE: https://nathancatania.com/posts/deploying-a-multi-node-kafka-cluster-with-docker
#       SEE: http://blog.sysco.no/devops/integration/scale-kafka-containers/

# DOCKER:
# FOR /f "tokens=*" %i IN ('docker ps -a -q') DO docker rm %i


if __name__ == '__main__':
    companies = pd.read_excel('data/companies.xlsx', header=0)
    symbols = companies.Symbol.values.tolist()

    for symbol in symbols[:3]:
        kwargs = {
            'bootstrap_servers': BOOTSTRAP_SERVERS,
            'topic_name': TOPIC_NAME + '_' + symbol,
        }

        producer = IEXProducer(**kwargs)
        producer.publish_stocks(symbol)

    from iexfinance.stocks import Stock
    from iexfinance.stocks import get_historical_data
    stock = Stock(symbols[0])
    stock.get_all().keys()
    stock.get_company()['companyName']

    start = datetime(2017, 1, 1)
    end = datetime(2018, 1, 1)

    historic = get_historical_data(symbols[0], start, end)

    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        TOPIC_NAME,
        group_id='group1',
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        # value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )

    msg_list = []
    for msg in consumer:
        msg_list.append(msg.value)
        break

    consumer.metrics()

    import json
    data_dict = json.loads(msg_list[0].decode())
    data_dict['chart']


    # Plots
    import matplotlib.pyplot as plt
    import seaborn as sns

    dates = [elem['date'] for elem in data_dict['chart']]
    close_prices = [elem['close'] for elem in data_dict['chart']]

    plot = sns.lineplot(dates, close_prices)
    temp = plot.set_xticklabels(labels=dates, rotation=45)

    dates = list(historic.keys())
    close_prices = [elem['close'] for elem in historic.values()]

    plot = sns.lineplot(dates, close_prices)
    temp = plot.set_xticklabels(labels=dates, rotation=45)


    # from iexfinance.utils.exceptions import IEXSymbolError
    # for symbol in symbols:
    #
    #     try:
    #         historic = get_historical_data(symbol, start, end)
    #         pd.DataFrame({'date': dates, 'close_price': close_prices}).to_csv(
    #             'data/historics/{}-historical.csv'.format(symbol), index=False, header=True, encoding='utf-8'
    #         )
    #
    #     except IEXSymbolError:
    #         pass

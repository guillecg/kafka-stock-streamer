import pandas as pd

from datetime import datetime

from streaming.iex_producer import IEXProducer


BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'iexfinance'

# IDEAS:
# Train with get_historical_data (one year), predict with Kafka streaming (one month)
# Visualization: simple dash with temporal series plot
# Seasonality/trend: advise wheter it's better to buy/sell/wait


if __name__ == '__main__':
    companies = pd.read_excel('data/companies.xlsx', header=0)
    symbols = companies.Symbol.values.tolist()

    kwargs = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'topic_name': TOPIC_NAME,
    }

    producer = IEXProducer(**kwargs)
    producer.publish_stocks(symbols[:10])

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
        TOPIC_NAME, group_id='group1', bootstrap_servers=BOOTSTRAP_SERVERS
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

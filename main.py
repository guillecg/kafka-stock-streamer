import json

from iexfinance.stocks import Stock
from iexfinance.altdata import get_social_sentiment

from kafka import KafkaProducer, KafkaConsumer

from flask import Flask, Response, render_template


BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'iexfinance'


consumer = KafkaConsumer(
    TOPIC_NAME, group_id='group1', bootstrap_servers=BOOTSTRAP_SERVERS
)

# Set the consumer in a Flask App
app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html', data=get_stream())

def get_stream():
    ''' Here is where we recieve streamed data from the Kafka Server and
    convert them to a Flask-readable format.
    '''
    for msg in consumer:
        yield msg.value


if __name__ == '__main__':
    aapl = Stock('AAPL')

    print('Current price:', aapl.get_price())
    print('Previous day prices:', aapl.get_previous_day_prices())

    dir(aapl)
    dir(aapl.get_time_series())

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )
    ack = producer.send(TOPIC_NAME, aapl.get_time_series())

    app.run(host='0.0.0.0', debug=True)

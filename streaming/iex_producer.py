import json

from iexfinance.stocks import Stock
from iexfinance.utils.exceptions import IEXSymbolError

from kafka import KafkaProducer

from utils.stdout import format_stdout


class IEXProducer(KafkaProducer):

    def __init__(self, **kwargs):
        super(IEXProducer, self).__init__()

        self.topic_name = ''
        self.bootstrap_servers = ''

        # Update class attributes with keyword arguments
        self.__dict__.update(kwargs)

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )


    def publish_stocks(self, symbols):

        for symbol in symbols:
            try:
                stock = Stock(symbol)
                ack = self.producer.send(self.topic_name, stock.get_all())
                print(format_stdout('SENT', w=5), ack)

            except IEXSymbolError:
                print(
                    format_stdout('ERROR', w=5),
                    '{} stock not found'.format(symbol)
                )

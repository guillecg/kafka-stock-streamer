from kafka import KafkaConsumer


class IEXConsumer(KafkaConsumer):

    def __init__(self, **kwargs):
        super(IEXConsumer, self).__init__(**kwargs)

    

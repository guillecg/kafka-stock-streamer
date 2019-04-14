import json

from iexfinance.stocks import Stock
from iexfinance.altdata import get_social_sentiment


if __name__ == '__main__':
    aapl = Stock('AAPL')

    print('Current price:', aapl.get_price())
    print('Previous day prices:', aapl.get_previous_day_prices())

import requests
from quixstreaming import *
import time
from dateutil import parser
import os
import traceback

# Create a client factory. Factory helps you create StreamingClient (see below) a little bit easier
security = SecurityOptions('{placeholder:broker.security.certificatepath}', "{placeholder:broker.security.username}", "{placeholder:broker.security.password}")
client = StreamingClient('{placeholder:broker.address}', security)

output_topic = client.open_output_topic('{placeholder:topic}')


from_currency = "BTC"
to_currency = "USD,GBP"

url = 'https://rest.coinapi.io/v1/exchangerate/{0}?filter_asset_id={1}'.format(from_currency, to_currency)
headers = {'X-CoinAPI-Key': '{placeholder:token}'}

stream = output_topic.create_stream("coin-api")

# Give the stream human readable name. This name will appear in data catalogue.
stream.properties.name = "Coin API"

# Save stream in specific folder in data catalogue to help organize your workspace.
stream.properties.location = "/Coin API"

while True:
    try:
        response = requests.get(url, headers=headers)

        data = response.json()

        rows = data['rates']

        for row in rows:
            # For every currency we send value.
            stream.parameters.buffer.add_timestamp(parser.parse(row['time'])) \
                .add_value("{0}-{1}".format(from_currency, row['asset_id_quote']), row['rate']) \
                .write()

            print("{0}-{1}: {2}".format(from_currency, row['asset_id_quote'], row['rate']))

        time.sleep(900)  # We sleep for 15 minute not to reach free account limit.
    except Exception:
        print(traceback.format_exc())


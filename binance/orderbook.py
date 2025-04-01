import json
import os
import websocket
import requests
from datetime import datetime, timezone
import time

config = {}
with open("config.json", "r") as file:
  config = json.load(file)

WEBSOCKET_STREAM_ENDPOINT = config['FUTURES']['WEBSOCKET_STREAM_ENDPOINT']
REST_API_ENDPOINT = config['FUTURES']['REST_API_ENDPOINT']

class BinanceWebSocketClient:

  def __init__(self, url = '', ticker='btcusdt', orderBook = None, checkLatency = False, checkLatencyRecords = 100):
    self.ws = None
    self.ticker = ticker
    self.url = f'{url}?streams={self.ticker}@depth@100ms'
    self.orderBook = orderBook
    self.totalLatency = 0
    self.checkLatency = checkLatency
    self.checkLatencyRecords = checkLatencyRecords
    self.checkLatencyRecordsCopy = checkLatencyRecords

  def onConnect(self):

    onMessageFunc = self.onMessage
    if (self.checkLatency):
      onMessageFunc = self.onCheckLatency

    self.ws = websocket.WebSocketApp(
      self.url,
      on_open=self.onOpen,
      on_message=onMessageFunc,
      on_close=self.onClose
    )
    self.ws.run_forever()

  def onOpen(self, ws):
    subscribe_message = {
      "method": "SUBSCRIBE",
      "params": [f"{self.ticker}@depth"],
      "id": 1
    }
    ws.send(json.dumps(subscribe_message))

  def onMessage(self, ws, message):
    data = json.loads(message)
    self.orderBook.create(data)

  def onCheckLatency(self, ws, message):
    data = json.loads(message)

    if (not data['data']['E']):
      return

    if (not self.checkLatencyRecords):
      self.onClose(ws)

    eventTime = datetime.fromtimestamp(data['data']['E'] / 1000, tz=timezone.utc)
    currentTime = datetime.now(timezone.utc)
    timeDiffInMs = int((currentTime - eventTime).total_seconds() * 1000)
    self.totalLatency += timeDiffInMs
    self.checkLatencyRecords -= 1
    averageLatency = self.totalLatency / (self.checkLatencyRecordsCopy - self.checkLatencyRecords)
    print(f'Total Latency = {self.totalLatency}, Records Remaining = {self.checkLatencyRecords}, Records Checked = {self.checkLatencyRecordsCopy - self.checkLatencyRecords}, Average Latency = {averageLatency}')

  def onClose(self, ws):
    print("Connection closed")
    ws.close()

  def getAverageLatency(self):
    return self.totalLatency / self.checkLatencyRecordsCopy


class OrderBook:

  def __init__(self, url = '', ticker = 'btcusdt', limit = 1000, bids = [], asks = []):
    self.bids = bids
    self.asks = asks
    self.ticker = ticker
    self.depthData = {}
    self.url = f'{url}/depth?symbol={self.ticker.upper()}&limit={limit}'
  
  def create(self, orderBookData: dict):
    if (not self.depthData):
      self.depthData = self.get_orderbook_depth()
    u = orderBookData['data']['u']
    U = orderBookData['data']['U']
    lastUpdateId = self.depthData['lastUpdateId']
    shouldDropEvent = (u < lastUpdateId)
    if (shouldDropEvent):
      return
    firstProcessedEvent = (U <= lastUpdateId) and (u >= lastUpdateId)
    if (firstProcessedEvent):
      self.bids = orderBookData['data']['b']
      self.asks = orderBookData['data']['a']
    else:
      self.bids = orderBookData['data']['b']
      self.asks = orderBookData['data']['a']
    self.display_order_book()
  
  def display_order_book(self):
    os.system('cls' if os.name == 'nt' else 'clear')
    order_book_str = f"{'Ask Price':>10} | {'Ask Size':>10} || {'Bid Price':>10} | {'Bid Size':>10}\n"
    order_book_str += "-" * 50 + "\n"
    
    asks = sorted(
      (ask for ask in self.asks if float(ask[1]) > 0),
      key=lambda x: float(x[0])
    ) if self.asks else []

    bids = sorted(
      (bid for bid in self.bids if float(bid[1]) > 0),
      key=lambda x: float(x[0]),
      reverse=True
    ) if self.bids else []

    max_rows = max(len(asks), len(bids))
    for i in range(max_rows):
      ask_price, ask_size = asks[i] if i < len(asks) else ("", "")
      bid_size, bid_price = bids[i] if i < len(bids) else ("", "")
      order_book_str += f"{str(ask_price):>10} | {str(ask_size):>10} || {str(bid_size):>10} | {str(bid_price):>10}\n"
    print(order_book_str)

  def get_orderbook_depth(self):

    depthData = {}
    depthResponse = requests.get(self.url)
    if depthResponse.status_code == 200:
      depthData = depthResponse.json()
    return depthData


class BinanceWebSocketService:

  def __init__(self, client: BinanceWebSocketClient):
    self.client = client

  def start_stream(self):
    self.client.onConnect()


if __name__ == "__main__":
  orderBook = OrderBook(REST_API_ENDPOINT, 'btcusdt')
  client = BinanceWebSocketClient(WEBSOCKET_STREAM_ENDPOINT, 'btcusdt', orderBook, False)
  service = BinanceWebSocketService(client)
  service.start_stream()
  print(client.getAverageLatency())

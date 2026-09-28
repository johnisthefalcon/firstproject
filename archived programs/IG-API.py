from lightstreamer.client import *

sub = Subscription("MERGE",["item1","item2","item3"],["stock_name","last_price"])
sub.setDataAdapter("QUOTE_ADAPTER")
sub.setRequestedSnapshot("yes")

class SubListener(SubscriptionListener):
  def onItemUpdate(self, update):
    print("UPDATE " + update.getValue("stock_name") + " " + update.getValue("last_price"))

sub.addListener(SubListener())

client = LightstreamerClient("http://push.lightstreamer.com","DEMO")
client.subscribe(sub)

import sys, logging

logging.basicConfig(level=logging.DEBUG, format="%(message)s", stream=sys.stdout)

loggerProvider = ConsoleLoggerProvider(ConsoleLogLevel.DEBUG)
LightstreamerClient.setLoggerProvider(loggerProvider)

client.connect()

# =============================================
# IG Markets API - Python Connection Example
# =============================================
# Install dependencies first:
#   pip install trading-ig pandas

import os
from trading_ig import IGService
import requests
from pathlib import Path
import csv

# --- CONFIGURATION ---
# Best practice: use environment variables rather than hardcoding credentials
# Set these in your shell or a .env file:
#   export IG_USERNAME="your_username"
#   export IG_PASSWORD="your_passfetch_historical_prices_by_epic_and_num_pointsword"
#   export IG_API_KEY="your_api_key"
#   export IG_ACC_TYPE="DEMO"   # or "LIVE"
#   export IG_ACC_NUMBER="your_account_number"

IG_USERNAME   = os.environ.get("IG_USERNAME", "KEMEJO08709786")
IG_PASSWORD   = os.environ.get("IG_PASSWORD", "Ex204lfbn27gl!")
IG_API_KEY    = os.environ.get("IG_API_KEY",  "8b830abfbd08fc4eee85781f3fbb2e70d1e8436a")
IG_ACC_TYPE   = os.environ.get("IG_ACC_TYPE", "LIVE")   # "DEMO" or "LIVE"
IG_ACC_NUMBER = os.environ.get("IG_ACC_NUMBER", "KTWBE")


def connect():
    """Create and return an authenticated IGService session."""
    print(f"Connecting to IG ({IG_ACC_TYPE}) ...")
    ig = IGService(
        username=IG_USERNAME,
        password=IG_PASSWORD,
        api_key=IG_API_KEY,
        acc_type=IG_ACC_TYPE,  # "DEMO" or "LIVE"
    )
    ig.create_session()
    print("✓ Session created successfully.")
    return ig


def get_account_info(ig: IGService):
    try:     
        """Switch to your account and print basic details."""
        account_info = ig.fetch_accounts()
        print(f"\nAccount info:\n{account_info}")
        data = ig.read_session()
        print("read_session: %s" % data)
        return account_info
        
    except Exception as ex:
        print("Problem: " + repr(ex))

def get_open_positions(ig: IGService):
    """Fetch and display all currently open positions."""
    positions = ig.fetch_open_positions()
    if positions.empty:
        print("\nNo open positions.")
        return 0
    else:
        print(f"\nOpen positions:\n{positions}")
    
    
        filtered = positions.iloc[:, [3,5,6, 8,15]]
    
        return filtered

def profit_loss(ig: IGService, epic: str = "IX.D.FTSE.DAILY.IP"):
    positions = ig.fetch_open_positions()
    
    message = []
    for _, row in positions.iterrows():
        level      = row['level']        
        net_change = row['netChange']
        size       = row['size']
        direction  = row['direction']
        instrumentName = row['instrumentName']

        bid = get_live_price(ig, epic)
        
        if direction == 'BUY':
            profit = (bid -level) * size
        else:  # SELL
            profit = -(bid-level) * size


        print(f"{row['instrumentName']} | {direction} | Profit: {profit:.2f}")
        message.append(f"{row['instrumentName']} | {direction} | Profit: {profit:.2f} {row['currency']}")

        telegram_bitbot(message)

def get_live_price(ig: IGService, epic: str = "IX.D.FTSE.DAILY.IP"):
    price = ig.fetch_market_by_epic(epic)
    bid = price["snapshot"]["bid"]
    
    return (bid)

def fetch_deal_id(ig: IGService, instrument):
    positions = ig.fetch_open_positions()
    for _, row in positions.iterrows():
        net_change = row['netChange']
        size       = row['size']
        direction  = row['direction']
        instrumentName = row['instrumentName']
        deal_id = row['dealId']

   

        print(f"{row['dealId']}")
        return(str({row['dealId']}))
        

      

def get_price_history(ig: IGService, storage_file, epic: str = "IX.D.FTSE.DAILY.IP",
                      resolution: str = "D", num_points: int = 10):
    """
    Fetch recent historical prices for a given EPIC.
    Common EPICs:
      CS.D.GBPUSD.TODAY.IP  -> GBP/USD
      IX.D.FTSE.DAILY.IP    -> FTSE 100
      IX.D.NASDAQ.IFA.IP    -> NASDAQ
   
    result = ig.fetch_historical_prices_by_epic(epic='IX.D.NASDAQ.IFA.IP')
    return result
    """
    # epic = 'CS.D.EURUSD.MINI.IP'
    epic = epic 
    # epic = "CS.D.GBPUSD.CFD.IP"  # sample CFD epic

    resolution = "5Min"
    # see from pandas.tseries.frequencies import to_offset
    # resolution = 'H'
    # resolution = '1Min'

    num_points = 6
    data = ig.fetch_historical_prices_by_epic_and_num_points(
        epic, resolution, num_points
    )    
    save_path = Path("~").expanduser() / storage_file
    Append_prices_to_file(save_path, data)
    print(data["prices"])

def Append_prices_to_file(path_to_file, data):
 
    save_path = path_to_file
    print(f"saving historic prices to {save_path}")

    # Check if file already exists to decide whether to write header
    file_exists = save_path.exists()

    data["prices"].to_csv(
        save_path,
        mode="a",                 # append instead of overwrite
        header=not file_exists,   # only write header if file is new
        date_format="%Y-%m-%dT%H:%M:%S%z"
    )
    
    # Read back, deduplicate, and overwrite
    with open(save_path, "r") as f:
        reader = csv.reader(f)
        header = next(reader)[:5]          # index col + first 5 columns
        rows = {row[0]: row[1:5] for row in reader}  # dict keyed by index, dedupes automatically
    
    sorted_rows = sorted(rows.items(), key=lambda x: x[1][0])  # sort by column 1 A-Z

    with open(save_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows([[index] + cols for index, cols in rows.items()])

def search_markets(ig: IGService, search_term: str = "FTSE"):
    """Search for markets by name."""
    results = ig.search_markets(search_term)
    print(f"\nMarket search results for '{search_term}':\n{results}")
    
    return results

def telegram_bitbot(message):
    
    TOKEN = "8042570662:AAFtTOk-H4eg7ssErDBWFSbXZMjCvrk-RfM"
    chat_id = "8690491455"
    message = message
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
    print(requests.get(url).json()) 

def open_buy_trade(ig: IGService, epic:str = 'IX.D.FTSE.DAILY.IP'):
    ig.create_open_position(currency_code='GBP', 
                            direction='BUY', 
                            epic=epic, 
                            expiry='DFB', 
                            force_open='true', 
                            guaranteed_stop='false',
                            level='', 
                            limit_distance=None, 
                            limit_level=None,
                            order_type='MARKET', 
                            quote_id=None, 
                            size=0.1, 
                            stop_distance=35, 
                            stop_level=None, 
                            trailing_stop='false', 
                            trailing_stop_increment=None)
    get_open_positions(ig)
    telegram_bitbot(get_open_positions(ig))



def open_sell_trade(ig: IGService, epic:str = 'IX.D.FTSE.DAILY.IP'):
    ig.create_open_position(currency_code='GBP', direction='SELL', epic=epic, expiry='DFB', force_open='true', guaranteed_stop='false',level='', limit_distance=None, limit_level=None,
order_type='MARKET', quote_id=None, size=0.1, stop_distance=35, stop_level=None, trailing_stop='false', trailing_stop_increment=None)
    
def close_buy_trade(ig: IGService, epic:str = 'IX.D.FTSE.DAILY.IP'): 
    dealid = str(fetch_deal_id(ig, 'FTSE100')).strip("{}'")
    telegram_bitbot("Closing positions"+" " + dealid)  
    telegram_bitbot(get_open_positions(ig))  
    profit_loss(ig)
    
    
    ig.close_open_position(
                            direction='SELL', 
                            epic=None, 
                            expiry='DFB', 
                            level=None,
                            deal_id=dealid,
                            #force_open=True,
                            order_type='MARKET', 
                            quote_id=None, 
                            size=0.1, 
                            )
     

def activity(ig: IGService, delta):
    from datetime import datetime, timedelta

    to_date = datetime.now()
    from_date = to_date - timedelta(hours=delta)
    act = ig.fetch_transaction_history(from_date=from_date, to_date=to_date)

    # Keep only columns 2, 3, 5, 7, 9 (0-indexed: 1, 2, 4, 6, 8)
    filtered = act.iloc[:, [0,3, 5, 8,9, 10]]

    telegram_bitbot(filtered)


# --- MAIN ---
if __name__ == "__main__":
    ig = connect()
    #get_account_info(ig)
    #get_open_positions(ig)  
    get_price_history(ig)
    #get_price_history(ig,"Nprice.csv", "IX.D.NIKKEI.DAILY.IP") #Fprice / Dprice.csv, "IX.D.FTSE.DAILY.IP"/ "IX.D.DAX.IMF.IP" 
    #search_markets(ig, "DAX")
    #telegram_bitbot("new")
    #open_buy_trade(ig)
    #close_buy_trade(ig)
    #fetch_deal_id(ig, "FTSE 100")    
    #fetchpositions(ig)
    #profit_loss(ig)
    #get_open_positions_dealid(ig)
    #activity(ig, 24)
    

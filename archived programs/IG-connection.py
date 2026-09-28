# =============================================
# IG Markets API - Python Connection Example
# =============================================
# Install dependencies first:
#   pip install trading-ig pandas

import os
from trading_ig import IGService

# --- CONFIGURATION ---
# Best practice: use environment variables rather than hardcoding credentials
# Set these in your shell or a .env file:
#   export IG_USERNAME="your_username"
#   export IG_PASSWORD="your_password"
#   export IG_API_KEY="your_api_key"
#   export IG_ACC_TYPE="DEMO"   # or "LIVE"
#   export IG_ACC_NUMBER="your_account_number"

IG_USERNAME   = os.environ.get("IG_USERNAME", "KEMEJO08709786")
IG_PASSWORD   = os.environ.get("IG_PASSWORD", "Ex204lfbn27gl!")
IG_API_KEY    = os.environ.get("IG_API_KEY",  "cf8695a72e953cf522e53cbdc9298217413cdf1d")
IG_ACC_TYPE   = os.environ.get("IG_ACC_TYPE", "DEMO")   # "DEMO" or "LIVE"
IG_ACC_NUMBER = os.environ.get("IG_ACC_NUMBER", "Z6AYI6")


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
    """Switch to your account and print basic details."""
    account_info = ig.switch_account(IG_ACC_NUMBER, default_account=False)
    print(f"\nAccount info:\n{account_info}")
    return account_info


def get_open_positions(ig: IGService):
    """Fetch and display all currently open positions."""
    positions = ig.fetch_open_positions()
    if positions.empty:
        print("\nNo open positions.")
    else:
        print(f"\nOpen positions:\n{positions}")
    return positions


def get_price_history(ig: IGService, epic: str = "CS.D.GBPUSD.TODAY.IP",
                      resolution: str = "D", num_points: int = 10):
    """
    Fetch recent historical prices for a given EPIC.
    Common EPICs:
      CS.D.GBPUSD.TODAY.IP  -> GBP/USD
      IX.D.FTSE.DAILY.IP    -> FTSE 100
      IX.D.NASDAQ.IFA.IP    -> NASDAQ
    """
    prices = ig.fetch_historical_prices_by_epic_and_num_points(
        epic=epic,
        resolution=resolution,  # e.g. "D" (daily), "H" (hourly), "M" (minute)
        num_points=num_points,
    )
    print(f"\nPrice history for {epic}:\n{prices['prices']}")
    return prices


def search_markets(ig: IGService, search_term: str = "FTSE"):
    """Search for markets by name."""
    results = ig.search_markets(search_term)
    print(f"\nMarket search results for '{search_term}':\n{results}")
    return results


# --- MAIN ---
if __name__ == "__main__":
    ig = connect()
    get_account_info(ig)
    get_open_positions(ig)
    get_price_history(ig)
    search_markets(ig)

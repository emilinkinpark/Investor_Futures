import requests

def get_symbol_info(symbol):
    response = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo")
    data = response.json()
    for pair in data['symbols']:
        if pair['symbol'] == symbol:
            return {
                'tick_size': float(pair['filters'][0]['tickSize']),  # Price precision
                'step_size': float(pair['filters'][1]['stepSize']),  # Quantity precision
            }
    return None

symbol_info = get_symbol_info("HVPERUSDT")
print(symbol_info)
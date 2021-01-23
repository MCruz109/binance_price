import websocket
import json
import sqlite3
import sys


def on_open(ws):
    print('Connection Established')
    
    try: 
        ws.SQLITE3_cursor.execute("""CREATE TABLE prices (
                                   event_time text,
                                   symbol text,
                                   kline_start_time text,
                                   kline_close_time text,
                                   interval text,
                                   first_trade_id text,
                                   last_trade_id text,
                                   open_price text,
                                   close_price text,
                                   high_price text,
                                   low_price text,
                                   base_asset_volume text,
                                   number_of_trades text,
                                   is_kline_closed text,
                                   quote_asset_volume text,
                                   taker_buy_base_asset_volume text,
                                   taker_quote_asset_volume text
                                   )""")
        print("Created new price TABLE")
        
    except:
        print("price TABLE already exists")

        
def on_close(ws):
    print('Connection Closed')
    
    
def on_error(ws, error):    
    print(error)

    
def on_message(ws, message):
    json_message = json.loads(message)
    print(json_message['E'], json_message['s'], 
          json_message['k']['c'], json_message['k']['x'])
    
    to_add = (str(json_message['E']),
              str(json_message['s']),
              str(json_message['k']['t']),
              str(json_message['k']['T']),
              str(json_message['k']['i']), 
              str(json_message['k']['f']), 
              str(json_message['k']['L']),
              str(json_message['k']['o']),
              str(json_message['k']['c']),
              str(json_message['k']['h']),
              str(json_message['k']['l']),
              str(json_message['k']['v']), 
              str(json_message['k']['n']), 
              str(json_message['k']['x']),
              str(json_message['k']['q']),
              str(json_message['k']['V']),
              str(json_message['k']['Q']))
    
    sql_query = f"INSERT INTO prices VALUES {to_add}"
    ws.SQLITE3_cursor.execute(sql_query)
    ws.SQLITE3_conn.commit()

    
if __name__ == "__main__":
    trade_symbol = sys.argv[1]
    timeframe = sys.argv[2]
    socket = f"wss://stream.binance.com:9443/ws/{trade_symbol.lower()}@kline_{timeframe}"
    print(socket)

    ws = websocket.WebSocketApp(socket, 
                                on_open=on_open, 
                                on_close=on_close, 
                                on_message=on_message,
                                on_error = on_error)

    ws.SQLITE3_conn = sqlite3.connect(f"{trade_symbol}_{timeframe}.db")
    ws.SQLITE3_cursor = ws.SQLITE3_conn.cursor()
    ws.run_forever()
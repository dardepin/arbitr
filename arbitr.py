#!/usr/bin/python3.11

import os;
import sys;
import time;
import json;
import logging;
import urllib3;
import requests;
import threading;

from threading import Lock;
from threading import Event;
# from threading import Thread;
from datetime import datetime;
# from logging import StreamHandler;

import influxdb_client;
from influxdb_client import Point, BucketRetentionRules; #, InfluxDBClient, WritePrecision, BucketsApi;
# from influxdb_client.client.write_api import SYNCHRONOUS;

import binance;
from binance import Client; #, ThreadedWebsocketManager, ThreadedDepthCacheManager;
from okx import MarketData as MarketData;
from pybit.unified_trading import HTTP;
import gate_api;
from gate_api.exceptions import ApiException, GateApiException;
from kucoin.client import Market;

from config import *;

#aux classes
class Ticker:
    def __init__(self, name:str, price:float, prefix = ''):
        self.name = name.replace(prefix, '');
        self.price = price; # round it to 10 immediately?

class Log:
    def __init__(self):
        self.logtofile = app_log_to_file; # boolean
        self.logger = logging.getLogger('arbitr_logger');
        return;

    def log_init(self):
        try:
            self.logger.setLevel(logging.INFO);
            std_log = logging.StreamHandler(sys.stdout);
            self.logger.addHandler(std_log);
            format = logging.Formatter('[%(asctime)s][%(levelname)s]: %(message)s');
            std_log.setFormatter(format);
            if self.logtofile:
                now = datetime.now();
                file_log = logging.FileHandler(now.strftime('arbitr_%Y-%m-%d_%H-%M-%S') + '.log');
                file_log.setFormatter(format);
                self.logger.addHandler(file_log);
        except Exception as ex:
            print(str(ex));
            return False;
        return True;

class Db:
    def __init__(self):
        self.token = db_token;
        self.url = db_url;
        self.bucket = db_bucket;
        self.org = db_org;
        self.logger = logging.getLogger('arbitr_logger');

        self.client = influxdb_client.InfluxDBClient(url = self.url, token = self.token, org = self.org);
        self.write = self.client.write_api(); #write_options = SYNCHRONOUS);
        return;

    def db_init(self):
        return self.create_bucket();

    def create_bucket(self):
        try:
            buckets_api = self.client.buckets_api();

            if buckets_api.find_bucket_by_name(self.bucket) == None:
                retention_rules = BucketRetentionRules(type = 'expire', every_seconds = 2592000);
                crypto_bucket = buckets_api.create_bucket(bucket_name = self.bucket, retention_rules = retention_rules, org = self.org);
                self.logger.info('Bucket `CRYPTO` was newly created');
        except urllib3.exceptions.NewConnectionError as ex:
            self.logger.exception('Error connecting to database: %s', self.url);
            return False;
        return True;

    def store(self, pair:str, market:str, price:float):
        point = (Point(pair).tag('market', market).field('price', price));
        self.write.write(bucket = self.bucket, org = self.org, record = point);
        # influxdb_client.exceptions.InfluxDBClientError
        return;

    def store(self, market:str, ticker:Ticker):
        point = Point(ticker.name).tag('market', market).field('price', ticker.price);
        self.write.write(bucket = self.bucket, org = self.org, record = point);
        return;
        
# market classes
class OKX:
    def __init__(self):
        self.key = okx_key;
        self.secret = okx_secret;
        self.password = okx_password;

        self.server_time = False;
        self.flag = '0'; # 0 - prod, 1 - test
        self.type = 'SPOT';
        self.logger = logging.getLogger('arbitr_logger');

        self.tickers = [];

        self.client = MarketData.MarketAPI(self.key, self.secret, self.password, self.server_time, self.flag,);
        return;

    def get_tickers(self): # 10 requests per second
        self.tickers = [];
        try:
            newtickers = self.client.get_tickers(instType = self.type);
            if newtickers['code'] != '0' or newtickers['msg'] != '':
                self.log.logger.warning('OKX::get_tickers::get_tickers() error; code: %s, msg: %s', newtickers['code'], newtickers['msg']); return False;

            for ticker in newtickers['data']:
                    symbol = ticker['instId']; price = ticker['last'];
                    if symbol == None or price == None: self.logger.warning('OKX::get_tickers: price for %s is %s', symbol, price); continue;
                    self.tickers.append(Ticker(symbol, round(float(price), 10), '-'));

        except Exception as ex:
            self.log.logger.exception('OKX::get_tickers() exception; msg: %s', ex); return False;
        return True;

class Bybit:
    def __init__(self):
        self.key = bybit_key;
        self.secret = bybit_secret;
        self.testnet = False;
        self.tickers = [];
        self.type = 'spot';
        self.logger = logging.getLogger('arbitr_logger');

        self.client = HTTP(api_key = self.key, api_secret = self.secret, testnet = self.testnet);
        return;

    def get_tickers(self):
        self.tickers = [];
        try:
            newtickers = self.client.get_tickers(category = self.type);
            if newtickers['retCode'] != 0 or newtickers['retMsg'] != 'OK':
                self.lo.logger.error('BYBIT::get_tickers() error; code: %s, msg: %s', newtickers['retCode'], newtickers['retMsg']); return False;

            for ticker in newtickers['result']['list']:
                    symbol = ticker['symbol']; price = ticker['lastPrice'];
                    if symbol == None or price == None: self.logger.warning('Bybit::get_tickers: price for %s is %s', symbol, price); continue;

                    self.tickers.append(Ticker(symbol, round(float(price), 10)));
        except Exception as ex:
            self.log.logger.exception('BYBIT::get_tickers() exception; msg: %s', ex); return False;
            
        return True;

class Kraken:
    def __init__(self):
        # key & secret no really required
        self.key = kraken_key;
        self.secret = kraken_secret;
        self.logger = logging.getLogger('arbitr_logger');

        self.tickers = [];
        return;

    def get_tickers(self):
        self.tickers = [];
        try:
            newtickers = requests.get('https://api.kraken.com/0/public/Ticker').json();
        
            error = newtickers['error'];
            if error != []:
                self.log.logger.error('KRAKEN::get_tickers() error; msg: %s', error); return False;
            newtickers = newtickers['result'];

            for ticker in newtickers:
                symbol = ticker; price = newtickers[ticker]['c'][0];
                if symbol == None or price == None: self.logger.warning('Kraken::get_tickers: price for %s is %s', symbol, price); continue;
                self.tickers.append(Ticker(symbol, round(float(price), 10)));

        except requests.exceptions.RequestException as ex:
            self.log.logger.warning('KRAKEN::get_tickers::requests.get() exception; msg: %s', ex); return False;
        except Exception as ex:
            self.log.logger.exception('KRAKEN::get_tickers() exception; msg: %s', ex); return False;
        return True;

class Binance:
    def __init__(self):
        self.key = binance_key;
        self.secret = binance_secret;
        self.logger = logging.getLogger('arbitr_logger');

        self.client = Client();
        self.tickers = [];
        return;

    def get_tickers(self): # weigth: 4
        self.tickers = [];
        try:
            newtickers = self.client.get_all_tickers();
            if 'code' in newtickers and 'msg' in newtickers:
                self.log.logger.error('Binance::get_tickers::get_all_tickers() error; code: %s, msg: %s', newtickers['code'], newtickers['code']); return False;

            for ticker in newtickers:
                symbol = ticker['symbol']; price = ticker['price'];
                if symbol == None or price == None: self.logger.warning('Binance::get_tickers: price for %s is %s', symbol, price); continue;
                self.tickers.append(Ticker(symbol, round(float(price), 10)));

        except binance.exceptions.BinanceRequestException as ex:
            self.logger.exception('BINANCE::get_tickers::get_all_tickers() BinanceRequestException; code: %s, msg: %s', ex.status_code, ex.message); return False;
        except binance.exceptions.BinanceAPIException as ex:
            self.logger.exception('BINANCE::get_tickers::get_all_tickers() BinanceRequestException; code: %s, msg: %s', ex.status_code, ex.message); return False;
        except Exception as ex:
            self.log.logger.exception('BINANCE::get_tickers() exception; msg: %s', ex); return False;
        return True;

class Coinbase:
    def __init__(self):
        self.logger = logging.getLogger('arbitr_logger');
        self.currencies = coinbase_currencies;
        self.tickers = [];
        return;

    def get_tickers(self):
        self.tickers = [];
        try:
            for currency in self.currencies:
                newtickers = requests.get('https://api.coinbase.com/v2/exchange-rates?currency=' + currency).json()['data']['rates'];

                for ticker in newtickers:
                    symbol = ticker;
                    price = newtickers[ticker];
                    if symbol == None or price == None:
                        self.logger.warning('Coinbase::get_tickers: price for %s is %s', symbol + currency, price); continue;
                    if ticker == '00' or ticker == currency: continue;
                    
                    self.tickers.append(Ticker(ticker + currency, round(1 / float(price), 10)));
        
        except requests.exceptions.RequestException as ex:
            self.log.logger.warning('Coinbase::get_tickers::requests.get() exception; msg: %s', ex); return False;
        except Exception as ex:
            self.log.logger.exception('Coinbase::get_tickers() exception; msg: %s', ex); return False;
        return len(self.tickers);

class Gate_io:
    def __init__(self):
        self.logger = logging.getLogger('arbitr_logger');

        self.config = gate_api.Configuration(host = 'https://api.gateio.ws/api/v4');
        self.client = gate_api.ApiClient(self.config);
        self.instance = gate_api.SpotApi(self.client);
        self.tickers = [];
        return;

    def get_tickers(self):
        self.tickers = [];
        try:
            newtickers = self.instance.list_tickers();

            for ticker in newtickers:
                symbol = ticker.currency_pair; price = ticker.last;
                if symbol == None or price == None: self.logger.warning('Gate.io::get_tickers: price for %s is %s', symbol, price); continue;
                self.tickers.append(Ticker(symbol, round(float(price), 10), '_'));

        except GateApiException as ex:
            self.logger.exception('GATE.IO::get_tickers::list_tickers() Gate API exception; code: %s, msg: %s', ex.label, ex.message); return False;
        except ApiException as ex:
            self.logger.exception('GATE.IO::get_tickers::list_tickers() Spot API exception; msg: %s', ex); return False;
        except Exception as ex:
            self.logger.exception('GATE.IO::get_tickers() exception; msg: %s', ex); 
        return True;

class Kucoin:
    def __init__(self):
        self.tickers = [];
        self.client = Market(url = 'https://api.kucoin.com');
        self.logger = logging.getLogger('arbitr_logger');
        return;

    def get_tickers(self):
        self.tickers = [];
        try:
            newtickers = self.client.get_all_tickers()['ticker'];
            for ticker in newtickers:
                symbol = ticker['symbolName']; price = ticker['last'];
                if symbol == None or price == None: self.logger.warning('Kucoin::get_tickers: price for %s is %s', symbol, price); continue;
                self.tickers.append(Ticker(symbol, round(float(price), 10), '-'));
        except Exception as ex:
            self.logger.exception('KUCOIN::get_tickers::get_all_tickers() exception; msg: %s', ex); return False;
        return True;

class Bitfinex:
    def __init__(self):
        self.tickers = [];

        self.key = bitfinex_key;
        self.secret = bitfinex_secret;
        self.logger = logging.getLogger('arbitr_logger');
        return;

    def get_tickers(self):
        self.tickers = [];
        headers = {'accept': 'application/json'};
        try:
            newtickers = requests.get('https://api-pub.bitfinex.com/v2/tickers?symbols=ALL', headers = headers).json();

            for ticker in newtickers:
                symbol = ticker[0]; price = ticker[7];
                if symbol == None or price == None: self.logger.warning('Bitfinex::get_tickers: price for %s is %s', symbol, price); continue;
                if symbol[0] == 'f' or 'TEST' in symbol: continue;
                self.tickers.append(Ticker(symbol.replace(':', ''), round(float(price), 10), 't'));

        except requests.exceptions.RequestException as ex:
            self.log.logger.exception('Bitfinex::get_tickers::requests.get() exception; msg: %s', ex); return False;
        except Exception as ex:
            self.log.logger.exception('Bitfinex::get_tickers() exception; msg: %s', ex); return False;
        return True;

class Mexc:
    def __init__(self):
        self.tickers = [];

        self.key = mexc_key;
        self.secret = mexc_secret;
        self.logger = logging.getLogger('arbitr_logger');

    def get_tickers(self):
        self.tickers = [];
        try:
            newtickers = requests.get('https://api.mexc.com//api/v3/ticker/price').json();

            for ticker in newtickers:
                symbol = ticker['symbol']; price = ticker['price'];
                if symbol == None or price == None: self.logger.warning('MEXC::get_tickers: price for %s is %s', symbol, price); continue;
                self.tickers.append(Ticker(symbol, round(float(price), 10)));
        
        except requests.exceptions.RequestException as ex:
            self.log.logger.exception('MEXC::get_tickers::requests.get() exception; msg: %s', ex); return False;
        except Exception as ex:
            self.log.logger.exception('MEXC::get_tickers() exception; msg: %s', ex); return False;
        return True;

class Bitget:
    def __init__(self):
        self.tickers = [];

        self.key = bitget_key;
        self.secret = bitget_secret;
        self.logger = logging.getLogger('arbitr_logger');
        return;

    def get_tickers(self): # 20 times / 1s
        self.tickers = [];
        try:
            newtickers = requests.get('https://api.bitget.com/api/v2/spot/market/tickers').json();
            if newtickers['code'] != '00000' or newtickers['msg'] != 'success':
                self.logger.error('Bitget::get_tickers::requests.get() returned error;code: %s, msg: %s', newtickers['code'] != '00000', newtickers['msg']); return False;

            for ticker in newtickers['data']:
                symbol= ticker['symbol']; price = ticker['lastPr'];
                if symbol == None or price == None: self.logger.warning('Bitget::get_tickers: price for %s is %s', symbol, price); continue;
                self.tickers.append(Ticker(symbol, round(float(price), 10)));

        except requests.exceptions.RequestException as ex:
            self.log.logger.exception('Bitget::get_tickers::requests.get() exception; msg: %s', ex); return False;
        except Exception as ex:
            self.log.logger.exception('Bitget::get_tickers() exception; msg: %s', ex); return False;
        return True;

class Crypto_com:
    def __init__(self):
        self.tickers = [];

        self.logger = logging.getLogger('arbitr_logger');
        return;

    def get_tickers(self):
        self.tickers = [];
        try:
            newtickers = requests.get('https://api.crypto.com/exchange/v1/public/get-tickers').json();
            if newtickers['code'] != 0:
                self.logger.error('Crypto.com::get_tickers::requests.get() returned error;code: %s, msg: %s', newtickers['code'] != '00000', newtickers['msg']); return False;
            else:
                newtickers = newtickers['result']['data'];
                for ticker in newtickers:
                    symbol = ticker['i']; price = ticker['a'];
                    if symbol == None or price == None: self.logger.warning('Crypto.com::get_tickers: price for %s is %s', symbol, price); continue;
                    if '-' in ticker['i']: continue;
                    self.tickers.append(Ticker(symbol, round(float(price), 10), '_'));
        except requests.exceptions.RequestException as ex:
            self.log.logger.exception('Crypto.com::get_tickers::requests.get() exception; msg: %s', ex); return False;
        except Exception as ex:
            self.log.logger.exception('Crypto.com::get_tickers() exception; msg: %s', ex); return False;
        return True;

# main class
class Arbitr:
    def __init__(self):
        self.log = Log();
        self.db = Db();

        self.binance = Binance();
        self.okx = OKX();
        self.bybit = Bybit();
        self.kraken = Kraken();
        self.coinbase = Coinbase();
        self.gate_io = Gate_io();
        self.kucoin = Kucoin();
        self.bitfinex = Bitfinex();
        self.mexc = Mexc();
        self.bitget = Bitget();
        self.crypto_com = Crypto_com();
        return;

    def real_init(self):
        if not self.log.log_init() or not self.db.db_init(): return False;
        return True;

    def worker_binance(self, event:Event, lock:Lock):
        while True:
            starttime = datetime.now();
            if not self.binance.get_tickers(): break;
            with lock:
                for ticker in self.binance.tickers:
                    self.db.store('BINANCE', ticker);
                self.log.logger.info('Tickers from BINANCE was updated');

            # stop event
            if event.is_set(): break;
            sleeptime = datetime.now() - starttime;
            if sleeptime.seconds >= 60: continue;
            time.sleep(60.0 - sleeptime.total_seconds());
        return;

    def worker_okx(self, event:Event, lock:Lock):
        while True:
            starttime = datetime.now();
            if not self.okx.get_tickers(): break;
            with lock:
                for ticker in self.okx.tickers:
                    self.db.store('OKX', ticker);
                self.log.logger.info('Tickers from OKX was updated');

            # stop event
            if event.is_set(): break;
            sleeptime = datetime.now() - starttime;
            if sleeptime.seconds >= 60: continue;
            time.sleep(60.0 - sleeptime.total_seconds());
        return;

    def worker_bybit(self, event:Event, lock:Lock):
        while True:
            starttime = datetime.now();
            if not self.bybit.get_tickers(): break;
            with lock:
                for ticker in self.bybit.tickers:
                    self.db.store('BYBIT', ticker)
                self.log.logger.info('Tickers from BYBIT was updated');

            if event.is_set(): break;
            sleeptime = datetime.now() - starttime;
            if sleeptime.seconds >= 60: continue;
            time.sleep(60.0 - sleeptime.total_seconds());
        return;

    def worker_kraken(self, event:Event, lock:Lock):
        while True:
            starttime = datetime.now();
            if not self.kraken.get_tickers(): break;
            with lock:
                for ticker in self.kraken.tickers:
                    self.db.store('KRAKEN', ticker);
                self.log.logger.info('Tickers from KRAKEN was updated');

            if event.is_set(): break;
            sleeptime = datetime.now() - starttime;
            if sleeptime.seconds >= 60: continue;
            time.sleep(60.0 - sleeptime.total_seconds());
        return;

    def worker_coinbase(self, event:Event, lock:Lock):
        while True:
            starttime = datetime.now();
            if not self.coinbase.get_tickers(): break;
            with lock:
                for ticker in self.coinbase.tickers:
                    # self.db.store(ticker, 'COINBASE', round(1 / float(self.coinbase.tickers[ticker]), 10));
                    self.db.store('COINBASE', ticker);
                self.log.logger.info('Tickers from COINBASE was updated');
        
            if event.is_set(): break;
            sleeptime = datetime.now() - starttime;
            if sleeptime.seconds >= 60: continue;
            time.sleep(60.0 - sleeptime.total_seconds());
        return;

    def worker_gate_io(self, event:Event, lock:Lock):
        while True:
            starttime = datetime.now();
            if not self.gate_io.get_tickers(): break;
            with lock:
                for ticker in self.gate_io.tickers:
                    self.db.store('GATE.IO', ticker);
                self.log.logger.info('Tickers from GATE.IO was updated');

            if event.is_set(): break;
            sleeptime = datetime.now() - starttime;
            if sleeptime.seconds >= 60: continue;
            time.sleep(60.0 - sleeptime.total_seconds());
        return;

    def worker_kucoin(self, event:Event, lock:Lock):
        while True:
            starttime = datetime.now();
            if not self.kucoin.get_tickers(): break;
            with lock:
                for ticker in self.kucoin.tickers:
                    self.db.store('KUCOIN', ticker);
                self.log.logger.info('Tickers from KUCOIN was updated');

            if event.is_set(): break;
            sleeptime = datetime.now() - starttime;
            if sleeptime.seconds >= 60: continue;
            time.sleep(60.0 - sleeptime.total_seconds());
        return;

    def worker_bitfinex(self, event:Event, lock:Lock):
        while True:
            starttime = datetime.now();
            if not self.bitfinex.get_tickers(): break;
            with lock:
                for ticker in self.bitfinex.tickers:
                    self.db.store('BITFINEX', ticker);
                self.log.logger.info('Tickers from BITFINEX was updated');

            if event.is_set(): break;
            sleeptime = datetime.now() - starttime;
            if sleeptime.seconds >= 60: continue;
            time.sleep(60.0 - sleeptime.total_seconds());
        return;

    def worker_mexc(self, event:Event, lock:Lock):
        while True:
            starttime = datetime.now();
            if not self.mexc.get_tickers(): break;
            with lock:
                for ticker in self.mexc.tickers:
                    self.db.store('MEXC', ticker);
                self.log.logger.info('Tickers from MEXC was updated');

            if event.is_set(): break;
            sleeptime = datetime.now() - starttime;
            if sleeptime.seconds >= 60: continue;
            time.sleep(60.0 - sleeptime.total_seconds());
        return;

    def worker_bitget(self, event:Event, lock:Lock):
        while True:
            starttime = datetime.now();
            if not self.bitget.get_tickers(): break;
            with lock:
                for ticker in self.bitget.tickers:
                    self.db.store('BITGET', ticker);
                self.log.logger.info('Tickers from BITGET was updated');

            if event.is_set(): break;
            sleeptime = datetime.now() - starttime;
            if sleeptime.seconds >= 60: continue;
            time.sleep(60.0 - sleeptime.total_seconds());
        return;

    def worker_crypto_com(self, event:Event, lock:Lock):
        while True:
            starttime = datetime.now();
            if not self.crypto_com.get_tickers(): break;
            with lock:
                for ticker in self.crypto_com.tickers:
                    self.db.store('CRYPTO.COM', ticker);
                self.log.logger.info('Tickers from CRYPTO.COM was updated');

            if event.is_set(): break;
            sleeptime = datetime.now() - starttime;
            if sleeptime.seconds >= 60: continue;
            time.sleep(60.0 - sleeptime.total_seconds());
        return;

    # main work loop
    def work(self):
        workers = [self.worker_kucoin, self.worker_binance, self.worker_okx, self.worker_bybit, self.worker_kraken, self.worker_coinbase, self.worker_gate_io, self.worker_bitfinex, self.worker_mexc, self.worker_bitget, self.worker_crypto_com];
        threads = [];
        event = Event(); # stop event, will be not used?
        lock = Lock(); # to push data to db

        for worker in workers:
            threads.append(threading.Thread(target = worker, args = (event, lock)));

        for thread in threads:
            thread.start();
            
        # event.set();
        for thread in threads:
            thread.join();
        return;
       
if __name__ == "__main__":
    arbitr = Arbitr();
    if arbitr.real_init():
        arbitr.log.logger.info('Started');
        arbitr.work();


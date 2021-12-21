#!/usr/bin/env python
# coding: utf-8
import requests
import re
import json
import traceback
import sys
from functools import lru_cache
import argparse
import datetime as dt

import pandas as pd

from web3 import Web3
from web3.auto import w3
from web3.contract import Contract
from web3._utils.events import get_event_data

from web3._utils.abi import exclude_indexed_event_inputs, get_abi_input_names, get_indexed_event_inputs, normalize_event_input_types
from web3.exceptions import MismatchedABI, LogTopicError
from web3.types import ABIEvent
from eth_utils import event_abi_to_log_topic, to_hex
from hexbytes import HexBytes


parser = argparse.ArgumentParser(
    description='Parse data from wallet into accounting format.')
parser.add_argument('wallet_address', type=str,
                    help='The wallet to fetch data from')

args = parser.parse_args()

WALLET_ADDRESS = args.wallet_address

API_KEY = '5XB7T2Z1WG5P8QFS81X128M3994WK39BA2'

DATETIME = dt.datetime.now()



def decode_tuple(t, target_field):
    output = dict()
    for i in range(len(t)):
        if isinstance(t[i], (bytes, bytearray)):
            output[target_field[i]['name']] = to_hex(t[i])
        elif isinstance(t[i], (tuple)):
            output[target_field[i]['name']] = decode_tuple(
                t[i], target_field[i]['components'])
        else:
            output[target_field[i]['name']] = t[i]
    return output


def decode_list_tuple(l, target_field):
    output = l
    for i in range(len(l)):
        output[i] = decode_tuple(l[i], target_field)
    return output


def decode_list(l):
    output = l
    for i in range(len(l)):
        if isinstance(l[i], (bytes, bytearray)):
            output[i] = to_hex(l[i])
        else:
            output[i] = l[i]
    return output


def convert_to_hex(arg, target_schema):
    """
    utility function to convert byte codes into human readable and json serializable data structures
    """
    output = dict()
    for k in arg:
        if isinstance(arg[k], (bytes, bytearray)):
            output[k] = to_hex(arg[k])
        elif isinstance(arg[k], (list)) and len(arg[k]) > 0:
            target = [
                a for a in target_schema if 'name' in a and a['name'] == k][0]
            if target['type'] == 'tuple[]':
                target_field = target['components']
                output[k] = decode_list_tuple(arg[k], target_field)
            else:
                output[k] = decode_list(arg[k])
        elif isinstance(arg[k], (tuple)):
            target_field = [a['components']
                            for a in target_schema if 'name' in a and a['name'] == k][0]
            output[k] = decode_tuple(arg[k], target_field)
        else:
            output[k] = arg[k]
    return output


@lru_cache(maxsize=None)
def _get_contract(address, abi):
    """
    This helps speed up execution of decoding across a large dataset by caching the contract object
    It assumes that we are decoding a small set, on the order of thousands, of target smart contracts
    """
    if isinstance(abi, (str)):
        abi = json.loads(abi)

    contract = w3.eth.contract(
        address=Web3.toChecksumAddress(address), abi=abi)
    return (contract, abi)


def decode_tx(address, input_data, abi):
    if abi is not None:
        try:
            (contract, abi) = _get_contract(address, abi)
            func_obj, func_params = contract.decode_function_input(input_data)
            target_schema = [
                a['inputs'] for a in abi if 'name' in a and a['name'] == func_obj.fn_name][0]
            decoded_func_params = convert_to_hex(func_params, target_schema)
            return (func_obj.fn_name, json.dumps(decoded_func_params), json.dumps(target_schema))
        except:
            e = sys.exc_info()[0]
        return ('decode error', repr(e), None)
    else:
        return ('no matching abi', None, None)


def get_contract_abi(address, contract_json):

    address_obj = contract_json.get(address)

    if not address_obj or not address_obj.get('abi'):

        res = requests.get(
            'https://api.bscscan.com/api?module=contract&action=getabi&address=%s&apikey=%s' % (address, API_KEY))

        res = res.json()

        if not address_obj:
            address_obj = {}

        # TODO: check for existing info from another functions

        if res.get('status') == '1':
            address_obj = {'abi': res["result"]}
            contract_json[address] = address_obj

            return res['result']
        else:
            print('Failed to fetch ABI for %s' % address)

    else:
        return address_obj['abi']


def get_contract_decimals(address, contract_json):

    address_obj = contract_json.get(address)

    if not address_obj or not address_obj.get('decimals'):

        if not address_obj:
            address_obj = {}

        abi = get_contract_abi(address, contract_json)
        contract = web3.eth.contract(address=address, abi=abi)
        
        decimals = contract.functions.decimals().call()

        address_obj['decimals'] = decimals
        return decimals
        
    else:
        return int(address_obj['decimals'])
#     res.json()['decimals']


def get_contract_symbol(address, contract_json):

    address_obj = contract_json.get(address)

    if not address_obj or not address_obj.get('symbol'):

        abi = get_contract_abi(address, contract_json)
        contract = web3.eth.contract(address=address, abi=abi)
        
        symbol = contract.functions.symbol().call()

        if not address_obj:
            address_obj = {}
        
        address_obj['symbol'] = symbol

        contract_json[address] = address_obj

        return symbol
    else:
        return address_obj['symbol']


def get_transactions_by_address(address):

    res = requests.get(
        'https://api.bscscan.com/api?module=account&action=txlist&address=%s&startblock=0&endblock=99999999&sort=asc&apikey=%s' % (address, API_KEY)
    )

    res = res.json()

    if res['message'] == 'OK':

        return res['result']


WALLET_ADDRESS = WALLET_ADDRESS.upper()


with open('contract_info.json', 'r') as f:
    contract_info = json.load(f)

bsc = "https://bsc-dataseed.binance.org/"
ankr_url = 'https://rpc.ankr.com/bsc'

web3 = Web3(Web3.HTTPProvider(bsc))

print(web3.isConnected())


@lru_cache(maxsize=None)
def _get_topic2abi(abi):
    if isinstance(abi, (str)):
        abi = json.loads(abi)

    event_abi = [a for a in abi if a['type'] == 'event']
    topic2abi = {event_abi_to_log_topic(_): _ for _ in event_abi}
    return topic2abi


@lru_cache(maxsize=None)
def _get_hex_topic(t):
    hex_t = HexBytes(t)
    return hex_t


def decode_log(data, topics, abi):
    if abi is not None:
        try:
            topic2abi = _get_topic2abi(abi)

            log = {
                'address': None,  # Web3.toChecksumAddress(address),
                'blockHash': None,  # HexBytes(blockHash),
                'blockNumber': None,
                'data': data,
                'logIndex': None,
                'topics': [_get_hex_topic(_) for _ in topics],
                'transactionHash': None,  # HexBytes(transactionHash),
                'transactionIndex': None
            }
            event_abi = topic2abi[log['topics'][0]]
            evt_name = event_abi['name']

            data = get_event_data(w3.codec, event_abi, log)['args']
            target_schema = event_abi['inputs']
            decoded_data = convert_to_hex(data, target_schema)

            return (evt_name, json.dumps(decoded_data), json.dumps(target_schema))
        except Exception:
            return ('decode error', traceback.format_exc(), None)

    else:
        return ('no matching abi', None, None)


def handle_native_transfer(transaction):

    amount = int(transaction['value']) * 1E-18
    symbol = 'BNB'
    _from = transaction['from']

    transaction_type = 'Withdraw' if _from.upper() == WALLET_ADDRESS else 'Deposit'

    if transaction_type == 'Withdraw':

        transaction_logs = web3.eth.get_transaction_receipt(
                transaction['hash'])

        fee = int(transaction['gas']) * int(transaction['gasPrice']) / (1 * 10 ** 18)

    transaction_dict = {

        'Date': dt.datetime.fromtimestamp(int(transaction['timeStamp'])),
        'Buy': "",
        'Currency (Buy)': "",
        'Sell': "",
        'Currency (Sell)': "",
        'Type': transaction_type,
        'Fiat value (Buy)': "",
        'Fiat value (Sell)': "",
        'Fee': '' if transaction_type == 'Deposit' else fee,
        'Currency (Fee)': '' if transaction_type == 'Deposit' else "BNB",
        'Fiat value (Fee)': "",
        'Exchange': "",
        'Wallet': "",
        'Account': "",
        'Transfer-Code': transaction['hash']
    }

    if transaction_type == 'Deposit':

        transaction_dict['Buy'] = amount
        transaction_dict['Currency (Buy)'] = symbol
    else:
        transaction_dict['Sell'] = amount
        transaction_dict['Currency (Sell)'] = symbol

    return transaction_dict


def parse_ERC20_transfer(transaction):

    transaction_hash = transaction['hash']

    receiving_adress = transaction['to']

    web3_transaction = web3.eth.get_transaction(transaction_hash)

    input_ = web3_transaction.input

    abi = get_contract_abi(web3_transaction.to,  contract_info)
    output = decode_tx(web3_transaction.to, input_, abi)
    print('function called: ', output[0])

    #     print(output)

    transaction_logs = web3.eth.get_transaction_receipt(transaction_hash)

    token_contract = transaction_logs.to

    decimals = get_contract_decimals(token_contract, contract_info)

    abi = get_contract_abi(token_contract, contract_info)

    symbol = get_contract_symbol(token_contract, contract_info)

    last_transaction = transaction_logs.logs[-1]

    output = decode_log(
        last_transaction['data'],
        last_transaction['topics'],
        abi
    )

    args = json.loads(output[1])
    _from = args.get('from', args.get('_from'))
    _to = args.get('to', args.get('_to'))
    amount = args.get('_value', args.get('value'))

    amount = amount / (1 * 10 ** decimals)

    fee = int(transaction['gas']) * int(transaction['gasPrice']) / (1 * 10 ** 18)

    transaction_type = 'Withdraw' if _from.upper() == WALLET_ADDRESS else 'Deposit'
    transaction_dict = {

        'Date': dt.datetime.fromtimestamp(int(transaction['timeStamp'])),
        'Buy': "",
        'Currency (Buy)': "",
        'Sell': "",
        'Currency (Sell)': "",
        'Type': transaction_type,
        'Fiat value (Buy)': "",
        'Fiat value (Sell)': "",
        'Fee': fee,
        'Currency (Fee)': "BNB",
        'Fiat value (Fee)': "",
        'Exchange': "",
        'Wallet': "",
        'Account': "",
        'Transfer-Code': transaction_hash
        # 'Comment'

    }

    if transaction_type == 'Deposit':

        transaction_dict['Buy'] = amount
        transaction_dict['Currency (Buy)'] = symbol
    else:
        transaction_dict['Sell'] = amount
        transaction_dict['Currency (Sell)'] = symbol

    return transaction_dict

transaction_list = []
transaction_not_parsed = []

def parse_all_transactions(address):

    transactions = get_transactions_by_address(WALLET_ADDRESS.lower())    

    print('%d transactions to parse' % len(transactions))

    for t in transactions:

        if t['isError'] != '0':
            continue

        transaction_hash = t['hash']

        transaction = web3.eth.get_transaction(transaction_hash)
        input_ = transaction.input

        if t['input'] == '0x':
            # native token transfer
            transaction_list.append(handle_native_transfer(t))
            continue
    
        abi = get_contract_abi(transaction.to,  contract_info)
        output = decode_tx(transaction.to, input_, abi)
        print('function called: ', output[0])


        if output[0] == 'transfer':
            transaction_list.append(parse_ERC20_transfer(t))
        
        elif output[0] == 'swapExactTokensForTokens':

            print('arguments: ', json.dumps(json.loads(output[1]), indent=2))

            args = json.loads(output[1])

            decimals = get_contract_decimals(args["path"][0], contract_info)
            symbol = get_contract_symbol(args["path"][0], contract_info)
            amount_out = args["amountIn"] / (1 * 10 ** decimals)

            output_decimals = get_contract_decimals(
                args["path"][-1], contract_info)
            output_symbol = get_contract_symbol(
                args["path"][-1], contract_info)

            transaction_logs = web3.eth.get_transaction_receipt(
                transaction_hash)

            last_transaction = transaction_logs.logs[-1]

            pair_abi = '[{"inputs":[],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"spender","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"},{"indexed":true,"internalType":"address","name":"to","type":"address"}],"name":"Burn","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0In","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1In","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount0Out","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1Out","type":"uint256"},{"indexed":true,"internalType":"address","name":"to","type":"address"}],"name":"Swap","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint112","name":"reserve0","type":"uint112"},{"indexed":false,"internalType":"uint112","name":"reserve1","type":"uint112"}],"name":"Sync","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"constant":true,"inputs":[],"name":"DOMAIN_SEPARATOR","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"MINIMUM_LIQUIDITY","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"PERMIT_TYPEHASH","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"burn","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getReserves","outputs":[{"internalType":"uint112","name":"_reserve0","type":"uint112"},{"internalType":"uint112","name":"_reserve1","type":"uint112"},{"internalType":"uint32","name":"_blockTimestampLast","type":"uint32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"_token0","type":"address"},{"internalType":"address","name":"_token1","type":"address"}],"name":"initialize","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"kLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"mint","outputs":[{"internalType":"uint256","name":"liquidity","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"nonces","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"permit","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"price0CumulativeLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"price1CumulativeLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"skim","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"amount0Out","type":"uint256"},{"internalType":"uint256","name":"amount1Out","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"swap","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"sync","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"}]'

            output = decode_log(
                last_transaction['data'],
                last_transaction['topics'],
                pair_abi
            )

            print('event emitted: ', output[0])
            print('arguments: ', json.dumps(json.loads(output[1]), indent=2))
            print()

            args = json.loads(output[1])

            amount_in = args['amount0Out'] / (1 * 10 ** output_decimals)

            fee = int(transaction['gas']) * int(transaction['gasPrice']) / (1 * 10 ** 18)


            transaction_dict = {
                'Date': dt.datetime.fromtimestamp(int(t['timeStamp'])),
                'Buy': "",
                'Currency (Buy)': "",
                'Sell': "",
                'Currency (Sell)': "",
                'Type': "Trade",
                'Fiat value (Buy)': "",
                'Fiat value (Sell)': "",
                'Fee': fee,
                'Currency (Fee)': "BNB",
                'Fiat value (Fee)': "",
                'Exchange': "",
                'Wallet': "",
                'Account': "",
                'Transfer-Code': transaction_hash
                # 'Comment'

            }

            transaction_dict['Sell'] = amount_out
            transaction_dict['Currency (Sell)'] = symbol

            transaction_dict['Buy'] = amount_in
            transaction_dict['Currency (Buy)'] = output_symbol


            transaction_list.append(transaction_dict)
        elif output[0] == 'approve':

            input_ = transaction['input']

            transaction_logs = web3.eth.get_transaction_receipt(
            transaction_hash)

            logs = transaction_logs['logs'][0]

            abi = get_contract_abi(transaction.to,  contract_info)

            output = decode_log(
                logs['data'],
                logs['topics'],
                abi
            )

            fee = int(transaction['gas']) * int(transaction['gasPrice']) / (1 * 10 ** 18)

            transaction_dict = {
                'Date': dt.datetime.fromtimestamp(int(t['timeStamp'])),
                'Buy': "",
                'Currency (Buy)': "",
                'Sell': "",
                'Currency (Sell)': "",
                'Type': "Expense",
                'Fiat value (Buy)': "",
                'Fiat value (Sell)': "",
                'Fee': fee,
                'Currency (Fee)': "BNB",
                'Fiat value (Fee)': "",
                'Exchange': "",
                'Wallet': "",
                'Account': "",
                'Transfer-Code': transaction_hash
                # 'Comment'

            }

            transaction_list.append(transaction_dict)
        elif output[0] == 'multicall':

            print('Not yet implemented')

        elif output[0] == 'addLiquidity':

            input_ = transaction['input']

            transaction_logs = web3.eth.get_transaction_receipt(
            transaction_hash)

            logs = transaction_logs['logs'][0]

            abi = get_contract_abi(transaction.to,  contract_info)

            output = decode_log(
                logs['data'],
                logs['topics'],
                abi
            )

            fee = int(transaction['gas']) * int(transaction['gasPrice']) / (1 * 10 ** 18)

            transaction_dict = {
                'Date': dt.datetime.fromtimestamp(int(t['timeStamp'])),
                'Buy': "",
                'Currency (Buy)': "",
                'Sell': "",
                'Currency (Sell)': "",
                'Type': "Expense",
                'Fiat value (Buy)': "",
                'Fiat value (Sell)': "",
                'Fee': fee,
                'Currency (Fee)': "BNB",
                'Fiat value (Fee)': "",
                'Exchange': "",
                'Wallet': "",
                'Account': "",
                'Transfer-Code': transaction_hash
                # 'Comment'

            }

            transaction_list.append(transaction_dict)
            
        else:
            transaction_not_parsed.append(t['hash'])
            print('Not supported', output[0])




parse_all_transactions(WALLET_ADDRESS)


with open('contract_info.json', 'w') as f:
    json.dump(contract_info, f, indent=4)


columns = [
    'Date',
    'Type',
    'Buy',
    'Currency (Buy)',
    'Fiat value (Buy)',
    'Sell',
    'Currency (Sell)',
    'Fiat value (Sell)',
    'Fee',
    'Currency (Fee)',
    'Fiat value (Fee)',
    'Exchange',
    'Wallet',
    'Account',
    'Transfer-Code',
    'Comment'
]

df = pd.DataFrame(transaction_list)

df['Comment'] = 'Parsed via explorer CSV parser (%s) at %s' % ('BNB', DATETIME)

# reorder columns
df = df[columns]

df.to_clipboard()

df.to_csv('/tmp/output_%s.csv' % WALLET_ADDRESS, index=False)


print('%d transactions processed' % df.shape[0])
print('%d transactions were not parsed successfully' % len(transaction_not_parsed))
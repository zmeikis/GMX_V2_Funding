from web3 import Web3

infura_url = 'https://ultra-weathered-cherry.arbitrum-mainnet.quiknode.pro/db618ebf616227632e38072b1964c29f21c45e78/'

w3 = Web3(Web3.HTTPProvider(infura_url))

# Check if connected successfully
print("Is Connected:", w3.is_connected())
EMIITTER = w3.to_checksum_address('0xC8ee91A54287DB53897056e12D9819156D3822Fb')

EVENT_LOG1_DATA_TYPE = (
        '((string,address)[],(string,address[])[]),'      # addressItems
        '((string,uint256)[],(string,uint256[])[]),'      # uintItems
        '((string,int256)[],(string,int256[])[]),'        # intItems
        '((string,bool)[],(string,bool[])[]),'            # boolItems
        '((string,bytes32)[],(string,bytes32[])[]),'      # bytes32Items
        '((string,bytes)[],(string,bytes[])[]),'          # bytesItems
        '((string,string)[],(string,string[])[])'         # stringItems
    )

EVENT_LOG1_TYPES_TO_DECODE = [
    'address',         # msgSender
    'string',          # eventName
    f'({EVENT_LOG1_DATA_TYPE})'
]

EVENT_LOG1_RAW_HASH = "0x137a44067c8961cd7e1d876f4754a5a3a75989b4552f1843fc69c3b372def160"

def parse_event1_data(log):
    decoded = w3.codec.decode(EVENT_LOG1_TYPES_TO_DECODE, log.data)
    return decoded

def parse_event1_name(log):
    decoded = parse_event1_data(log)
    return decoded[1]

GOOD_EVENT1_NAMES = ['OraclePriceUpdate', 'FundingFeeAmountPerSizeUpdated', 'ClaimableFundingAmountPerSizeUpdated', 'CumulativeBorrowingFactorUpdated', 'FundingUpdated', 'ClaimableFundingUpdated', 'CumulativeFundingUpdated', 'OpenInterestUpdated']

def is_event1_good(log):
    return parse_event1_name(log) in GOOD_EVENT1_NAMES


START = 321032038
END = 341698643

from tqdm import tqdm

def scan_logs(w3, start, end, step=1_000):
    """Yield raw logs that carry FundingUpdated."""
    logs = []
    
    for block_start in tqdm(range(start, end + 1, step), desc="Scanning blocks"):
        chunk_end = min(block_start + step - 1, end)

        try:
            logs_chunk = w3.eth.get_logs({
                "fromBlock": block_start,
                "toBlock": chunk_end,
                "address": EMIITTER,
                "topics": [[EVENT_LOG1_RAW_HASH]]
            })
            good_logs_chunk = [log for log in logs_chunk if is_event1_good(log)]
            logs += good_logs_chunk
            # print(f"Found {len(good_logs_chunk)} good logs in block {block_start} to {chunk_end}")
    
        except Exception as e:
            print(e)
            print("DUMPTING DATA AND CLOSING")
            return logs

        
    return logs


logs = scan_logs(w3, START, END)

class CumulativeBorrowingFactorUpdate:
    def __init__(self, market: str, delta: int, nextValue: int, isLong: bool, blockNumber: int):
        self.market = market
        self.delta = delta
        self.nextValue = nextValue
        self.isLong = isLong
        self.blockNumber =  blockNumber

    market: str
    delta: int
    nextValue: int
    isLong: bool
    blockNumber: int

def decodeCumulativeBorrowingFactorUpdate(log) -> CumulativeBorrowingFactorUpdate:
    decoded = parse_event1_data(log)
    
    tuple = decoded[2]

    market_token = tuple[0][0][0][1]
    
    delta = tuple[1][0][0][1]
    nextValue = tuple[1][0][1][1]

    isLong = tuple[3][0][0][1]

    return CumulativeBorrowingFactorUpdate(market=market_token, delta=delta, nextValue=nextValue, isLong=isLong, blockNumber=log.blockNumber)

class OpenInterestUpdate:
    def __init__(self, market: str, collateral: str, next_value: int, delta: int, isLong: bool, blockNumber: int):
        self.market = market
        self.collateral = collateral
        self.next_value = next_value
        self.delta = delta
        self.isLong = isLong
        self.blockNumber =  blockNumber

    market: str
    collateral: str
    delta: int
    value: int
    isLong: bool
    blockNumber: int

def decodeOpenInterestUpdate(log) -> OpenInterestUpdate:
    decoded = parse_event1_data(log)
    
    tuple = decoded[2]

    market_token = tuple[0][0][0][1]
    collateral_token = tuple[0][0][1][1]
    
    next_value = tuple[1][0][0][1]
    delta = tuple[2][0][0][1]

    isLong = tuple[3][0][0][1]

    return OpenInterestUpdate(market=market_token, collateral=collateral_token, next_value=next_value, delta=delta, isLong=isLong, blockNumber=log.blockNumber)


class FundingFeeAmountPerSizeUpdate:
    def __init__(self, market: str, collateral: str, delta: int, value: int, isLong: bool, blockNumber: int):
        self.market = market
        self.collateral = collateral
        self.delta = delta
        self.value = value
        self.isLong = isLong
        self.blockNumber =  blockNumber

    market: str
    collateral: str
    delta: int
    value: int
    isLong: bool
    blockNumber: int

def decodeFundingFeeAmountPerSizeUpdate(log) -> FundingFeeAmountPerSizeUpdate:
    decoded = parse_event1_data(log)
    
    tuple = decoded[2]

    market_token = tuple[0][0][0][1]
    collateral_token = tuple[0][0][1][1]
    
    delta = tuple[1][0][0][1]
    value = tuple[1][0][1][1]

    isLong = tuple[3][0][0][1]

    return FundingFeeAmountPerSizeUpdate(market=market_token, collateral=collateral_token, delta=delta, value=value, isLong=isLong, blockNumber=log.blockNumber)


class OracleUpdate:
    def __init__(self, token: str, min_price: int, max_price: int, timestamp: int, blockNumber: int):
        self.token = token
        self.min_price = min_price
        self.max_price = max_price
        self.timestamp = timestamp
        self.blockNumber = blockNumber
        

    token: str
    min_price: int
    max_price: int
    timestamp: int
    blockNumber: int



def decodeOraclePriceUpdate(log) -> OracleUpdate:
    decoded = parse_event1_data(log)

    # address = decoded[0]
    # name = decoded[1]
    tuple = decoded[2]
    market = tuple[0]
    market_token = market[0][0][1]
    # market_provider = market[1]
    price_data = tuple[1]
    min_price = price_data[0][0][1]
    max_price = price_data[0][1][1] 
    timestamp = price_data[0][2][1]

    return OracleUpdate(token=market_token, min_price=min_price, max_price=max_price, timestamp=timestamp, blockNumber=log.blockNumber)


import csv
import os
from typing import Iterable


def _should_write_header(path: str) -> bool:
    """
    Return True if the file at `path` is either missing or empty.
    """
    return not os.path.exists(path) or os.path.getsize(path) == 0


def dump_oracle_updates(
    updates: Iterable["OracleUpdate"],
    path: str,
    *,
    include_header: bool = True,
    newline: str = ""
) -> None:
    """
    Append OracleUpdate objects to `path` in CSV format.

    Columns: token,min_price,max_price,timestamp,blockNumber
    """
    rows = (
        (u.token, u.min_price, u.max_price, u.timestamp, u.blockNumber)
        for u in updates
    )

    write_header = include_header and _should_write_header(path)

    with open(path, "a", newline=newline, encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(("token", "min_price", "max_price", "timestamp", "blockNumber"))
        writer.writerows(rows)

def dump_open_interest_updates(
    updates: Iterable["OpenInterestUpdate"],
    path: str,
    *,
    include_header: bool = True,
    newline: str = ""
) -> None:
    """
    Append OpenInterestUpdate objects to `path` in CSV format.

    Columns: market,collateral,next_value,delta,isLong,blockNumber
    """
    rows = (
        (u.market, u.collateral, u.next_value, u.delta, u.isLong, u.blockNumber)
        for u in updates
    )

    write_header = include_header and _should_write_header(path)

    with open(path, "a", newline=newline, encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(("market", "collateral", "next_value", "delta", "isLong", "blockNumber"))
        writer.writerows(rows)

def dump_funding_updates(
    updates: Iterable["FundingFeeAmountPerSizeUpdate"],
    path: str,
    *,
    include_header: bool = True,
    newline: str = ""
) -> None:
    """
    Append FundingFeeAmountPerSizeUpdate objects to `path` in CSV format.

    Columns: market,collateral,delta,value,isLong,blockNumber
    """
    rows = (
        (u.market, u.collateral, u.delta, u.value, u.isLong, u.blockNumber)
        for u in updates
    )

    write_header = include_header and _should_write_header(path)

    with open(path, "a", newline=newline, encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(("market", "collateral", "delta", "value", "isLong", "blockNumber"))
        writer.writerows(rows)


def dump_borrowing_updates(
    updates: Iterable["CumulativeBorrowingFactorUpdate"],
    path: str,
    *,
    include_header: bool = True,
    newline: str = ""
) -> None:
    """
    Append CumulativeBorrowingFactorUpdate objects to `path` in CSV format.

    Columns: market,delta,nextValue,isLong,blockNumber
    """
    rows = (
        (u.market, u.delta, u.nextValue, u.isLong, u.blockNumber)
        for u in updates
    )

    write_header = include_header and _should_write_header(path)

    with open(path, "a", newline=newline, encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(("market", "delta", "nextValue", "isLong", "blockNumber"))
        writer.writerows(rows)


first_n_logs = logs

oracle_path = "oracle_updates_final.csv"
borrowing_path = "borrowing_updates_final.csv"
funding_path = "funding_updates_final.csv"
open_interest_path = "open_interest_updates_final.csv"

for log_ in tqdm(first_n_logs):
    # print(parse_event1_data(log_))
    # try:
    name = parse_event1_name(log_)
    if name == "OraclePriceUpdate":
        dump_oracle_updates([decodeOraclePriceUpdate(log_)], oracle_path)
    elif name == "FundingFeeAmountPerSizeUpdated":
        dump_funding_updates([decodeFundingFeeAmountPerSizeUpdate(log_)], funding_path)
    elif name == "CumulativeBorrowingFactorUpdated":
        dump_borrowing_updates([decodeCumulativeBorrowingFactorUpdate(log_)], borrowing_path)
    elif name == "OpenInterestUpdated":
        dump_open_interest_updates([decodeOpenInterestUpdate(log_)], open_interest_path)
    # except Exception as e:
    #     print(e)
    #     print(parse_event1_data(log_))












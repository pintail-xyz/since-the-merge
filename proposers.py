import requests
import csv
import psycopg2
import time
from datetime import timedelta

MERGE_SLOT = 4700013
MERGE_BLOCK = 15537393
CL_NODE_URL = 'http://localhost:5052'
FEEFILE = 'fee_recipient_transactions.csv'
OUTFILE = 'proposers.csv'


# connect to mev-inspect-py database

connection = psycopg2.connect(
    host="127.0.0.1",
    port=5432,
    user="postgres",
    password="password",
    database="mev_inspect"
)
cursor = connection.cursor()


# query to calculate actual value received by fee recipient

sql = (
    'select sum((gas_price - base_fee_per_gas) * gas_used), '
    '       sum(coinbase_transfer) '
    'from miner_payments where block_number = %s;'
)


# query CL node for validator indices of proposers in given epoch

def get_proposers(epoch):
    r = requests.get(CL_NODE_URL+f'/eth/v1/validator/duties/proposer/{epoch}')
    data = r.json()['data']
    return [data[i]['validator_index'] for i in range(32)]


# load the builder payee information

with open(FEEFILE) as f:
    rows = csv.reader(f)
    fee_header = ['block_number','transaction_hash','miner_address','transaction_to_address']
    if next(rows) != fee_header:
        print(FEEFILE + 'formatted incorrectly')
        exit()

    builder_payees = []
    for row in rows:
        block_number = int(row[0])
        payee = row[3]
        if len(builder_payees) == 0 or block_number != builder_payees[-1][0]:
            builder_payees.append([block_number, payee])
        else:
            builder_payees[-1][1] = 'multiple'


# prepare output CSV file

header = [
        'slot',
        'val_index',
        'block_number',
        'net_fees',
        'coinbase_transfer',
        'fee_recipient',
        'builder_payee'
]

with open(OUTFILE, 'w') as f:
    writer = csv.writer(f)
    writer.writerow(header)


# iterate through slots retrieving block info for each slot
# until we reach the final block saved in mev-inspect-py db

slot = MERGE_SLOT
proposers = get_proposers(slot // 32)
rows = []

cursor.execute('SELECT max(block_number) FROM miner_payments;')
start_block = MERGE_BLOCK
end_block = int(cursor.fetchone()[0])

last_update = 0
start_time = time.time()
while True:
    if slot % 32 == 0:
        proposers = get_proposers(slot // 32)

    # retrieve block from CL client (to reconcile block and slot numbers)
    json = requests.get(CL_NODE_URL + f'/eth/v2/beacon/blocks/{slot}').json()
    if 'data' in json:
        execution_payload = json['data']['message']['body']['execution_payload']
        block_number = int(execution_payload['block_number'])
        if block_number > end_block:
            break

        # fee recipient corresponds to the block builder
        fee_recipient = execution_payload['fee_recipient']


        # look up the builder payee (assumed to be EL address of proposer)
        if len(builder_payees) == 0 or block_number < builder_payees[0][0]:
            builder_payee = fee_recipient
        elif block_number == builder_payees[0][0]:
            builder_payee = builder_payees.pop(0)[1]
        else:
            raise Exception('whoops')
            
        # get mev-inspect-py data for this block
        cursor.execute(sql, (block_number,))
        result = cursor.fetchone()
        net_fees, coinbase_transfer = result

        # prepare row for writing to CSV
        row = [
            slot,
            proposers[slot % 32],
            block_number,
            net_fees,
            coinbase_transfer,
            fee_recipient,
            builder_payee
        ]
    else:
        # no block in this slot
        row = [slot, proposers[slot % 32], None, None, None, None, None]

    with open(OUTFILE, 'a') as f:
        writer = csv.writer(f)
        writer.writerow(row)

    
    slot += 1

    # progress update
    t = time.time()
    if t - last_update > 0.1:
        elapsed = timedelta(seconds = int(t - start_time))
        perc = 100 * (block_number - start_block + 1) / (
            end_block + 1 - start_block
        )
        print(f"{elapsed} / {perc:.2f}% complete", end='\r')
        last_update = t


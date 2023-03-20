from substrateinterface import SubstrateInterface
from substrateinterface.utils.ss58 import ss58_decode, ss58_encode
from datetime import datetime
import pandas as pd
import pandas_gbq
pd.set_option('display.max_columns', None)
pd.options.display.max_colwidth = 100

import sys
sys.path.insert(0, '/srv/shiny-server/')
from pyCrypto.substrate.contracts import *

# Get Shiden block numbers by day
block_numbers = pandas_gbq.read_gbq("select distinct date, CAST(startBN as FLOAT64) as startBN from `parity-data-infra-evaluation.substrate_etl_metrics.chainlog_daily` where chain = 'shiden' order by date desc limit 45;", progress_bar_type=None)
# block_numbers = pandas_gbq.read_gbq("select distinct date, chain, CAST(startBN as FLOAT64) as startBN from `parity-data-infra-evaluation.substrate_etl_metrics.chainlog_daily` where chain in ('rocodo','alephzero','shiden') order by date desc limit 45;", progress_bar_type=None)

# Get Owner informtion
def getOwner(chain, url, date, block_hash):
  substrate = SubstrateInterface(url)
  result = substrate.query_map("Contracts", "OwnerInfoOf", block_hash = block_hash, page_size = 100)

  owner = []
  for (a, b) in result:
    outi = {'date':date,
      'chain':chain,
      'url':url,
      'code_hash':a.value,
      'owner':b.value['owner'],
      'deposit':str(b.value['deposit']),
      'refcount':str(b.value['refcount'])}
    owner.append(outi)
  return(pd.DataFrame(owner))

# Get Contract Info
def getContractInfo(chain, url, block_hash):
  substrate = SubstrateInterface(url)
  result = substrate.query_map("Contracts", "ContractInfoOf", block_hash = block_hash, page_size = 100)
  addr = []
  for (a, b) in result:
    # print(f"b is {b}")
    # (a,b) = result[0]
    if b is None:
      outi = {'account':a.value,
          'trie_id':'',
          'code_hash':'',
          'storage_bytes':'',
          'storage_byte_deposit':'',
          'storage_item_deposit':'',
          'storage_base_deposit':''}
      addr.append(outi)
    else:
      try:
        outi = {'account':a.value,
          'trie_id':b.value['trie_id'],
          'code_hash':b.value['code_hash'],
          'storage_bytes':str(b.value['storage_bytes']),
          'storage_byte_deposit':str(b.value['storage_byte_deposit']),
          'storage_item_deposit':str(b.value['storage_item_deposit']),
          'storage_base_deposit':str(b.value['storage_base_deposit'])}
        addr.append(outi)
      except:
        outi = {'account':a.value,
            'trie_id':b.value['trie_id'],
            'code_hash':b.value['code_hash'],
            'storage_bytes':'',
            'storage_byte_deposit':'',
            'storage_item_deposit':'',
            'storage_base_deposit':str(b.value['storage_deposit'])}
        addr.append(outi)
  addr_df = pd.DataFrame(addr)
      
  substrate = SubstrateInterface(url)
  bal = []
  for account in addr_df['account']:
    # print(account)
    balances = substrate.query('System', 'Account', params = [account], block_hash = block_hash)
    balances_free = str(balances.value['data']['free'])
    balances_reserved = str(balances.value['data']['reserved'])
    balances_misc_frozen = str(balances.value['data']['misc_frozen'])
    balances_fee_frozen = str(balances.value['data']['fee_frozen'])
    outi = {'account':account, 'contract_bal_free':balances_free, 'contract_bal_reserved':balances_reserved, 'contract_bal_misc_frozen':balances_misc_frozen, 'contract_bal_fee_frozen':balances_fee_frozen}
    bal.append(outi)
  balances_df = pd.DataFrame(bal)

  addr_bal = pd.merge(addr_df, balances_df, on='account', how='left')
  return(addr_bal)

# Get account balances
def getBalances(url, block_hash):
  substrate = SubstrateInterface(url)
  bal = []
  for account in addr_df['account']:
    # print(account)
    balances = substrate.query('System', 'Account', params = [account], block_hash = block_hash)
    balances_free = str(balances.value['data']['free'])
    balances_reserved = str(balances.value['data']['reserved'])
    balances_misc_frozen = str(balances.value['data']['misc_frozen'])
    balances_fee_frozen = str(balances.value['data']['fee_frozen'])
    outi = {'account':account, 'contract_bal_free':balances_free, 'contract_bal_reserved':balances_reserved, 'contract_bal_misc_frozen':balances_misc_frozen, 'contract_bal_fee_frozen':balances_fee_frozen}
    bal.append(outi)
  return(pd.DataFrame(bal))

# Get contract language
def getContractLanguage(url, block_hash):
  substrate = SubstrateInterface(url)
  result = substrate.query_map(module="Contracts", storage_function="CodeStorage", block_hash = block_hash, page_size = 100)
  data = []
  for (a, b) in result:
    code = b.value['code']
    code = code.replace("0x","")
    hexcode = bytes.fromhex(code)
    l = getLanguage(hexcode)
    data.append({'code_hash':a.value, 'language':l})
  return(pd.DataFrame(data))

# Get contract version
def getContractVersion(url, block_hash):
  
  url = 'wss://rococo-contracts-rpc.polkadot.io'
  substrate = SubstrateInterface(url)
  df = substrate.get_metadata_storage_functions()
  tmp = pd.DataFrame(df)
  tmp[tmp['module_id'] == 'Contracts']
  # result = substrate.query_map(module="Contracts", storage_function="palletversion", block_hash = block_hash, page_size = 100)

# helper function
def getContracts(chain, url, ss58_format, offset = 0):
  # url = 'wss://shiden.api.onfinality.io/public-ws'; chain='shiden'; ss58_format = 5; offset = 2
  # url = 'wss://rococo-contracts-rpc.polkadot.io'; chain='rococo'; ss58_format = 42
  # url = 'wss://ws.test.azero.dev'; chain='alephzero'; ss58_format=42;offset=13
  
  
  # Pass 0: Get owner
  substrate = SubstrateInterface(url)

  # Determine block_hash   
  if offset > 0:  
    # remove zeros and missing values
    bn = block_numbers['startBN'][block_numbers['startBN'] > 0].dropna()
    block_delta = int(bn.iloc[0]) - int(bn.iloc[offset])
    if chain == 'alephzero':
      block_delta = block_delta * 12 # Shiden is 12sec and Alephzero is 1sec
    latest_hash = substrate.get_chain_finalised_head()
    latest_block = substrate.get_block_number(latest_hash)
    target_block = latest_block - block_delta
    block_hash = substrate.get_block_hash(int(target_block))
  else:
    block_hash = None

  # Get date  
  timestamp = substrate.query(module='Timestamp',storage_function='Now',block_hash=block_hash).value
  date = datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')

  # Check if data is already there
  tmp = pandas_gbq.read_gbq(f"""select count(code_hash) as N
  from `parity-data-infra-evaluation.roger.contract-language-balances`
  where chain = '{chain}' and date = '{date}';""", progress_bar_type=None)
  if tmp['N'][0] > 20:
    print(f"Already done {chain} {date}")
    return None
  
  # Get Owner informtion
  owner_df = getOwner(chain, url, date, block_hash)

  # Get contract language
  contracts_df = getContractLanguage(url, block_hash)
  both = pd.merge(contracts_df, owner_df, how = "inner", on = "code_hash")

  # Get Contract Info & Balance
  addr_bal = getContractInfo(chain, url, block_hash)
  # Put it all together and save to database    
  comb = pd.merge(both, addr_bal, how = "outer", on = "code_hash")
  pandas_gbq.to_gbq(comb, 'parity-data-infra-evaluation.roger.contract-language-balances', if_exists="append")

def contract_lang(start, end):  
  # urls = [{'chain':'alephzero', 'url':'wss://ws.test.azero.dev', 'ss58_format':42}]
  # url = urls[0]
  urls = [{'chain':'rococo', 'url':'wss://rococo-contracts-rpc.polkadot.io', 'ss58_format':42},
          {'chain':'shiden', 'url':'wss://shiden.api.onfinality.io/public-ws', 'ss58_format':5},
          {'chain':'alephzero', 'url':'wss://ws.test.azero.dev', 'ss58_format':42}]

  for u in urls:
    for offset in range(start, end):
      try:
        getContracts(chain = u['chain'], url = u['url'], ss58_format = u['ss58_format'], offset = offset)
        print(f"Finished for {url}")
      except:
        print(f"error for {url['chain']}")

# Check number of observations saved in database
def check():
  tmp = pandas_gbq.read_gbq("""select chain, DATE(date) as date, count(chain) as N
  from `parity-data-infra-evaluation.roger.contract-language-balances`
  group by chain, date order by chain, date;""", progress_bar_type=None)
  tmp[tmp['chain']=='rococo']
  print(tmp)
  # view_table_expriation(table_name = 'contract-language')
  
def main():
  print(f"{datetime.now()} Starting...")
  contract_lang(start = 0, end = 3)
  print(f"{datetime.now()} Done...")
  # check()
  
if __name__ == "__main__":
    main()
    

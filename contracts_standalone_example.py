# Helper function to heuristically determine what language the smart contract was 
# written in, either ink!, ask!, or solidity.  Adapted from some open-source 
# [Github gist code](https://gist.github.com/mfornos/7848c7d9d67eefe0e41418981a52212d) 
# by [Marc](https://github.com/mfornos)
import sys
from ppci import wasm

def getLanguage(ba):
  m = wasm.Module(ba)
  sections = m.get_definitions_per_section()
  imports = sections['import']
  start = sections['start']
  customs = sections['custom']
  mem = next(filter(lambda x : x.kind == 'memory', imports))
  
  def hasName(name):
    lambda x : x.name == name
  
  # Tentative heuristics
  # (!) NOT tested with a reasonable sample set of binaries
  
  # Solang
  # * emits memory as the first import
  # * has custom sections named 'name' and 'producers'
  isSolang = bool(
    imports[0].kind == 'memory'
    and not start
    and customs
    and any(filter(hasName('name'), customs))
  )
  
  # Ask!
  # * has a start section
  # * has custom section named 'sourceMappingURL'
  # * imports declare memory after functions
  isAsk = bool(
    imports[0].kind != 'memory'
    and start
    and customs
    and any(filter(hasName('sourceMappingURL'), customs))
  )
  
  # Ink!
  # * no custom sections
  # * no start section
  # * imports declare memory after functions
  isInk = bool(
    imports[0].kind != 'memory'
    and not start
    and not customs
  )
  
  if isInk:
    return("ink!")
  if isAsk:
    return("ask!")
  if isSolang:
    return("solidity")
  return("unknown")


# Example code to extract smart contract code and determine langauge
# roger@parity.io
from substrateinterface import SubstrateInterface
from substrateinterface.utils.ss58 import ss58_decode, ss58_encode
from datetime import datetime
import pandas as pd
import pandas_gbq
pd.set_option('display.max_columns', None)
pd.options.display.max_colwidth = 100

# Get Shiden block numbers by day
# NB: I get the startBN for Shiden from Polkaholic's API so I can extract the data as of midnight UTC.
# These block numbers are hard coded for this example.
block_numbers = pd.DataFrame(data = {
  'date': ['2023-03-28','2023-03-27','2023-03-26','2023-03-25'],
  'startBN': [3671807,3665039,3658275,3651515]
})

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
  
  # Get Owner informtion
  owner_df = getOwner(chain, url, date, block_hash)
  
  # Get contract language
  contracts_df = getContractLanguage(url, block_hash)
  both = pd.merge(contracts_df, owner_df, how = "inner", on = "code_hash")
  
  # Get Contract Info & Balance
  addr_bal = getContractInfo(chain, url, block_hash)
  # Put it all together and save to database    
  comb = pd.merge(both, addr_bal, how = "outer", on = "code_hash")
  comb.to_csv("contracts_raw_data.csv", mode='a', index=False)

def contract_lang(start, end):  
  # urls = [{'chain':'alephzero', 'url':'wss://ws.test.azero.dev', 'ss58_format':42}]
  # u = urls[0]
  urls = [{'chain':'rococo', 'url':'wss://rococo-contracts-rpc.polkadot.io', 'ss58_format':42},
          {'chain':'shiden', 'url':'wss://shiden.api.onfinality.io/public-ws', 'ss58_format':5}]

  for u in urls:
    for offset in range(start, end):
      getContracts(chain = u['chain'], url = u['url'], ss58_format = u['ss58_format'], offset = offset)
      print(f"Finished for {u}")

def main():
  contract_lang(start = 0, end = 2)

if __name__ == "__main__":
  main()



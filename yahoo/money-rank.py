
from pathlib import Path

import json

price_titles = {}
for name in Path('./blobs').glob('*'):
  obj = json.load(name.open())
  price = obj['price']
  price = ( int( price.replace(',','') ) )
  title = ( obj['title'] )
  
  if price_titles.get(price) is None:
    price_titles[price] = []  
  price_titles[price].append(title)

for price, titles in sorted( price_titles.items(), key=lambda x:x[0]):
  print(f'{price}\t{json.dumps(titles, ensure_ascii=False)}')


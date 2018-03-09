from pathlib import Path

import gzip

import bs4

import re

import concurrent.futures

import hashlib

import json
def _map(arg):
  key, names = arg
  for name in names:
    html = ( gzip.decompress(open(name,'rb').read()).decode() )
    soup = bs4.BeautifulSoup(html)
    
    url = soup.find('link', {'rel':'canonical'})
    print(url)
    mkeyword = soup.find('meta', {'name':'keywords'})
    descript = soup.find('meta', {'name':'description'})
    if url is None:
      continue
    url = url.get('href')
    if mkeyword is None or descript is None:
      continue
    mkeyword = mkeyword.get('content')
    descript = descript.get('content')
    if re.search(r'store.shopping.yahoo.co.jp', url) is None:
      continue

    [ s.extract() for s in soup('script') ]
    ha = hashlib.sha256(bytes(url, 'utf8')).hexdigest()
    body = soup.find('div', {'class':'dvBody'})
    if body is None:
      continue
    body = body.text
    print(url)
    print(mkeyword)
    print(descript)

    price = soup.find('span', {'class':'elNum'})
    print('price', price)
    if price is None:
      continue
    
    price = price.text

    body = re.sub(r'\s{1,}', ' ', body)
    obj = { 'body':body, 'price':price,  'mkeyword':mkeyword, 'url':str(url), 'description':descript, 'title':soup.title.text }
    #print(obj)
    json.dump(obj, fp=open(f'blobs/{ha}', 'w'), indent=2, ensure_ascii=False)

args = {}
for index, name in enumerate(Path('../../scraping-designs/yahoo-shop-scrape/htmls').glob('*')):
  key = index%16
  if args.get(key) is None:
    args[key] = []
  args[key].append( name )
args = [(key, names) for key, names in args.items()]
#_map(args[-1])
with concurrent.futures.ProcessPoolExecutor(max_workers=16) as exe:
  exe.map(_map, args)

from pathlib import Path

import json

import MeCab

from collections import Counter

import hashlib

import pickle, gzip

import sys

import math

import re
if '--blobs' in sys.argv:
  m = MeCab.Tagger('-Owakati')
  for save, p in [('yahoo','./yahoo/blobs/')]:#[('rakuten', './rakuten/blobs/'), ('yahoo','./yahoo/blobs/')]:
    for name in Path(p).glob('*'):
      obj = json.load(fp=open(name))
      #print( list(obj.keys()) )
      keywords    = dict( Counter( m.parse(obj['mkeyword']).split() ) )
      description = dict( Counter( m.parse(obj['description']).split() ) )
      title       = dict( Counter( m.parse(obj['title']).split() ) )
      body        = dict( Counter( m.parse(obj['body']).split() ) )
      url         = obj['url']

      data = [url, title, keywords, description, body] 
      data = gzip.compress(pickle.dumps(data))
      ha   = hashlib.sha256(data).hexdigest()
      if Path(f'blobs/{save}_{ha}').exists():
        continue
      print(ha)
      open(f'blobs/{save}_{ha}', 'wb').write( data )

if '--term_doc' in sys.argv:
  term_doc =  {}
  for name in Path('blobs/').glob('*'):
    data = pickle.loads(gzip.decompress(open(name, 'rb').read()))
    # pop url
    # print(data)
    _, title, keywords, description, body = data

    for keys in [title.keys(), keywords.keys(), description.keys()]:
      for term in keys:
        if term_doc.get(term) is None:
          term_doc[term] = 0
        term_doc[term] += 1

  # term_docのレアリティが高い言葉（文字化けのユニークな何かなど）を削る
  for term in list(term_doc.keys()):
    #if term_doc[term] <= 3:
    #  del term_doc[term]
    #  continue
    if re.search(r'\d{1,}', term) is not None:
      del term_doc[term]
      continue
  json.dump(term_doc, fp=open('term_doc.json', 'w'), indent=2, ensure_ascii=False)

if '--term_index' in sys.argv:
  
  term_doc = json.load(fp=open('term_doc.json'))
  term_index = {}
  for name in Path('blobs/').glob('*'):
    data = pickle.loads(gzip.decompress(open(name, 'rb').read()))
    _, title, keywords, description, body = data
    for keys in [title.keys(), keywords.keys(), description.keys()]:
      for term in keys:

        if term not in term_doc:
          continue

        if term_index.get(term) is None:
          term_index[term] = len(term_index) 
  json.dump(term_index, fp=open('term_index.json', 'w'), indent=2, ensure_ascii=False)


if '--tfidf' in sys.argv:
  term_doc = json.load(fp=open('term_doc.json'))

  for name in Path('blobs/').glob('*'):
    hashname = str(name).split('/').pop()
    
    if Path(f'tfidf/{hashname}').exists():
      continue
    
    print(hashname)
    data = pickle.loads(gzip.decompress(open(name, 'rb').read()))

    url_data = {}
    url, title, keywords, description, body = data
    body = { term : math.log(freq+1.) / term_doc[term] for term, freq in body.items() if term_doc.get(term)}
    description = { term : math.log(freq+1.) / term_doc[term] for term, freq in description.items() if term_doc.get(term)}
    keywords = { term : math.log(freq+1.) / term_doc[term] for term, freq in keywords.items() if term_doc.get(term)}
    title = { term : math.log(freq+1.) / term_doc[term] for term, freq in title.items() if term_doc.get(term)}
    url_data[url] = [ title, keywords, description, body ]
    open(f'tfidf/{hashname}', 'wb').write( gzip.compress(pickle.dumps(url_data)) )

import gzip
import pickle


from pathlib import Path

import sys

import math
import hashlib
import json

import plyvel

import concurrent.futures
db = plyvel.DB('feats.ldb', create_if_missing=True) 
if '--memory' in sys.argv:
  for name in Path('tfidf/').glob('*'):
    obj = pickle.loads(gzip.decompress(name.open('rb').read()))
    for url, data in obj.items():
      db.put(bytes(url, 'utf8'), pickle.dumps(data))
      print(url)

if '--sim' in sys.argv:

  def sim(arg):
    s, t = arg
    score  = 0.
    for term, weight in s.items():
      if t.get(term) is not None:
        score += t[term] * weight

    v1 = math.sqrt( sum([v**2 for v in s.values()]) )
    v2 = math.sqrt( sum([v**2 for v in t.values()]) )
    return score/(v1+v2)

  def _map(arg):
    url, val = arg
    source = pickle.loads(val)
    try:   
      key_scores = {} 
      for chunk in Path('./tfidf').glob('*'):
        for _url, target in pickle.loads( gzip.decompress(chunk.open('rb').read()) ).items():
          if 'yahoo' in _url:
            continue
          # title x title3
          scores = []
          for triple in zip(['title', 'keywords', 'description', 'body'], source, target):
            head = triple[0] 
            s    = sim(triple[1:])
            #print(head, s)
            scores.append( (head, s) )
          
          key_scores[ (url, _url) ] = scores
          #print(sum(map(lambda x:x[1], scores)))
      ha = hashlib.sha256(bytes(url, 'utf8')).hexdigest()
      key_scores = [(key,scores) for key, scores in key_scores.items()]
      key_scores = sorted(key_scores, key=lambda xs:sum(map(lambda x:x[1], xs[1])))[-10:]
      print(url)
      print(key_scores)
      pickle.dump(key_scores, open(f'similarity/{ha}', 'wb'))
    except Exception as ex:
      print(f'deep error {ex}')

  args = [(url.decode(), val) for url, val in filter(lambda x:'yahoo' in x[0].decode(), db.iterator())]
  #_map(args[-1])
  #[_map(arg) for arg in args]
  with concurrent.futures.ProcessPoolExecutor(max_workers=16) as exe:
    exe.map(_map, args)

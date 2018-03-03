import gzip
import pickle


from pathlib import Path

import sys

import math
import hashlib
import json

import plyvel

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


  onmemory = pickle.load(open('onmemory.pkl','rb'))
  for url in filter(lambda x:'yahoo' in x, list(onmemory.keys())):
    print(url)
    source = onmemory[url]
      
    key_scores = {}
    for _url, target in onmemory.items():
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
    pickle.dump(key_scores, open(f'similarity/{ha}', 'wb'))

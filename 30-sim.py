import gzip
import pickle


from pathlib import Path

import sys

import math
import hashlib
import json

import plyvel

import concurrent.futures

import pickle
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

  def _sort(x):
    x = dict(x)
    x1 = x['title']
    x2 = x['keyword']
    return x1 + x2

  def _map(arg):
      path = arg 
      surl, source = tuple(pickle.loads(gzip.decompress(path.open('rb').read())).items())[0]
      print(surl)
      print(source)
      print(type(source))
      key_scores = {} 
      for _path in Path('./tfidf').glob('*'):
        turl, target = tuple( pickle.loads(gzip.decompress(_path.open('rb').read())).items() )[0]
        key = (surl, turl)

        sims = []
        for s, t in zip(source[:3], target[:3]):
          sims.append( sim( (s, t) ) )
        key_scores[ key ] = sims
        #print( key, sims )

      key_scores = sorted( [ (key,sims) for key,sims in key_scores.items()], key=lambda x:sum(x[1])*-1 )[:10]

      key_scores = dict(key_scores)
      
      ha = hashlib.sha256(bytes(str(path), 'utf8')).hexdigest()
      print(key_scores)
      pickle.dump(key_scores, open(f'similarity/{ha}', 'wb') )
  args = [path for path in Path('./tfidf').glob('*')]
  _map(args[-1])
  with concurrent.futures.ProcessPoolExecutor(max_workers=16) as exe:
    exe.map(_map, args)

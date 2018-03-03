import pickle
import gzip

from pathlib import Path

for name in Path('tfidf/').glob('*'):
  url_data = pickle.loads(gzip.decompress(name.open('rb').read()))

  for url, data in url_data.items():
    print(url)

    term_score = {}
    for entory in data:
      for term, score in entory.items():
        if term_score.get(term) is None:
          term_score[term] = 0
        term_score[term] += score

    for term, score in sorted(term_score.items(), key=lambda x:x[1]*-1)[:10]:
      print(url, term, score)

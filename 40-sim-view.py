
from pathlib import Path

import pickle


for name in Path('./similarity').glob('*'):
  obj = pickle.load(name.open('rb'))
  for key, sims in obj.items():
    print(key, sims)

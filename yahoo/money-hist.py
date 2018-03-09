
import json

import math

log_freq = {}
key_freq = {}

for name in open('dump.txt'):
  
  head, tail = name.split('\t')
  head = int(head)
  tail = json.loads(tail)

  key = head // 1000
  #print( head , len(tail) )

  if key_freq.get(key) is None:
    key_freq[key] = 0
  key_freq[key] += len(tail)

  log = int(math.log10(head) * 10)//5
  if log_freq.get(log) is None:
    log_freq[log] = 0
  log_freq[log] += len(tail)

for key, freq in sorted(key_freq.items(), key=lambda x:x[0]):
  #print(key*1000, freq)
  ...

for log, freq in sorted(log_freq.items(), key=lambda x:x[0]):
  print(log*0.5, math.log(freq+1))

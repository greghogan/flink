#!/usr/bin/env python

import collections
import json
import sys

results = collections.defaultdict(list)

with open(sys.argv[1], 'r') as file:
  for line in file:
    try:
      data = json.loads(line)
      results[(data['algorithm'],data['scale'],data['idType'])].append(data)
    except:
      pass

last_scale = 0
last_algorithm = ""

for algorithm, scale, idType in sorted(results):
  count = 0
  total_runtime_ms = 0

  for result in results[(algorithm,scale,idType)]:
    count += 1
    total_runtime_ms += result['runtime_ms']

  if last_algorithm and last_algorithm != algorithm:
    print

  if last_scale > 0 and last_scale != scale:
    print

  last_algorithm = algorithm
  last_scale = scale

  print('{}, scale={}, {:<6}: runtime={:>8.3f} ({})'.format(algorithm, scale, idType, total_runtime_ms / 1000.0 / count, count))

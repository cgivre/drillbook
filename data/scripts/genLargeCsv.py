import os

try:
  os.mkdir('../gen')
except OSError:
  pass
out = open('../gen/large.csv', 'w')
for i in range(10000000) :
  c = chr(i % 26 + ord('a'))
  out.write( (c * 80) + '\n' )
out.close()


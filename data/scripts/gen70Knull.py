import os

try:
  os.mkdir('../gen')
except OSError:
  pass
out = open('../gen/70knulls.json', 'w')
for i in range(70000) :
  out.write( '{a: null}\n' )
out.write( '{a: "gotcha!"}\n' )
out.close()


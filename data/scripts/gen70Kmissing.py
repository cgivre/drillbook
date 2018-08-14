import os

try:
  os.mkdir('../gen')
except OSError:
  pass
out = open('../gen/70kmissing.json', 'w')
for i in range(70000) :
  out.write( '{a: ' + str(i) + '}\n' )
out.write( '{a: 70001, b: "hi there!"}\n' )
out.close()


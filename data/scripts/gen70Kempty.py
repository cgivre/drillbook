import os

try:
  os.mkdir('../gen')
except OSError:
  pass
out = open('../gen/70kempty.json', 'w')
for i in range(70001) :
  out.write( '{a: ' + str(i) + ', b: []}\n' )
out.write( '{a: 70001, b: ["Fred", "Barney"]}\n' )
out.close()


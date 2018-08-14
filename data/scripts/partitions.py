import os
import os.path
import datetime

base = "../sales"
try:
  os.mkdir(base)
except OSError:
  pass
date = datetime.date(2017, 6, 1)
oneDay = datetime.timedelta(1)
date = date - oneDay
for i in range(300) :
  date = date + oneDay
  dir0 = date.isoformat()
  path = os.path.join(base, dir0)
  try:
    os.mkdir(path)
  except OSError:
    pass
  for j in range(5) :
    out = open(os.path.join(path, "store" + str(j + 1) + ".csvh"), 'w')
    out.write( "date,product,quantity,price\n" )
    out.write( dir0 + ",1000,1,10.00\n" )
    out.write( dir0 + ",2000,1,0.20\n" )
    out.close()

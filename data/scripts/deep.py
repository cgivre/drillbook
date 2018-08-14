import os
import os.path
import datetime
import re

base = "../deep"
try:
  os.mkdir(base)
except OSError:
  pass
date = datetime.date(2017, 6, 1)
oneDay = datetime.timedelta(1)
date = date - oneDay
for i in range(300) :
  date = date + oneDay
  datestr = date.isoformat()
  path = os.path.join(base, datestr.replace("-", "/"))
  try:
    os.makedirs(path)
  except OSError:
    pass
  for j in range(5) :
    out = open(os.path.join(path, "store" + str(j + 1) + ".csvh"), 'w')
    out.write( "date,product,quantity,price\n" )
    out.write( datestr + ",1000,1,10.00\n" )
    out.write( datestr + ",2000,1,0.20\n" )
    out.close()

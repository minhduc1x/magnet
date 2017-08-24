import sys
import csv

tabin = csv.reader(sys.stdin, dialect=csv.excel)
commaout = csv.writer(sys.stdout, dialect=csv.excel_tab)
for row in tabin:
  commaout.writerow(row)
import os 
import sys
import csv

location = os.getcwd() # get present working directory location here
counter = 0
for file in os.listdir(location):
    try:
        if file.endswith(".csv"):
		counter += 1 
		with open(file,'r') as f:
			filename = os.path.basename(file)
			header = os.path.splitext(filename)[0]
			name = header + '.tsv'
			
			with open (name,'w+') as f1:
				tabin = csv.reader(f,dialect=csv.excel)
				commaout = csv.writer (f1, dialect=csv.excel_tab)
				for row in tabin:
					commaout.writerow(row)
							
			
			
		
        
    except Exception as e:
        raise e
        print "No files found here!"

print "Total files converted:\t", counter
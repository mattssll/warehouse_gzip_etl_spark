import glob
myFiles = glob.glob("./products2/*")

import csv
for file in myFiles:
    with open(file) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            row.replace('\0', '')

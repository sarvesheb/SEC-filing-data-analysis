from sec_edgar_downloader import Downloader
import time
import os
import csv
dl = Downloader(os.getcwd())

ft=input("Enter types (1. 10-Q 2. 10-K 3. 8-K) :\n")
date1=input("Enter start date (YYYYMMDD): ")
date2=input("Enter end date (YYYYMMDD): ")

num=int(input("Enter number of tickers to download: "))
total=0

filing_type=["10-Q","10-K","8-K"]
data_file=open('ticker_list.csv', mode='r')
data_file.seek(0)
csv_reader = csv.reader(data_file)
for row in csv_reader:     
    total=total+1

data_file.seek(0)
curr=1
positionStr = 'Current company: ' + str(curr).rjust(5) + '     Total company: ' + str(total).rjust(6)
print(positionStr)
time1=time.time()
for ticker in csv_reader:
    if ticker[0]=="Ticker":
        continue
    print(ticker[0])
    try:
        if "1" in ft:
            dl.get(filing_type[0], ticker[0], after_date=date1, before_date=date2)
    except:
        try:
            print("Internet error occurred- retrying")
            dl.get(filing_type[0], ticker[0], after_date=date1, before_date=date2)
        except:
            print("Couldn't resolve, moving to next ticker")
    try:

        if "2" in ft:   
            dl.get(filing_type[1], ticker[0], after_date=date1, before_date=date2)
    except:
        try:
                
            print("Internet error occurred- retrying")
            dl.get(filing_type[1], ticker[0], after_date=date1, before_date=date2)
        except:
            print("Couldn't resolve, moving to next ticker")
            
    try:
        if "3" in ft:
            dl.get(filing_type[2], ticker[0], after_date=date1, before_date=date2)
    except:
        try:
            print("Internet error occurred- retrying")
            dl.get(filing_type[2], ticker[0], after_date=date1, before_date=date2)
        except:
            print("Couldn't resolve, moving to next ticker")
    curr=curr+1
    positionStr = 'Current ticker: ' + str(curr).rjust(5) + '     Total tickers: ' + str(total).rjust(6)
    print(positionStr)

    if curr==num:
        break  
print("Time taken to download is: ",time.time()-time1)
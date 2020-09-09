#from sec_edgar_downloader import Downloader
import csv
import os
import pipes as p
import subprocess
import time

si = subprocess.STARTUPINFO()
#si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
si.dwFlags |= subprocess.HIGH_PRIORITY_CLASS
si.wShowWindow = subprocess.SW_HIDE # default 
si.dwFlags |= subprocess.DETACHED_PROCESS #= 0x00000008
DETACHED_PROCESS = 0x00000008
HIGH_PRIORITY_CLASS=0x00000080

with open("data.csv", "a+") as f:
    f.seek(0)
    first_line=f.readline().strip()
    if len(first_line)==0:
        data_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        data_writer.writerow(['Ticker', 'Filing Type','Date', 'Search Word', 'Occurrences','Total Words','CIK','SIC','SIC Code'])
    f.close()

file_names= [a[6:len(a)-4] for a in os.listdir() if a.startswith("group") and a.endswith(".txt")]
total_param=['Ticker', 'Filing Type','Date','File Size(MB)',]+file_names +['Total Words']

with open("grouped_data.csv", "a+") as f:
    f.seek(0)
    first_line=f.readline().strip()
    if len(first_line)==0:
        data_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        data_writer.writerow(total_param)
    f.close()

ft=input("Enter types to analzye(1. 10-Q 2. 10-K 3. 8-K) :\n")
num=int(input("Enter number of tickers to analyze: "))
total=0

filing_type=["10-Q","10-K","8-K"]
data_file=open('ticker_list.csv', mode='r')
data_file.seek(0)
csv_reader = csv.reader(data_file)
for row in csv_reader:     
    total=total+1

data_file.seek(0)
curr=0
positionStr = 'Current company: ' + str(curr).rjust(5) + '     Total company: ' + str(total).rjust(6)
print(positionStr)
time1=time.time()
for ticker in csv_reader:
    if ticker[0]=="Ticker":
        continue
    print(ticker[0])
    if "1" in ft:
        #dl.get(filing_type[0], ticker[0], after_date=date1, before_date=date2)
        command = f"python scraper.py {ticker[0]} {filing_type[0]}"
        p1= subprocess.Popen(command, shell=False, stdout=subprocess.PIPE,creationflags=DETACHED_PROCESS)#HIGH_PRIORITY_CLASS)
    if "2" in ft:   
        #dl.get(filing_type[1], ticker[0], after_date=date1, before_date=date2)
        command = f"python scraper.py {ticker[0]} {filing_type[1]}"
        p2= subprocess.Popen(command, shell=False, stdout=subprocess.PIPE,creationflags=DETACHED_PROCESS)#HIGH_PRIORITY_CLASS)
    if "3" in ft:
        #dl.get(filing_type[2], ticker[0], after_date=date1, before_date=date2)
        command = f"python scraper.py {ticker[0]} {filing_type[2]}"
        p3= subprocess.Popen(command, shell=False, stdout=subprocess.PIPE,creationflags=DETACHED_PROCESS)#HIGH_PRIORITY_CLASS)
    if "1" in ft:
        p1.wait()
    if "2" in ft:
        p2.wait()
    if "3" in ft:    
        p3.wait()
    
    curr=curr+1


    if curr==num:
        print("Time taken for analysis: ",time.time()-time1)
        break
    positionStr = 'Current ticker: ' + str(curr).rjust(5) + '     Total tickers: ' + str(total).rjust(6)
    print(positionStr)








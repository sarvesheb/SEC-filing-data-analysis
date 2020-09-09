from sec_edgar_downloader import Downloader
import os
import bs4
import re
import sys
import csv
import mmap
from filelock import Timeout, FileLock
file_path = "data.csv"
lock_path = "data.csv.lock"

sys.setrecursionlimit(10000)


ticker=sys.argv[1]
ticker=ticker.replace("['","")
ticker=ticker.replace("']","")

filing=sys.argv[2]
all_words=[]

folder=f"sec_edgar_filings/{ticker}/{filing}"
all_files=os.listdir(folder)


with open('words.txt', mode='r') as word_file:
    word_file.seek(0)
    csv_reader = csv.reader(word_file)
    for word in csv_reader:
        all_words.append(str(word[0].lower()))
    word_file.close()




file_names= [a[6:len(a)-4] for a in os.listdir() if a.startswith("group") and a.endswith(".txt")]
all_groups={}

for name2 in file_names:
    with open(f'group_{name2}.txt', mode='r') as word_file:
        word_file.seek(0)
        csv_reader = csv.reader(word_file)
        temp_list=[]
        for word in csv_reader:
            temp_list.append(str(word[0].lower()))
        all_groups[f"{name2}"]= temp_list
        word_file.close()




def write_csv(ticker,filing,date,word,word_count,total_words,cik,sic,sic_code):
    # found=0

    with FileLock(lock_path, timeout=100):
        with open('data.csv', mode='a+', newline='') as data_file:
                data_writer = csv.writer(data_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)                
                data_writer.writerow([ticker, filing, date, my_word, word_count,total_words,cik,sic,sic_code])
                data_file.close()

def write_grouping(ticker,filing,date,filename,values2,total_words):
    group_path=f"{folder}/{filename}"
    file_size=round((os.stat(group_path).st_size)/(1024**2) , 3)
    
    combined_list=[]
    #print(values2,"ahahhah")
    for y in values2.keys():
        combined_list.append(values2[y])
        

    total_param=[ticker, filing,date,file_size]+combined_list+[total_words]

    with FileLock("grouped_data.lock", timeout=100):
        with open('grouped_data.csv', mode='a+', newline='') as data_file:
                data_writer = csv.writer(data_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)                
                data_writer.writerow(total_param)
                data_file.close()
word_dict={}
for file1 in all_files:


    with open(f"{folder}/{file1}", 'rb', 0) as file, \
        mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as s:
        num=s.find(b'</DOCUMENT>')
        html_report_part1=s[0:num+12].decode()


    with open(f"{folder}/{file1}", mode='a+') as g:
        g.truncate(num+12)

    #html_report_part1 = open(f"{folder}/{file}",'r').read()

    #html_report_part1=html_report_part1.decode('utf-8')
    soup = bs4.BeautifulSoup(html_report_part1, 'lxml')

    doc=soup.find("document")

    date1=soup.find("acceptance-datetime")
    date1=date1.string.encode('utf-8')
    pos1=date1.find(b"FILED AS OF DATE:")
    pos2=date1.find(b"\n",pos1)
    final_date=date1[pos1:pos2]
    r=final_date.find(b"\t")
    final_date=final_date[r+2:]
    final_date=final_date.decode('utf-8')
    final_date=final_date[0:4]+'-'+final_date[4:6]+'-'+final_date[6:8]

    mangled=soup.find("acceptance-datetime")
    mangled=mangled.string.encode('utf-8')
    cik_pos1=mangled.find(b"CENTRAL INDEX KEY:")
    cik_pos2=mangled.find(b"\n",cik_pos1)
    cik=mangled[cik_pos1:cik_pos2]
    r=cik.find(b"\t")
    cik=cik[r+2:].decode('utf-8')
    cik=cik.strip()

    mangled1=soup.find("acceptance-datetime")
    mangled1=mangled1.string.encode('utf-8')
    sic_pos1=mangled1.find(b"STANDARD INDUSTRIAL CLASSIFICATION:")
    sic_pos2=mangled1.find(b"\n",sic_pos1)
    sic=mangled1[sic_pos1:sic_pos2]
    r=sic.find(b"\t")
    sic_code=sic[sic.find(b"[") +1 : sic.find(b"]")]
    sic_code=sic_code.decode('utf-8').replace(" ","")
    sic=sic[r+1:sic.find(b'[')].decode('utf-8')
    sic=sic.strip()

    
    word_count=0
    total_words=0
    
    text2=doc.get_text()
    text3=text2
    text2=text2.encode('utf-8')
    text2=(text2.replace(b"\n",b" ")).lower()
    text2=(text2.replace(b"  ",b" "))
    total_words=total_words+len(text2.split())


    for my_word in all_words:
        try:
            word_dict[my_word]=word_dict[my_word]+text2.count(b"%b"%my_word.encode())  
        except:
            word_dict[my_word]=text2.count(b"%b"%my_word.encode())  
    temp_group_dict={}
    i=0
    while i<len(text3)-1:
        if text3[i].islower() and text3[i+1].isupper():
            text3=text3[0:i+1]+" "+text3[i+1:len(text3)]
        i=i+1
    text3=text3.lower()
    for groups in all_groups.keys():
        total2=0

        for group_words in all_groups[groups]:
            cur=0
            while 1==1:
                cur=text3.find(group_words,cur)
                if cur==-1:
                    break
                if text3[cur-1].isalpha() or text3[cur+len(group_words)].isalpha():
                    #print(text3[cur-1],text3[cur+len(group_words)])
                    #print(text3[cur-30:cur+30])
                    cur=cur+1
                    continue
                #print(text3[cur-20:cur],text3[cur+len(group_words):cur+len(group_words)+20])
                #print(text3[cur-30:cur+30])
                cur=cur+1
                total2=total2+1
                

        temp_group_dict.update({groups:total2})
        
        
        
    write_grouping(ticker,filing,final_date,file1,temp_group_dict,total_words)

        
    for my_word in all_words:
        write_csv(ticker, filing, final_date, my_word, word_dict[my_word],total_words,cik,sic,sic_code)
    word_dict.clear()


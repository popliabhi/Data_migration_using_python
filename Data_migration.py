import csv
import MySQLdb
import os

mydb = MySQLdb.connect(host='localhost',   
    user='root',
    passwd='#Patran92',
    db='mydb')

cursor = mydb.cursor()
base_path= '/Users/abhipopli/dump_db'
dump_files=os.listdir(base_path)


for file in dump_files:
  
    if file.split('.')[1] == 'csv':
        
        print (file)
        with open(base_path+file,'r') as csvfile:
            csvreader=csv.reader(csvfile,delimiter=',')
            print(csvreader)
            next(csvreader)
            for row in csvreader:
                if row != []:
                    try:
                        row[0] = int(row[0])
                    except:
                        continue
                    cursor.execute('INSERT INTO applicationurl(appid,appurl)'\
                                   'VALUES("%s","%s")', row)
mydb.commit()
cursor.close()
print ("Done")

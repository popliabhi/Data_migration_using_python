import csv, json
import MySQLdb
import os
import ast



def dataType(val, current_type):
    try:
        # Evaluates numbers to an appropriate type, and strings an error
        t = ast.literal_eval(val)
    except ValueError:
        return 'varchar'
    except SyntaxError:
        return 'varchar'
    if type(t) in [int, float]:
        if (type(t) in [int]) and current_type not in ['float', 'varchar']:
           # Use smallest possible int type
            if (-32768 < t < 32767) and current_type not in ['int', 'bigint']:
                return 'smallint'
            elif (-2147483648 < t < 2147483647) and current_type not in ['bigint']:
                return 'int'
            else:
                return 'bigint'
        if type(t) is float and current_type not in ['varchar']:
            return 'decimal'
    else:
        return 'varchar'




mydb = MySQLdb.connect(host='localhost',   
    user='root',
    passwd='***',
    db='db')
cursor = mydb.cursor()
base_path= '***'
dump_files=os.listdir(base_path)


for file in dump_files:
      if file.split('.')[1] == 'csv':
        with open(base_path+'/'+file,'r') as csvfile:
            longest, headers, type_list, elements =[], [], [],[]
            csvreader=csv.reader(csvfile,delimiter=',')
            for row in csvreader:
                if len(headers) == 0:
                    headers = row
                    for col in row:
                        longest.append(0)
                        type_list.append('')
                else:
                    for i in range(len(row)):
                    # NA is the csv null value
                        if type_list[i] == 'varchar' or row[i] == 'NA':
                            pass
                        else:
                            var_type = dataType(row[i], type_list[i]) 
                            type_list[i] = var_type
                        
                        if len(row[i]) > longest[i]:
                            longest[i] = len(row[i])
                
                
                if(row != headers):
                         if row!="":
                            try:
                                row[0] = int(row[0])
                            except:
                                 continue                             
                            
                            if "'" in str(row):

                                row=(str(row).replace("'","\""))
                                row=eval(json.dumps(row))
                                print(type(row))
                                elements.append(row)
                            else:
                                elements.append(row)

            #Create Table
            sql_create = "CREATE TABLE " + file.split('.')[0]+ "("
            for i in range(len(headers)):
                if type_list[i] == 'varchar':
                    sql_create = (sql_create + '\n{} varchar({}),').format(headers[i].lower(), str(longest[i]))
                else:
                    sql_create = (sql_create + '\n' + '{} {}' + ',').format(headers[i].lower(), type_list[i])
                
            sql_create = sql_create[:-1] + ");"
            print(sql_create)
            cursor.execute(sql_create) 
            

            s_count =""
            for i in range(len(headers)):
                s_count+="%s"
            # print(s_count)
            # print(file.split(".")[0])
            # print(headers)
            str_header = ",".join(x for x in headers)
            # print (str_header)
            # print(elements)

            #Insert Values into tables
            sql_insert="Insert into "+ file.split(".")[0] +"("+str_header+")values("+s_count+");"
            print(sql_insert)
            cursor.executemany(sql_insert,elements)
            
            
mydb.commit()
cursor.close()
print ("Done")

import csv
import MySQLdb
import os

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
    passwd='####',
    db='db')

cursor = mydb.cursor()
base_path= 'Path_to_directory'
dump_files=os.listdir(base_path)


for file in dump_files:
      if file.split('.')[1] == 'csv':
        with open(base_path+'/'+file,'r') as csvfile:
            longest, headers, type_list =[],[],[]
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
            
            sql = "CREATE TABLE " + file.split('.')[0]+ "("
            for i in range(len(headers)):
                if type_list[i] == 'varchar':
                    sql = (sql + '\n{} varchar({}),').format(headers[i].lower(), str(longest[i]))
                else:
                    sql = (sql + '\n' + '{} {}' + ',').format(headers[i].lower(), type_list[i])

            sql = sql[:-1] + ");"
            print(sql)
                 
            cursor.execute(sql)
mydb.commit()
cursor.close()
print ("Done")
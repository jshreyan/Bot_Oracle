import cx_Oracle
import os
import time
from configparser import ConfigParser

CONFIG = ConfigParser()
CONFIG.read('config.ini')

input('Press Enter to Start Extracting..')

os.environ.update([('NLS_LANG', '.UTF8'),('ORA_NCHAR_LITERAL_REPLACE', 'TRUE'),])
timenow = time.strftime('%Y-%m-%d_%H%M%S')
start = time.time()
print('Start:',timenow,'\n')

INSTANCE = CONFIG.get('DETAILS', 'INSTANCENAME') #'HR'
CONNECTSTRING = CONFIG.get('DETAILS', 'CONNECTSTRING') #'hr/hr@LOCALHOST:1521/xe'
OUTPUT_LOCATION = CONFIG.get('DETAILS', 'OUTPUT_LOCATION')+'\\'+INSTANCE #C:\\Users\\ADMIN\\Desktop\\FILES\\
querystring = CONFIG.get('DETAILS', 'QUERY')
#QUERY = SELECT object_name, object_type, DBMS_METADATA.get_ddl (CASE  WHEN object_type = 'PACKAGE' THEN 'PACKAGE_SPEC' WHEN object_type = 'PACKAGE BODY' THEN 'PACKAGE_BODY' WHEN object_type = 'MATERIALIZED VIEW' THEN 'MATERIALIZED_VIEW' WHEN object_type IN ('JOB', 'PROGRAM', 'SCHEDULE') THEN 'PROCOBJ' ELSE object_type END, object_name,owner) AS source  FROM all_objects u WHERE OBJECT_NAME LIKE 'A%%' AND OBJECT_NAME NOT IN ('XYZ') and owner='HR'
dirOUT = OUTPUT_LOCATION

def connect_oracle(CONNECTSTRING):
    print('Connecting to Oracle=> '+CONNECTSTRING)
    connection = cx_Oracle.connect(CONNECTSTRING)
    cursor = connection.cursor()
    print('Connected to Oracle.\n')
    print('OUTPUT FOLDER:',dirOUT)
    return cursor,connection
    
def export_objects(cursor,querystring):
    p_out_txt_or = cursor.var(str)
    print('Executing Query..')
    cursor.execute(querystring)
    print('Query Executed.')
    print('Extraction in progress..\n')
    for objname, objtype, sourcecode in cursor.fetchall():
        print('\n>> '+objname, "::", objtype)
        save_object(objname,objtype,sourcecode.read())

def save_object(objname,objtype,sourcecode):
    new_folder_dir = dirOUT+"\\FILES_"+timenow+"\\"
    try:
        os.makedirs(new_folder_dir)
    except:
        None
    ext = find_extention(objtype)
    new_file_name = new_folder_dir+objname+ext
    newFile = open(new_file_name, 'w')
    newFile.write(sourcecode)
    newFile.close()
    print('   New file created => '+new_file_name) 
    
def find_extention(objtype):
    if objtype == 'PACKAGE':
       ext = '.pks'
    elif objtype == 'PACKAGE BODY':
       ext = '.pkb'
    elif objtype == 'PROCEDURE':
       ext = '.prc'
    elif objtype == 'FUNCTION':
       ext = '.fnc'
    elif objtype == 'TRIGGER':
       ext = '.trg'
    elif objtype == 'TABLE':
       ext = '.tbl'
    elif objtype == 'TYPE':
       ext = '.typ'
    elif objtype == 'INDEX':
       ext = '.idx'
    else:
       ext = '.sql'
    #print('ext:',ext)
    return ext

def process_files():
    cursor,connection = connect_oracle(CONNECTSTRING)
    print("<====================================================>\n")
    p_out_txt = export_objects(cursor,querystring)
    cursor.close()
    connection.close()

process_files()

print("<====================================================>")
end = time.time()
print('\nTime taken:',round(end - start,2), 'Seconds')
print('\nEnd of Script.')

input('\nPress Enter to Exit..')

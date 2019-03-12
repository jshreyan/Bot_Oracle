import cx_Oracle
import os
import time
os.environ.update([('NLS_LANG', '.UTF8'),('ORA_NCHAR_LITERAL_REPLACE', 'TRUE'),])
timenow = time.strftime('%Y-%m-%d_%H%M%S')
start = time.time()
print('Start:',timenow)

INSTANCE = 'MPESA'
#CONNECTSTRING = 'system/Md!!@LOCALHOST:1521/xe'
#CONNECTSTRING = 'apps/apps@DISHA_CRMUAT'
#CONNECTSTRING = 'apps/EMZCI9#B@10.94.142.76:1526/CRMdISHA'
CONNECTSTRING = 'apps/eymdt2#f@10.94.49.11:1526/CRMVMT'

dirOUT = 'C:\\Users\\IBM_ADMIN\\Desktop\\CRM\\'+INSTANCE

querystring = """SELECT object_name, object_type,  DBMS_METADATA.get_ddl (
       CASE  WHEN object_type = 'PACKAGE' THEN 'PACKAGE_SPEC'
             WHEN object_type = 'PACKAGE BODY' THEN 'PACKAGE_BODY'
             WHEN object_type = 'MATERIALIZED VIEW' THEN 'MATERIALIZED_VIEW'
             WHEN object_type IN ('JOB', 'PROGRAM', 'SCHEDULE') THEN 'PROCOBJ'
             ELSE object_type END, object_name,owner)  AS source
  FROM all_objects u
 WHERE     TRUNC (last_ddl_time) >= TO_DATE ('01-JAN-2012', 'DD-MON-YYYY')
       AND owner IN ('APPS', 'HUTCHCS')
       AND object_type IN ('PACKAGE','PACKAGE BODY','PROCEDURE','VIEW', 'TABLE','FUNCTION','SYNONYM','MATERIALIZED VIEW','JOB','SEQUENCE','TRIGGER','TYPE')
       AND (object_name LIKE 'XX%' OR object_name LIKE 'HUTCH%' OR object_name LIKE 'VEL%' OR object_name LIKE 'VF%')
       AND object_name NOT IN ('HUTCH_PURGE_COL_INFO')"""


def connect_oracle(CONNECTSTRING):
    print('Connecting to Oracle=> '+CONNECTSTRING)
    connection = cx_Oracle.connect(CONNECTSTRING)
    cursor = connection.cursor()
    print('Connected to Oracle.\n')
    return cursor
    
def export_objects(cursor,querystring):
    p_out_txt_or = cursor.var(str)
    print('Executing Query..')
    cursor.execute(querystring)
    print('Query Executed.\n')
    for objname, objtype, sourcecode in cursor.fetchall():
        print('\n',objname, "\t", objtype)
        save_object(objname,objtype,sourcecode.read())

def save_object(objname,objtype,sourcecode):
    new_folder_dir = dirOUT+"\\CRM_"+timenow+"\\"
    try:
        os.makedirs(new_folder_dir)
    except:
        None
    ext = find_extention(objtype)
    new_file_name = new_folder_dir+objname+ext
    newFile = open(new_file_name, 'w')
    newFile.write(sourcecode)
    newFile.close()
    print('New file created=> '+new_file_name) 
    
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
#    elif objtype == 'TABLE':
#       ext = '.tbl'
#    elif objtype == 'TYPE':
#       ext = '.typ'
#    elif objtype == 'INDEX':
#       ext = '.idx'
    else:
       ext = '.sql'
    #print('ext:',ext)
    return ext

def process_files():
    cursor = connect_oracle(CONNECTSTRING)
    print("<====================================================>\n")
    p_out_txt = export_objects(cursor,querystring)

process_files()


end = time.time()
print('\nTime taken:',end - start, 'Seconds')
print('\n\nEnd of Script.')

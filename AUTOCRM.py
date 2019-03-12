import cx_Oracle
import os
import time
os.environ.update([('NLS_LANG', '.UTF8'),('ORA_NCHAR_LITERAL_REPLACE', 'TRUE'),])
timenow = time.strftime('%Y-%m-%d_%H%M%S')

#CONNECTSTRING = 'system/Md!!@LOCALHOST:1521/xe'
CONNECTSTRING = 'apps/apps@DISHA_CRMUAT'

dirIN = 'C:\\Users\\IBM_ADMIN\\Desktop\\AD Final\\CRM\\IN'
dirOUT = 'C:\\Users\\IBM_ADMIN\\Desktop\\AD Final\\CRM\\OUT'

def connect_oracle(CONNECTSTRING):
    print('Connecting to Oracle=> '+CONNECTSTRING)
    connection = cx_Oracle.connect(CONNECTSTRING)
    cursor = connection.cursor()
    print('Connected to Oracle.\n')
    return cursor
    
def run_replace_plsql(module,cursor,p_in_txt):
    p_out_txt_or = cursor.var(str)
    cursor.callproc('replace_email_ids',[p_in_txt,module, p_out_txt_or])
    #print('Procedure Called.')
    return p_out_txt_or.getvalue()

def save_new_file(new_file_path,new_text):
    newFile = open(new_file_path, 'w')
    newFile.write(new_text)
    newFile.close()
    print('New file created=> '+new_file_path)

def process_files():
    cursor = connect_oracle(CONNECTSTRING)
    folders = os.listdir(dirIN)
    for folder in folders: 
        print("\n  <=========================="+folder+"==========================>\n")
        folderdir = dirIN+"\\"+folder
        files = os.listdir(folderdir)
        new_folder_dir = dirOUT+"\\CRM_"+timenow+"\\"+folder
        os.makedirs(new_folder_dir)
        for file in files:
            var_txt,var_file,p_out_txt = "","",""
            #print(folderdir+'\\'+file)
            var_file = open(folderdir+'\\'+file)
            var_txt = var_file.read()
            #print(var_txt)
            p_out_txt = run_replace_plsql(folder+'/'+file,cursor,var_txt)
            save_new_file(new_folder_dir+'\\'+file,p_out_txt)
            var_file.close()

process_files()

print('\n\nEnd of Script.')
 

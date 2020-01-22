from errbot import BotPlugin, botcmd
import sys
import teradatasql
import paramiko
from pprint import pprint #for use with query results


#connection information for tdata
hostn,usern,passw='192.168.170.128','dbc','dbc'  
#open ssh connection to set variables
userver=paramiko.SSHClient()
userver.set_missing_host_key_policy(paramiko.AutoAddPolicy())
userver.connect(hostname=hostn,username="root",password="root")

#Initialise Vars section

gcfr_work_dir_home='~/GCFR_lab/'
gcfr_ddl='~/GCFR_lab/GCFR_DDL_Code/'
gcfr_implementation_dir='~/GCFR_lab/GCFR_DDL_Code/GCFR_Implementation_Kit/'
stdin,stdout,stderr=userver.exec_command("echo `date +%Y``date +%m``date +%d`'_'`date +%H``date +%M``date +%S`")
dts=(stdout.read().decode('ascii').rstrip())#timestamp variable 
#target directory variables
target_dir=gcfr_work_dir_home+dts  
gcfr_ddl_target_executors=target_dir+'/GCFR_DDL_Code'
userver.close()
#close the onnection


#schema setup start
def gcfr_schema_init():
    global hostn,usern,passw
    sqlScript= """
CREATE DATABASE GCFR_MAIN FROM DBC
AS PERM=200e7
;

create  USER GCFR_USER FROM GCFR_MAIN
AS PASSWORD=ilikecats
   PERM=0
   -- TEMPORARY=1e9
   DEFAULT DATABASE=GCFR_MAIN
   NO FALLBACK
;
GRANT ALL ON GCFR_MAIN TO DBC;
GRANT EXECUTE PROCEDURE ON GCFR_MAIN TO DBC WITH GRANT OPTION;
GRANT UDTUSAGE ON SYSUDTLIB TO GCFR_MAIN WITH GRANT OPTION;
GRANT UDTUSAGE ON SYSUDTLIB TO GCFR_MAIN;
GRANT EXECUTE FUNCTION ON GCFR_MAIN TO GCFR_USER;
GRANT EXECUTE PROCEDURE ON GCFR_MAIN TO GCFR_MAIN WITH GRANT OPTION;
GRANT ALL ON GCFR_MAIN TO GCFR_USER;
grant select on DBC to GCFR_USER with grant option;
grant select on DBC to GCFR_MAIN with grant option;
grant select on SYS_CALENDAR to GCFR_USER with grant option; 
grant EXECUTE PROCEDURE on sqlj to GCFR_USER;
"""
    
    sqlScriptArray=sqlScript.split(';')    
    with teradatasql.connect(host=hostn,user=usern,password=passw) as sessionTD:
        with sessionTD.cursor() as tdCur:
            for x in sqlScriptArray:
                print(x.strip())
                results=[]
            for queryline in sqlScriptArray:
                print(queryline.strip())
                tdCur.execute(queryline.strip())
                results+=tdCur.fetchall()
            print(results)
        #schema setup function end   



#rollback function
def gcfr_schema_decommission(db_admin_user,parent_schema,schema_prefix):
    global hostn,usern,passw
    sqlScript= """
                            delete DATABASE """+schema_prefix+"""M_OPR;
                            drop database """+schema_prefix+"""M_OPR ;
                            delete database """+schema_prefix+"""T_OPR;
                            drop database """+schema_prefix+"""T_OPR;
                            delete database """+schema_prefix+"""V_OPR ;
                            drop database """+schema_prefix+"""V_OPR ;
                            delete database """+schema_prefix+"""_OPR; 
                            drop database """+schema_prefix+"""_OPR;
                            drop user """+schema_prefix+"""_ETL_USR;
                            delete DATABASE """+schema_prefix+"""V_SEM ;
                            drop DATABASE """+schema_prefix+"""V_SEM ;
                            delete DATABASE """+schema_prefix+"""T_SEM ;
                            drop DATABASE """+schema_prefix+"""T_SEM ;
                            delete DATABASE """+schema_prefix+"""_SEM ;
                            drop DATABASE """+schema_prefix+"""_SEM ;
                            delete DATABASE """+schema_prefix+"""V_OUT ;
                            drop DATABASE """+schema_prefix+"""V_OUT ;
                            delete DATABASE """+schema_prefix+"""V_INP ;
                            drop DATABASE """+schema_prefix+"""V_INP ;
                            delete DATABASE """+schema_prefix+"""_TXFM ;
                            drop DATABASE """+schema_prefix+"""_TXFM ;
                            delete DATABASE """+schema_prefix+"""V_UTLFW; 
                            drop DATABASE """+schema_prefix+"""V_UTLFW ;
                            delete DATABASE """+schema_prefix+"""T_UTLFW ;
                            drop DATABASE """+schema_prefix+"""T_UTLFW ;
                            delete DATABASE """+schema_prefix+"""_UTLFW; 
                            drop DATABASE """+schema_prefix+"""_UTLFW ;
                            delete database """+schema_prefix+"""V_SRCI ;
                            drop database """+schema_prefix+"""V_SRCI ;
                            delete DATABASE """+schema_prefix+"""T_SRCI ;
                            drop DATABASE """+schema_prefix+"""T_SRCI ;
                            delete DATABASE """+schema_prefix+"""_SRCI ;
                            drop DATABASE """+schema_prefix+"""_SRCI ;
                            delete DATABASE """+schema_prefix+"""V_STG ;
                            drop DATABASE """+schema_prefix+"""V_STG ;
                            delete DATABASE """+schema_prefix+"""T_STG ;
                            drop DATABASE """+schema_prefix+"""T_STG ;
                            delete DATABASE """+schema_prefix+"""_STG ;
                            drop DATABASE """+schema_prefix+"""_STG ;
                            delete DATABASE """+schema_prefix+"""T_WRK ;
                            drop DATABASE """+schema_prefix+"""T_WRK ;
                            delete DATABASE """+schema_prefix+"""T_TMP ;
                            drop DATABASE """+schema_prefix+"""T_TMP ;
                            delete DATABASE """+schema_prefix+"""_TMP ;
                            drop DATABASE """+schema_prefix+"""_TMP;
                            delete DATABASE """+schema_prefix+"""P_PP ;
                            drop DATABASE """+schema_prefix+"""P_PP ;
                            delete DATABASE """+schema_prefix+"""P_CP ;
                            drop DATABASE """+schema_prefix+"""P_CP ;
                            delete DATABASE """+schema_prefix+"""P_FF ;
                            drop DATABASE """+schema_prefix+"""P_FF ;
                            delete DATABASE """+schema_prefix+"""P_UT ;
                            drop DATABASE """+schema_prefix+"""P_UT ;
                            delete DATABASE """+schema_prefix+"""P_BB ;
                            drop DATABASE """+schema_prefix+"""P_BB ;
                            delete DATABASE """+schema_prefix+"""P_API ;
                            drop DATABASE """+schema_prefix+"""P_API ;
                            delete DATABASE """+schema_prefix+"""M_GCFR ;
                            drop DATABASE """+schema_prefix+"""M_GCFR ;
                            delete DATABASE """+schema_prefix+"""V_GCFR ;
                            drop DATABASE """+schema_prefix+"""V_GCFR ;
                            delete DATABASE """+schema_prefix+"""T_GCFR ;
                            drop DATABASE """+schema_prefix+"""T_GCFR ;
                            delete DATABASE """+schema_prefix+"""_GCFR ;
                            drop DATABASE """+schema_prefix+"""_GCFR ;
                            delete DATABASE """+schema_prefix+"""V_BASE ;
                            drop DATABASE """+schema_prefix+"""V_BASE ;
                            delete DATABASE """+schema_prefix+"""T_BASE ;
                            drop DATABASE """+schema_prefix+"""T_BASE ;
                            delete DATABASE """+schema_prefix+"""_BASE ;
                            drop DATABASE """+schema_prefix+"""_BASE ;
                            delete database """+parent_schema+""";
                            delete user """+db_admin_user+""";
                            drop user """+db_admin_user+""";
                            drop database """+parent_schema+""";
"""
    
    sqlScriptArray=sqlScript.split(';')    
    with teradatasql.connect(host=hostn,user=usern,password=passw) as sessionTD:
        with sessionTD.cursor() as tdCur:
         for x in sqlScriptArray:
            print(x.strip())
            results=[]
         for queryline in sqlScriptArray:
            print(queryline.strip())
            tdCur.execute(queryline.strip())
            results+=tdCur.fetchall()
         pprint(results)
 #rollback function end   



    
    

def gcfr_token_replace(db_admin_user,parent_schema,schema_prefix):
    print(db_admin_user)
    print(parent_schema)
    print(schema_prefix)
    userver=paramiko.SSHClient()
    userver.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    userver.connect(hostname=hostn,username="root",password="root")
    stdin,stdout,stderr=userver.exec_command ("""
                                              cp """+gcfr_ddl+"""GCFR_Implementation_Kit/Token_replacement_values.tokens """+gcfr_ddl+"""GCFR_Implementation_Kit/Token_replacement_values.tkn
                                              sed -i 's/000_ADMINUSER_000/"""+db_admin_user+"""/g' """+gcfr_ddl+"""GCFR_Implementation_Kit/Token_replacement_values.tkn
                                              sed -i 's/000_PARENTSCHEMA_000/"""+parent_schema+"""/g' """+gcfr_ddl+"""GCFR_Implementation_Kit/Token_replacement_values.tkn
                                              sed -i 's/000_SCHEMAPREFIX_000/"""+schema_prefix+"""/g' """+gcfr_ddl+"""GCFR_Implementation_Kit/Token_replacement_values.tkn
                                              
                                              """
                                              )
    while not stderr.channel.exit_status_ready():
        print('Please wait...')
        userverOut=stdout.read().decode('ascii').rstrip()
        print(userverOut)
        userver.close()



#gcfr_directory_deploy function start.  This creates the file structure and copies files.  
def gcfr_directory_deploy():
    global target_dir, gcfr_implementation_dir, gcfr_ddl, gcfr_ddl_target_executors,gcfr_work_dir_home,dts
    userver=paramiko.SSHClient()
    userver.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    userver.connect(hostname=hostn,username="root",password="root")

    stdin,stdout,stderr=userver.exec_command ("""
                                             mkdir """+target_dir+"""
                                             cd """ +gcfr_implementation_dir+"""
                                             dos2unix *
                                             pwd
                                             ls -ltr
                                             echo 'Copying directory structure into runtime folder' """+target_dir+
                                             """
                                             cp -r """+gcfr_ddl+""" """+target_dir+
                                             """
                                             cd """+gcfr_ddl_target_executors+"""
                                             #Creating log folder.
                                             if [ -d " """+gcfr_ddl_target_executors+"""/GCFR_Implementation_Kit/log" ]
                                             then
                                                 echo "Log folder already exists. Skipping creation..."
                                             else
                                                 mkdir """+gcfr_ddl_target_executors+"""/GCFR_Implementation_Kit/log
                                             fi
                                             sed -e 's/\\\/\//g' """+gcfr_ddl_target_executors+"""/GCFR_Implementation_Kit/cvs_exclusion.txt > """+gcfr_ddl_target_executors+"""/GCFR_Implementation_Kit/cvs_exclusion2.txt
                                             while read line   
                                             do
                                             rm """+gcfr_ddl_target_executors+"""/GCFR_Implementation_Kit/$line 2> /dev/null
                                             done < """+gcfr_ddl_target_executors+"""/GCFR_Implementation_Kit/cvs_exclusion2.txt
                                             rm """+gcfr_ddl_target_executors+"""/GCFR_Implementation_Kit/cvs_exclusion2.txt 
                                             
                                             
                                             #token replacement stage
                                             pwd
                                             echo Replacing tokens in all files ...
                                             for fname in $(find -mindepth 0 -name '*.bteq' | grep '.' | sed -e s/\.//); 
                                              do
                                                echo $fname
                                                echo "File is: """+gcfr_ddl_target_executors+"""$fname"
                                                sed -f """+gcfr_ddl+"""GCFR_Implementation_Kit/Token_replacement_values.tkn  """+gcfr_ddl_target_executors+""""$fname" > """+gcfr_ddl_target_executors+""""$fname"2 
                                             	mv """+gcfr_ddl_target_executors+""""$fname"2 """+gcfr_ddl_target_executors+""""$fname"
                                              done
                                             
                                              for fname in $(find -mindepth 0 -name '*.ddl' | grep '.' | sed -e s/\.//); 
                                              do
                                                echo $fname
                                                echo "File is: """+gcfr_ddl_target_executors+"""$fname"
                                                sed -f """+gcfr_ddl+"""GCFR_Implementation_Kit/Token_replacement_values.tkn  """+gcfr_ddl_target_executors+""""$fname" > """+gcfr_ddl_target_executors+""""$fname"2 
                                             	mv """+gcfr_ddl_target_executors+""""$fname"2 """+gcfr_ddl_target_executors+""""$fname"
                                              done
                                                
                                              for fname in $(find -mindepth 0 -name '*.DDL' | grep '.' | sed -e s/\.//); 
                                              do
                                                echo $fname
                                                echo "File is: """+gcfr_ddl_target_executors+"""$fname"
                                                sed -f """+gcfr_ddl+"""GCFR_Implementation_Kit/Token_replacement_values.tkn  """+gcfr_ddl_target_executors+""""$fname" > """+gcfr_ddl_target_executors+""""$fname"2 
                                             	mv """+gcfr_ddl_target_executors+""""$fname"2 """+gcfr_ddl_target_executors+""""$fname"
                                              done
                                              
                                              for fname in $(find -mindepth 0 -name '*.sh' | grep '.' | sed -e s/\.//); 
                                              do
                                                echo $fname
                                                echo "File is: """+gcfr_ddl_target_executors+"""$fname"
                                                sed -f """+gcfr_ddl+"""GCFR_Implementation_Kit/Token_replacement_values.tkn  """+gcfr_ddl_target_executors+""""$fname" > """+gcfr_ddl_target_executors+""""$fname"2 
                                             	mv """+gcfr_ddl_target_executors+""""$fname"2 """+gcfr_ddl_target_executors+""""$fname"
                                              done
                                              
                                              for fname in $(find -mindepth 0 -name '*.cpp' | grep '.' | sed -e s/\.//); 
                                              do
                                                echo $fname
                                                echo "File is: """+gcfr_ddl_target_executors+"""$fname"
                                                sed -f """+gcfr_ddl+"""GCFR_Implementation_Kit/Token_replacement_values.tkn  """+gcfr_ddl_target_executors+""""$fname" > """+gcfr_ddl_target_executors+""""$fname"2 
                                             	mv """+gcfr_ddl_target_executors+""""$fname"2 """+gcfr_ddl_target_executors+""""$fname"
                                              done
                                              
                                              echo Token Replacement Done.
                                             """
                                             )
    #THIS IS Not te proper way to use exit status .
    while not stderr.channel.exit_status_ready():
        print('Please wait...')
        userverOut=stdout.read().decode('ascii').rstrip()
        print(userverOut)
        userver.close()


#gcfr_directory_deploy function end


def gcfr_bteq_looper():
    global target_dir, gcfr_implementation_dir, gcfr_ddl, gcfr_ddl_target_executors,gcfr_work_dir_home,dts

    bteq_list=['Step_01_CreateDatabases_v0.1.bteq'
              ,'Step_02_CreateGCFRTables_v0.1.bteq'
              ,'Step_03_AssignPermissions_v0.1.bteq'
              ,'Step_04_CreateGCFRViews_v0.1.bteq'
              ,'Step_05_CreateUTLFWTables_v0.1.bteq'
              ,'Step_06_CreateUTLFWViews_v0.1.bteq'
              ,'Step_07_CreateGCFRMacros_v0.1.bteq'
              ,'Step_08_RegisterLookupValues_v0.1.bteq'
              ,'runsetupJAVA_XSP_option.sh'
              ,'Step_09_CreateGCFR_BuildBlock_Procs_v0.1.bteq'
              ,'runsetupException_Support_For_Unicode.sh'
              ,'Step_10_CreateGCFR_Util_Procs_v0.1.bteq'
              ,'Step_11_CreateGCFR_FuncFlow_Procs_v0.1.bteq'
              ,'Step_12_CreateGCFR_CtrlPattern_Procs_v0.1.bteq'
              ,'Step_13_CreateGCFR_ProcPattern_Procs_v0.1.bteq']
    
    userver=paramiko.SSHClient()
    userver.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    userver.connect(hostname=hostn,username="root",password="root")

    for sub_command in bteq_list:
        if sub_command.find('bteq') >= 0:
            command_full="Logon_and_Run_BTEQ_Script.sh "+sub_command
        else:
            command_full=sub_command
        print('Executing '+command_full+'...')
        stdin,stdout,stderr=userver.exec_command("""
                                                 cd """+gcfr_ddl_target_executors+"""/GCFR_Implementation_Kit/
                                                 sh """+command_full+"""
                                                 RC=$?
                                                 if [ $RC != 0 ]
                                                     then
                                                         {
                                                         echo 'PyGCFR catcher'
                                                         echo 'Please check the log in """+gcfr_ddl_target_executors+"""/GCFR_Implementation_Kit/log'
                                                         }
                                                 fi
                                                 """)
        while not stderr.channel.exit_status_ready():
            print('Please wait...')
            userverOut=stdout.read().decode('ascii').rstrip()
            print(userverOut)
            if userverOut.find('PyGCFR catcher') >= 0:
                print('Fail state')
                userver.close()
                sys.exit('abnormal termination')
    userver.close()


    
def gcfr_install():
    gcfr_directory_deploy()
    gcfr_schema_init()                
    gcfr_token_replace('dbc','GCFR_MAIN','GPRD1')
    gcfr_bteq_looper() 

def gcfr_t1_wrapper():
    userver=paramiko.SSHClient()
    userver.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    userver.connect(hostname=hostn,username="root",password="root")
    stdin,stdout,stderr=userver.exec_command("""
    sh GCFR_Wrapper2.sh
    bteq<tier1_deploy.sh                              
    """)
    userverOut=stdout.read().decode('ascii').rstrip()
    print(userverOut)
    userver.close()



class GCFR_Pythonised(BotPlugin):
    """
    Pythonised GCR installer, up to tier 1 for now.  
    """

    @botcmd  # flags a command
    def gcfr_go(self, msg, args):  # a command callable with !tryme
        """
        GCFR Installer
        """
        gcfr_install()
        return 'GCFR *is installed* !'  # This string format is markdown.

    @botcmd  # flags a command
    def gcfr_T1(self, msg, args):  # a command callable with !tryme
        """
        GCFR Installer
        """
        gcfr_t1_wrapper()
        return 'GCFR tier 1 metadata *is installed* !'  # This string format is markdown.

    

    
    @botcmd  # flags a command
    def gcfr_gone(self, msg, args):  # a command callable with !tryme
        """
        GCFR UnInstaller
        """
        gcfr_schema_decommission('GCFR_USER','GCFR_MAIN','GPRD1')
        return 'It  is *gone* !'  # This string format is markdown.

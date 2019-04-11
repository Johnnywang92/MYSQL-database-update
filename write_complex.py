    #!/usr/bin/env python
from __future__ import print_function
import click
import os
import sys
import MySQLdb
import pandas as pd
import math
import sys
reload(sys)  
sys.setdefaultencoding('utf8')

def mysql_conn(ip,user,passwd,db,port):
    try:
        conn = MySQLdb.connect(host=ip,user=user,
               passwd=passwd,db=db,port=port,charset='utf8')
        cur = conn.cursor()
        return conn,cur
    except MySQLdb.Error,e:
        print(e.args)
        sys.exit(1)

def insert_db(conn,cursor,command):
    try:
        cursor.execute(command)
        conn.commit()
    except MySQLdb.Error,e:
        print(e.args)
        sys.exit(1)

def read_data(fpath):
    if fpath.endswith('xlsx'):
        df = pd.read_excel(fpath)
    else:
        df = pd.read_csv(fpath, sep='\t')        
    return df

@click.command()
@click.argument('file1')
@click.option('--posindex','-idx')
def write_kinae(file1,posindex):
    conn,cur = mysql_conn("ip","user","passwd","db",port)    ###modify by user
    cursor = conn.cursor()
    database_name = file1.strip().split('/')[-1].split('.')[0]
    try:
        command = "drop table %s;"%(database_name)
        cursor.execute(command)
        conn.commit()
    except:
        pass
    usedb = {}
    with open(file1) as f:
        heads = f.readline().strip().split('\t')
        heads = [i.strip() for i in heads] ##delate the sapce in heads
	dicb = {}
        for he in heads:
            dicb[he] = 'varchar(25)'
        dicb['chr'] = 'Text'
        dicb['Start'] = 'INT'
        dicb['Stop'] = 'INT'
        dicb['Domain_related_disease_1'] = 'Text'
	dicb['Domain_related_disease_2'] = 'Text'
	dicb['Onset_age_02'] = 'Text'
	dicb['Info'] = 'Text'
	dicb['Description'] = 'Text'
	dicb['Hotspot'] = 'Text'
	dicb['Onset_age_01'] = 'Text'
	dicb['Domain'] = 'Text'
	#dicb['gene'] = 'varchar(100)'
        #dicb['ranges'] = 'varchar(100)'
        #dicb['varflag'] = 'varchar(100)'
        #dicb['varflag'] = 'varchar(100)'
        command = "create table %s(searchpos varchar(100) NOT NULL,"%(database_name)
        for he in heads:
            rowname = '%s %s NOT NULL,'%(he,dicb[he])
            command = command + rowname
        command = command.strip(',') + ');'
        cursor.execute(command)
        conn.commit()
        insertlist = []
        n = 0
        left_file = open('leftfile.txt','w')
        for line in f:
            rows = line.strip('\n').split('\t')
            try:
                rows[1] = int(rows[1])
                rows[2] = int(rows[2])
            except:
                continue
            for pos in range(rows[1],rows[2]+1):
                search_key = '_'.join([rows[0],str(pos)])
                out = tuple([search_key] + rows)
                insertlist.append(str(out))
                if len(insertlist) == 8000:
                    n+=1
                    print(n)
                    command = "insert into %s values%s"%(database_name,','.join(insertlist))
                    try: 
                        insert_db(conn,cur,command)
                    except:
                        sys.exit(1)
                    insertlist = []
    if insertlist:
        command = "insert into %s values%s"%(database_name,','.join(insertlist))
        try: 
            insert_db(conn,cur,command)
        except:
            sys.exit(1)
    command = "alter table %s add index posindex(searchpos);"%(database_name)
    cursor.execute(command)
    conn.commit()     

    conn.close()
    cur.close()
    left_file.close()


if __name__ == '__main__':
    write_kinae()

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
    with open(file1) as f:
        heads = f.readline().strip().split('\t')
        dicb = {}
        for he in heads:
            dicb[he] = 'varchar(25)'
        dicb['POS'] = 'INT'
        dicb['REF'] = 'varchar(300)'
        dicb['ALT'] = 'varchar(300)'
        command = "create table %s(searchpos varchar(600) NOT NULL ,"%(database_name)
        for he in heads:
            rowname = '%s %s NOT NULL,'%(he,dicb[he])
            command = command + rowname
        command = command + 'CONSTRAINT %s PRIMARY KEY(searchpos));'%(database_name)
        cursor.execute(command)
        conn.commit()
        insertlist = []
        n = 0
        left_file = open('leftfile.txt','w')
        for line in f:
            if 'POS' in line:
                continue
            rows = line.strip('\n').split('\t')
            if len(rows[2]) > 300 or len(rows[3])> 300:
                print(len(rows[2]),len(rows[3]))
                print('\t'.join(rows),file=left_file)
                continue
            if rows[1] == 'POS':
                continue
            rows[1] = int(rows[1])
            search_key = '_'.join([rows[0],str(rows[1]),rows[2],rows[3]])
            out = tuple([search_key] + rows)
            if len(search_key) > 600:
                rows = [str(p) for p in rows]
                print('\t'.join(rows),file=left_file)
                continue
            insertlist.append(str(out))
            ####
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

    conn.close()
    cur.close()
    left_file.close()


if __name__ == '__main__':
    write_kinae()

#!/usr/bin/env python
#coding=utf-8
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
#    print (database_name)
    try:
        command = "drop table %s;"%(database_name)
        cursor.execute(command)
        conn.commit()
    except:
        pass

    f  = read_data(file1)
    heads = f.keys()
    heads = [i.strip().replace(' ','_') for i in heads]
    dicb = {}
    for he in heads:
        dicb[he] = 'varchar(300)'
    dicb['Disease_Chinese_Name'] = 'TEXT'
    dicb['others'] = 'TEXT'
    command = "create table %s("%(database_name)
    for he in heads:
        rowname = '%s %s NOT NULL,'%(he,dicb[he])
        command = command + rowname
    command = command.strip(',') + ');'
    cursor.execute(command)
    conn.commit()
    insertlist = []
    for i,rows in f.iterrows():
        rows = [str(il) for il in rows]
        insertlist.append(str(tuple(rows)))
        if len(insertlist) == 8000:
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


if __name__ == '__main__':
    write_kinae()
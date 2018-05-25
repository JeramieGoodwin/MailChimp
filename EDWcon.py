# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 15:59:59 2017
@author: jeramie.goodwin
"""
class EDWConnection(object):
    def __init__(self, PASSWORD=None, USERNAME=None,
                 SERVER=None, DB=None, CRED_PATH=None, FNAME=None):
        self.pwd = PASSWORD
        self.unm = USERNAME
        self.srvr = SERVER
        self.db = DB
        self.cred_path = CRED_PATH
        self.fname = FNAME

    def get_creds(self):
        """Extracts credentials from a given path and text file
            ARGS:
                cred_path := absolute file path to text file
                fnam := filename to be read"""
        try:
            with f as open(self.cred_path+self.fname, 'r'):
                cred_list = [line.strip() for line in f]
        except FileExistsError:
            print("File name: {} does not exist in directory. Please enter a\
                   valid file name").format(self.fname)
        return cred_list

    def connect(self, DRIVER='{SQL Server Native Client 11.0}'):
        """This is a module for making a connection to the EDW data warehouse.
        Requirements:
            user_name = SQL login name administered by SQL admin
            pwd = Users SQL password
        Returns a list:
            [connection,cursor,engine]
            """
        import pyodbc
        from sqlalchemy import create_engine

        # Connect to EDW
        if (PASSWORD == None & USERNAME == None & SERVER == None & DB == None):
            cred_list = self.get_creds()
            srvr = cred_list[0]
            db = cred_list[1]
            unm = cred_list[2]
            pwd = cred_list[3]
            drv = DRIVER
            # With pyodbc
            """  Connects to SQL Database """
            engine = create_engine('mssql+pyodbc://'+unm+':'+pwd+\
                                   '@'+srvr+'/'+db+'?driver='+drv)

            cnxn = pyodbc.connect('DRIVER='+DRIVER+';PORT=1433;SERVER='+srvr\
                                  +';PORT=1443;DATABASE='+db+';UID='\
                                  +unm+';PWD='+pwd)
            curs = cnxn.cursor()
        else:
            engine = create_engine('mssql+pyodbc://'+self.unm+':'+self.pwd+\
                                   '@'+self.srvr+'/'+self.db+'?driver='+drv)

            cnxn = pyodbc.connect('DRIVER='+DRIVER+';PORT=1433;SERVER='+self.srvr\
                                  +';PORT=1443;DATABASE='+self.db+';UID='\
                                  +self.unm+';PWD='+self.pwd)
            curs = cnxn.cursor()
        return [cnxn, curs, engine]

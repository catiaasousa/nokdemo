import os
import sqlite3
from flask import g, flash
import pandas as pd
import requests
import json
from nokdemo import app


class DB:
    def connect_db(self):
        """Connects to the specific database."""
        rv = sqlite3.connect(app.config['DATABASE'])
        rv.row_factory = sqlite3.Row
        return rv

    def get_db(self):
        """Opens a new database connection if there is none yet for the
        current application context.
        """
        if not hasattr(g, 'sqlite_db'):
            g.sqlite_db = self.connect_db()
        return g.sqlite_db

    def close_db(self, error):
        """Closes the database again at the end of the request."""
        if hasattr(g, 'sqlite_db'):
            g.sqlite_db.close()

    def init_db(self):
        db = self.get_db()
        with app.open_resource('schema.sql', mode='r') as f:
            db.cursor().executescript(f.read())
        db.commit()

    def query_db(self, query, args=(), one=False):
        cur = self.get_db().execute(query, args)
        rv = cur.fetchall()
        cur.close()
        return (rv[0] if rv else None) if one else rv


class CSV:
    def allowed_file(self, filename):
        return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

    def readCSV (self):
        csvFile = pd.read_csv(app.config['CSV'], usecols=app.config['CSV_COLS'], dtype=str)
        csvFile = csvFile.rename(columns={c: c.replace(' ', '') for c in csvFile.columns})
        return csvFile




class Data:
    def __init__(self, releasePart, cgr, mssID, mssName, dxCauseId, dxCauseDesc, topCGRGroups, totalCalls, spcName):
        self.releasePart = releasePart,
        self.cgr = cgr,
        self.mssID = mssID,
        self.mssName = mssName,
        self.dxCauseId = dxCauseId,
        self.dxCauseDesc = dxCauseDesc,
        self.topCGRGroups = topCGRGroups,
        self.totalCalls = totalCalls
        self.spcname = spcName


    @property
    def ReleasePart(self):
        return self.releasePart

    @ReleasePart.setter
    def ReleasePart(self):

        # Get traffic source from top Release Part
        sql =  'select ReleasePart, Count(ReleasePart) as c \
        from ' + app.config['SOURCE_TABLE'] + ' \
        group by ReleasePart \
        ORDER BY c DESC limit 1'

        self.releasePart = DB.query_db(self, sql, one=True)['ReleasePart']


    @property
    def circuit_group(self):
        return self.cgr
    
    @circuit_group.setter
    def circuit_group(self, releasepart):
        # Release Part = 0x1 --> ACGR
        # Release Part != 0x1 --> BCGR
        if releasepart == '0x1':
            self.cgr = 'ACGR'
        else:
            self.cgr = 'BCGR'
    

    @property
    def mssid(self):
        return self.mssID
        
    @mssid.setter
    def mssid(self,cgrid):
        # Get MSS from Release Part A/B CGR
        sql =  'select TNES, COUNT(*) as c \
                from ' + app.config['SOURCE_TABLE'] + ' \
                WHERE ' + cgrid + ' <> "" \
                group by TNES \
                ORDER BY c DESC LIMIT 1'

        self.mssID = DB.query_db(self, sql, one=True)['TNES']


    @property
    def mssname(self):
        return self.mssName

    @mssname.setter
    def mssname(self, mssid):
        # Get MSS name
        sql = 'select MSS_name from tblMSS where TNES = "' + mssid + '"'
        self.mssName = DB.query_db(self, sql, one=True)['MSS_Name']

    
    @property
    def dxcauseid(self):
        return self.dxCauseId

    @dxcauseid.setter
    def dxcauseid(self, mssid):
        # Get DX Cause and Total Calls
        sql =  'select DXcause, count(*) as calls from ' + app.config['SOURCE_TABLE'] + ' \
                left join tblClearCodes on ' + app.config['SOURCE_TABLE'] + '.DXCause = tblClearCodes.Code \
                where ' + app.config['SOURCE_TABLE'] + '.TNES = "' + mssid + '" and tblClearCodes.HighImpact = "1" \
                group by ' + app.config['SOURCE_TABLE'] + '.DXcause \
                order by calls desc limit 1'
        result = DB.query_db(self, sql, one=True)
        self.dxCauseId = result['DXcause']
        self.totalCalls = result['calls']

    
    @property
    def topcircuits(self):
        return self.topCGRGroups

    @topcircuits.setter
    def topcircuits(self, cgr):
        sql ='select TNES, ' + cgr + ', COUNT(' + cgr + ') as countcgr from ' + app.config['SOURCE_TABLE'] + ' where TNES in ( \
                select TNES from (select TNES, COUNT(*) as c from ' + app.config['SOURCE_TABLE'] + ' \
                WHERE ' + cgr + ' <> "" \
                group by TNES \
                ORDER BY c DESC LIMIT 1)) \
                GROUP BY ' + cgr + ' \
                order by countcgr desc limit 4'
        
        self.topCGRGroups = DB.query_db(self, sql, one=False)


    @property
    def spcname(self):
        return self.spcname

    @spcname.setter
    def spcName(self, net, spc):
        #sql ='SELECT NOME FROM tblNA WHERE NetWork=? AND DPC=?'
        self.spcName = DB.query_db(self, 'SELECT NOME FROM tblNA WHERE NetWork=? AND DPC=?', [net,spc], one=False)



class ReleasePart:

    def __init__(self, id, name=None):
        self.id = id
        self.name = name
        self.circuits = []

    def addCircuit(self, circuit):
        self.circuits.append(circuit)



class MSS:
    def __init__(self, id, name):
        self.id = id
        self.name = name
        self.cgrs = []
        self.dxcauses = []

    def addCGR(self,cgr):
        self.cgrs.append(cgr)

    def addDXCause(self, dxcause):
        self.dxcauses.append(dxcause)


class CGR:
    def __init__(self, id, net, spc, mss, releasepart):
        self.id = id
        self.net = net
        self.spc = spc
        self.mss = mss
        self.releasepart = releasepart

        mss.addCGR(self)
        releasepart.addCircuit(self)

    def show(self):
        print('CGR ID: \t', self.id)
        print('NET: \t', self.net)
        print('SPC: \t', self.spc)
        print('MSS: \t', self.mss)
        print('PART: \t', self.releasepart)



class DXCause:
    def __init__(self, id, desc, mss):
        self.id = id
        self.desc = desc
        self.mss = mss
        self.totalCalls = 0

        mss.addDXCause(self)



class Ticket:
    def __init__(self, summary, description, project, type):
        self.summary = summary
        self.description = description
        self.project = project
        self.type = 'Task'
        self.cgrs = []

    def addCGR(self, cgr):
        self.cgrs.append(cgr)


    @property
    def summary(self):
        return self.summary
    
    @summary.setter
    def summary(self, dxCauseId, dxCauseDesc):
        self.summary = 'Increase of clear code %s (%s)' % (dxCauseId, dxCauseDesc)

    @property
    def description(self):
        return self.description
    
    @description.setter
    def description(self, dxCauseId, dxCauseDesc, totalCalls, cgr):
        self.description = 'We received an alarm regarding the increase of clear code %s (%s) \
        with a total of %d calls, the release part is %s.\
        The affected CGRs are:' % (dxCauseId, dxCauseDesc, totalCalls, cgr)


    def show(self):
        print('Summary:\t',self.summary)
        print('Description:\t',self.description)
        print('type:\t',self.type)
        print('Project:\t',self.project)
        for cgr in self.cgrs:
            cgr.show()
    

    def send2stack(self):

        # Set the webhook_url
        webhook_url = 'https://localhost/api/v1/webhooks/nokdemo'
        stack_data = {"summary": self.summary,
        "type": self.type,
        "description": self.description,
        "project": self.project
        }
        
        headers = {'Content-Type': 'application/json',
        'St2-Api-Key': 'ZTNlNjM0NzY1ZDE0ZDEyZjNmNTc3MzNiMjUwOGI4MWQ1MGFmM2RlMGNjNmIwMGM3Yzg4MDllYmMxZTI4NjNhOA'
        }

        response = requests.post(webhook_url, data=json.dumps(stack_data), headers=headers, verify=False)

        if response.status_code != 202:
            raise ValueError('Request to stack returned an error %s, the response is:\n%s'% (response.status_code, response.text))

    








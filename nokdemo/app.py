import os
import sys
import sqlite3
import pandas as pd
import json
import requests
import time
import datetime
from flask_wtf import Form
from werkzeug.utils import secure_filename
from flask import Flask, request, session, g, redirect, url_for, abort, render_template, flash, Markup
from nokdemo.forms import CircuitForm, MSSForm
from tabulate import tabulate

sys._enablelegacywindowsfsencoding()

app = Flask(__name__) # create the application instance :)
app.config.from_object(__name__) # load config from this file , nokdemo.py

# Load default config and override config from an environment variable
app.config.update(dict(
    DATABASE=os.path.join(app.root_path, 'data/nokdemo.db'),
    SECRET_KEY='nokdemo123',
    USERNAME='admin',
    PASSWORD='default',
    UPLOAD_FOLDER = os.path.join(app.root_path, 'uploads'),
    ALLOWED_EXTENSIONS = ['txt', 'csv'],
    CSV = os.path.join(app.root_path, 'uploads', 'source.csv'),
    CSV_COLS = ['TNES','A CGR','B CGR','A Direction Number','B Address Number','B MSRN','Call Start Time','DX Cause','Release Part'],
    SOURCE_TABLE = 'tblData',
    WEBHOOK_URL = 'https://192.168.230.131/api/v1/webhooks/nokdemo',
    ST2_API_KEY = 'ZTNlNjM0NzY1ZDE0ZDEyZjNmNTc3MzNiMjUwOGI4MWQ1MGFmM2RlMGNjNmIwMGM3Yzg4MDllYmMxZTI4NjNhOA'
))

app.config.from_envvar('NOKDEMO_SETTINGS', silent=True)


class CSV:
    def allowed_file(self, filename):
        return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

    def readCSV (self):
        csvFile = pd.read_csv(app.config['CSV'], usecols=app.config['CSV_COLS'], dtype=str)
        csvFile = csvFile.rename(columns={c: c.replace(' ', '') for c in csvFile.columns})
        return csvFile


class DB:
    def connect_db(self):
        rv = sqlite3.connect(app.config['DATABASE'])
        rv.row_factory = sqlite3.Row
        return rv

    def get_db(self):
        if not hasattr(g, 'sqlite_db'):
            g.sqlite_db = self.connect_db()
        return g.sqlite_db

    def close_db(self, error):
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


class Data():
    dbObj = DB()
    
    def __init__(self, rpart=None, mssi=None, dxcause=None, topcircuits=None):
        self.rpart = rpart
        self.mssi = mssi
        self.dxcause = dxcause
        self.topcircuits = []
        

    def show_data(self):
        print_myinfo(self.rpart['Name'])
        print_myinfo(self.mssi['MSS_Name'])
        print_myinfo(self.dxcause['DXCause'])
        print_myinfo(self.dxcause['Name'])

    def set_data(self):
        self.rpart = self.get_releasePart()
        self.mssi = self.get_mssInfo(self.rpart)
        self.dxcause = self.get_dxCauseInfo(self.mssi)
        #self.topcircuits = self.get_topCircuits(self.rpart['Name'])

    def get_Data(self):
        rpart = self.get_releasePart()
        mssi = self.get_mssInfo(rpart)
        dxcause = self.get_dxCauseInfo(mssi)
        topcircuits = self.get_topCircuits(rpart['Name'])
        mydataObj = Data(rpart, mssi, dxcause, topcircuits)
        return mydataObj

    def add_circuit(self, circuit):
        self.topcircuits.append(circuit)

    def get_releasePart(self):
        # Get traffic source from top Release Part
        sql =  'select ReleasePart, Count(ReleasePart) as c, tblReleaseParts.Name \
        from ' + app.config['SOURCE_TABLE'] + ' \
        left join tblReleaseParts on ' + app.config['SOURCE_TABLE'] + '.ReleasePart = tblReleaseParts.Code \
        group by ' + app.config['SOURCE_TABLE'] + '.ReleasePart \
        ORDER BY c DESC limit 1'
        #ReleasePart = namedtuple('ReleasePart', 'id countrp name')

        return self.dbObj.query_db(sql, one=True)

    def get_mssInfo(self, cgrid):
        # Get MSS from Release Part A/B CGR
        #print_myinfo(cgrid['Name'])
        sql =  'select ' + app.config['SOURCE_TABLE'] + '.TNES, COUNT(*) as c, tblMSS.MSS_Name \
        from ' + app.config['SOURCE_TABLE'] + ' \
        left join tblMSS on ' + app.config['SOURCE_TABLE'] + '.TNES = tblMSS.TNES \
        WHERE ' + app.config['SOURCE_TABLE'] + '.' + cgrid['Name'] + ' <> "" \
        group by ' + app.config['SOURCE_TABLE'] + '.TNES \
        ORDER BY c DESC LIMIT 1'

        return self.dbObj.query_db(sql, one=True)


    def get_topCircuits(self, cgr):
        sql ='select TNES, ' + cgr + ', COUNT(' + cgr + ') as countcgr from ' + app.config['SOURCE_TABLE'] + ' where TNES in ( \
                select TNES from (select TNES, COUNT(*) as c from ' + app.config['SOURCE_TABLE'] + ' \
                WHERE ' + cgr + ' <> "" \
                group by TNES \
                ORDER BY c DESC LIMIT 1)) \
                GROUP BY ' + cgr + ' \
                order by countcgr desc limit 4'
        
        return self.dbObj.query_db(sql, one=False)


    def get_dxCauseInfo(self, mssid):
        # Get DX Cause and Total Calls
        sql =  'select DXcause, count(*) as calls, tblClearCodes.Name from ' + app.config['SOURCE_TABLE'] + ' \
                left join tblClearCodes on ' + app.config['SOURCE_TABLE'] + '.DXCause = tblClearCodes.Code \
                where ' + app.config['SOURCE_TABLE'] + '.TNES = "' + mssid['TNES'] + '" and tblClearCodes.HighImpact = "1" \
                group by ' + app.config['SOURCE_TABLE'] + '.DXcause \
                order by calls desc limit 1'

        return self.dbObj.query_db(sql, one=True)


class Circuit():
    dbObj = DB()

    def __init__(self, mss, cgr, countcgr, net=None, spc=None, name=None):
        self.mss = mss
        self.cgr = cgr
        self.countcgr = countcgr
        self.net = net
        self.spc = spc
        self.name = name


    def set_spcname(self, net, spc):
        sqlstr = 'SELECT NOME FROM tblNA WHERE NetWork="'+ net + '" AND DPC="' + spc + '"'
        if self.dbObj.query_db(sqlstr, one=True):
            return self.dbObj.query_db(sqlstr, one=True)['NOME']
        else:
            return ''

    def set_spc(self, spc):
        self.spc = spc
    
    def set_net(self, net):
        self.net = net


csvObj = CSV()
dataObj = Data()


# This function sends ticket data to StackStorm
def send2stack(summary, description):
    """Sends a ticket info to StackStorm WebHook.

    :param summary: Ticket/Issue summary.
    :param description: String with ticket/issue description 
    """
    
    stack_data = {"summary": summary,
    "type": "Task",
    "description": description,
    "project": "ND"
    }
    
    headers = {'Content-Type': 'application/json', 'St2-Api-Key': app.config['ST2_API_KEY']}

    response = requests.post(app.config['WEBHOOK_URL'], data=json.dumps(stack_data), headers=headers, verify=False)

    if response.status_code != 202:
        raise ValueError('Request to stack returned an error %s, the response is:\n%s'% (response.status_code, response.text))



def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

def add_sourceData(csv):
    dbObj = DB()
    db = dbObj.get_db()
    csv.to_sql(app.config['SOURCE_TABLE'], db, if_exists="replace")
    db.commit()
    flash('New entries were successfully inserted on database')
    return redirect(url_for('mss'))


def print_myinfo(mystr):
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    str2 = '[MY INFO] : ' + st + '\t' + str(mystr)
    print (str2)



################### View functions #######################

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        # if user does not select file, browser also
        # submit a empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            add_sourceData(csvObj.readCSV())
            return redirect(url_for('mss'))

    return render_template('index.html')



@app.route('/mss', methods=['GET','POST'])
def mss():
    dataObj = Data()
    dataObj.set_data()

    mystr = Markup('<p>Please enter the following CGR commands on %s console \
    and then get the NET and SPC values for each CGR Group!</p>') % (dataObj.mssi['MSS_Name'])
    flash(mystr)

    circuits = dataObj.get_topCircuits(dataObj.rpart['Name'])
    for crc in circuits:
        #crcObj = Circuit(crc['TNES'], dataObj.rpart['Name'], crc['countcgr'])
        crcObj = Circuit(crc['TNES'], crc[1], crc['countcgr'])
        dataObj.add_circuit(crcObj)

    form = MSSForm()
    if form.validate_on_submit():
        for idx, tc in enumerate(dataObj.topcircuits):
            tc.net = form.cgrs.data[idx]['net']
            tc.spc = form.cgrs.data[idx]['spc']
            tc.name = tc.set_spcname(tc.net, tc.spc)
        session['circuits'] = json.dumps([ob.__dict__ for ob in dataObj.topcircuits])
        session.pop('_flashes', None)
        return redirect(url_for('show_details'))
    zipped = zip(dataObj.topcircuits, form.cgrs)
    return render_template('mss.html', form=form, zip=zipped)


@app.route('/details', methods=['GET', 'POST'])
def show_details():
    dbObj = Data()
    msg = 'A ticket is being created on Jira with the following statements'
    circuit_info = json.loads(session.get('circuits'))
    detailObj = dbObj.get_Data()

    ticket_data = dict(
        SUMMARY = '',
        DESCRIPTION = '',
        TYPE = '',
        AFFECTED_CGRS = []
    )

    table_rows = [[circuit['cgr'], circuit['countcgr'], circuit['mss'], circuit['name']] for circuit in circuit_info]

    table_str = tabulate(table_rows, headers=['CGR','Events','MSS','SPC'], tablefmt="jira")
    ticket_data['SUMMARY'] = 'Increase of clear code %s (%s)' % (detailObj.dxcause['DXcause'], detailObj.dxcause['Name'])
    ticket_data['DESCRIPTION'] = 'We received an alarm regarding the increase of clear code %s (%s) \
    with a total of %d calls, the release part is %s.\n\
    The affected CGRs are: \n\n %s' % (detailObj.dxcause['DXcause'], detailObj.dxcause['Name'], detailObj.dxcause['calls'], detailObj.rpart['Name'], table_str)
    html_desc = ticket_data['DESCRIPTION'].split(":")
    flash(msg)
     
    send2stack(ticket_data['SUMMARY'], ticket_data['DESCRIPTION'])

    if request.method == 'POST':
        return redirect(url_for('upload_file'))
    
    return render_template('details.html', items=circuit_info, cgr=detailObj.rpart['Name'], summary=ticket_data['SUMMARY'], description=html_desc[0])

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0')

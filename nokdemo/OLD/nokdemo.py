# coding=utf-8
# all the imports
import os
import sqlite3
from flask import Flask, request, session, g, redirect, url_for, abort, render_template, flash, Markup
from flask_wtf import Form
from werkzeug.utils import secure_filename
import pandas as pd
import requests
import json
from forms import MSSCommandsForm


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
    SOURCE_TABLE = 'tblData'
))

app.config.from_envvar('NOKDEMO_SETTINGS', silent=True)


def send(summary, description):

    # Set the webhook_url
    webhook_url = 'https://localhost/api/v1/webhooks/nokdemo'
    stack_data = {"summary": summary,
    "type": "Task",
    "description": description,
    "project": "ND"
    }
    
    headers = {'Content-Type': 'application/json',
    'St2-Api-Key': 'ZTNlNjM0NzY1ZDE0ZDEyZjNmNTc3MzNiMjUwOGI4MWQ1MGFmM2RlMGNjNmIwMGM3Yzg4MDllYmMxZTI4NjNhOA'
    }

    response = requests.post(webhook_url, data=json.dumps(stack_data), headers=headers, verify=False)

    if response.status_code != 202:
        raise ValueError('Request to stack returned an error %s, the response is:\n%s'% (response.status_code, response.text))


def connect_db():
    """Connects to the specific database."""
    rv = sqlite3.connect(app.config['DATABASE'])
    rv.row_factory = sqlite3.Row
    return rv


def get_db():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'sqlite_db'):
        g.sqlite_db = connect_db()
    return g.sqlite_db


@app.teardown_appcontext
def close_db(error):
    """Closes the database again at the end of the request."""
    if hasattr(g, 'sqlite_db'):
        g.sqlite_db.close()


def init_db():
    db = get_db()
    with app.open_resource('schema.sql', mode='r') as f:
        db.cursor().executescript(f.read())
    db.commit()


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']


def readCSV ():
    csvFile = pd.read_csv(app.config['CSV'], usecols=app.config['CSV_COLS'], dtype=str)
    csvFile = csvFile.rename(columns={c: c.replace(' ', '') for c in csvFile.columns})
    return csvFile


def add_sourceData(csv):
    db = get_db()
    csv.to_sql(app.config['SOURCE_TABLE'], db, if_exists="replace")
    db.commit()
    flash('New entries were successfully inserted on database')
    return redirect(url_for('show_entries'))


@app.cli.command('initdb')
def initdb_command():
    """Initializes the database."""
    init_db()
    print('Initialized the database.')


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
            csvFile = readCSV()
            add_sourceData(csvFile)
            return redirect(url_for('mss'))

    return render_template('index.html')


@app.route('/mss', methods=['GET','POST'])
def mss():
    details = process_data()
    form = MSSCommandsForm(request.form)
    str = Markup('<p>Please enter the following CGR commands on %s console \
    and then get the NET and SPC values for each CGR Group!</p>') % (details['MSS_NAME'])
    flash(str)
    if request.method == 'POST' and form.validate():
        details['TOP_CGR-GROUPS']['NET'] = form.data.net
        details['TOP_CGR-GROUPS']['SPC'] = form.data.spc
        return redirect(url_for('show_details'))
    return render_template('mss.html', details=details, form=form)


@app.route('/details', methods=['GET', 'POST'])
def show_details():
    detail = process_data()
    ticket_data = dict(
        SUMMARY = '',
        HEADER = '',
        TYPE = '',
        DESCRIPTION = '',
        AFFECTED_CGRS = []
    )

    ticket_data['SUMMARY'] = 'Increase of clear code %s (%s)' % (detail['DX_CAUSE_ID'], detail['DX_CAUSE_DESC'])
    ticket_data['HEADER'] = 'We received an alarm regarding the increase of clear code %s (%s) \
    with a total of %d calls, the release part is %s.\
    The affected CGRs are:' % (detail['DX_CAUSE_ID'], detail['DX_CAUSE_DESC'], detail['TOTAL_CALLS'], detail['CGR'])
    ticket_data['AFFECTED_CGRS'] = detail['TOP_CGR_GROUPS']
    for c in ticket_data['AFFECTED_CGRS']:
        print_myinfo (c[2])

    cgr=detail['CGR']

    flash('A ticket is being created on Jira with the following statements')
    
    send(ticket_data['SUMMARY'], ticket_data['HEADER'])
    return render_template('details.html', items=detail['TOP_CGR_GROUPS'], cgr=cgr, summary=ticket_data['SUMMARY'], description=ticket_data['HEADER'])





def mssInfo(request):
    form = MSSCommandsForm(request.POST)
    mss = dict()
    if request.method == 'POST' and form.validate():
        mss['net'] = form.data.net
        mss['sps'] = form.data.sps
    return mss


@app.route('/sourcedata')
def show_entries():
    db = get_db()
    sql = 'select * from ' + app.config['SOURCE_TABLE']
    cur = db.execute(sql)
    entries = cur.fetchall()
    return render_template('show_entries.html', entries=entries)


def query_db(query, args=(), one=False):
    cur = get_db().execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv


def print_myinfo(str):
    print ('\n************************ MY INFO *************************\n')
    print (str)
    print('\n*********************** END MY INFO ***********************\n')

def getReleasePart():

    # Get traffic source from top Release Part
    sql =  'select ReleasePart, Count(ReleasePart) as c \
    from ' + app.config['SOURCE_TABLE'] + ' \
    group by ReleasePart \
    ORDER BY c DESC limit 1'

    result = query_db(sql, one=True)['ReleasePart']
    
    # Release Part = 0x1 --> ACGR
    # Release Part != 0x1 --> BCGR
    if result == "0x1":
        return "ACGR"
    else:
        return "BCGR"


def getMSS(cgr):
    mssInfo = dict(
        MSS_ID = '',
        MSS_NAME = ''
    )

    # Get MSS from Release Part A/B CGR
    sql =  'select TNES, COUNT(*) as c \
            from ' + app.config['SOURCE_TABLE'] + ' \
            WHERE ' + cgr + ' <> "" \
            group by TNES \
            ORDER BY c DESC LIMIT 1'

    mssInfo['MSS_ID'] = query_db(sql, one=True)['TNES']

    # Get MSS name
    sql = 'select MSS_name from tblMSS where TNES = "' + mssInfo['MSS_ID'] + '"'

    mssInfo['MSS_NAME'] = query_db(sql, one=True)['MSS_Name']

    return mssInfo


def getDXCause(mssID):

    dxCause = dict(
        DX_CAUSE_ID = '',
        DX_CAUSE_DESC = '',
        TOTAL_CALLS = 0
    )

    # Get DX Cause and Total Calls
    sql =  'select DXcause, count(*) as c from ' + app.config['SOURCE_TABLE'] + ' \
            left join tblClearCodes on ' + app.config['SOURCE_TABLE'] + '.DXCause = tblClearCodes.Code \
            where ' + app.config['SOURCE_TABLE'] + '.TNES = "' + mssID + '" and tblClearCodes.HighImpact = "1" \
            group by ' + app.config['SOURCE_TABLE'] + '.DXcause \
            order by c desc limit 1'
    result = query_db(sql, one=True)
    dxCause['DX_CAUSE_ID'] = result['DXcause']
    dxCause['TOTAL_CALLS'] = result['c']

    # Get Description from DX Cause ID
    sql = 'select Name from tblClearCodes where Code = "' + result['DXcause'] +'"'
    dxCause['DX_CAUSE_DESC'] = query_db(sql, one=True)['Name']

    return dxCause


def getTopCGR(cgr):
    topCGR = []
    sql ='select TNES, ' + cgr + ', COUNT(' + cgr + ') as countcgr from ' + app.config['SOURCE_TABLE'] + ' where TNES in ( \
            select TNES from (select TNES, COUNT(*) as c from ' + app.config['SOURCE_TABLE'] + ' \
            WHERE ' + cgr + ' <> "" \
            group by TNES \
            ORDER BY c DESC LIMIT 1)) \
            GROUP BY ' + cgr + ' \
            order by countcgr desc limit 4'
    
    topCGR = query_db(sql, one=False)
    return topCGR


def getSPCName(net, spc):
    #sql ='SELECT NOME FROM tblNA WHERE NetWork=? AND DPC=?'
    spcName = query_db('SELECT NOME FROM tblNA WHERE NetWork=? AND DPC=?', [net,spc], one=False)

    return spcName


def process_data():
    processed_data = dict(
        RELEASE_PART = '',
        CGR = '',
        MSS_ID = '',
        MSS_NAME = '',
        DX_CAUSE_ID = '',
        DX_CAUSE_DESC = '',
        TOP_CGR_GROUPS = [],
        TOTAL_CALLS = 0
    )

    # Get traffic source from top Release Part
    sql =  'select ReleasePart, Count(ReleasePart) as c \
from ' + app.config['SOURCE_TABLE'] + ' \
group by ReleasePart \
ORDER BY c DESC limit 1'

    #print_myinfo(sql)
    result = query_db(sql, one=True)
    processed_data['RELEASE_PART'] = result['ReleasePart']
    
    # Release Part = 0x1 --> ACGR
    # Release Part != 0x1 --> BCGR
    if result['ReleasePart'] == "0x1":
        processed_data['CGR'] = "ACGR"
    else:
        processed_data['CGR'] = "BCGR"
    
    # Get MSS from Release Part A/B CGR
    sql =  'select TNES, COUNT(*) as c \
            from ' + app.config['SOURCE_TABLE'] + ' \
            WHERE ' + processed_data['CGR'] + ' <> "" \
            group by TNES \
            ORDER BY c DESC LIMIT 1'

    processed_data['MSS_ID'] = query_db(sql, one=True)['TNES']

    # Get MSS name
    sql = 'select MSS_name from tblMSS where TNES = "' + processed_data['MSS_ID'] + '"'

    processed_data['MSS_NAME'] = query_db(sql, one=True)['MSS_Name']
    
    # Get DX Cause and Total Calls
    sql =  'select DXcause, count(*) as c from ' + app.config['SOURCE_TABLE'] + ' \
            left join tblClearCodes on ' + app.config['SOURCE_TABLE'] + '.DXCause = tblClearCodes.Code \
            where ' + app.config['SOURCE_TABLE'] + '.TNES = "' + processed_data['MSS_ID'] + '" and tblClearCodes.HighImpact = "1" \
            group by ' + app.config['SOURCE_TABLE'] + '.DXcause \
            order by c desc limit 1'
    result = query_db(sql, one=True)
    processed_data['DX_CAUSE_ID'] = result['DXcause']
    processed_data['TOTAL_CALLS'] = result['c']

    # Get Description from DX Cause ID
    sql = 'select Name from tblClearCodes where Code = "' + processed_data['DX_CAUSE_ID'] +'"'
    processed_data['DX_CAUSE_DESC'] = query_db(sql, one=True)['Name']

    # Get Top 4 CGR Groups
    cgr = processed_data['CGR']
    sql ='select TNES, ' + cgr + ', COUNT(' + cgr + ') as countcgr from ' + app.config['SOURCE_TABLE'] + ' where TNES in ( \
            select TNES from (select TNES, COUNT(*) as c from ' + app.config['SOURCE_TABLE'] + ' \
            WHERE ' + cgr + ' <> "" \
            group by TNES \
            ORDER BY c DESC LIMIT 1)) \
            GROUP BY ' + cgr + ' \
            order by countcgr desc limit 4'
    
    processed_data['TOP_CGR_GROUPS'] = query_db(sql, one=False)
    for c in processed_data['TOP_CGR_GROUPS']:
        print_myinfo (c[cgr])
    return processed_data


    


from flask_wtf import FlaskForm
from wtforms import Form, FieldList, FormField, BooleanField, StringField, PasswordField, validators, SubmitField

class CircuitForm(FlaskForm):
    net = StringField('NET')
    spc = StringField('SPC')
    def __init__(self, *args, **kwargs):
           kwargs['csrf_enabled'] = False
           super(CircuitForm, self).__init__(*args, **kwargs)

class MSSForm(FlaskForm):
    cgrs = FieldList(FormField(CircuitForm), min_entries = 4)
    submit = SubmitField('Submit')
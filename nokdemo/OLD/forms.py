from wtforms import Form, BooleanField, StringField, PasswordField, validators

class MSSCommandsForm(Form):
    net = StringField('NET', [validators.Length(min=4, max=25)])
    spc = StringField('SPC', [validators.Length(min=6, max=35)])
r"""spreadsync -- sync google spreadsheet with work dir

$ pip install google-api-python-client
Collecting google-api-python-client
  Downloading google_api_python_client-1.6.3-py2.py3-none-any.whl (53kB)
https://developers.google.com/sheets/api/quickstart/python

SHEET=1JM-25k... python spreadsync.py table_terms.csv SHEET 'table_terms!A:Y'
rows: 12

"""
from sys import stderr
import csv

from apiclient import discovery
from oauth2client import client
from oauth2client import tools  # ISSUE: powerful? starts web browser
from oauth2client.file import Storage


def main(argv, environ, cwd, home, Http,
         client_secret_file='client_secret.json'):
    """Fetch table terms and save to CSV
    """
    [saveAs, sheetKey, rangeName] = argv[1:4]
    spreadsheetId = environ[sheetKey]
    credentials = get_credentials(cwd / client_secret_file,
                                  home / '.credentials')
    http = credentials.authorize(Http())
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
    service = discovery.build('sheets', 'v4', http=http,
                              discoveryServiceUrl=discoveryUrl)

    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheetId, range=rangeName,
        majorDimension='ROWS').execute()
    rows = result.get('values', [])
    print('rows:', len(rows), file=stderr)

    with (cwd / saveAs).open('w') as out:
        dest = csv.writer(out)
        for row in rows:
            dest.writerow(row)


def get_credentials(
        client_secret,
        credential_dir,
        # If modifying these scopes, delete your previously saved credentials
        # at ~/.credentials/sheets.googleapis.com-python-quickstart.json
        scopes='https://www.googleapis.com/auth/spreadsheets.readonly',
        client_secret_file='client_secret.json',
        application_name='GROUSE Term Sync',
        app='sheets.googleapis.com-python-quickstart.json'):
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    if not credential_dir.exists():
        credential_dir.mkdir()
    credential_path = credential_dir / app

    store = Storage(str(credential_path))  # ISSUE: Ambient
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(
            str(client_secret), scopes)
        flow.user_agent = application_name
        credentials = tools.run_flow(flow, store)
        print('Storing credentials to %s' % credential_path, file=stderr)
    return credentials


if __name__ == '__main__':
    def _script():
        from os import environ
        from pathlib import Path
        from sys import argv

        from httplib2 import Http

        main(argv, environ, cwd=Path('.'), home=Path.home(), Http=Http)

    _script()

#!/usr/bin/python3

DEBUG = True
CONTACTS_QUERY = None

import config
import os

from collections import namedtuple

os.environ['TNS_ADMIN'] = '/usr/lib/oracle/11.2/client'

import cx_Oracle
import postgresql

ContactRow = namedtuple('ContactRow',
    ' '.join(  # All fields exported in SQL query
        ('memberno accountno mapno',
         'account_type contact_type member_name',
         'acct_street_no acct_street_unit acct_street_name',
         'acct_street_type acct_city acct_state acct_zip',
         'acct_country',
         'mbr_street_no mbr_street_unit mbr_street_name',
         'mbr_street_type mbr_city mbr_state mbr_zip',
         'mbr_country') ).split(' '))
Address = namedtuple('Address',
    'member_name number unit street city state zip country'.split(' '))

class AccountContacts:
    def __init__(self, member, account, location, a_type):
        self.member = member
        self.account = account
        self.location = location
        self.a_type = a_type
        self.contacts = dict()
    
    def add_contact(self, c_type, member, number, unit, s_name, s_type,
                    city, state, zipc, country):
        if not c_type in self.contacts:
            if member.endswith(','):
                member = member[:-1]
            address = Address(
                coalesce(member, ''),
                coalesce(number, ''),
                coalesce(unit, ''),
                acombine(s_name, s_type),
                coalesce(city, ''),
                coalesce(state, ''),
                coalesce(zipc, ''),
                coalesce(country, 'US')
            )
            self.contacts[c_type] = address
        else:
            self.add_contact(c_type + ' alt', member, number, unit,
                             s_name, s_type, city, state, zipc, country)

class CoalesceException(Exception):
    pass

def l_(source, *message, **kwargs):
    print('[{}]'.format(source), *message, **kwargs)

def l_g(*message, **kwargs):
    l_('General', *message, **kwargs)

def l_p(*message, **kwargs):
    l_('PostgreSQL', *message, **kwargs)

def l_o(*message, **kwargs):
    l_('Oracle', *message, **kwargs)

def coalesce(*args):
    for arg in args:
        if not arg is None:
            return str(arg)
    raise CoalesceException('No non-null values found')

def acombine(*args):
    return ' '.join(map(lambda s: coalesce(s, ''), args)).strip()

l_g('Loading queries...')

with open('ats_member_query.sql') as SQL:
    CONTACTS_QUERY = SQL.read().strip()

contacts = dict()
if CONTACTS_QUERY:
    l_o('Connecting to oracle:thin://{}@{}...'.format(
        config.ats_user, config.ats_host))
    ats_db = cx_Oracle.connect('{}/{}@{}'.format(
        config.ats_user, config.ats_pass, config.ats_host))
    l_o('Requesting cursor...')
    ats_cur = ats_db.cursor()
    l_o('Executing query...')
    ats_cur.execute(CONTACTS_QUERY)
    l_o('Pulling cursor...')
    last_bill_date = None
    for data in ats_cur.fetchall():
        row = ContactRow(*data)
        if not row.accountno in contacts:
            ac = AccountContacts(
                    row.memberno,
                    row.accountno,
                    row.mapno,
                    row.account_type
                )
            ac.add_contact(row.contact_type, row.member_name,
                row.acct_street_no, row.acct_street_unit,
                row.acct_street_name, row.acct_street_type,
                row.acct_city, row.acct_state, row.acct_zip, row.acct_country)
            ac.add_contact('Member ' + row.contact_type, row.member_name,
                row.mbr_street_no, row.mbr_street_unit,
                row.mbr_street_name, row.mbr_street_type,
                row.mbr_city, row.mbr_state, row.mbr_zip, row.mbr_country)
            contacts[row.accountno] = ac
    l_o('Done!')
    
    try:
        ats_cur.close()
        ats_db.close()
    except:
        pass
else:
    l_o('No query loaded...')

priority = (
    'Primary Contact', 'Member Primary Contact',
    'Owner Contact', 'Member Owner Contact',
    'Spouse Contact', 'Member Spouse Contact',
    'Power Of Attorney', 'Member Power Of Attorney',
    'Business Representative', 'Member Business Representative',
    'Relative', 'Member Relative'
)

l_g('Prioritizing contacts...')
final_contacts = list()
for _, ac in contacts.items():
    a = None
    for c_type in priority:
        if c_type in ac.contacts:
            a = AccountContacts(ac.member, ac.account, ac.location, ac.a_type)
            a.contacts[c_type] = ac.contacts[c_type]
            break
    if a:
        final_contacts.append(a)
    else:
        print('Not loaded', ac)
l_g('Done!')

if final_contacts:
    l_p('Connecting to pq://{}@{}/{}'.format(
        config.psql_user, config.psql_host, config.psql_db))
    psql_db = postgresql.open(host=config.psql_host, database=config.psql_db,
        user=config.psql_user, password=config.psql_pass)
    l_p('Preparing statements...')
    
    insert_query = psql_db.prepare('''

insert into cis.member_locations values
    ( $1, $2, $3, $4, $5, $6, $7, $8,
      $9, $10, $11, $12, $13, null )

'''.strip())

    l_p('Opening transaction...')
    with psql_db.xact():
        l_p('Truncating table...')
        psql_db.execute('''

truncate table cis.member_locations

'''.strip())
        l_p('Inserting members...')
        for ac in final_contacts:
            # print(ac.member, ac.a_type)
            for T, C in ac.contacts.items():
                insert_query(
                    ac.member, ac.account, str(ac.location).upper(),
                    ac.a_type, T, C.member_name, C.number , C.unit,
                    C.street, C.city, C.state, C.zip, C.country
                )
                break
        l_p('Committing transaction...')
    l_p('Done!')

try:
    #map(lambda x: x.close(), postgres_queries.values())
    for statement in postgres_queries.values():
        statement.close() if statement else None
    psql_db.close()
except:
    pass

l_g('Done!')

#!/usr/bin/python3

import config
import fiona
import postgresql

DO_PSQL_WRITE = True

from shapely.geometry import shape, mapping, Point, Polygon, MultiPolygon

def l_(source, *message, **kwargs):
    print('[{}]'.format(source), *message, **kwargs)

def l_g(*message, **kwargs):
    l_('General', *message, **kwargs)

def l_p(*message, **kwargs):
    l_('PostgreSQL', *message, **kwargs)

def l_f(*message, **kwargs):
    l_('fiona', *message, **kwargs)


SERVICES = 'Board District Members/Board District Members.shp'
DISTRICTS = 'BoardDistrict/BoardDistrictBoundary.shp'
OUTPUT_SHP = 'Member Services/Member Services.shp'

output_driver = output_schema = output_crs = None

found_districts = dict()
member_services = list()
member_locations = set()

l_g('Getting map locations of member services...')
l_p('Connecting...')
psql_db = postgresql.open(host=config.psql_host, database=config.psql_db,
    user=config.psql_user, password=config.psql_pass)

l_p('Preparing statement...')
select_query = psql_db.prepare('''

select "location"
from cis.member_locations
where "location" is not null 
and "location" != 'NONE'

'''.strip())

with psql_db.xact():
    l_p('Pulling locations...')
    for result in select_query():
        member_locations.add(result[0])

select_query.close()
psql_db.close()

l_p('Done!')

l_g('Performing spatial analysis...')
l_f('Loading...')
with fiona.drivers():
    l_f('Opening sevices...')
    with fiona.open(SERVICES) as source:
        l_f('Translating shapes...')
        output_driver = source.driver
        output_schema = source.schema
        output_crs = source.crs
        services = list(map(
            lambda entry: (entry['properties']['wmElementN'],
                           shape(entry['geometry']), entry
                ),
            [entry for entry in source]))
    l_f('Opening districts...')
    with fiona.open(DISTRICTS) as source:
        l_f('Translating shapes...')
        districts = list(map(
            lambda entry: (entry['properties']['DistrictID'],
                           shape(entry['geometry'])
                ),
            [entry for entry in source]))
    
    l_f('Relating service locations...')
    for loc, point, entry in services:
        if loc in member_locations:
            for district, polygon in districts:
                if point.within(polygon):
                    found_districts[loc] = int(district)
                    member_services.append(entry)
                    break
            if not loc in found_districts:
                l_f('ERROR', 'No district for', loc)
    
    l_f('Writing shapefile of member locations...')
    with fiona.open(OUTPUT_SHP, 'w',
            driver = output_driver,
            crs = output_crs,
            schema = output_schema) as output:
        for service in member_services:
            output.write(service)
    l_f('Done!')

if DO_PSQL_WRITE:
    l_g('Updating database with member service locations...')
    l_p('Connecting...')
    psql_db = postgresql.open(host=config.psql_host, database=config.psql_db,
        user=config.psql_user, password=config.psql_pass)
    
    l_p('Preparing statement...')
    update_query = psql_db.prepare('''
    
update cis.member_locations set district = $2
where location = $1
    
    '''.strip())
    
    with psql_db.xact():
        l_p('Updating districts...')
        for location, district in found_districts.items():
            update_query(location, district)
        l_p('Committing transaction...')
    
    update_query.close()
    psql_db.close()
    
    l_p('Done!')

'''
Provides a linear model for determining record matches for use in db.py
'''

from textdistance import jaro, jaro_winkler, smith_waterman
import logging
from psycopg2 import DatabaseError, sql


def check_match(r1, r2):
    '''
    Check whether two records match using a linear model and return a boolean.
    First check whether the records states match andzips are similar before 
    calculating the model.
    '''
    if r1['state'] != r2['state']:
        return False
    elif r1['zip'] != r2['zip']: #or \
                # (get_zip_score(r1['zip'], r2['zip']) < 0.7):
        return False
    else:
        name_score = get_name_score(r1['name'], r2['name'])
        b1, b2, s1, s2 = address_split(r1['address'], r2['address'])
        building_score = get_building_score(b1, b2)
        street_score = get_street_score(s1, s2)

        total_score = 2.5*building_score + 2.5*street_score + 5*name_score

        if total_score >= 8.75:
            return True
        else:
            return False

def check_match_fast(r1, r2):
    '''
    Check whether two records match using a linear model and return a boolean.
    This version intended for record matching with blocking where records
    are only compared to those in the same state and zip.
    '''
    name_score = get_name_score(r1['name'], r2['name'])
    b1, b2, s1, s2 = address_split(r1['address'], r2['address'])
    building_score = get_building_score(b1, b2)
    street_score = get_street_score(s1, s2)

    total_score = 2.5*building_score + 2.5*street_score + 5*name_score

    if total_score >= 8.75:
        return True
    else:
        return False

def get_building_score(b1, b2):
    return jaro_winkler.normalized_similarity(b1.upper(), b2.upper())
    
def get_street_score(s1, s2):
    return jaro_winkler.normalized_similarity(s1.upper(), s2.upper())

def get_name_score(n1, n2):
    return jaro.normalized_similarity(n1.upper(), n2.upper())

def get_zip_score(z1, z2):
    return jaro.normalized_similarity(str(z1).upper(), str(z2).upper())

def address_split(a1, a2):
    '''
    Separate the building number and street name for both arrdesses.
    '''
    a1 = str(a1).split(' ')
    b1, s1 = (a1[0], str(a1[1:]))
    a2 = str(a2).split(' ')
    b2, s2 = (a2[0], str(a2[1:]))
    return b1, b2, s1, s2

def blocking(conn, cur):
    '''
    Create a temp table for every state within our dataset and dump the relevant
    dirty data into each temp table for future record linkage.
    '''

    GET_STATES = """
        SELECT distinct(state)
        FROM ri_restaurants;
        """

    CREATE_TEMP_TABLE = """
        CREATE TEMP TABLE {} 
        AS
        SELECT id, name, facility_type, address, city, state, zip, location, false as clean
        FROM ri_restaurants 
        WHERE state = %s
            AND ( clean = false OR id IN ( SELECT primary_rest_id 
                                           FROM ri_linked )
                );
        """

    CREATE_TEMP_INDEX = """
        CREATE INDEX ON {} USING hash (zip);
        """

    GET_ZIPCODES = """
        SELECT DISTINCT zip FROM {};
        """

    try:
        cur.execute(GET_STATES)
    except (Exception, DatabaseError) as e:
        status = bad_request(e, conn, cur, rollback=True, status=400)
        return []

    # convert list of state dicts to list of states
    states_dicts = cur.fetchall()
    temp_names = [x['state'] + '_block' for x in states_dicts]
    states = [x['state'] for x in states_dicts]
    temp_zips = {}

    try:
        for temp_name, state in zip(temp_names, states):
            cur.execute(
                sql.SQL(CREATE_TEMP_TABLE)
                    .format(sql.Identifier(temp_name)),
                (state,))
            cur.execute(
                sql.SQL(CREATE_TEMP_INDEX)
                    .format(sql.Identifier(temp_name))
                )
            cur.execute(
                sql.SQL(GET_ZIPCODES)
                    .format(sql.Identifier(temp_name))
                )
            zip_dicts = cur.fetchall()
            state_zips = [x['zip'] for x in zip_dicts]
            temp_zips[temp_name] = state_zips
    except (Exception, DatabaseError) as e:
        status = bad_request(e, conn, cur, rollback=True, status=500)
        return []

    return temp_zips

def find_most_common(d, new_key, primary_key):
    ''' Helper function to find the most common key in a dict
    and replace the primary '''
    if d.get(new_key):
        d[new_key] = d[new_key] + 1
    else: 
        d[new_key] = 1
    if d[new_key] > d[primary_key]:
        return new_key

    return primary_key

def bad_request(e, conn, cur, rollback=True, status=400):
    """
    Helper function to handle bad request: rollback, close cursor,
    return correct status.
    """

    logging.error("DB error: %s", e)
    logging.error("Query attempted: %s", cur.query)
    if rollback:
        conn.rollback()
    cur.close()

    return status


def ok_request(cur, commit=True, status=200):
    """
    Helper function to handle OK request: commit, close cursor,
    return correct status.
    """

    if commit:
        self.conn.commit()
    cur.close()

    return status


if __name__ == "__main__":
    '''
    A test.
    '''
    test1 = {
        'name': "Matt's burgers",
        'address': '1547 Ora Dr.',
        'state': 'IA', 
        'zip': 50701
    }

    test2 = {
        'name': "Max's burger joint",
        'address': '1547 Odera Street',
        'state': 'IA', 
        'zip': 50790
    }

    test3 = {
        'name': "Mat's burger joint",
        'address': '1547 Odera Street',
        'state': 'IA', 
        'zip': 50790
    }

    test4 = {
        'name': "Linh's Diner",
        'address': '12 Prospect Blvd.',
        'state': 'IA', 
        'zip': 50701
    }

    print(f'First test should return False: {check_match(test1, test2)}')
    print(f'Second test should return True: {check_match(test1, test3)}')
    print(f'Third test should return False: {check_match(test1, test4)}')








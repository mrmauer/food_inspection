from psycopg2.extras import RealDictCursor
from psycopg2 import DatabaseError, sql
import logging
import match_records

"""
Wraps a single connection to the database with higher-level functionality.
"""
class DB:
    def __init__(self, connection):
        self.conn = connection


    def bad_request(self, e, cur, rollback=True, status=400):
        """
        Helper function to handle bad request: rollback, close cursor,
        return correct status.
        """

        logging.error("DB error: %s", e)
        logging.error("Query attempted: %s", cur.query)
        if rollback:
            self.conn.rollback()
        cur.close()

        return status


    def ok_request(self, cur, commit=True, status=200):
        """
        Helper function to handle OK request: commit, close cursor,
        return correct status.
        """

        if commit:
            self.conn.commit()
        cur.close()

        return status


    def reset_db(self):
        """
        Reset DB: Truncate ri_restaurants, ri_inspections
        """

        cur = self.conn.cursor(cursor_factory = RealDictCursor)

        TRUNCATE_TABLES = """
            TRUNCATE ri_restaurants, ri_inspections, ri_tweetmatch CASCADE;
            """

        DROP_IDX_NAME = """
            DROP INDEX IF EXISTS ri_restaurants_name_idx;
            """

        DROP_IDX_LOCATION = """
            DROP INDEX IF EXISTS ri_restaurants_location_idx;
            """

        DROP_IDX_NAME_ADDRESS = """
            DROP INDEX IF EXISTS ri_restaurants_name_address_idx;
            """

        commands = [TRUNCATE_TABLES, DROP_IDX_NAME, DROP_IDX_LOCATION, DROP_IDX_NAME_ADDRESS]

        for command in commands:
            try:
                cur.execute(command)
            except (Exception, DatabaseError) as e:
                return self.bad_request(e, cur, rollback=True, status=400)

        return self.ok_request(cur, commit=True, status=200)


    def find_restaurant(self, restaurant_id):
        """
        Searches for the restaurant with the given ID. Returns None if the
        restaurant cannot be found in the database.
        """

        cur = self.conn.cursor(cursor_factory = RealDictCursor)
        restaurant = None

        RESTAURANT_SEARCH = """
            SELECT * 
            FROM ri_restaurants
            WHERE id = %s;
            """

        # get the restaurant
        try:
            cur.execute(RESTAURANT_SEARCH,
                (restaurant_id, ))
            r = cur.fetchone()
        except (Exception, DatabaseError) as e:
            status = self.bad_request(e, cur, rollback=False, status=400)
            return status, restaurant

        # if restaurant doesn't exist tell the server
        if not r:
            status = 404
            cur.close()
            return status, restaurant
            # else extract values from tuple into dictionary and set status
        else:
            lon, lat = r['location'].strip('()').split(',')
            restaurant = {
                "id" : restaurant_id,
                "name": r['name'],
                "facility_type": r['facility_type'],
                "address": r['address'],
                "city": r['city'],
                "state": r['state'],
                "zip": r['zip'],
                "latitude": lat,
                "longitude": lon,
                "clean": r['clean']
                }
            status = self.ok_request(cur, commit=False, status=200)

        return status, restaurant


    def find_inspection(self, inspection_id):
        """
        Searches for the inspection with the given ID. Returns None if the
        inspection cannot be found in the database.
        """

        cur = self.conn.cursor(cursor_factory = RealDictCursor)
        inspection = None

        INSPECTION_SEARCH = """
            SELECT *
            FROM ri_inspections
            WHERE id = %s;
            """

        # get a tuple for the matching inspection
        try:
            cur.execute(INSPECTION_SEARCH,
                (inspection_id, ))
            i = cur.fetchone()
        except (Exception, DatabaseError) as e:
            status = self.bad_request(e, cur, rollback=False, status=400)
            return status, inspection

        if not i:
            status = 404
            cur.close()
            return status, inspection
        else:
            inspection = {
                "id": i['id'],
                "risk": i['risk'],
                "date": str(i['inspection_date']),
                "inspection_type": i['inspection_type'],
                "results": i['results'],
                "violations": i['violations'],
                "restaurant_id": i['restaurant_id']
                }
            status = self.ok_request(cur, commit=False, status=200)

        return status, inspection


    def find_inspections(self, restaurant_id):
        """
        Searches for all inspections associated with the given restaurant.
        Returns an empty list if no matching inspections are found.
        """

        cur = self.conn.cursor(cursor_factory = RealDictCursor)
        inspections = []

        INSPECTIONS_SEARCH = """
            SELECT *
            FROM ri_inspections
            WHERE restaurant_id = %s;
            """

        # get a list of inspection tuples
        try:
            cur.execute(INSPECTIONS_SEARCH,
                (restaurant_id, ))
            i = cur.fetchall()
        except (Exception, DatabaseError) as e:
            status = self.bad_request(e, cur, rollback=False, status=400)
            return status, inspections

        if i:
            for item in i:
                inspection = self.find_inspection(item['id'])[1]
                inspections.append(inspection)
        
        status = self.ok_request(cur, commit=False, status=200)

        return status, inspections


    def add_inspection_for_restaurant(self, inspection, restaurant):
        """
        Finds or creates the restaurant then inserts the inspection and
        associates it with the restaurant.
        """

        cur = self.conn.cursor(cursor_factory = RealDictCursor)
        restaurant_id = None

        RESTAURANT_SEARCH = """
            SELECT id 
            FROM ri_restaurants
            WHERE name = %s
            AND address = %s;
            """

        RESTAURANT_INSERT = """
            INSERT INTO ri_restaurants (name, facility_type, address, city, state, zip, location, clean)
            VALUES (%s, %s, %s, %s, %s, %s, point(%s), %s);
            """

        INSPECTION_SEARCH = """
            SELECT id 
            FROM ri_inspections
            WHERE id = %s;
            """

        INSPECTION_INSERT = """
            INSERT INTO ri_inspections (id, risk, inspection_date, inspection_type, results, violations, restaurant_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """

        try:
            cur.execute(RESTAURANT_SEARCH,
                (restaurant['name'], 
                 restaurant['address']))
            r = cur.fetchone()
        except (Exception, DatabaseError) as e:
            status = self.bad_request(e, cur, rollback=False, status=400)
            return status, restaurant_id

        try:
            cur.execute(INSPECTION_SEARCH,
                (inspection['id'],))
            i = cur.fetchone()
        except (Exception, DatabaseError) as e:
            status = self.bad_request(e, cur, rollback=False, status=400)
            return status, restaurant_id

        if not r and not i: 
            try:
                cur.execute(RESTAURANT_INSERT,
                    (restaurant['name'],
                     restaurant['facility_type'],
                     restaurant['address'],
                     restaurant['city'],
                     restaurant['state'],
                     restaurant['zip'],
                     restaurant['location'] if restaurant['location'] else None,
                     restaurant['clean']))
            except (Exception, DatabaseError) as e:
                status = self.bad_request(e, cur, rollback=True, status=400)
                return status, restaurant_id

            try:
                cur.execute(RESTAURANT_SEARCH,
                    (restaurant['name'], 
                     restaurant['address']))
                r = cur.fetchone()
            except (Exception, DatabaseError) as e:
                status = self.bad_request(e, cur, rollback=False, status=400)
                return status, restaurant_id

            try:
                cur.execute(INSPECTION_INSERT,
                    (inspection['id'],
                     inspection['risk'],
                     inspection['inspection_date'],
                     inspection['inspection_type'],
                     inspection['results'],
                     inspection['violations'],
                     r['id']))
            except (Exception, DatabaseError) as e:
                status = self.bad_request(e, cur, rollback=True, status=400)
                return status, restaurant_id

            status = self.ok_request(cur, commit=False, status=201)
            restaurant_id = r['id']

        else:
            if not i:
                try:
                    cur.execute(INSPECTION_INSERT,
                        (inspection['id'],
                         inspection['risk'],
                         inspection['inspection_date'],
                         inspection['inspection_type'],
                         inspection['results'],
                         inspection['violations'],
                         r['id']))
                except (Exception, DatabaseError) as e:
                    status = self.bad_request(e, cur, rollback=True, status=400)
                    return status, restaurant_id

            status = self.ok_request(cur, commit=False, status=200)
            restaurant_id = r['id']

        return status, restaurant_id


    def count_all_insp(self):
        '''
        Count the number of records in ri_inspections.
        '''

        cur = self.conn.cursor(cursor_factory = RealDictCursor)
        record = None

        COUNT = """
            SELECT count(id) as cnt
            FROM ri_inspections;
            """

        try:
            cur.execute(COUNT)
            r = cur.fetchone()
        except (Exception, DatabaseError) as e:
            status = self.bad_request(e, cur, rollback=False, status=400)
            return status, record

        record = r['cnt']
        status = self.ok_request(cur, commit=False, status=200)

        return status, record


    def bulk_loading(self, data):
        '''
        SELECT and INSERT entries from a bulk file.
        '''

        cur = self.conn.cursor(cursor_factory = RealDictCursor)

        CREATE_TEMP = """
            DROP TABLE IF EXISTS bulk_temp;
            CREATE TEMP TABLE bulk_temp (
                inspection_id varchar(16),
                name varchar(60) NOT NULL,
                aka_name varchar(60),
                facility_type varchar(50),
                risk varchar(30),
                address varchar(60),
                city varchar(30),
                state char(2),
                zip char(5),
                date date,
                inspection_type varchar(30),
                results varchar(30),
                violations text,
                latitude varchar(20),
                longitude varchar(20),
                location point,
                PRIMARY KEY (inspection_id)
                );
            """

        try: 
            cur.execute(CREATE_TEMP)
        except (Exception, DatabaseError) as e:
            return self.bad_request(e, cur, rollback=True, status=400)

        try:
            cur.copy_expert("COPY bulk_temp FROM STDIN CSV HEADER QUOTE '\"';" , data)
        except (Exception, DatabaseError) as e:
            return self.bad_request(e, cur, rollback=True, status=400)

        RESTAURANT_INSERT = """
            CREATE UNIQUE INDEX IF NOT EXISTS ri_restaurants_name_address_idx
            ON ri_restaurants(name, address);
            INSERT INTO ri_restaurants (name, facility_type, address, city, state, zip, location)
            SELECT name, facility_type, address, city, state, zip, location
            FROM bulk_temp
            ON CONFLICT (name, address) DO NOTHING;
            DROP INDEX IF EXISTS ri_restaurants_name_address_idx;
            """

        INSPECTION_INSERT = """
            INSERT INTO ri_inspections (id, risk, inspection_date, inspection_type, results, violations, restaurant_id)
            SELECT b.inspection_id, b.risk, b.date, b.inspection_type, b.results, b.violations, r.id
            FROM bulk_temp b
            JOIN ri_restaurants r
            ON r.name = b.name AND r.address = b.address
            ON CONFLICT (id) DO NOTHING;
            """

        try:
            cur.execute(RESTAURANT_INSERT)
        except (Exception, DatabaseError) as e:
            return self.bad_request(e, cur, rollback=True, status=400)

        try:
            cur.execute(INSPECTION_INSERT)
        except (Exception, DatabaseError) as e:
            return self.bad_request(e, cur, rollback=True, status=400)

        status = self.ok_request(cur, commit=True, status=200)

        return status


    def match_tweet(self, tweet):
        '''
        Receives a tweet and see if it matches a restaurant by name or location
        '''

        cur = self.conn.cursor(cursor_factory = RealDictCursor)
        matched = []

        try:
            tkey = tweet['tkey']
            tngram = tweet['ngram']
            tlat = tweet['lat']
            tlong = tweet['long']
        except KeyError as e:
            status = self.bad_request(e, cur, rollback=False, status=400)

        # create a bounding box to search for nearby points using the index
        if tlong and tlat:
            box = str(((tlong - 0.00302190, tlat - 0.00225001),
                (tlong + 0.00302190, tlat + 0.00225001)))
        else:
            box = None

        TWEET_MATCH = """
            SELECT id FROM ri_restaurants 
            WHERE UPPER(name) = ANY(ARRAY[%s])
            UNION
            SELECT id FROM ri_restaurants 
            WHERE box(%s) @> location;
            """        

        try:
            cur.execute(TWEET_MATCH,
                (tngram, box))
            r = cur.fetchall()
        except (Exception, DatabaseError) as e:
            status = self.bad_request(e, cur, rollback=True, status=400)
            return status, matched

        if not r:
            status = self.ok_request(cur, commit=False, status=200)
            return status, matched

        TWEET_INSERT = """
            INSERT INTO ri_tweetmatch (tkey, restaurant_id, match)

            (SELECT %s, id, 'both'::match_type FROM ri_restaurants 
            WHERE UPPER(name) = ANY(ARRAY[%s])
            INTERSECT
            SELECT %s, id, 'both'::match_type FROM ri_restaurants 
            WHERE box(%s) @> location)

            UNION

            (SELECT %s, id, 'name'::match_type FROM ri_restaurants 
            WHERE UPPER(name) = ANY(ARRAY[%s])
            EXCEPT
            SELECT %s, id, 'name'::match_type FROM ri_restaurants 
            WHERE box(%s) @> location)

            UNION

            (SELECT %s, id, 'geo'::match_type FROM ri_restaurants 
            WHERE box(%s) @> location
            EXCEPT
            SELECT %s, id, 'geo'::match_type FROM ri_restaurants 
            WHERE UPPER(name) = ANY(ARRAY[%s]))

            ON CONFLICT (tkey, restaurant_id) DO NOTHING;
            """

        try:
            cur.execute(TWEET_INSERT,
                    (tkey, tngram, tkey, box,
                     tkey, tngram, tkey, box,
                     tkey, box, tkey, tngram))
        except (Exception, DatabaseError) as e:
            status = self.bad_request(e, cur, rollback=True, status=400)
            return status, matched

        status = self.ok_request(cur, commit=True, status=200)
        for item in r:
            matched.append(item['id'])

        return status, matched


    def add_restaurants_index(self):
        '''
        Create two indexes for the restaurant tables if they don't already exist
        One for restaurant names and one for location.
        '''

        cur = self.conn.cursor(cursor_factory = RealDictCursor)

        CREATE_REST_NAME_IDX = """
            CREATE INDEX IF NOT EXISTS ri_restaurants_name_idx 
            ON ri_restaurants USING hash (UPPER(name));
            """

        CREATE_REST_LOC_IDX = """
            CREATE INDEX IF NOT EXISTS ri_restaurants_location_idx
            ON ri_restaurants USING gist (location);
            """

        try:
            cur.execute(CREATE_REST_NAME_IDX)
        except (Exception, DatabaseError) as e:
            return self.bad_request(e, cur, rollback=True, status=400)

        try:
            cur.execute(CREATE_REST_LOC_IDX)
        except (Exception, DatabaseError) as e:
            return self.bad_request(e, cur, rollback=True, status=400)

        status = self.ok_request(cur, commit=True, status=200)

        return status
        

    def get_tweets_by_insp(self, inspection_id):
        '''
        Look up tweets with a restaurant_id that matches with input inspections
        '''
        
        cur = self.conn.cursor(cursor_factory = RealDictCursor)
        tkeys = []
        status = None

        FIND_TWEETS_BY_INSP = """
            SELECT tkey 
            FROM ri_tweetmatch
            WHERE restaurant_id = (SELECT restaurant_id
                FROM ri_inspections
                WHERE id = %s);
            """

        try:
            cur.execute(FIND_TWEETS_BY_INSP, (inspection_id,))
            t = cur.fetchall()
        except (Exception, DatabaseError) as e:
            status = self.bad_request(e, cur, rollback=False, status=400)
            return status, tkeys

        status = self.ok_request(cur, commit=False, status=200)
        for item in t:
            tkeys.append(item['tkey'])

        return status, tkeys


    def find_linked_restaurants(self):
        '''
        Iterate through all not-clean restaurant records and compare with 
        other not-clean restaurant records using text distance algorithms.
        If multiple records are deemed similar enough according to text
        distance algorithms, output them as a dictionary of matches 
        (primary id = restaurant id with the longest name) and
        update the clean flag in ri_restaurants.
        '''

        cur = self.conn.cursor(cursor_factory = RealDictCursor)
        matches = []
        status = None

        DIRTY_RECORDS = """
            SELECT * FROM ri_restaurants 
            WHERE clean = false;
            """

        UPDATE_RECORDS = """
            UPDATE ri_restaurants
            SET clean = true 
            WHERE id = ANY(ARRAY[%s]);
            """

        try:
            cur.execute(DIRTY_RECORDS)
            r = cur.fetchall()
        except (Exception, DatabaseError) as e:
            status = self.bad_request(e, cur, rollback=False, status=400)
            return status, matches

        while len(r) >= 1:
            i = r[0]
            linked_dict = {'primary': i['id'], 'linked': [i['id']]}
            for j in r[1:]:
                if match_records.check_match(i, j):
                    linked_dict['linked'].append(j['id'])
                    if len(j['name']) > len(i['name']):
                        linked_dict['primary'] = j['id']

            try:
                cur.execute(UPDATE_RECORDS, (linked_dict['linked'],))
            except (Exception, DatabaseError) as e:
                status = self.bad_request(e, cur, rollback=True, status=400)
                return status, matches

            matches.append(linked_dict)

            try:
                cur.execute(DIRTY_RECORDS)
                r = cur.fetchall()
            except (Exception, DatabaseError) as e:
                status = self.bad_request(e, cur, rollback=False, status=400)
                return status, matches

        status = self.ok_request(cur, commit=False, status=200)

        return status, matches


    def find_and_update_linked_restaurants(self):
        '''
        After finding all similar restaurants, update ri_linked with pairs 
        of similar restaurants and update ri_restaurants with primary ids. 
        '''

        cur = self.conn.cursor(cursor_factory = RealDictCursor)
        status, matches = self.find_linked_restaurants()

        INSERT_LINKED_RECORDS = """
            INSERT INTO ri_linked (primary_rest_id, original_rest_id)
            VALUES
            (%s, %s);
            """

        UPDATE_INSPECTIONS = """
            UPDATE ri_inspections AS i
            SET restaurant_id  = l.primary_rest_id
            FROM ri_linked l 
            WHERE i.restaurant_id = l.original_rest_id;
            """

        for m in matches:
            for l in m['linked']:
                if m['primary'] != l:
                    try:
                        cur.execute(INSERT_LINKED_RECORDS, (m['primary'], l))
                    except (Exception, DatabaseError) as e:
                        status = self.bad_request(e, cur, rollback=True, status=400)
                        return status         

        try:
            cur.execute(UPDATE_INSPECTIONS)
        except (Exception, DatabaseError) as e:
            status = self.bad_request(e, cur, rollback=True, status=400)
            return status

        status = self.ok_request(cur, commit=True, status=200)

        return status


    def find_all_restaurants(self, inspection_id):
        '''
        Match the restaurant and any linked restaurants associated with an 
        input inspection. Receive an insepction id, return dictionary of 
        dictionaries of restaurant records.
        Cases include records that are dirty, unmatched, 
        a primary in a link, or an original in a link.
        '''

        cur = self.conn.cursor(cursor_factory = RealDictCursor)
        restaurants = None

        GET_FIRST_RESTAURANT = """
            SELECT id, name, facility_type, address, city, state, zip,
            location[1] latitude, location[0] longitude, clean
            FROM ri_restaurants
            WHERE id IN (SELECT restaurant_id FROM ri_inspections i
                         WHERE i.id = %s);
            """

        GET_PRIMARY_RESTAURANT = """
            SELECT id, name, facility_type, address, city, state, zip,
            location[1] latitude, location[0] longitude, clean
            FROM ri_restaurants r 
            WHERE r.id IN (SELECT primary_rest_id FROM ri_linked l
                           WHERE l.original_rest_id = %s)
            OR
                  r.id IN (SELECT primary_rest_id FROM ri_linked l
                           WHERE l.primary_rest_id = %s);
            """

        GET_LINKED_RESTAURANTS = """
            SELECT id, name, facility_type, address, city, state, zip,
            location[1] latitude, location[0] longitude, clean
            FROM ri_restaurants r
            WHERE r.id IN (SELECT original_rest_id FROM ri_linked l
                           WHERE l.primary_rest_id = %s);
            """

        try:
            cur.execute(GET_FIRST_RESTAURANT, (str(inspection_id),))
            first_restaurant = cur.fetchone()
        except (Exception, DatabaseError) as e:
            status = self.bad_request(e, cur, rollback=False, status=500)
            return status, restaurants

        if not first_restaurant:
            status = 404
            return status, restaurants
        elif not first_restaurant['clean']:
            return self.unmatched_restaurant(cur, first_restaurant)

        else:
            try:
                cur.execute(GET_PRIMARY_RESTAURANT,
                    (first_restaurant['id'], first_restaurant['id']))
                primary_restaurant = cur.fetchone()
            except (Exception, DatabaseError) as e:
                status = self.bad_request(e, cur, rollback=False, status=500)
                return status, restaurants

            if not primary_restaurant:
                return self.unmatched_restaurant(cur, first_restaurant)
            try:
                cur.execute(GET_LINKED_RESTAURANTS, (primary_restaurant['id'],))
                linked_restaurants = cur.fetchall()
            except (Exception, DatabaseError) as e:
                status = self.bad_request(e, cur, rollback=False, status=500)
                return status, restaurants

            restaurants = {
                'primary': primary_restaurant,
                'linked': linked_restaurants
                }

            status = self.ok_request(cur, commit=False, status=200)

            return status, restaurants


    def unmatched_restaurant(self, cur, restaurant):
        '''
        Helper funtion for find_all_restaurants to handle case when restaurant
        associated with an inspection is not linked to any other restaurants.
        '''

        status = self.ok_request(cur, commit=False, status=200)
        restaurants = {
            'primary': restaurant,
            'linked': []
            }

        return status, restaurants


    def find_linked_restaurants_fast(self):
        '''
        Iterate through all not-clean restaurant records with blocking and
        compare with other not-clean restaurant records using text distance
        algorithms. If multiple records are deemed similar enough according to text
        distance algorithms, output them as a dictionary of matches 
        (primary id = restaurant id with the longest name) and
        update the clean flag in ri_restaurants.
        '''

        cur = self.conn.cursor(cursor_factory = RealDictCursor)
        states = match_records.blocking(self.conn, cur)
        matches = []
        status = None

        DIRTY_RECORDS = """
            SELECT * FROM {}
            WHERE clean = false
            AND zip = %s;
            """

        UPDATE_TEMP_RECORDS = """
            UPDATE {}
            SET clean = true 
            WHERE id = ANY(ARRAY[%s])
            AND zip = %s;
            """

        for s in states:
            for z in states[s]: 
                try:
                    cur.execute(sql.SQL(DIRTY_RECORDS).format(sql.Identifier(s)), (z,))
                    r = cur.fetchall()
                except (Exception, DatabaseError) as e:
                    status = self.bad_request(e, cur, rollback=False, status=400)
                    return status, matches

                while len(r) >= 1:
                    i = r[0]
                    a_i = str(i['address']).split(' ')
                    n_i, s_i = (a_i[0], a_i[1:])
                    linked_dict = {'primary_name': i['name'],
                                   'types': {i['facility_type']: 1},
                                   'primary_type': i['facility_type'],
                                   'primary_street': s_i,
                                   'street_nums': {n_i: 1}, 
                                   'primary_num': n_i,
                                   'cities': {i['city']: 1},  
                                   'primary_city': i['city'],                             
                                   'primary_state': s[0:2],
                                   'primary_zip': i['zip'],
                                   'primary_loc': i['location'],
                                   'linked': [i['id']]}

                    for j in r[1:]:
                        if match_records.check_match_fast(i, j):
                            a_j = str(j['address']).split(' ')
                            n_j, s_j = (a_j[0], a_j[1:])
                            linked_dict['linked'].append(j['id'])

                            linked_dict['primary_num'] = match_records.find_most_common(
                                linked_dict['street_nums'], n_j, linked_dict['primary_num'])
                                
                            linked_dict['primary_type'] = match_records.find_most_common(
                                linked_dict['types'], j['facility_type'], linked_dict['primary_type'])

                            linked_dict['primary_city'] = match_records.find_most_common(
                                linked_dict['cities'], j['city'], linked_dict['primary_city'])

                            if len(j['name']) > len(linked_dict['primary_name']):
                                linked_dict['primary_name'] = j['name']
                            if len(s_j) > len(linked_dict['primary_street']):
                                linked_dict['primary_street'] = s_j
                            if len(j['location']) > len(linked_dict['primary_loc']):
                                linked_dict['primary_loc'] = j['location']

                    try:
                        cur.execute(sql.SQL(UPDATE_TEMP_RECORDS).format(
                            sql.Identifier(s)), (linked_dict['linked'], z))
                    except (Exception, DatabaseError) as e:
                        status = self.bad_request(e, cur, rollback=True, status=400)
                        return status, matches

                    linked_dict['primary_add'] = linked_dict['primary_num'] + ' ' + ' '.join(linked_dict['primary_street'])[:-1]
                    matches.append(linked_dict)

                    try:
                        cur.execute(sql.SQL(DIRTY_RECORDS).format(sql.Identifier(s)), (z,))
                        r = cur.fetchall()
                    except (Exception, DatabaseError) as e:
                        status = self.bad_request(e, cur, rollback=False, status=400)
                        return status, matches

        status = self.ok_request(cur, commit=False, status=200)

        return status, matches


    def find_and_update_linked_restaurants_fast(self):
        '''
        After finding all similar restaurants, update ri_linked with pairs 
        of similar restaurants and update ri_restaurants with primary ids. 
        '''

        cur = self.conn.cursor(cursor_factory = RealDictCursor)
        status, matches = self.find_linked_restaurants_fast()

        INSERT_PRIMARY_RECORD = """
            INSERT INTO ri_restaurants (name, facility_type, address, zip, city, state, location, clean)
            VALUES
            (%s, %s, %s, %s, %s, %s, %s, true);
            """

        INSERT_LINKED_RECORDS = """
            INSERT INTO ri_linked (primary_rest_id, original_rest_id)
            VALUES
            (%s, %s);
            """

        UPDATE_INSPECTIONS = """
            UPDATE ri_inspections AS i
            SET restaurant_id  = l.primary_rest_id
            FROM ri_linked l 
            WHERE i.restaurant_id = l.original_rest_id;
            """

        UPDATE_RESTAURANTS = """
            UPDATE ri_restaurants
            SET clean = true 
            WHERE id IN (SELECT primary_rest_id 
                         FROM ri_linked);
            """

        for m in matches:
            if len(m['linked']) >= 2:
                try:
                    cur.execute(INSERT_PRIMARY_RECORD, (m['primary_name'],
                                                        m['primary_type'],
                                                        m['primary_add'],
                                                        m['primary_zip'],
                                                        m['primary_city'],
                                                        m['primary_state'],
                                                        m['primary_loc']))

                    cur.execute("""SELECT max(id) FROM ri_restaurants;""")
                    idx = cur.fetchone()['max']
                except (Exception, DatabaseError) as e:
                    status = self.bad_request(e, cur, rollback=True, status=400)
                    return status  

                for l in m['linked']:
                    try:
                        cur.execute(INSERT_LINKED_RECORDS, (idx, l))
                    except (Exception, DatabaseError) as e:
                        status = self.bad_request(e, cur, rollback=True, status=400)
                        return status         

        try:
            cur.execute(UPDATE_INSPECTIONS)
        except (Exception, DatabaseError) as e:
            status = self.bad_request(e, cur, rollback=True, status=400)
            return status

        try:
            cur.execute(UPDATE_RESTAURANTS)
        except (Exception, DatabaseError) as e:
            status = self.bad_request(e, cur, rollback=True, status=400)
            return status, matches


        status = self.ok_request(cur, commit=True, status=200)

        return status

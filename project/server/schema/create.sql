DROP TABLE IF EXISTS ri_inspections;
DROP TABLE IF EXISTS ri_tweetmatch;
DROP TYPE IF EXISTS match_type;
DROP TABLE IF EXISTS ri_restaurants;
CREATE TYPE match_type AS ENUM ('geo', 'name', 'both');

CREATE TABLE ri_restaurants (
    id serial,
    name varchar(100) NOT NULL,
    facility_type varchar(50),
    address varchar(60),
    city varchar(30),
    state char(2),
    zip char(5),
    location point,
    clean boolean DEFAULT FALSE,
    PRIMARY KEY (id)
);

CREATE TABLE ri_inspections (
    id varchar(16),
    risk varchar(50),
    inspection_date date,
    inspection_type varchar(50),
    results varchar(50),
    violations text,
    restaurant_id int NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (restaurant_id) REFERENCES ri_restaurants
);

CREATE TABLE ri_tweetmatch (
    tkey varchar(50),
    restaurant_id int,
    match match_type NOT NULL,
    PRIMARY KEY (tkey,restaurant_id),
    FOREIGN KEY (restaurant_id) REFERENCES ri_restaurants
);


CREATE TABLE ri_linked (
    primary_rest_id int,
    original_rest_id int,
    PRIMARY KEY (primary_rest_id, original_rest_id),
    FOREIGN KEY (primary_rest_id) REFERENCES ri_restaurants,
    FOREIGN KEY (original_rest_id) REFERENCES ri_restaurants
);


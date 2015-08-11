CREATE KEYSPACE IF NOT EXISTS adunblockevent WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};
USE adunblockevent;
CREATE TABLE IF NOT EXISTS detect (
    userid uuid PRIMARY KEY,
    partner text,
    apikey uuid,
    eventtype text,
    eventvalue int,
    eventdate timestamp,
    useragent text
);


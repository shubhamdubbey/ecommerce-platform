CREATE USER order_user        WITH PASSWORD 'order_pass';
CREATE USER inventory_user    WITH PASSWORD 'inventory_pass';
CREATE USER payment_user      WITH PASSWORD 'payment_pass';
CREATE USER notification_user WITH PASSWORD 'notification_pass';
CREATE USER query_user        WITH PASSWORD 'query_pass';

CREATE DATABASE order_db        OWNER order_user;
CREATE DATABASE inventory_db    OWNER inventory_user;
CREATE DATABASE payment_db      OWNER payment_user;
CREATE DATABASE notification_db OWNER notification_user;
CREATE DATABASE query_db        OWNER query_user;

REVOKE ALL ON DATABASE order_db        FROM PUBLIC;
REVOKE ALL ON DATABASE inventory_db    FROM PUBLIC;
REVOKE ALL ON DATABASE payment_db      FROM PUBLIC;
REVOKE ALL ON DATABASE notification_db FROM PUBLIC;
REVOKE ALL ON DATABASE query_db        FROM PUBLIC;
CREATE TABLE fact_price_history
(
  id integer unique,
  dateid integer not null,
  currencyid integer not null,
  priceusd decimal not null,
  primary key(id,dateid,currencyid),
  foreign key(dateid) references dimension_date(dateid),
  foreign key(currencyid) references dimension_currencies(currencyid)
);
CREATE TABLE dimension_date
(
  dateid integer unique,
  dateday integer not null,
  datemonth integer not null,
  dateyear integer not null,
  primary key(dateid)
);

CREATE TABLE dimension_currencies
(
  currencyid integer unique,
  currencyname varchar(50),
  primary key(currencyid)
);

// Load tables into Redshift
COPY dimension_date(dateid,dateday,datemonth,dateyear)
from
  's3://stuffalexylaura/dimension_date_export.csv'
REGION 'eu-west-1'
CREDENTIALS
'aws_access_key_id=XXXXX;aws_secret_access_key=YYYYY'
csv;

COPY dimension_currencies(currencyname, currencyid)
from
  's3://stuffalexylaura/dimension_currencies_export.csv'
REGION 'eu-west-1'
CREDENTIALS
'aws_access_key_id=XXXXX;aws_secret_access_key=YYYYY'
csv;

COPY fact_price_history(id,dateid,currencyid,priceusd)
from
  's3://stuffalexylaura/fact_table_pricing_history.csv'
REGION 'eu-west-1'
CREDENTIALS
'aws_access_key_id=XXXXX;aws_secret_access_key=YYYYY'
csv;
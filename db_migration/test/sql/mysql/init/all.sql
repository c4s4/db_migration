DROP TABLE IF EXISTS pet;

CREATE TABLE pet (
  id INTEGER NOT NULL AUTO_INCREMENT,
  name VARCHAR(20) NOT NULL,
  age INTEGER NOT NULL,
  species VARCHAR(10) NOT NULL,
  PRIMARY KEY  (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

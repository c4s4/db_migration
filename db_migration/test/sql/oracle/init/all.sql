-- clean schema
BEGIN
  FOR cur_rec IN (SELECT object_name, object_type
                  FROM   user_objects
                  WHERE  object_type IN ('TABLE', 'VIEW', 'PACKAGE', 'PROCEDURE', 'FUNCTION', 'SEQUENCE')) LOOP
    BEGIN
      IF cur_rec.object_name = 'INSTALL_' OR cur_rec.object_name = 'SCRIPTS_' OR
         cur_rec.object_name = 'INSTALL_SEQUENCE' THEN
        CONTINUE;
      end IF;
      IF cur_rec.object_type = 'TABLE' THEN
        EXECUTE IMMEDIATE 'DROP ' || cur_rec.object_type || ' "' || cur_rec.object_name || '" CASCADE CONSTRAINTS';
      ELSE
        EXECUTE IMMEDIATE 'DROP ' || cur_rec.object_type || ' "' || cur_rec.object_name || '"';
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.put_line('FAILED: DROP ' || cur_rec.object_type || ' "' || cur_rec.object_name || '"');
    END;
  END LOOP;
END;
/
-- create table PET
CREATE TABLE PET (
  ID NUMBER(10) NOT NULL,
  NAME VARCHAR(20) NOT NULL,
  AGE NUMBER(2) NOT NULL,
  SPECIES VARCHAR(10) NOT NULL,
  PRIMARY KEY  (id)
);
CREATE SEQUENCE PET_SEQUENCE
  START WITH 1
  INCREMENT BY 1
  CACHE 100;

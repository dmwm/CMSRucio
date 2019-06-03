CREATE OR REPLACE TRIGGER ACCOUNT_AVOID_UPDATE_DELETE BEFORE UPDATE OR DELETE OF ACCOUNT on ACCOUNTS
  FOR EACH ROW
BEGIN
    IF (:OLD.account <> :NEW.account) THEN
	 raise_application_error(-20101,'Update or delete operation on the ACCOUNT column values is not allowed!');
    END IF;
END;
/
CREATE OR REPLACE TRIGGER CHECK_DID_UNIQUENESS
AFTER INSERT on DIDS
  FOR EACH ROW
DECLARE
    -- PRAGMA AUTONOMOUS_TRANSACTION;
    n number	    := 0;
BEGIN
    -- ver 1.1, The AND did_type != :NEW.did_type is removed so that Oracle does NOT visit the table, but resolves the query from the index only
    BEGIN
	SELECT SUM(1) INTO n FROM DELETED_DIDS
	WHERE	   scope = :NEW.scope
	       AND name = :NEW.name;
	IF (n >= 1) THEN
	   RAISE_APPLICATION_ERROR  (-20101, 'Primary key constraint DELETED_DIDS_PK violated');
	END IF;
    EXCEPTION
	WHEN NO_DATA_FOUND THEN NULL;
    END ;
END;
/
CREATE OR REPLACE TRIGGER MIGRATE_DELETED_DID
AFTER DELETE on DIDS
 FOR EACH ROW
BEGIN
       MERGE INTO DELETED_DIDS D
       USING DUAL
       ON (D.scope = :OLD.SCOPE and D.name = :OLD.NAME)
       WHEN NOT MATCHED THEN
       INSERT (SCOPE, NAME, ACCOUNT, DID_TYPE,
	       IS_OPEN, MONOTONIC, HIDDEN, OBSOLETE, COMPLETE, IS_NEW,
	       AVAILABILITY, SUPPRESSED, BYTES, LENGTH, MD5, ADLER32,
	       EXPIRED_AT, DELETED_AT, UPDATED_AT, CREATED_AT, EVENTS, GUID, PROJECT,
	       DATATYPE, RUN_NUMBER, STREAM_NAME, PROD_STEP, VERSION, CAMPAIGN,
	       task_id, panda_id, lumiblocknr, provenance, phys_group,
	       transient, accessed_at, closed_at, purge_replicas, is_archive, constituent) VALUES
	       (:OLD.SCOPE, :OLD.NAME, :OLD.ACCOUNT, :OLD.DID_TYPE,
		:OLD.IS_OPEN, :OLD.MONOTONIC, :OLD.HIDDEN, :OLD.OBSOLETE, :OLD.COMPLETE, :OLD.IS_NEW,
		:OLD.AVAILABILITY, :OLD.SUPPRESSED, :OLD.BYTES, :OLD.LENGTH, :OLD.MD5, :OLD.ADLER32,
		:OLD.EXPIRED_AT, sys_extract_utc(systimestamp), :OLD.UPDATED_AT, :OLD.CREATED_AT, :OLD.EVENTS,
		:OLD.GUID, :OLD.PROJECT,
		:OLD.DATATYPE, :OLD.RUN_NUMBER, :OLD.STREAM_NAME, :OLD.PROD_STEP, :OLD.VERSION, :OLD.CAMPAIGN,
		:OLD.task_id, :OLD.panda_id, :OLD.lumiblocknr, :OLD.provenance, :OLD.phys_group,
		:OLD.transient, :OLD.accessed_at, :OLD.closed_at, :OLD.purge_replicas,
		 :OLD.is_archive, :OLD.constituent);
END;
/
CREATE OR REPLACE TRIGGER SCOPE_AVOID_UPDATE_DELETE
BEFORE UPDATE OR DELETE OF SCOPE on SCOPES
  FOR EACH ROW
BEGIN
    IF (:OLD.scope <> :NEW.scope) THEN
	 raise_application_error(-20102,'Update or delete on the SCOPE column values is not allowed!');
    END IF;
END;
/

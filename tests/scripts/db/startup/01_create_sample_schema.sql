ALTER SESSION SET CONTAINER=ORCLPDB1;
@?/demo/schema/human_resources/hr_main.sql hr users temp log
GRANT DBA TO HR;

BEGIN
  DBMS_SCHEDULER.CREATE_JOB (
    job_name        => 'HR.DO_NOTHING_JOB',
    job_type        => 'PLSQL_BLOCK',
    job_action      => 'BEGIN NULL; END;',
    start_date      => SYSTIMESTAMP,
    repeat_interval => 'FREQ=DAILY;BYHOUR=12;BYMINUTE=0;BYSECOND=0',
    enabled         => TRUE,
    comments        => 'This job does nothing');
END;
/
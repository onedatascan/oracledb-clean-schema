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


DECLARE
  jobno BINARY_INTEGER;
BEGIN
  SYS.DBMS_IJOB.SUBMIT(
    job       => 9999999,
    luser     => 'HR',
    puser     => 'HR',
    cuser     => 'HR',
    what      =>'BEGIN null; END;',
    next_date => sysdate + 1/24/60,
    interval  => 'SYSDATE + 1',
    broken    => false,
    nlsenv    => '',
    env       => ''
  );
END;
/

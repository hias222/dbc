--------------------------
----query v$session
--- Version 0.4 2013-09-23
---------------------------
--- session variables
--
-- dont forget grant execute on sys.dbms_lock to usr1


DECLARE

  -- collections for sessions
  type tmp_sestab IS TABLE OF v$session%rowtype INDEX BY pls_integer;
  type sestab IS TABLE OF v$session%rowtype INDEX BY VARCHAR2(50);
  type round_sestab IS TABLE OF v$session%rowtype INDEX BY VARCHAR2(50);
  -- other variables
  g_ash                  sys.dbms_debug_vc2coll := NEW
                                                   sys.dbms_debug_vc2coll();
  g_mysid                NUMBER;
  g_sum_samples          NUMBER;
  g_sample_number        NUMBER;
  g_round_number_max     NUMBER;
  g_sample_waitms        NUMBER;
  g_only_active_sessions VARCHAR2(6);
  g_sessions             sestab;
  g_empty_sessions       sestab;
  g_output               VARCHAR2(10);
  v_inst_seq_nr          NUMBER;
  ----------------------------
  ---- cursor for output
  ------------------------
  type refCursor IS REF CURSOR;
  c_sessions sys_refCursor;
   -----
  --functions
  -------
  FUNCTION sitem(p IN VARCHAR2) RETURN VARCHAR2 AS
  BEGIN
    --   RETURN '<' || TRANSLATE(p, '<>', '__') || '>';
    RETURN '"' || TRANSLATE(p, '"', '_') || '";';
  END; -- sitem varchar2
  FUNCTION sitem(p IN NUMBER) RETURN VARCHAR2 AS
  BEGIN
    --RETURN '<' || TO_CHAR(p) || '>';
    RETURN '"' || TO_CHAR(p) || '";';
  END; -- sitem number
  FUNCTION sitem(p IN DATE) RETURN VARCHAR2 AS
  BEGIN
    --RETURN '<' || TO_CHAR(p, 'YYYY-MM-DD HH24:MI:SS') || '>';
    RETURN '"' || TO_CHAR(p, 'YYYY-MM-DD HH24:MI:SS') || '";';
  END; -- sitem date
  ---------------------------
  --reset_ash
  ----------------------
  PROCEDURE reset_ash IS
  BEGIN
    --g_ash_samples_taken := 0;
    -- clear g_ash
    g_ash := NEW sys.dbms_debug_vc2coll();
  END; -- reset_ash
  ---------------------------
  --get_users Session
  ----------------------
  PROCEDURE get_userses(g_sample_number NUMBER,
                        p_samplecount   NUMBER,
                        p_samplewaitms  NUMBER) IS
    tmp_sessions tmp_sestab;
    g_sess_round round_sestab;
    ash_i        VARCHAR2(50);
    g_count_rwos NUMBER;
  BEGIN
    g_count_rwos := 1;
    FOR g IN 1 .. p_samplecount LOOP
      SELECT
      /*+ unnest */
      /* get_sessi/on_list:2 */
       * bulk collect
        INTO tmp_sessions
        FROM v$session s
       WHERE 1=1 --type = 'USER' 
          AND s.sid != g_mysid;
      
      --g_sessions := g_empty_sessions;
      FOR i IN 1 .. tmp_sessions.count LOOP
        --g_sessions(tmp_sessions(i).inst_id || ',' || tmp_sessions(i).sid) := tmp_sessions(i);
        --g_sessions(g_ash_samples_taken || ',' || g_line) := tmp_sessions(i);
        g_sess_round(tmp_sessions(i).sid || tmp_sessions(i).sql_id || tmp_sessions(i).prev_sql_id || tmp_sessions(i).sql_exec_id || tmp_sessions(i).prev_exec_id) := tmp_sessions(i);
      END LOOP;
      --- waiting
      sys.dbms_lock.sleep(p_samplewaitms / 1000);
    END LOOP;
    ---map to g_Sessions
    g_sessions := g_empty_sessions;
    ash_i      := g_sess_round.first;
    WHILE ash_i IS NOT NULL LOOP
      g_count_rwos := g_count_rwos + 1;
      g_sessions(trim(to_char(g_sample_number,'00000')) || trim(to_char(g_count_rwos,'000000'))) := g_sess_round(ash_i);
      ash_i := g_sess_round.next(ash_i);
    END LOOP;
  END; -- get_sessions
  ---------------------------
  -- output data
  ----------------------
  PROCEDURE output(p_txt IN VARCHAR2) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    IF (g_output = 'out') THEN
      dbms_output.put_line(p_txt);
    END IF;
  
    -- */
  END; -- output

  
  /*---------------------------------------------------
  -- proc for getting ash style samples from gv$session
  ---------------------------------------------------*/
  PROCEDURE extract_ash IS
    ash_i VARCHAR2(30);
    s     v$session%rowtype;
  BEGIN
    -- keep track how many times we sampled gv$session so we could calculate averages later on
    -- g_ash_samples_taken := g_ash_samples_taken + 1;
    --output('g_sessions.count='||g_sessions.count);
    ash_i := g_sessions.first;
    WHILE ash_i IS NOT NULL LOOP
      s := g_sessions(ash_i);
      IF
      -- active, on cpu
      --(s.status = 'ACTIVE') OR (g_get_all ='true')
       (s.status = 'ACTIVE' AND s.state != 'WAITING') OR -- active, waiting for non-idle wait
       (s.status = 'ACTIVE' AND s.state = 'WAITING' AND
       s.wait_class != 'Idle') OR (g_only_active_sessions != 'true') THEN
        -- output('extract_ash: i='||i||' sid='||s.sid||' hv='||s.sql_hash_value||' sqlid='||s.sql_id);
        -- if not actually waiting for anything, clear the past wait event details
        IF s.state != 'WAITING' THEN
          s.state      := 'ON CPU';
          s.event      := 'ON CPU';
          s.wait_class := 'ON CPU'; --TODO: What do we need to do for 9i here?
          s.p1         := NULL;
          s.p2         := NULL;
          s.p3         := NULL;
        END IF;
        g_ash.extend;
        -- max length 1000 bytes (due to dbms_debug_vc2coll)
        g_ash(g_ash.count) := SUBSTR(sitem(ash_i) -- 1  --name
                                     || sitem(s.sid) --  2
                                     || sitem(s.serial#) --  3
                                     || sitem(s.audsid) --  4
                                     || sitem(s.username) --  5  -- 30 bytes
                                     || sitem(s.machine) --  6  -- 64 bytes
                                     || sitem(s.terminal) --  7  -- 30 bytes
                                     || sitem(s.osuser) --  8  -- 30 bytes
                                     || sitem(s.program) --  9  -- 48 bytes
                                     || sitem(s.event) --  10  -- 64 bytes
                                     || sitem(s.wait_class) --  11  -- 64 bytes, 10g+
                                     || sitem(s.state) --  12
                                     || sitem(s.sql_hash_value) -- 13
                                     || sitem(s.sql_id) -- 14   -- 10g+
                                     || sitem(s.sql_child_number) -- 15   -- 10g+
                                     || sitem(s.sql_exec_start) -- 16   -- 10g+
                                     || sitem(s.prev_sql_id) -- 17   -- 10g+
                                     || sitem(s.PREV_CHILD_NUMBER) -- 18   -- 10g+
                                     --PREV_CHILD_NUMBER
                                     || sitem(s.prev_exec_start) -- 18   -- 10g+
                                     || sitem(s.plsql_entry_object_id) -- 19
                                     || sitem(s.plsql_entry_subprogram_id) -- 20
                                     || sitem(s.plsql_object_id) -- 21
                                     || sitem(s.plsql_subprogram_id) -- 22
                                     || sitem(s.module) -- 23  -- 48 bytes
                                     || sitem(s.action) -- 24  -- 32 bytes
                                     || sitem(s.client_identifier) -- 25  -- 64 bytes
                                     || sitem(s.client_info) -- 26  -- 64 bytes
                                     || sitem(s.service_name) -- 27  -- 64 bytes, 10g+
                                     || sitem(s.ecid) -- 28  -- 64 bytes, 10g+
                                     || sitem(s.status) --  29
                                     || sitem(to_number(s.sql_exec_id) - v_inst_seq_nr) --30
                                     || sitem (to_number(s.prev_exec_id) - v_inst_seq_nr) --31
                                     --
                                     --Indeed, it looks like the 25th bit (2^24) is always pre-set to 1, while
                                     --the least significant 24 bits represent how many times this SQL ID has been executed in an instance
                                     --(I have tested this with a loop � the 24 least significant bits do get used fully for representing
                                     --the SQL ID�s execution count in the instance and once it reaches 0xFFFFFF � or 0x1FFFFFF with that pre-set 25th bit,
                                     --it wraps to 0�1000000 � the 25th bit still remaining set!). So the SQL_EXEC_ID can reliably only track 2^24 �
                                     --1 SQL executions in an instance and then the counter wraps to beginning. This is why you should include SQL_EXEC_START
                                     --(date datatype with 1 sec precision) column in your performance monitoring queries as well, to distinguish between
                                     --SQL executions with a colliding SQL_EXEC_ID. As long as you�re executing your SQL statement less
                                     --than 16.7 million times per second per instance, this should be fine :-)
                                     --|| sitem(TO_CHAR(s.prev_exec_id,'XXXXXXXX'))
                                     --TO_CHAR(sql_exec_id,'XXXXXXXX')
                                    ,
                                     1,
                                     1000);
        --output( g_ash(g_ash.count));
      END IF; -- sample is of an active session
      ash_i := g_sessions.next(ash_i);
    END LOOP;
  EXCEPTION
    WHEN no_data_found THEN
      dbms_output.put_line('error in extract_ash(): no_data_found for item ');
  END; -- extract_ash
  ---
 
  --- start

BEGIN
  --  BEGIN
  --    EXECUTE immediate 'ALTER SESSION SET NLS_DATE_FORMAT = ''YYYY-MM-DD HH24:MI:SS.SSSSS''';
  --    EXECUTE immediate 'alter session set nls_timestamp_format = ''YYYY-MM-DD HH24:MI:SS.SSSSS''';
  --  END;
  SELECT sid INTO g_mysid FROM v$mystat WHERE rownum = 1;

  --output out or trace or file
  -- if file please configure directory SESSION_DATA 

  g_output := 'out';
  
  -- for tracing own session
  --g_mysid             := 1;
  g_only_active_sessions := 'false';
  ------------------------------
  -- change this
  ---------------------------
  -- Calculation
  -- 250 ms * 20 rounds * 10 samples * 10 writes = 500s running script
  ----------------------------------
  -- after g_sum_samples number of files writes
  -- the query stops
  g_sum_samples := 1;
  -- after g_sample_number 
  -- transfer to output or file writing
  -- everything is cleared
  g_sample_number := 1;
  -- after g_round_number_max value all sessions are snapped
  -- the v$session is queried this times, after that number sessions are stored 
  -- and it starts empty
  g_round_number_max := ?;
  -- wait time in between two rounds in ms
  g_sample_waitms := ?;
  --------------------------------------
 
  IF (g_output = 'trace') THEN
    dbms_output.put_line('find it e.g. /opt/oracle/11.2/diag/rdbms/elvndb/ELVNDB/trace/ELVNDB_ora_xxxx.trc');
  END IF;
  ------ get instance_number
  select to_number(to_char((instance_number*1000000)),'XXXXXXX') hex_instance into v_inst_seq_nr from v$instance;
  
  ------------------------------------
  -- here we start
  
    FOR c IN 1 .. g_sample_number LOOP
      BEGIN
        get_userses(c, g_round_number_max, g_sample_waitms);
        extract_ash;
      END;
    END LOOP;
    --return c_sessions;
    --out_session_list();
    OPEN c_sessions FOR
          SELECT to_char(column_value) rec
            FROM TABLE(CAST(g_ash AS sys.dbms_debug_vc2coll));
    ? := c_sessions;
    --reset_ash();
  
END;


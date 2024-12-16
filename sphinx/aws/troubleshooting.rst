Troubleshooting AWS
***************************

Lambda
===================

If a Lambda function consistently fails, logging "Killed", the timeout period (which
defaults to 3 seconds) may be too short.

Redshift
===================

Gotchas
-------------

Granting CREATE on a database to a role does not grant privileges to schemas or tables
already in that database.



Useful Queries
------------------
List errors::

    SELECT process, errcode, linenum as line, error,
        TRIM(error) AS err
        FROM stl_error;

List tables::

    SHOW TABLES FROM SCHEMA dev.public;

List all and current user::

    SELECT usesysid AS user_id,
           usename AS username,
           usecreatedb AS db_create,
           usesuper AS is_superuser,
           valuntil AS password_expiration
    FROM pg_user
    ORDER BY user_id

    SELECT CURRENT_USER;

List user permissions::

    SELECT
        u.usename,
        s.schemaname,
        has_schema_privilege(u.usename,s.schemaname,'create') AS user_has_select_permission,
        has_schema_privilege(u.usename,s.schemaname,'usage') AS user_has_usage_permission
    FROM
        pg_user u
    CROSS JOIN
        (SELECT DISTINCT schemaname FROM pg_tables) s
    WHERE
        u.usename = 'IAMR:specnet_workflow_role'
        AND s.schemaname = 'public';

    SELECT
        u.usename,
        s.schemaname,
        has_schema_privilege(u.usename,s.schemaname,'create') AS user_has_select_permission,
        has_schema_privilege(u.usename,s.schemaname,'usage') AS user_has_usage_permission
    FROM
        pg_user u
    CROSS JOIN
        (SELECT DISTINCT schemaname FROM pg_tables) s
    WHERE
        u.usename IN ('IAM:aimee.stewart', 'IAMR:specnet_workflow_role')
        AND s.schemaname IN ('public', 'redshift_spectrum');


    SELECT has_database_privilege('IAMR:specnet_workflow_role', 'dev', 'temp');


    SELECT
    u.usename,
    t.schemaname||'.'||t.tablename,
    has_table_privilege(u.usename,t.tablename,'select') AS user_has_select_permission,
    has_table_privilege(u.usename,t.tablename,'insert') AS user_has_insert_permission,
    has_table_privilege(u.usename,t.tablename,'update') AS user_has_update_permission,
    has_table_privilege(u.usename,t.tablename,'delete') AS user_has_delete_permission,
    has_table_privilege(u.usename,t.tablename,'references') AS user_has_references_permission
FROM
    pg_user u
CROSS JOIN
    pg_tables t
WHERE
    u.usename = 'IAMR:specnet_workflow_role'
    AND t.tablename = 'myTableName'


Get last error message::

    -- Get last error message
    SELECT query_id, start_time, line_number, column_name, column_type, error_message
        FROM sys_load_error_detail ORDER BY start_time DESC;
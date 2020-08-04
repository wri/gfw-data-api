# https://www.postgresql.org/docs/11/functions-admin.html#FUNCTIONS-ADMIN-SET-TABLE
configuration_settings_functions = ["current_setting", "set_config"]

# https://www.postgresql.org/docs/11/functions-admin.html#FUNCTIONS-ADMIN-SIGNAL-TABLE
server_signaling_functions = [
    "pg_cancel_backend",
    "pg_reload_conf",
    "pg_rotate_logfile",
    "pg_terminate_backend",
]


# https://www.postgresql.org/docs/11/functions-admin.html#FUNCTIONS-ADMIN-BACKUP-TABLE
backup_control_functions = [
    "pg_create_restore_point",
    "pg_current_wal_flush_lsn",
    "pg_current_wal_insert_lsn",
    "pg_current_wal_lsn",
    "pg_start_backup",
    "pg_stop_backup",
    "pg_is_in_backup",
    "pg_backup_start_time",
    "pg_switch_wal",
    "pg_walfile_name",
    "pg_walfile_name_offset",
    "pg_wal_lsn_diff",
]

# https://www.postgresql.org/docs/11/functions-admin.html#FUNCTIONS-RECOVERY-INFO-TABLE
recovery_information_functions = [
    "pg_is_in_recovery",
    "pg_last_wal_receive_lsn",
    "pg_last_wal_replay_lsn",
    "pg_last_xact_replay_timestamp",
]


# https://www.postgresql.org/docs/11/functions-admin.html#FUNCTIONS-RECOVERY-CONTROL-TABLE
recovery_control_functions = [
    "pg_is_wal_replay_paused",
    "pg_wal_replay_pause",
    "pg_wal_replay_resume",
]


# https://www.postgresql.org/docs/11/functions-admin.html#FUNCTIONS-SNAPSHOT-SYNCHRONIZATION-TABLE
snapshot_synchronization_functions = ["pg_export_snapshot"]


# https://www.postgresql.org/docs/11/functions-admin.html#FUNCTIONS-REPLICATION-TABLE
replication_sql_functions = [
    "pg_create_physical_replication_slot",
    "pg_drop_replication_slot",
    "pg_create_logical_replication_slot",
    "pg_logical_slot_get_changes",
    "pg_logical_slot_peek_changes",
    "pg_logical_slot_get_binary_changes",
    "pg_logical_slot_peek_binary_changes",
    "pg_replication_slot_advance",
    "pg_replication_origin_create",
    "pg_replication_origin_drop",
    "pg_replication_origin_oid",
    "pg_replication_origin_session_setup",
    "pg_replication_origin_session_reset",
    "pg_replication_origin_session_is_setup",
    "pg_replication_origin_session_progress",
    "pg_replication_origin_xact_setup",
    "pg_replication_origin_xact_reset",
    "pg_replication_origin_advance",
    "pg_replication_origin_progress",
    "pg_logical_emit_message",
]

# https://www.postgresql.org/docs/11/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE
database_object_size_functions = [
    "pg_column_size",
    "pg_database_size",
    "pg_database_size",
    "pg_indexes_size",
    "pg_relation_size",
    "pg_relation_size",
    "pg_size_bytes",
    "pg_size_pretty",
    "pg_size_pretty",
    "pg_table_size",
    "pg_tablespace_size",
    "pg_tablespace_size",
    "pg_total_relation_size",
]

# https://www.postgresql.org/docs/11/functions-admin.html#FUNCTIONS-ADMIN-DBLOCATIONd
database_object_location_functions = [
    "pg_relation_filenode",
    "pg_relation_filepath",
    "pg_filenode_relation",
]


# https://www.postgresql.org/docs/11/functions-admin.html#FUNCTIONS-ADMIN-COLLATION
collation_management_functions = [
    "pg_collation_actual_version",
    "pg_import_system_collations",
]


# https://www.postgresql.org/docs/11/functions-admin.html#FUNCTIONS-ADMIN-INDEX-TABLE
index_maintenance_functions = [
    "brin_summarize_new_values",
    "brin_summarize_range",
    "brin_desummarize_range",
    "gin_clean_pending_list",
]

# https://www.postgresql.org/docs/11/functions-admin.html#FUNCTIONS-ADMIN-GENFILE-TABLE
generic_file_access_functions = [
    "pg_ls_dir",
    "pg_ls_logdir",
    "pg_ls_waldir",
    "pg_read_file",
    "pg_read_binary_file",
    "pg_stat_file",
]

# https://www.postgresql.org/docs/11/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS-TABLE
advisory_lock_functions = [
    "pg_advisory_lock",
    "pg_advisory_lock_shared",
    "pg_advisory_unlock",
    "pg_advisory_unlock_all",
    "pg_advisory_unlock_shared",
    "pg_advisory_xact_lock",
    "pg_advisory_xact_lock_shared",
    "pg_try_advisory_lock",
    "pg_try_advisory_lock_shared",
    "pg_try_advisory_xact_lock",
    "pg_try_advisory_xact_lock_shared",
    "pg_try_advisory_xact_lock_shared",
]


# https://www.postgresql.org/docs/11/functions-event-triggers.html#FUNCTIONS-EVENT-TRIGGER-TABLE-REWRITE
table_rewrite_information = [
    "pg_event_trigger_table_rewrite_oid",
    "pg_event_trigger_table_rewrite_reason",
]

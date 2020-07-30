# https://www.postgresql.org/docs/11/functions-info.html#FUNCTIONS-INFO-SESSION-TABLE
session_information_functions = [
    "current_query",
    "current_schema",
    "current_schemas",
    "inet_client_addr",
    "inet_client_port",
    "inet_server_addr",
    "inet_server_port",
    "pg_backend_pid",
    "pg_blocking_pids",
    "pg_conf_load_time",
    "pg_current_logfile",
    "pg_my_temp_schema",
    "pg_is_other_temp_schema",
    "pg_jit_available",
    "pg_listening_channels",
    "pg_notification_queue_usage",
    "pg_postmaster_start_time",
    "pg_safe_snapshot_blocking_pids",
    "pg_trigger_depth",
    "version",
]

session_information_value_functions = [
    "current_catalog",
    "current_role",
    "current_schema",
    "current_user",
    "session_user",
    "user",
]

# https://www.postgresql.org/docs/11/functions-info.html#FUNCTIONS-INFO-ACCESS-TABLE
access_privilege_inquiry_functions = [
    "has_any_column_privilege",
    "has_column_privilege",
    "has_database_privilege",
    "has_foreign_data_wrapper_privilege",
    "has_function_privilege",
    "has_language_privilege",
    "has_schema_privilege",
    "has_sequence_privilege",
    "has_server_privilege",
    "has_table_privilege",
    "has_tablespace_privilege",
    "has_type_privilege",
    "pg_has_role",
    "row_security_active",
]


# https://www.postgresql.org/docs/11/functions-info.html#FUNCTIONS-INFO-SCHEMA-TABLE
schema_visibility_inquiry_functions = [
    "pg_collation_is_visible",
    "pg_conversion_is_visible",
    "pg_function_is_visible",
    "pg_opclass_is_visible",
    "pg_operator_is_visible",
    "pg_opfamily_is_visible",
    "pg_statistics_obj_is_visible",
    "pg_table_is_visible",
    "pg_ts_config_is_visible",
    "pg_ts_dict_is_visible",
    "pg_ts_parser_is_visible",
    "pg_ts_template_is_visible",
    "pg_type_is_visible",
]

# https://www.postgresql.org/docs/11/functions-info.html#FUNCTIONS-INFO-SCHEMA-TABLE
system_catalog_information_functions = [
    "format_type",
    "pg_get_constraintdef",
    "pg_get_expr",
    "pg_get_functiondef",
    "pg_get_function_arguments",
    "pg_get_function_identity_arguments",
    "pg_get_function_result",
    "pg_get_indexdef",
    "pg_get_keywords",
    "pg_get_ruledef",
    "pg_get_serial_sequence",
    "pg_get_statisticsobjdef",
    "pg_get_triggerdef",
    "pg_get_userbyid",
    "pg_get_viewdef",
    "pg_index_column_has_property",
    "pg_index_has_property",
    "pg_indexam_has_property",
    "pg_options_to_table",
    "pg_tablespace_databases",
    "pg_tablespace_location",
    "pg_typeof",
    "pg_collation_for",
    "to_regclass",
    "to_regproc",
    "to_regprocedure",
    "to_regoper",
    "to_regoperator",
    "to_regtype",
    "to_regnamespace",
    "to_regrole",
]

# https://www.postgresql.org/docs/11/functions-info.html#FUNCTIONS-INFO-OBJECT-TABLE
object_information_and_addressing_functions = [
    "pg_describe_object",
    "pg_identify_object",
    "pg_identify_object_as_address",
    "pg_get_object_address",
]

# https://www.postgresql.org/docs/11/functions-info.html#FUNCTIONS-INFO-COMMENT-TABLE
comment_information_functions = [
    "col_description",
    "obj_description",
    "shobj_description",
]

# https://www.postgresql.org/docs/11/functions-info.html#FUNCTIONS-TXID-SNAPSHOT
transaction_ids_and_snapshots = [
    "txid_current",
    "txid_current_if_assigned",
    "txid_current_snapshot",
    "txid_snapshot_xip",
    "txid_snapshot_xmax",
    "txid_snapshot_xmin",
    "txid_visible_in_snapshot",
    "txid_status",
]


# https://www.postgresql.org/docs/11/functions-info.html#FUNCTIONS-COMMIT-TIMESTAMP
committed_transaction_information = [
    "pg_xact_commit_timestamp",
    "pg_last_committed_xact",
]


# https://www.postgresql.org/docs/11/functions-info.html#FUNCTIONS-CONTROLDATA
control_data_functions = [
    "pg_control_checkpoint",
    "pg_control_system",
    "pg_control_init",
    "pg_control_recovery",
]

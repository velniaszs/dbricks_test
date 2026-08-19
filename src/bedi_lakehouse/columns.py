"""Reserved column names.

Technical columns are underscore-prefixed and belong to the framework; business columns come from
the mapping and never collide with these. The four historisation columns are used unchanged in
silver and in the declarative ``meta`` tables alike — there is no second naming scheme.
"""

# Routing and identity
TENANT_ID = "tenant_id"
ENTITY_KEY = "entity_key"
PAYLOAD = "payload"

# Bronze provenance and ordering
ROW_HASH = "_row_hash"
INGEST_SEQ = "_ingest_seq"
INGEST_TS = "_ingest_ts"
BATCH_ID = "_batch_id"
SOURCE_FILE = "_source_file"
LOAD_MODE = "_load_mode"
SCHEMA_VER = "_schema_ver"
CORRUPT_RECORD = "_corrupt_record"

# Quality
DQ_STATUS = "_dq_status"

# Historisation — identical in silver and in meta
VERSION_NO = "version_no"
VALID_FROM = "valid_from"
VALID_TO = "valid_to"
IS_CURRENT = "is_current"
CHANGE_REASON = "_change_reason"

# Open-ended validity. A sentinel rather than NULL so that BETWEEN predicates work unchanged.
INFINITY = "9999-12-31 00:00:00"

CHANGE_NEW = "new"
CHANGE_CHANGED = "changed"
CHANGE_DELETED = "deleted"

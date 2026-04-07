# Scripting

Status: draft
Last updated: 2026-04-07

## 1. Purpose and Normative Intent
This specification defines normative scripting rules for `sqlct`.

- It defines how database metadata is transformed into deterministic object scripts in the schema folder.
- It defines required output behavior for currently implemented object classes and required minimum behavior for planned object classes.
- It uses requirement language: MUST, MUST NOT, SHOULD, MAY.

## 2. Scope
- Covers discovery inputs, script generation rules, output-write contract, formatting/determinism guarantees, compatibility-guided reconciliation.
- Applies to schema-object scripting behavior used by `sqlct` workflows.
- Does not define command UX, exit-code policy, or general CLI flow (see `specs/01-cli.md`).
- Does not define folder mapping taxonomy (see `specs/03-schema-folder.md`).
- Does not define command behavior or configuration persistence for selective data scripting beyond its interaction with scripting.

## 3. Source Inputs
### 3.1 SQL Server Inputs
- System catalog metadata and object definitions are the primary source for script content.
- Scripting MUST use SQL metadata for:
  - object identity (schema/name/type),
  - object definitions for programmable objects,
  - table column and constraint/index/foreign-key metadata,
  - permission metadata,
  - extended properties.

### 3.2 File Inputs
- Existing object files in the schema folder MAY be used as compatibility formatting references.
- Compatibility references MUST influence formatting only as defined in this spec.
- Folder mapping and naming expectations are defined in `specs/03-schema-folder.md`.

### 3.3 Config Inputs
- Configuration in `sqlct.config.json` (see `specs/02-config.md`) MAY affect discovery scope and compare behavior.
- Comparison options MUST NOT silently rewrite persisted script content beyond rules in this scripting spec.

## 4. Object Discovery
- Discovery MUST enumerate supported object types from catalog metadata.
- Discovery MUST ignore system objects by default.
- Discovery SHOULD preserve compatibility with projects that include security and storage object groups.
- Database-scoped objects with no explicit schema MUST be mapped consistently with schema-folder rules.
- `Schema` discovery covers user-defined schemas and excludes `dbo`, `sys`, and `INFORMATION_SCHEMA`.
- `Role` discovery covers user-defined roles and fixed roles that have non-system members tracked in role membership metadata.
- `TableType` discovery covers user-defined table types.
- `XmlSchemaCollection` discovery covers user-defined XML schema collections and excludes collections in `sys` and `INFORMATION_SCHEMA`.
- `MessageType` and `Contract` discovery covers user-defined Service Broker objects and excludes SQL Server-owned broker artifacts named `DEFAULT` and broker/notification artifacts whose names start with `http://schemas.microsoft.com/SQL/`.
- `Service` discovery covers user-defined Service Broker services and excludes broker/notification artifacts whose names start with `http://schemas.microsoft.com/SQL/`.
- `Queue` discovery covers user-defined Service Broker queues and excludes SQL Server-owned queues `ServiceBrokerQueue`, `QueryNotificationErrorsQueue`, and `EventNotificationErrorsQueue`.
- `Route` discovery covers user-defined Service Broker routes and excludes SQL Server-created route `AutoCreatedLocal`.
- `EventNotification` discovery covers database event notifications from `sys.event_notifications`.
- `ServiceBinding` discovery covers remote service bindings from `sys.remote_service_bindings`.
- `FullTextCatalog` discovery covers user-defined full-text catalogs.
- `FullTextStoplist` discovery covers user-defined full-text stoplists and excludes system stoplists.
- `SearchPropertyList` discovery covers registered search property lists and registered search properties when `sys.registered_search_property_lists` is available.
- Discovery MUST gracefully skip feature-specific metadata paths not available on the current SQL Server edition/version (for example external/full-text/security-policy catalogs) without failing whole-run scripting.
- Discovery MUST include permissions and extended properties needed by scripting rules.
- Discovery order MUST be deterministic.

## 5. Supported Object Types
### 5.1 Implemented Coverage (Normative Now)
- Tables
- Table-scoped DML triggers (emitted within table scripts that inline trigger DDL with the owning table)
- Views
- Stored Procedures
- Functions (`FN`, `TF`, `IF`)
- Sequences
- Schema
- Role
- User
- Synonym
- UserDefinedType
- TableType
- XmlSchemaCollection
- PartitionFunction
- PartitionScheme
- MessageType
- Contract
- Queue
- Service
- Route
- EventNotification
- ServiceBinding
- FullTextCatalog
- FullTextStoplist
- SearchPropertyList

### 5.2 Additional Defined Coverage (Normative Minimum)
The following types are defined in this specification family and not fully implemented in the current SQL scripting engine. For each type, implementation MUST define deterministic discovery, deterministic output shape, and batch framing:

- Standalone Trigger (excluding table-scoped DML triggers already emitted inline with tables), DdlTrigger, Rule
- Certificate, SymmetricKey, AsymmetricKey
- SecurityPolicy
- ExternalDataSource, ExternalFileFormat, ExternalTable
- Data scripts for explicit tracked tables:
  - scoped to tables listed in `data.trackedTables`,
  - one data script file per tracked table,
  - file naming and placement aligned to `specs/03-schema-folder.md`,
  - deterministic for identical source rows and configuration,
  - initial output emitted as deterministic `INSERT` statements.

## 6. Global Scripting Rules

### 6.1 Statement Framing
- Batch separators for schema-object scripts MUST be emitted as `GO` on its own line.
- Programmable objects (views, procedures, functions, and table-scoped DML triggers) MUST include:
  - `SET QUOTED_IDENTIFIER <ON|OFF>` + `GO`
  - `SET ANSI_NULLS <ON|OFF>` + `GO`
- Programmable-object body MUST be followed by `GO`.
- Object-level permissions and extended properties MUST be emitted after object DDL body.
- Canonical programmable-object whitespace MUST be:
  - no blank line between the final header `GO` and the first definition line,
  - no blank line between the final definition line and the trailing `GO`.
- The canonical programmable-object whitespace rule applies to views, stored procedures, functions, and table-scoped DML triggers unless compatibility reconciliation explicitly preserves reference spacing (see Section 9).

### 6.2 Statement Ordering
- Statement ordering MUST be deterministic.
- For each object, grants and extended properties MUST appear in deterministic order after base DDL/module body and any required subordinate statements (for example indexed-view index statements).

### 6.3 Permission Emission
- Statements MUST be generated from `sys.database_permissions` joined to `sys.database_principals`.
- `GRANT_WITH_GRANT_OPTION` -> `GRANT ... WITH GRANT OPTION`
- `DENY` -> `DENY ...`
- otherwise -> `GRANT ...`
- Permissions MUST be ordered deterministically by grantee then permission name.

### 6.4 Extended Property Emission
- Single quotes in names/values MUST be escaped.
- Programmable-object extended properties:
  - default style `short` emits `EXEC sp_addextendedproperty ...`.
  - style `sys_named` emits `EXEC sys.sp_addextendedproperty ...`.
  - object-level properties first (ordered by property name),
  - then parameter-level properties for procedures/functions (ordered by parameter name, then property name).
- View extended properties:
  - view-level properties first (ordered by property name),
  - then index-level properties (ordered by index name, then property name).
- Table extended properties:
  - table-level properties first (ordered by property name),
  - then column-level properties (ordered by column name, then property name),
  - then constraint-level properties (ordered by constraint name, then property name),
  - then index-level properties (ordered by index name, then property name),
  - then trigger-level properties (ordered by trigger name, then property name).
- Schema extended properties:
  - schema-level properties only (ordered by property name).
- Principal extended properties for tracked users and roles:
  - principal-level properties only (ordered by property name).
- Sequence extended properties:
  - sequence-level properties only (ordered by property name).
- Synonym extended properties:
  - synonym-level properties only (ordered by property name).
- User-defined type extended properties:
  - type-level properties only (ordered by property name).
- Partition-function extended properties:
  - partition-function-level properties only (ordered by property name).
- Partition-scheme extended properties:
  - partition-scheme-level properties only (ordered by property name).

### 6.5 Type Formatting Rules
- User-defined types MUST be emitted as `[schema].[type]`.
- Built-in types MUST be emitted as `[type]` except sequence `AS` rendering (see Section 8.5).
- Canonical built-in type token rendering MUST use lower-case type names inside brackets (for example `[int]`, `[nvarchar]`, `[sysname]`, `[hierarchyid]`).
- System alias/CLR types that may appear schema-qualified in metadata or compatibility exports (for example `[sys].[sysname]`, `[sys].[hierarchyid]`) MUST still use canonical built-in rendering in baseline output unless compatibility reconciliation preserves a semantically equivalent token spelling under Section 9.
- Length/precision formatting:
  - `varchar`, `varbinary`, `nvarchar`: `MAX` when SQL length is `-1`.
  - `nvarchar`/`nchar`: length is `max_length / 2`.
  - `decimal`/`numeric`: `(precision, scale)`.
  - `datetime2`, `datetimeoffset`, `time`: default scale `7` MUST omit explicit `(7)`.
- The canonical unbounded-length marker MUST be uppercase `MAX`.

### 6.6 Output Write Contract
- Output paths and naming MUST remain aligned with `specs/03-schema-folder.md`.
- Output encoding, line-ending policy, and trailing-newline policy MUST follow project conventions defined in `specs/03-schema-folder.md`.
- Scripting MUST NOT emit mixed line endings within a single file.
- File-write behavior MUST be deterministic for identical inputs.

### 6.7 Deterministic Processing and Idempotence
- Object processing order MUST be deterministic.
- For identical metadata, configuration, compatibility inputs, and schema-folder baseline, scripting MUST produce byte-identical output files.
- Repeated `sqlct pull` runs with unchanged source metadata MUST produce no effective content changes in scripted files.

## 8. Per-Object Scripting Rules
### 8.1 Tables
#### 8.1.1 Table Header and Column Block
- Base statement MUST start with `CREATE TABLE [schema].[table]`.
- Columns MUST be emitted in `sys.columns.column_id` order.
- Computed column format:
  - `[Column] AS <computed_definition><persisted><nullability>`
- `PERSISTED` MUST be emitted for computed columns when metadata marks the computed expression as persisted.
- `NOT NULL` MUST be emitted for computed columns only when metadata marks the computed column as non-nullable and the computed expression is persisted.
- Non-persisted computed columns MUST omit explicit nullability, even when catalog metadata reports `is_nullable = 0`.
- Non-computed column format:
  - `[Column] <type> <NULL|NOT NULL><rowguidcol><identity><default>`
- `ROWGUIDCOL` MUST be emitted when present, immediately after `<NULL|NOT NULL>` and before identity/default clauses.
- XML columns bound to an XML schema collection MUST render the bound type token as:
  - `[xml] (<CONTENT|DOCUMENT> [schema].[collection])`
- Identity format:
  - `IDENTITY(seed, increment)` when identity metadata exists.
  - `NOT FOR REPLICATION` MUST be appended immediately after the identity clause when the identity column is marked `NOT FOR REPLICATION`.
- Default format:
  - Named default: `CONSTRAINT [name] DEFAULT <definition>`
  - Unnamed default: `DEFAULT <definition>`

#### 8.1.2 Storage and Compression
- Table close line MUST include storage target when available:
  - `ON [<data_space>]`
  - `ON [<data_space>] ([<partition_column>])` when the table storage target is a partition scheme and the partitioning column is available from catalog metadata.
  - `TEXTIMAGE_ON [<lob_data_space>]` when present.
- Table compression MUST be omitted when `NONE`.
- For `PAGE`/`ROW`, compression block MUST be:
  - `WITH`
  - `(`
  - `DATA_COMPRESSION = <PAGE|ROW>`
  - `)`

#### 8.1.3 Table-Scoped DML Triggers
- Only user-defined, table-scoped DML triggers are in scope for table scripting; standalone/database/server triggers remain out of scope for this section.
- Trigger metadata MUST be sourced from table-owned trigger metadata and ordered by trigger name.
- Trigger definition text MUST come from `OBJECT_DEFINITION`.
- Trigger framing MUST follow programmable-object framing rules from Section 6.1.
- Table-scoped DML triggers MUST be emitted immediately after the base table `CREATE` block and its `GO`.
- Table-scoped DML triggers MUST NOT also be emitted as standalone trigger object scripts in the same export.

#### 8.1.4 Post-Create Statement Order
After base table `CREATE` block and its `GO`, statements MUST be emitted in this exact order:
1. Table-scoped DML triggers
2. CHECK constraints
3. Key constraints (PRIMARY KEY / UNIQUE)
4. Non-constraint indexes
5. XML indexes
6. Foreign keys
7. Grants
8. Extended properties
9. Full-text indexes
10. Lock escalation (only when not `TABLE`)

Each emitted statement MUST be followed by `GO`.

#### 8.1.5 CHECK Constraints
- When no compatibility check line is reused, constraints MUST come from `sys.check_constraints` ordered by name.
- `NOT FOR REPLICATION` MUST be emitted when set.
- Constraint definition text MUST be wrapped as `CHECK (<definition>)` unless compatibility line is reused.

#### 8.1.6 Key Constraints (PRIMARY KEY / UNIQUE)
- Source metadata MUST come from `sys.key_constraints` joined to `sys.indexes`.
- Key constraints MUST be scripted in `object_id` order.
- Constraint type mapping:
  - `PRIMARY_KEY_CONSTRAINT` -> `PRIMARY KEY`
  - otherwise -> `UNIQUE`
- Clustered mapping:
  - index type contains `CLUSTERED` -> `CLUSTERED`
  - otherwise -> `NONCLUSTERED`
- Key columns MUST be ordered by `key_ordinal` and include `DESC` on descending keys.
- Constraint-level `WITH` options MUST include `STATISTICS_INCREMENTAL=ON` when the backing index statistics are incremental.
- Constraint-level `WITH` options MUST include `DATA_COMPRESSION = <PAGE|ROW>` when compression is not `NONE`.
- When both constraint-level options are present, they MUST be emitted in this order:
  - `WITH (STATISTICS_INCREMENTAL=ON, DATA_COMPRESSION = <PAGE|ROW>)`
- `ON [data_space]` MUST be emitted when available.
- `ON [data_space] ([partition_column])` MUST be emitted when the backing index is partitioned and the partitioning column is available from catalog metadata.

#### 8.1.7 Non-Constraint Indexes
- Include only indexes where:
  - not primary key,
  - not unique constraint,
  - type in `CLUSTERED`/`NONCLUSTERED`,
  - not hypothetical.
- Index list MUST be ordered by index name.
- Key columns MUST be ordered by `key_ordinal`, then `index_column_id`.
- Included columns MUST be emitted in `INCLUDE (...)` when present.
- Filtered index predicate MUST be emitted as `WHERE <filter_definition>` when present.
- Index `WITH` options MUST include `STATISTICS_INCREMENTAL=ON` when the backing index statistics are incremental.
- Index compression MUST emit `DATA_COMPRESSION = <PAGE|ROW>` when not `NONE`.
- When both index-level options are present, they MUST be emitted in this order:
  - `WITH (STATISTICS_INCREMENTAL=ON, DATA_COMPRESSION = <PAGE|ROW>)`
- `ON [data_space]` MUST be emitted when available.
- `ON [data_space] ([partition_column])` MUST be emitted when the index is partitioned and the partitioning column is available from catalog metadata.

#### 8.1.8 XML Indexes
- XML indexes MUST be sourced from `sys.xml_indexes` together with column metadata.
- XML indexes MUST be ordered by XML `index_id`.
- Primary XML index format MUST be:
  - `CREATE PRIMARY XML INDEX [name]`
  - `ON [schema].[table] ([column])`
- Secondary XML index format MUST be:
  - `CREATE XML INDEX [name]`
  - `ON [schema].[table] ([column])`
  - `USING XML INDEX [primary_xml_index]`
  - `FOR <PATH|PROPERTY|VALUE>`

#### 8.1.9 Foreign Keys
- Foreign keys MUST be ordered by foreign key name.
- Column lists MUST follow `constraint_column_id`.
- `ON DELETE` and `ON UPDATE` clauses MUST be emitted only when action is not `NO_ACTION`.

#### 8.1.10 Table Extended Properties
- Table-level extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'SCHEMA', N'<schema>', 'TABLE', N'<table>', NULL, NULL`
- Column-level extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'SCHEMA', N'<schema>', 'TABLE', N'<table>', 'COLUMN', N'<column>'`
- Constraint-level extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'SCHEMA', N'<schema>', 'TABLE', N'<table>', 'CONSTRAINT', N'<constraint>'`
- Index-level extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'SCHEMA', N'<schema>', 'TABLE', N'<table>', 'INDEX', N'<index>'`
- Trigger-level extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'SCHEMA', N'<schema>', 'TABLE', N'<table>', 'TRIGGER', N'<trigger>'`
- Table extended-property output order MUST be:
  1. table-level (by property name),
  2. column-level (by column name, then property name),
  3. constraint-level (by constraint name, then property name),
  4. index-level (by index name, then property name),
  5. trigger-level (by trigger name, then property name).

#### 8.1.11 Full-Text Indexes
- Full-text indexes MUST be sourced from `sys.fulltext_indexes`, `sys.fulltext_index_columns`, `sys.fulltext_catalogs`, `sys.indexes`, and `sys.columns`.
- Full-text index base statement MUST be:
  - `CREATE FULLTEXT INDEX ON [schema].[table] KEY INDEX [unique_key_index] ON [catalog]`
- Indexed full-text columns MUST be emitted as one `ALTER FULLTEXT INDEX ... ADD (...)` statement per indexed column in `column_id` order.
- Full-text column clause format MUST be:
  - `[column] LANGUAGE <language_id>`
  - `[column] TYPE COLUMN [type_column] LANGUAGE <language_id>` when a type column is configured.

#### 8.1.12 Lock Escalation
- `ALTER TABLE [schema].[table] SET ( LOCK_ESCALATION = <value> )` MUST be emitted only when lock escalation is present and not `TABLE`.

### 8.2 Views
- Views are scripted through programmable-object framing rules with object type `V` and level type `VIEW`.
- SET/GO framing MUST be emitted before the definition.
- Definition text MUST come from `OBJECT_DEFINITION`.
- Indexed-view indexes are in scope for compatibility exports that persist indexed-view DDL in the view file.
- When view-owned indexes exist, they MUST be emitted after the view definition `GO` and before grants.
- View index rendering MUST follow non-constraint index rules from Section 8.1.7, replacing the table target with the view target.
- Indexed-view indexes MUST NOT be emitted as separate standalone object scripts in the same export.
- View-specific blank-line and indentation controls MUST apply:
  - compatibility inference,
  - override substitution,
  - default fallback.
- When no compatible reference spacing is preserved, view emission MUST use the canonical programmable-object whitespace rules from Section 6.1.
- Grants MUST be emitted after definition `GO` and after any indexed-view index statements.
- View-level extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'SCHEMA', N'<schema>', 'VIEW', N'<view>', NULL, NULL`
- View index-level extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'SCHEMA', N'<schema>', 'VIEW', N'<view>', 'INDEX', N'<index>'`
- View extended properties MUST be emitted after grants in this order:
  1. view-level (by property name),
  2. index-level (by index name, then property name).

### 8.3 Stored Procedures
- Stored procedures are scripted through programmable-object framing rules with object type `P` and level type `PROCEDURE`.
- Definition text MUST come from `OBJECT_DEFINITION`.
- Regex replacements from overrides MUST be applied before compatibility line-map reconciliation.
- Compatibility definition line-map reconciliation MUST be applied when reference file exists.
- When no compatible reference spacing is preserved, procedure emission MUST use the canonical programmable-object whitespace rules from Section 6.1.
- Grants and extended properties MUST follow module body.
- Procedure-level extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'SCHEMA', N'<schema>', 'PROCEDURE', N'<procedure>', NULL, NULL`
- Procedure parameter-level extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'SCHEMA', N'<schema>', 'PROCEDURE', N'<procedure>', 'PARAMETER', N'@<parameter>'`
- Procedure extended properties MUST be emitted in this order:
  1. procedure-level (by property name),
  2. parameter-level (by parameter name, then property name).

### 8.4 Functions
- Functions are scripted through programmable-object framing rules with object types `FN`, `TF`, `IF` and level type `FUNCTION`.
- Definition text MUST come from `OBJECT_DEFINITION`.
- Regex replacements from overrides MUST be applied before final emission.
- When no compatible reference spacing is preserved, function emission MUST use the canonical programmable-object whitespace rules from Section 6.1.
- Grants and extended properties MUST follow module body.
- Function-level extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'SCHEMA', N'<schema>', 'FUNCTION', N'<function>', NULL, NULL`
- Function parameter-level extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'SCHEMA', N'<schema>', 'FUNCTION', N'<function>', 'PARAMETER', N'@<parameter>'`
- Function extended properties MUST be emitted in this order:
  1. function-level (by property name),
  2. parameter-level (by parameter name, then property name).
- Function scripting SHOULD support compatibility definition line-map reconciliation using the same algorithm as stored procedures.

### 8.5 Sequences
- Sequence metadata MUST be sourced from `sys.sequences` joined with schema and type metadata.
- Output shape MUST be:
  - `CREATE SEQUENCE [schema].[name]`
  - `AS <type>`
  - `START WITH <value>`
  - `INCREMENT BY <value>`
  - `MINVALUE <value>`
  - `MAXVALUE <value>`
  - `<CYCLE|NO CYCLE>`
  - `<CACHE ...|NO CACHE>`
  - `GO`
- Sequence type rendering:
  - system type MUST be unbracketed (for example `AS int`),
  - user-defined type MUST be bracketed as `[schema].[type]`.
- Cache rendering:
  - if cached with explicit size: `CACHE <size>`,
  - if cached without explicit size: `CACHE ` (trailing space preserved),
  - if not cached: `NO CACHE`.
- Sequence-level extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'SCHEMA', N'<schema>', 'SEQUENCE', N'<sequence>', NULL, NULL`
- Sequence extended properties MUST be emitted after the sequence `GO`, ordered by property name.

### 8.6 Schemas
- Schema scripts MUST emit one `CREATE SCHEMA [name]` statement for each user-defined schema that is in scope.
- `dbo`, `sys`, and `INFORMATION_SCHEMA` MUST NOT be emitted as schema object files.
- When schema ownership metadata is present, `AUTHORIZATION [owner]` MUST be emitted on the following line.
- Schema scripts MUST end with `GO`.
- Schema-level extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'SCHEMA', N'<schema>', NULL, NULL, NULL, NULL`
- Schema extended properties MUST be emitted after the base schema `GO`, ordered by property name.

### 8.7 Roles
- User-defined roles MUST emit `CREATE ROLE [name]` and optional `AUTHORIZATION [owner]`, followed by `GO`.
- Fixed roles are in scope only when they have non-system members that are tracked through role membership metadata.
- Role membership statements MUST be emitted after the base role DDL, ordered by member name.
- Role membership statements MUST use:
  - `EXEC sp_addrolemember N'<role>', N'<member>'`
- System-principal memberships for `dbo`, `guest`, `INFORMATION_SCHEMA`, and `sys` MUST NOT be emitted.
- Role-level extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'USER', N'<role>', NULL, NULL, NULL, NULL`
- Role extended properties MUST be emitted after the base role DDL and any role-membership statements, ordered by property name.

### 8.8 Users
- User scripts MUST emit one `CREATE USER` statement and end with `GO`.
- Supported user-authentication shapes are:
  - `WITHOUT LOGIN`
  - `FOR LOGIN [login]`
  - `FROM EXTERNAL PROVIDER`
  - `FOR CERTIFICATE [certificate]`
  - `FOR ASYMMETRIC KEY [key]`
- `WITH DEFAULT_SCHEMA=[schema]` MUST be emitted only when a non-empty, non-`dbo` default schema applies to the emitted user shape.
- Contained database users that require `WITH PASSWORD` remain unsupported in the current scripting engine and MUST fail explicitly rather than emit lossy output.
- User-level extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'USER', N'<user>', NULL, NULL, NULL, NULL`
- User extended properties MUST be emitted after the base user `GO`, ordered by property name.

### 8.9 Synonyms
- Synonym scripts MUST emit:
  - `CREATE SYNONYM [schema].[name] FOR <base_object_name>`
  - `GO`
- Base-object text MUST come from synonym metadata and MUST NOT be schema-normalized or re-quoted beyond what SQL Server returns.
- Synonym-level extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'SCHEMA', N'<schema>', 'SYNONYM', N'<synonym>', NULL, NULL`
- Synonym extended properties MUST be emitted after the synonym `GO`, ordered by property name.

### 8.10 UserDefinedType
- Alias user-defined type scripts MUST emit:
  - `CREATE TYPE [schema].[name] FROM <base_type> <NULL|NOT NULL>`
  - `GO`
- Base-type formatting MUST reuse the general type-formatting rules from Section 6.5.
- User-defined-type extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'SCHEMA', N'<schema>', 'TYPE', N'<type>', NULL, NULL`
- User-defined-type extended properties MUST be emitted after the type `GO`, ordered by property name.

### 8.11 TableType
- Table-type metadata MUST be sourced from `sys.table_types` together with the table-like metadata attached to `type_table_object_id`.
- Output MUST emit:
  - `CREATE TYPE [schema].[name] AS TABLE`
  - `(`
  - columns and inline constraints in deterministic order
  - `)`
  - `GO`
- Table-type column formatting MUST reuse the applicable column rules from Section 8.1.1.
- Inline table-type body order MUST be:
  1. columns by `column_id`,
  2. CHECK constraints by constraint name,
  3. key constraints (`PRIMARY KEY` / `UNIQUE`) by constraint name.
- Inline entries inside the table-type body MUST be comma-separated.
- Table-type key-constraint formatting MUST reuse the applicable constraint rules from Section 8.1.6, except the constraints remain inside the `AS TABLE (...)` body rather than being emitted after `GO`.
- Table-type scripting MUST NOT emit storage clauses, non-constraint indexes, XML indexes, foreign keys, triggers, full-text indexes, permissions, or extended properties.

### 8.12 XmlSchemaCollection
- XML schema collection metadata MUST be sourced from `sys.xml_schema_collections`, schema metadata, and `XML_SCHEMA_NAMESPACE`.
- XML schema collection scripts MUST emit:
  - `CREATE XML SCHEMA COLLECTION [schema].[name] AS <schema_definition>`
  - `GO`
- The `<schema_definition>` payload MUST be the trimmed XML schema namespace text returned for the collection.
- XML schema collection permissions MUST use `ON XML SCHEMA COLLECTION::[schema].[name]`.
- XML schema collection-level extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'SCHEMA', N'<schema>', 'XML SCHEMA COLLECTION', N'<collection>', NULL, NULL`
- XML schema collection extended properties MUST be emitted after grants, ordered by property name.

### 8.13 MessageType
- Message-type metadata MUST be sourced from `sys.service_message_types` together with owner metadata and XML schema collection metadata when XML-schema validation applies.
- Message-type scripts are database-scoped and MUST use schema-less file naming and display.
- Output MUST emit:
  - `CREATE MESSAGE TYPE [name]`
  - optional `AUTHORIZATION [owner]`
  - `VALIDATION = <NONE|EMPTY|WELL_FORMED_XML>`
  - or `VALIDATION = VALID_XML WITH SCHEMA COLLECTION [schema].[collection]`
  - `GO`
- Message-type permissions MUST use `ON MESSAGE TYPE::[name]`.
- Message-type extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'MESSAGE TYPE', N'<message_type>', NULL, NULL, NULL, NULL`
- Message-type extended properties MUST be emitted after grants, ordered by property name.

### 8.14 Contract
- Contract metadata MUST be sourced from `sys.service_contracts`, `sys.service_contract_message_usages`, `sys.service_message_types`, and owner metadata.
- Contract scripts are database-scoped and MUST use schema-less file naming and display.
- Output MUST emit:
  - `CREATE CONTRACT [name]`
  - optional `AUTHORIZATION [owner]`
  - `(`
  - zero or more `[message_type] SENT BY <INITIATOR|TARGET|ANY>` items ordered by message-type name
  - `)`
  - `GO`
- Contracts with no message usages MUST still emit an empty `()` body.
- Contract permissions MUST use `ON CONTRACT::[name]`.
- Contract-level extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'CONTRACT', N'<contract>', NULL, NULL, NULL, NULL`
- Contract extended properties MUST be emitted after grants, ordered by property name.

### 8.15 Queue
- Queue metadata MUST be sourced from `sys.service_queues` together with schema metadata, activation metadata, and queue storage metadata when available.
- Queue scripts MUST emit:
  - `CREATE QUEUE [schema].[name]`
  - optional `WITH ...`
  - optional `ON [data_space]`
  - `GO`
- Queue option order inside `WITH ...` MUST be:
  1. `STATUS = <ON|OFF>`
  2. `RETENTION = <ON|OFF>`
  3. `POISON_MESSAGE_HANDLING (STATUS = <ON|OFF>)`
  4. optional `ACTIVATION (...)`
- Activation option order inside `ACTIVATION (...)` MUST be:
  1. `STATUS = <ON|OFF>`
  2. `PROCEDURE_NAME = [schema].[procedure]`
  3. `MAX_QUEUE_READERS = <count>`
  4. `EXECUTE AS <principal>`
- `ACTIVATION (...)` MUST be emitted only when activation metadata is enabled or otherwise non-default for the queue.
- Queue permissions MUST use object-permission emission against `[schema].[name]`.
- Queue-level extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'SCHEMA', N'<schema>', 'QUEUE', N'<queue>', NULL, NULL`
- Queue extended properties MUST be emitted after grants, ordered by property name.

### 8.16 Service
- Service metadata MUST be sourced from `sys.services`, the referenced queue metadata, `sys.service_contract_usages`, `sys.service_contracts`, and owner metadata.
- Service scripts are database-scoped and MUST use schema-less file naming and display.
- Output MUST emit:
  - `CREATE SERVICE [name]`
  - optional `AUTHORIZATION [owner]`
  - `ON QUEUE [queue_schema].[queue_name]`
  - optional `([contract_1], [contract_2], ...)` ordered by contract name
  - `GO`
- Service permissions MUST use `ON SERVICE::[name]`.
- Service-level extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'SERVICE', N'<service>', NULL, NULL, NULL, NULL`
- Service extended properties MUST be emitted after grants, ordered by property name.

### 8.17 Route
- Route metadata MUST be sourced from `sys.routes` together with owner metadata.
- Route scripts are database-scoped and MUST use schema-less file naming and display.
- Output MUST emit:
  - `CREATE ROUTE [name]`
  - optional `AUTHORIZATION [owner]`
  - `WITH <option_list>`
  - `GO`
- Route options MUST be emitted in this order when present:
  1. `SERVICE_NAME = '<service_name>'`
  2. `BROKER_INSTANCE = '<guid>'`
  3. `LIFETIME = <seconds>`
  4. `ADDRESS = '<address>'`
  5. `MIRROR_ADDRESS = '<mirror_address>'`
- Route string literals MUST escape embedded single quotes.
- Route permissions MUST use `ON ROUTE::[name]`.
- Route-level extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'ROUTE', N'<route>', NULL, NULL, NULL, NULL`
- Route extended properties MUST be emitted after grants, ordered by property name.

### 8.18 EventNotification
- Event-notification metadata MUST be sourced from `sys.event_notifications` and `sys.events`, together with queue metadata when queue-scoped output is required.
- Event-notification scripts are database-scoped and MUST use schema-less file naming and display.
- Output MUST emit:
  - `CREATE EVENT NOTIFICATION [name]`
  - `<ON DATABASE|ON SERVER|ON QUEUE [schema].[queue]>`
  - `FOR <event_1>, <event_2>, ...`
  - `TO SERVICE '<service_name>'`
  - optional `, '<broker_instance>'`
  - `GO`
- Event names MUST be emitted in deterministic order by event type name.
- Event-notification scripting MUST preserve the effective subscribed event set rather than the original event-group shorthand used at creation time.
- Service-name and broker-instance string literals MUST escape embedded single quotes.
- SQL Server does not expose a dedicated event-notification permission class in `sys.database_permissions`; event-notification scripts MUST NOT emit permissions.
- Event-notification extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'EVENT NOTIFICATION', N'<event_notification>', NULL, NULL, NULL, NULL`
- Event-notification extended properties MUST be emitted after the base statement `GO`, ordered by property name.

### 8.19 ServiceBinding
- Service-binding metadata MUST be sourced from `sys.remote_service_bindings` together with remote-principal metadata.
- Service-binding scripts are database-scoped and MUST use schema-less file naming and display.
- Output MUST emit:
  - `CREATE REMOTE SERVICE BINDING [name]`
  - `TO SERVICE '<remote_service_name>'`
  - `WITH USER = [principal]`
  - optional `, ANONYMOUS = ON`
  - `GO`
- Remote-service string literals MUST escape embedded single quotes.
- Service-binding permissions MUST use `ON REMOTE SERVICE BINDING::[name]`.
- Service-binding extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'REMOTE SERVICE BINDING', N'<binding>', NULL, NULL, NULL, NULL`
- Service-binding extended properties MUST be emitted after grants, ordered by property name.

### 8.20 FullTextCatalog
- Full-text catalog metadata MUST be sourced from `sys.fulltext_catalogs`.
- Full-text catalog scripts are database-scoped and MUST use schema-less file naming and display.
- Output MUST emit:
  - `CREATE FULLTEXT CATALOG [name]`
  - optional `AS DEFAULT`
  - `WITH ACCENT_SENSITIVITY = <ON|OFF>`
  - `GO`
- Full-text catalog permissions MUST use `ON FULLTEXT CATALOG::[name]`.
- SQL Server does not expose full-text catalog entries in `sys.extended_properties`; full-text catalog scripts MUST NOT emit extended properties.

### 8.21 FullTextStoplist
- Full-text stoplist metadata MUST be sourced from `sys.fulltext_stoplists` and `sys.fulltext_stopwords`.
- Full-text stoplist scripts are database-scoped and MUST use schema-less file naming and display.
- Output MUST emit:
  - `CREATE FULLTEXT STOPLIST [name]`
  - `GO`
  - zero or more `ALTER FULLTEXT STOPLIST [name] ADD '<stopword>' LANGUAGE <language_id>` statements ordered by `language_id`, then stopword text
  - `GO` after each `ALTER` statement
- Full-text stoplist scripting MUST preserve the effective stopword set rather than the original construction source; emitted output MUST NOT require `FROM SYSTEM STOPLIST` or `FROM <other_stoplist>` to recreate the active stopword list.
- Full-text stoplist string literals MUST escape embedded single quotes.
- Full-text stoplist permissions MUST use `ON FULLTEXT STOPLIST::[name]`.
- SQL Server does not expose full-text stoplist entries in `sys.extended_properties`; full-text stoplist scripts MUST NOT emit extended properties.

### 8.22 SearchPropertyList
- Search-property-list metadata MUST be sourced from `sys.registered_search_property_lists` and `sys.registered_search_properties` when those catalog views are available.
- Search-property-list scripts are database-scoped and MUST use schema-less file naming and display.
- Output MUST emit:
  - `CREATE SEARCH PROPERTY LIST [name]`
  - `GO`
  - zero or more `ALTER SEARCH PROPERTY LIST [name] ADD '<property_name>' WITH (PROPERTY_SET_GUID = '<guid>', PROPERTY_INT_ID = <int>[, PROPERTY_DESCRIPTION = '<description>'])` statements
  - `GO` after each `ALTER` statement
- Search-property entries MUST be emitted in deterministic order by property name, then property-set GUID, then property integer ID.
- Search-property-list scripting MUST preserve the effective registered property set rather than the original construction source.
- Property-name and property-description string literals MUST escape embedded single quotes.
- Search-property-list permissions MUST use `ON SEARCH PROPERTY LIST::[name]`.
- SQL Server does not expose search-property-list entries in `sys.extended_properties`; search-property-list scripts MUST NOT emit extended properties.

### 8.23 Partition Functions
- Partition-function metadata MUST come from `sys.partition_functions`, `sys.partition_parameters`, `sys.types`, and `sys.partition_range_values`.
- Output MUST emit one `CREATE PARTITION FUNCTION` statement with deterministic boundary ordering and end with `GO`.
- System base types MAY be emitted bracketed or unbracketed only when compatibility reconciliation preserves the reference spelling; otherwise canonical output remains deterministic per the implementation.
- Partition-function extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'PARTITION FUNCTION', N'<function>', NULL, NULL, NULL, NULL`
- Partition-function extended properties MUST be emitted after the partition-function `GO`, ordered by property name.

### 8.24 Partition Schemes
- Partition-scheme metadata MUST come from `sys.partition_schemes`, `sys.partition_functions`, and destination data-space metadata.
- Output MUST emit one `CREATE PARTITION SCHEME` statement that references the target partition function and ordered destination filegroups, followed by `GO`.
- Empty or missing destination lists MUST still emit a valid `TO (...)` clause shape consistent with discovered metadata.
- Partition-scheme extended properties MUST use:
  - `EXEC sp_addextendedproperty ..., 'PARTITION SCHEME', N'<scheme>', NULL, NULL, NULL, NULL`
- Partition-scheme extended properties MUST be emitted after the partition-scheme `GO`, ordered by property name.

### 8.25 TableData
- Table-data scripting applies only to tables explicitly listed in `data.trackedTables`.
- One data script file MUST be emitted per tracked table, even when the table currently has zero rows.
- An empty tracked table MUST still produce a file at the expected `Data/Schema.Table_Data.sql` path; that file contains no SQL statements.
- Data scripts MUST contain only:
  - `INSERT INTO ... VALUES ...;` statements,
  - `SET IDENTITY_INSERT [schema].[table] ON;` and `SET IDENTITY_INSERT [schema].[table] OFF;` when identity preservation is required.
- Data scripts MUST NOT emit `DELETE`, `TRUNCATE`, transaction wrappers, constraint-disabling statements, or `GO` batch separators.
- Data scripts MUST use one single-row `INSERT` statement per row.
- `INSERT` statement shape MUST be:
  - `INSERT INTO [schema].[table] ([column_1], [column_2], ...) VALUES (<literal_1>, <literal_2>, ...);`
- The target column list MUST include all insertable columns in `sys.columns.column_id` order.
- Non-insertable columns MUST be excluded:
  - computed columns,
  - `rowversion` / `timestamp` columns,
  - hidden columns,
  - generated-always columns.
- When the table has an identity column and at least one row is emitted, the identity column value MUST be included in the `INSERT` column list and the file MUST wrap the emitted `INSERT` statements with:
  - `SET IDENTITY_INSERT [schema].[table] ON;`
  - `SET IDENTITY_INSERT [schema].[table] OFF;`
- Row ordering MUST be deterministic:
  - when a primary key exists, rows MUST be ordered by primary-key columns ascending in key ordinal order,
  - otherwise rows MUST be ordered by the canonical scripted literal tuple for the emitted columns, compared lexicographically in column order.
- Data-row literal rendering MUST be deterministic and use invariant culture.
- Literal rendering rules:
  - `NULL` values MUST be rendered as `NULL`.
  - `bit`, integer, decimal, numeric, money, smallmoney, float, and real values MUST be rendered as unquoted invariant numeric literals.
  - `uniqueidentifier` values MUST be rendered as quoted canonical text literals.
  - `char`, `varchar`, and `text` values MUST be rendered as `'...'` with embedded single quotes doubled.
  - `nchar`, `nvarchar`, `ntext`, and `xml` values MUST be rendered as `N'...'` with embedded single quotes doubled.
  - `binary`, `varbinary`, and `image` values MUST be rendered as `0x...` uppercase hexadecimal.
  - `date` values MUST be rendered as `'YYYY-MM-DD'`.
  - `datetime`, `smalldatetime`, and `datetime2` values MUST be rendered as quoted ISO-style timestamps.
  - `datetimeoffset` values MUST be rendered as quoted ISO 8601 timestamps with offset.
  - `time` values MUST be rendered as quoted `HH:MM:SS.fffffff`.
  - `hierarchyid` values MUST be rendered as `hierarchyid::Parse(N'...')`.
  - `geometry` and `geography` values MUST be rendered as `geometry::STGeomFromWKB(0x..., <srid>)` and `geography::STGeomFromWKB(0x..., <srid>)`.
- Insertable column types not covered by this section or not safely serializable by the implementation MUST fail explicitly rather than emit lossy output.

## 9. Compatibility-Guided Reconciliation
When compatibility reference files are available, `sqlct` MAY apply reconciliation per object after script generation and before writing files.

- Reconciliation MUST be formatting-only and MUST NOT change SQL semantics, object identity, or required statement ordering.
- Reconciliation MAY preserve programmable-object blank-line placement immediately:
  - after the final header `GO` and before the first definition line,
  - after the final definition line and before the trailing `GO`.
- Reconciliation MAY align generated definition lines to uniquely matched reference-definition lines.
- Reconciliation MAY preserve semantically equivalent built-in/system type-token spelling for table columns when metadata resolves to the same canonical type (for example `[sys].[sysname]` vs `[sysname]`, `[sys].[hierarchyid]` vs `[hierarchyid]`, `(max)` vs `(MAX)`).
- Reconciliation MAY preserve semantically equivalent computed-column expression token spelling when metadata resolves to the same computed expression semantics (for example explicit default `CONVERT(..., (0))` versus the omitted default-style form).
- Table reconciliation MAY preserve compatible reference formatting within the `CREATE TABLE` block, including the table close line and semantically equivalent column type-token spelling already allowed by this section.
- Table reconciliation MAY reuse the full reference `CREATE TABLE` block only when the normalized generated block and normalized reference block are semantically identical in column order, column semantics, and storage clause semantics.
- Table reconciliation MAY reuse compatible CHECK-constraint statement lines.
- Table reconciliation MAY preserve compatible relative ordering of key-constraint, non-constraint-index, and XML-index statements within the post-create block segment between CHECK constraints and foreign keys.
- When a reference `CREATE TABLE` block is not semantically identical after normalization, reconciliation MUST keep the generated canonical `CREATE TABLE` block.
- Reconciliation MUST NOT be used to change:
  - user-defined type qualification,
  - XML schema collection binding,
  - nullability,
  - identity/default clauses,
  - `ROWGUIDCOL`,
  - key/index/foreign-key semantics,
  - required statement ordering.
- When compatibility inputs are missing, incomplete, or non-matching, scripting MUST continue with deterministic canonical formatting.
- Baseline `sqlct` scripting MUST NOT require compatibility files.

## 10. Normalization for Diff/Status
- Script generation MUST emit canonical scripting output per this document and MUST NOT include diff/status-specific normalization.
- `status` and `diff` normalization behaviors are external contracts defined in `specs/01-cli.md` and `specs/05-output-formats.md`.
- Scripting and comparison normalization responsibilities MUST remain decoupled.

## 11. Error and Unsupported Behavior
- Missing SQL object metadata for requested object MUST fail with an error.
- SQL connection/query failures MUST fail scripting.
- Unsupported folder entries MUST emit warning-level diagnostics and MUST be skipped.
- Missing compatibility file MUST NOT fail scripting; reconciliation is skipped for that object.
- Error-to-exit-code mapping remains governed by CLI/error specifications (`specs/01-cli.md`, `specs/06-errors.md`).

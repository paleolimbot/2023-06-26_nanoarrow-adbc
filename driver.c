
#include <string.h>

#include <adbc.h>
#include "nanoarrow.h"

// A little bit of hack, but we really do need placeholders for the private
// data for driver/database/connection/statement even though we don't use them.
// A real driver *would* use them, but also, the way to mark AdbcDriver and
// friends as released is to set the private_data to NULL. Therefore, we need
// something that is *not* null to put there at the very least.
struct SimpleCsvDriverPrivate {
  int not_empty;
};

struct SimpleCsvDatabasePrivate {
  int not_empty;
};

struct SimpleCsvConnectionPrivate {
  int not_empty;
};

struct SimpleCsvStatementPrivate {
  int not_empty;
};

static void ReleaseError(struct AdbcError* error) {
  ArrowFree(error->message);
  error->release = NULL;
}

static void SetErrorConst(struct AdbcError* error, const char* value) {
  if (error == NULL) {
    return;
  }

  memset(error, 0, sizeof(struct AdbcError));
  int64_t value_size = strlen(value);
  error->message = (char*)ArrowMalloc(value_size + 1);
  if (error->message == NULL) {
    error->message = "Failed to allocate memory for error message";
    return;
  }

  memcpy(error->message, value, value_size);
  error->release = &ReleaseError;
}

static AdbcStatusCode SimpleCsvDriverRelease(struct AdbcDriver* driver,
                                        struct AdbcError* error) {
  if (driver->private_data == NULL) {
    return ADBC_STATUS_OK;
  }

  ArrowFree(driver->private_data);
  driver->private_data = NULL;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvDatabaseNew(struct AdbcDatabase* database,
                                      struct AdbcError* error) {
  struct SimpleCsvDatabasePrivate* database_private =
      (struct SimpleCsvDatabasePrivate*)ArrowMalloc(sizeof(struct SimpleCsvDatabasePrivate));
  if (database_private == NULL) {
    SetErrorConst(error, "failed to allocate SimpleCsvDatabasePrivate");
    return ADBC_STATUS_INTERNAL;
  }

  memset(database_private, 0, sizeof(struct SimpleCsvDatabasePrivate));
  database->private_data = database_private;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvDatabaseInit(struct AdbcDatabase* database,
                                       struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvDatabaseSetOption(struct AdbcDatabase* database,
                                            const char* key, const char* value,
                                            struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvDatabaseRelease(struct AdbcDatabase* database,
                                          struct AdbcError* error) {
  if (database->private_data == NULL) {
    return ADBC_STATUS_OK;
  }

  ArrowFree(database->private_data);
  database->private_data = NULL;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvConnectionCommit(struct AdbcConnection* connection,
                                           struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode SimpleCsvConnectionGetInfo(struct AdbcConnection* connection,
                                            uint32_t* info_codes,
                                            size_t info_codes_length,
                                            struct ArrowArrayStream* stream,
                                            struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode SimpleCsvConnectionGetObjects(
    struct AdbcConnection* connection, int depth, const char* catalog,
    const char* db_schema, const char* table_name, const char** table_types,
    const char* column_name, struct ArrowArrayStream* stream, struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode SimpleCsvConnectionGetTableSchema(
    struct AdbcConnection* connection, const char* catalog, const char* db_schema,
    const char* table_name, struct ArrowSchema* schema, struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode SimpleCsvConnectionGetTableTypes(struct AdbcConnection* connection,
                                                  struct ArrowArrayStream* stream,
                                                  struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode SimpleCsvConnectionInit(struct AdbcConnection* connection,
                                         struct AdbcDatabase* database,
                                         struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvConnectionNew(struct AdbcConnection* connection,
                                        struct AdbcError* error) {
  struct SimpleCsvConnectionPrivate* connection_private =
      (struct SimpleCsvConnectionPrivate*)ArrowMalloc(sizeof(struct SimpleCsvConnectionPrivate));
  if (connection_private == NULL) {
    SetErrorConst(error, "failed to allocate SimpleCsvConnectionPrivate");
    return ADBC_STATUS_INTERNAL;
  }

  memset(connection_private, 0, sizeof(struct SimpleCsvConnectionPrivate));
  connection->private_data = connection_private;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvConnectionReadPartition(struct AdbcConnection* connection,
                                                  const uint8_t* serialized_partition,
                                                  size_t serialized_length,
                                                  struct ArrowArrayStream* out,
                                                  struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode SimpleCsvConnectionRelease(struct AdbcConnection* connection,
                                            struct AdbcError* error) {
  if (connection->private_data == NULL) {
    return ADBC_STATUS_OK;
  }

  ArrowFree(connection->private_data);
  connection->private_data = NULL;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvConnectionRollback(struct AdbcConnection* connection,
                                             struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode SimpleCsvConnectionSetOption(struct AdbcConnection* connection,
                                              const char* key, const char* value,
                                              struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvStatementBind(struct AdbcStatement* statement,
                                        struct ArrowArray* values,
                                        struct ArrowSchema* schema,
                                        struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}  // NOLINT(whitespace/indent)

static AdbcStatusCode SimpleCsvStatementBindStream(struct AdbcStatement* statement,
                                              struct ArrowArrayStream* stream,
                                              struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode SimpleCsvStatementExecutePartitions(struct AdbcStatement* statement,
                                                     struct ArrowSchema* schema,
                                                     struct AdbcPartitions* partitions,
                                                     int64_t* rows_affected,
                                                     struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}  // NOLINT(whitespace/indent)

static AdbcStatusCode SimpleCsvStatementExecuteQuery(struct AdbcStatement* statement,
                                                struct ArrowArrayStream* out,
                                                int64_t* rows_affected,
                                                struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode SimpleCsvStatementGetParameterSchema(struct AdbcStatement* statement,
                                                      struct ArrowSchema* schema,
                                                      struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode SimpleCsvStatementNew(struct AdbcConnection* connection,
                                       struct AdbcStatement* statement,
                                       struct AdbcError* error) {
  struct SimpleCsvStatementPrivate* statement_private =
      (struct SimpleCsvStatementPrivate*)ArrowMalloc(sizeof(struct SimpleCsvStatementPrivate));
  if (statement_private == NULL) {
    SetErrorConst(error, "failed to allocate SimpleCsvStatementPrivate");
    return ADBC_STATUS_INTERNAL;
  }

  memset(statement_private, 0, sizeof(struct SimpleCsvStatementPrivate));
  statement->private_data = statement_private;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvStatementPrepare(struct AdbcStatement* statement,
                                           struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode SimpleCsvStatementRelease(struct AdbcStatement* statement,
                                           struct AdbcError* error) {
  if (statement->private_data == NULL) {
    return ADBC_STATUS_OK;
  }

  ArrowFree(statement->private_data);
  statement->private_data = NULL;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvStatementSetOption(struct AdbcStatement* statement,
                                             const char* key, const char* value,
                                             struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvStatementSetSqlQuery(struct AdbcStatement* statement,
                                               const char* query,
                                               struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

// TODO: I think you can omit functions that would normally just return
// ADBC_STATUS_NOT_IMPLEMENTED and the driver manager fills them in.
// I don't know if that's considered good practice or not.
static AdbcStatusCode SimpleCsvDriverInitFunc(int version, void* raw_driver,
                                         struct AdbcError* error) {
  if (version != ADBC_VERSION_1_0_0) return ADBC_STATUS_NOT_IMPLEMENTED;
  struct AdbcDriver* driver = (struct AdbcDriver*)raw_driver;
  memset(driver, 0, sizeof(struct AdbcDriver));

  struct SimpleCsvDriverPrivate* driver_private =
      (struct SimpleCsvDriverPrivate*)ArrowMalloc(sizeof(struct SimpleCsvDriverPrivate));
  if (driver_private == NULL) {
    SetErrorConst(error, "failed to allocate SimpleCsvDriverPrivate");
    return ADBC_STATUS_INTERNAL;
  }

  memset(driver_private, 0, sizeof(struct SimpleCsvDriverPrivate));
  driver->private_data = driver_private;

  driver->DatabaseInit = &SimpleCsvDatabaseInit;
  driver->DatabaseNew = SimpleCsvDatabaseNew;
  driver->DatabaseRelease = SimpleCsvDatabaseRelease;
  driver->DatabaseSetOption = SimpleCsvDatabaseSetOption;

  driver->ConnectionCommit = SimpleCsvConnectionCommit;
  driver->ConnectionGetInfo = SimpleCsvConnectionGetInfo;
  driver->ConnectionGetObjects = SimpleCsvConnectionGetObjects;
  driver->ConnectionGetTableSchema = SimpleCsvConnectionGetTableSchema;
  driver->ConnectionGetTableTypes = SimpleCsvConnectionGetTableTypes;
  driver->ConnectionInit = SimpleCsvConnectionInit;
  driver->ConnectionNew = SimpleCsvConnectionNew;
  driver->ConnectionReadPartition = SimpleCsvConnectionReadPartition;
  driver->ConnectionRelease = SimpleCsvConnectionRelease;
  driver->ConnectionRollback = SimpleCsvConnectionRollback;
  driver->ConnectionSetOption = SimpleCsvConnectionSetOption;

  driver->StatementBind = SimpleCsvStatementBind;
  driver->StatementBindStream = SimpleCsvStatementBindStream;
  driver->StatementExecutePartitions = SimpleCsvStatementExecutePartitions;
  driver->StatementExecuteQuery = SimpleCsvStatementExecuteQuery;
  driver->StatementGetParameterSchema = SimpleCsvStatementGetParameterSchema;
  driver->StatementNew = SimpleCsvStatementNew;
  driver->StatementPrepare = SimpleCsvStatementPrepare;
  driver->StatementRelease = SimpleCsvStatementRelease;
  driver->StatementSetOption = SimpleCsvStatementSetOption;
  driver->StatementSetSqlQuery = SimpleCsvStatementSetSqlQuery;

  driver->release = SimpleCsvDriverRelease;

  return ADBC_STATUS_OK;
}

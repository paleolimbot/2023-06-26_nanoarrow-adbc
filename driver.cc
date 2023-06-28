
#include <stdlib.h>
#include <string>

#include "adbc.h"
#include "simple_csv_reader.h"

// A little bit of hack, but we really do need placeholders for the private
// data for driver/database/connection/statement even though we don't use them.
// A real driver *would* use them, but also, the way to mark AdbcDriver and
// friends as released is to set the private_data to nullptr. Therefore, we need
// something that is *not* null to put there at the very least.
struct SimpleCsvDriverPrivate {
  int not_empty;
};

struct SimpleCsvDatabasePrivate {
  std::string filename;
};

struct SimpleCsvConnectionPrivate {
  std::string filename;
};

struct SimpleCsvStatementPrivate {
  std::string filename;
};

static void ReleaseError(struct AdbcError* error) {
  free(error->message);
  error->release = nullptr;
}

static void SetErrorConst(struct AdbcError* error, const char* value) {
  if (error == nullptr) {
    return;
  }

  memset(error, 0, sizeof(struct AdbcError));
  int64_t value_size = strlen(value);
  error->message = (char*)malloc(value_size + 1);
  if (error->message == nullptr) {
    return;
  }

  memcpy(error->message, value, value_size);
  error->release = &ReleaseError;
}

static AdbcStatusCode SimpleCsvDriverRelease(struct AdbcDriver* driver,
                                             struct AdbcError* error) {
  if (driver->private_data == nullptr) {
    return ADBC_STATUS_OK;
  }

  auto driver_private = reinterpret_cast<SimpleCsvDriverPrivate*>(driver->private_data);
  delete driver_private;
  driver->private_data = nullptr;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvDatabaseNew(struct AdbcDatabase* database,
                                           struct AdbcError* error) {
  database->private_data = new SimpleCsvDatabasePrivate();
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvDatabaseSetOption(struct AdbcDatabase* database,
                                                 const char* key, const char* value,
                                                 struct AdbcError* error) {
  if (std::string(key) == "uri") {
    auto database_private =
        reinterpret_cast<SimpleCsvDatabasePrivate*>(database->private_data);
    database_private->filename = value;
    return ADBC_STATUS_OK;
  } else {
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
}

static AdbcStatusCode SimpleCsvDatabaseInit(struct AdbcDatabase* database,
                                            struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvDatabaseRelease(struct AdbcDatabase* database,
                                               struct AdbcError* error) {
  if (database->private_data == nullptr) {
    return ADBC_STATUS_OK;
  }

  auto database_private =
      reinterpret_cast<SimpleCsvDatabasePrivate*>(database->private_data);
  delete database_private;
  database->private_data = nullptr;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvConnectionNew(struct AdbcConnection* connection,
                                             struct AdbcError* error) {
  connection->private_data = new SimpleCsvConnectionPrivate();
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvConnectionInit(struct AdbcConnection* connection,
                                              struct AdbcDatabase* database,
                                              struct AdbcError* error) {
  auto connection_private =
      reinterpret_cast<SimpleCsvConnectionPrivate*>(connection->private_data);
  auto database_private =
      reinterpret_cast<SimpleCsvDatabasePrivate*>(database->private_data);
  connection_private->filename = database_private->filename;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvConnectionRelease(struct AdbcConnection* connection,
                                                 struct AdbcError* error) {
  if (connection->private_data == nullptr) {
    return ADBC_STATUS_OK;
  }

  auto connection_private =
      reinterpret_cast<SimpleCsvConnectionPrivate*>(connection->private_data);
  delete connection_private;
  connection->private_data = nullptr;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvStatementNew(struct AdbcConnection* connection,
                                            struct AdbcStatement* statement,
                                            struct AdbcError* error) {
  auto statement_private = new SimpleCsvStatementPrivate();
  auto connection_private =
      reinterpret_cast<SimpleCsvConnectionPrivate*>(connection->private_data);
  statement_private->filename = connection_private->filename;
  statement->private_data = statement_private;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvStatementRelease(struct AdbcStatement* statement,
                                                struct AdbcError* error) {
  if (statement->private_data == nullptr) {
    return ADBC_STATUS_OK;
  }

  auto statement_private =
      reinterpret_cast<SimpleCsvStatementPrivate*>(statement->private_data);
  delete statement_private;
  statement->private_data = nullptr;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvStatementSetSqlQuery(struct AdbcStatement* statement,
                                                    const char* query,
                                                    struct AdbcError* error) {
  if (std::string(query) == "") {
    return ADBC_STATUS_OK;
  } else {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }
}

static AdbcStatusCode SimpleCsvStatementExecuteQuery(struct AdbcStatement* statement,
                                                     struct ArrowArrayStream* out,
                                                     int64_t* rows_affected,
                                                     struct AdbcError* error) {
  auto statement_private =
      reinterpret_cast<SimpleCsvStatementPrivate*>(statement->private_data);
  InitSimpleCsvArrayStream(statement_private->filename.c_str(), out);
  *rows_affected = -1;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvDriverInitFunc(int version, void* raw_driver,
                                              struct AdbcError* error) {
  if (version != ADBC_VERSION_1_0_0) return ADBC_STATUS_NOT_IMPLEMENTED;
  struct AdbcDriver* driver = (struct AdbcDriver*)raw_driver;
  memset(driver, 0, sizeof(struct AdbcDriver));
  driver->private_data = new SimpleCsvDriverPrivate();

  driver->DatabaseNew = SimpleCsvDatabaseNew;
  driver->DatabaseSetOption = SimpleCsvDatabaseSetOption;
  driver->DatabaseInit = &SimpleCsvDatabaseInit;
  driver->DatabaseRelease = SimpleCsvDatabaseRelease;

  driver->ConnectionNew = SimpleCsvConnectionNew;
  driver->ConnectionInit = SimpleCsvConnectionInit;
  driver->ConnectionRelease = SimpleCsvConnectionRelease;

  driver->StatementNew = SimpleCsvStatementNew;
  driver->StatementSetSqlQuery = SimpleCsvStatementSetSqlQuery;
  driver->StatementExecuteQuery = SimpleCsvStatementExecuteQuery;
  driver->StatementRelease = SimpleCsvStatementRelease;

  driver->release = SimpleCsvDriverRelease;

  return ADBC_STATUS_OK;
}
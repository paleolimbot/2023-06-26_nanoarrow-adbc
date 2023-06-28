
library(adbcdrivermanager)

simple_csv_drv <- adbc_driver(
  "build/libadbc_simple_csv_driver.dylib",
  "SimpleCsvDriverInit"
)

adbc_database_init(simple_csv_drv, uri = "test.csv") |>
  read_adbc("") |>
  as.data.frame()

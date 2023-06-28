
project(AdbcSimpleCsvDriver)

set(CMAKE_CXX_STANDARD 11)

add_library(
    adbc_simple_csv_driver
    simple_csv_reader.cc
    driver.cc
    nanoarrow.c)

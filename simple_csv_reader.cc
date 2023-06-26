
#include <cerrno>
#include <fstream>
#include <string>
#include <vector>

#include "nanoarrow.hpp"

enum class ScanResult { UNINITIALIZED, FIELD_SEP, LINE_SEP, DONE, ERROR };

class SimpleCsvScanner {
 public:
  SimpleCsvScanner(const std::string& filename) : input_(filename, std::ios::binary) {}

  ScanResult NextField(ArrowStringView* out) { return ScanResult::ERROR; }

  ScanResult ReadLine(std::vector<std::string>* values) {
    ArrowStringView view;
    while ((status_ = NextField(&view)) == ScanResult::FIELD_SEP) {
      values->push_back(std::string(view.data, view.size_bytes));
    }

    return status_;
  }

 private:
  std::ifstream input_;
  nanoarrow::UniqueBuffer buffer_;
  ScanResult status_;

  static int64_t find_char(const ArrowStringView& src, char value) {
    for (int64_t i = 0; i < src.size_bytes; i++) {
      if (src.data[i] == value) {
        return i;
      }
    }

    return src.size_bytes;
  }
};

class SimpleCsvArrayBuilder {
 public:
  SimpleCsvArrayBuilder(const std::string& filename) : scanner_(filename) {
    ArrowErrorSet(&last_error_, "Internal error");
  }

  int get_schema(ArrowSchema* out) {
    NANOARROW_RETURN_NOT_OK(ReadSchemaIfNeeded());
    NANOARROW_RETURN_NOT_OK(ArrowSchemaDeepCopy(schema_.get(), out));
    return NANOARROW_OK;
  }

  int get_array(ArrowArray* out) {
    NANOARROW_RETURN_NOT_OK(ReadSchemaIfNeeded());
    NANOARROW_RETURN_NOT_OK(InitArrayIfNeeded());

    while (status_ != ScanResult::DONE) {
      NANOARROW_RETURN_NOT_OK(ReadLine());
    }

    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(array_.get(), &last_error_));
    ArrowArrayMove(array_.get(), out);
    return NANOARROW_OK;
  }

  const char* get_last_error() {
    return last_error_.message;
  }

 private:
  ScanResult status_;
  SimpleCsvScanner scanner_;
  std::vector<std::string> fields_;
  ArrowError last_error_;
  nanoarrow::UniqueSchema schema_;
  nanoarrow::UniqueArray array_;

  int ReadSchemaIfNeeded() {
    if (schema_->release != nullptr) {
      return NANOARROW_OK;
    }

    fields_.clear();
    status_ = scanner_.ReadLine(&fields_);

    if (status_ == ScanResult::ERROR) {
      ArrowErrorSet(&last_error_, "Scan error");
      return EINVAL;
    }

    ArrowSchemaInit(schema_.get());
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeStruct(schema_.get(), fields_.size()));
    for (int64_t i = 0; i < schema_->n_children; i++) {
      NANOARROW_RETURN_NOT_OK(
          ArrowSchemaSetType(schema_->children[i], NANOARROW_TYPE_STRING));
      NANOARROW_RETURN_NOT_OK(
          ArrowSchemaSetName(schema_->children[i], fields_[i].c_str()));
    }

    return NANOARROW_OK;
  }

  int InitArrayIfNeeded() {
    if (array_->release != nullptr) {
      return NANOARROW_OK;
    }

    NANOARROW_RETURN_NOT_OK(
        ArrowArrayInitFromSchema(array_.get(), schema_.get(), &last_error_));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(array_.get()));
    return NANOARROW_OK;
  }

  int ReadLine() {
    fields_.clear();
    status_ = scanner_.ReadLine(&fields_);

    if (status_ == ScanResult::ERROR) {
      ArrowErrorSet(&last_error_, "Scan error");
      return EINVAL;
    }

    if (schema_->n_children != fields_.size()) {
      ArrowErrorSet(&last_error_, "Expected %ld fields but found %ld fields",
                        (long)schema_->n_children, (long)fields_.size());
      return EINVAL;
    }

    ArrowStringView view;
    for (int64_t i = 0; i < (schema_->n_children - 1); i++) {
      view.data = fields_[i].data();
      view.size_bytes = fields_[i].size();
      NANOARROW_RETURN_NOT_OK(
            ArrowArrayAppendString(array_->children[array_->n_children - 1], view));
    }

    return NANOARROW_OK;
  }
};

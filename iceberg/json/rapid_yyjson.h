// RapidJSON-like iface for yyjson

#include <memory>
#include <string>

namespace json {

#include "yyjson.h"

class Value {
 public:
  Value(yyjson_val* val = {}) : value_(val) {}

  bool Valid() const { return !!value_; }

  bool IsNull() const { return yyjson_get_type(value_) == YYJSON_TYPE_NULL; }
  // Boolean type, subtype: TRUE, FALSE.
  bool IsBool() const { return yyjson_get_type(value_) == YYJSON_TYPE_BOOL; }
  // Number type, subtype: UINT, SINT, REAL.
  bool IsNumber() const { return yyjson_get_type(value_) == YYJSON_TYPE_NUM; }
  // String type, subtype: NONE, NOESC.
  bool IsString() const { return yyjson_get_type(value_) == YYJSON_TYPE_STR; }
  bool IsArray() const { return yyjson_get_type(value_) == YYJSON_TYPE_ARR; }
  bool IsObject() const { return yyjson_get_type(value_) == YYJSON_TYPE_OBJ; }

  bool GetBool() const { return yyjson_get_bool(value_); }
  int GetInt() const { return yyjson_get_int(value_); }
  double GetDouble() const { return yyjson_get_real(value_); }

  std::string_view GetString() const {
    if (const char* ptr = yyjson_get_str(value_)) {
      return std::string_view(ptr);
    }
    return {};
  }

  size_t GetStringLength() const { return yyjson_get_len(value_); }

  // Object functions

  /// @warning This function takes a linear search time.
  Value GetMember(std::string_view name) const { return yyjson_obj_get(value_, name.data()); }
  bool HasMember(std::string_view name) const { return GetMember(name).Valid(); }

  Value operator[](std::string_view name) const {
    // assert(IsObject());
    return GetMember(name);
  }

  // Array functions
  // TODO: iterator with yyjson_arr_iter_with, yyjson_arr_iter_has_next, yyjson_arr_iter_next

  size_t Size() const { return yyjson_arr_size(value_); }
  Value GetFirstElement() const { return yyjson_arr_get_first(value_); }
  static Value GetNextElement(Value val) { return unsafe_yyjson_get_next(val.value_); }
  Value GetElement(size_t idx) const { return yyjson_arr_get(value_, idx); }

  Value operator[](size_t idx) const {
    // assert(IsArray());
    return GetElement(idx);
  }

 protected:
  yyjson_val* value_;
};

class Document : public Value {
 public:
  Document() = default;
  Document(std::string_view json, yyjson_read_flag flags = 0) { Parse(json, flags); }

  void Parse(std::string_view json, yyjson_read_flag flags = 0) {
    doc_ = std::shared_ptr<yyjson_doc>(yyjson_read(json.data(), json.length(), flags), yyjson_doc_free);
    value_ = yyjson_doc_get_root(doc_.get());
  }

 private:
  std::shared_ptr<yyjson_doc> doc_;
};

}  // namespace json

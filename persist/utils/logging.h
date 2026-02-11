#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_PERSIST_LOGGING_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_PERSIST_LOGGING_H

#include "fmtlog-inl.h"
#include "google/cloud/status.h"
#include <string_view>
#include <google/bigtable/v2/data.pb.h>

#define DBG(...) logd(__VA_ARGS__)
#define LERROR(...) loge(__VA_ARGS__)
#define LWARN(...) logw(__VA_ARGS__)

static inline void ConfigureEmulatorLogging() {
  fmtlog::setLogFile("emulator.log", false);
  fmtlog::setLogLevel(fmtlog::WRN);
  fmtlog::setThreadName("main");
}

// Custom fmt formatter for status
template <> struct fmt::formatter<google::cloud::Status> : fmt::formatter<std::string_view> {
  template <typename FormatContext>
  auto format(google::cloud::Status status, FormatContext& ctx) const {
    return formatter<std::string_view>::format(status.message(), ctx);
  }
};

// Custom fmt formatter for KindCase
template <> struct fmt::formatter<google::bigtable::v2::Value::KindCase> : fmt::formatter<std::string_view> {
  template <typename FormatContext>
  auto format(google::bigtable::v2::Value::KindCase kind_case, FormatContext& ctx) const {
    std::string_view name = "unknown";
    switch (kind_case) {
      case google::bigtable::v2::Value::KindCase::kRawValue: name = "raw_value"; break; 
      case google::bigtable::v2::Value::KindCase::kRawTimestampMicros: name = "raw_timestamp_micros"; break; 
      case google::bigtable::v2::Value::KindCase::kBytesValue: name = "bytes"; break; 
      case google::bigtable::v2::Value::KindCase::kStringValue: name = "string"; break; 
      case google::bigtable::v2::Value::KindCase::kIntValue: name = "int"; break; 
      case google::bigtable::v2::Value::KindCase::kBoolValue: name = "bool"; break; 
      case google::bigtable::v2::Value::KindCase::kFloatValue: name = "float"; break; 
      case google::bigtable::v2::Value::KindCase::kTimestampValue: name = "timestamp"; break; 
      case google::bigtable::v2::Value::KindCase::kDateValue: name = "date"; break; 
      case google::bigtable::v2::Value::KindCase::kArrayValue: name = "array"; break; 
      case google::bigtable::v2::Value::KindCase::KIND_NOT_SET: name = "not_set"; break; 
    }
    return formatter<std::string_view>::format(name, ctx);
  }
};

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_PERSIST_LOGGING_H
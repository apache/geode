#pragma once

#include <cstdint>
#include <memory>
#include <string>

namespace gemfire {
class ByteArray {
 public:
  static ByteArray fromString(const std::string &str);

  static ByteArray fromString(const wchar_t *wstr);

  static std::string toString(const ByteArray &bytes);

  ByteArray();

  ByteArray(const ByteArray &other);

  ByteArray(const uint8_t *bytes, const std::size_t size);

  virtual ~ByteArray();

  ByteArray &operator=(const ByteArray &other);

  operator const uint8_t *() const { return m_bytes.get(); }

  operator uint8_t *() { return m_bytes.get(); }

  const uint8_t *get() const { return m_bytes.get(); }

  std::size_t size() const { return m_size; }

  std::string toString() const;

 private:
  std::shared_ptr<uint8_t> m_bytes;
  std::size_t m_size;
};
}  // namespace gemfire

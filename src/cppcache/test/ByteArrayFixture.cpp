#include "ByteArrayFixture.hpp"

::testing::AssertionResult ByteArrayFixture::assertByteArrayEqual(
    const char* expectedStr, const char* bytesStr, const char* expected,
    const gemfire::ByteArray& bytes) {
  // One would normally just use std::regex but gcc 4.4.7 is lacking.
  const std::string actual(gemfire::ByteArray::toString(bytes));
  std::string::size_type actualPos = 0;
  const std::string pattern(expected);
  std::string::size_type patternPos = 0;
  while (patternPos < pattern.length()) {
    if (patternPos + 5 <= pattern.length() && '\\' == pattern[patternPos] &&
        'h' == pattern[patternPos + 1] && '{' == pattern[patternPos + 2] &&
        ('0' <= pattern[patternPos + 3] && pattern[patternPos + 3] <= '9')) {
      patternPos += 3;
      std::string::size_type startPos = patternPos;
      while (patternPos < pattern.length() && '}' != pattern[patternPos]) {
        ++patternPos;
      }
      const int number =
          std::stoi(pattern.substr(startPos, (patternPos - startPos)));

      std::string::size_type pos =
          actual.find_first_not_of("0123456789ABCDEFabcdef", actualPos);
      if (pos == std::string::npos) {
        pos = actual.length();
      }
      if ((pos - actualPos) < number) {
        break;
      }
      actualPos += number;
    } else if (pattern[patternPos] != actual[actualPos++]) {
      break;
    }
    ++patternPos;
  }

  if (patternPos != pattern.length() || actualPos != actual.length()) {
    return ::testing::AssertionFailure() << pattern << " != " << actual;
  }
  return ::testing::AssertionSuccess();
}

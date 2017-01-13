#include <string.h>  // for ::memset()

#include <stdint.h>
#define NOMINMAX
#include <cstdint>

#include <gtest/gtest.h>

#include <gfcpp/DataInput.hpp>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/SharedPtr.hpp>
#include "ByteArrayFixture.hpp"

using namespace gemfire;

class TestDataInput {
 public:
  explicit TestDataInput(const char *str)
      : m_byteArray(ByteArray::fromString(str)),
        m_dataInput(m_byteArray.get(), m_byteArray.size()) {
    // NOP
  }

  explicit TestDataInput(const wchar_t *str)
      : m_byteArray(ByteArray::fromString(str)),
        m_dataInput(m_byteArray.get(), m_byteArray.size()) {
    // NOP
  }

  operator const DataInput &() const { return m_dataInput; }

  operator DataInput &() { return m_dataInput; }

  void read(uint8_t *value) { m_dataInput.read(value); }

  void read(int8_t *value) { m_dataInput.read(value); }

  void readBoolean(bool *value) { m_dataInput.readBoolean(value); }

  void readBytesOnly(uint8_t *buffer, uint32_t len) {
    m_dataInput.readBytesOnly(buffer, len);
  }

  void readBytesOnly(int8_t *buffer, uint32_t len) {
    m_dataInput.readBytesOnly(buffer, len);
  }

  void readBytes(uint8_t **buffer, int32_t *len) {
    m_dataInput.readBytes(buffer, len);
  }

  void readBytes(int8_t **buffer, int32_t *len) {
    m_dataInput.readBytes(buffer, len);
  }

  void readInt(uint16_t *value) { m_dataInput.readInt(value); }

  void readInt(int16_t *value) { m_dataInput.readInt(value); }

  void readInt(uint32_t *value) { m_dataInput.readInt(value); }

  void readInt(int32_t *value) { m_dataInput.readInt(value); }

  void readInt(uint64_t *value) { m_dataInput.readInt(value); }

  void readInt(int64_t *value) { m_dataInput.readInt(value); }

  void readArrayLen(int32_t *len) { m_dataInput.readArrayLen(len); }

  void readUnsignedVL(int64_t *value) { m_dataInput.readUnsignedVL(value); }

  void readFloat(float *value) { m_dataInput.readFloat(value); }

  void readDouble(double *value) { m_dataInput.readDouble(value); }

  void readASCII(char **value, uint16_t *len = NULL) {
    m_dataInput.readASCII(value, len);
  }

  void readASCIIHuge(char **value, uint32_t *len = NULL) {
    m_dataInput.readASCIIHuge(value, len);
  }

  void readUTF(char **value, uint16_t *len = NULL) {
    m_dataInput.readUTF(value, len);
  }

  void readUTFHuge(char **value, uint32_t *len = NULL) {
    m_dataInput.readUTFHuge(value, len);
  }

  void readUTFNoLen(wchar_t **value, uint16_t decodedLen) {
    m_dataInput.readUTFNoLen(value, decodedLen);
  }

  void readUTF(wchar_t **value, uint16_t *len = NULL) {
    m_dataInput.readUTF(value, len);
  }

  void readUTFHuge(wchar_t **value, uint32_t *len = NULL) {
    m_dataInput.readUTFHuge(value, len);
  }

  template <class PTR>
  void readObject(SharedPtr<PTR> &ptr,
                  bool throwOnError = DINP_THROWONERROR_DEFAULT) {
    m_dataInput.readObject(ptr, throwOnError);
  }

  bool readNativeBool() { return m_dataInput.readNativeBool(); }

  int32_t readNativeInt32() { return m_dataInput.readNativeInt32(); }

  bool readNativeString(CacheableStringPtr &csPtr) {
    return m_dataInput.readNativeString(csPtr);
  }

  void readDirectObject(SerializablePtr &ptr, int8_t typeId = -1) {
    m_dataInput.readDirectObject(ptr, typeId);
  }

  void readObject(SerializablePtr &ptr) { m_dataInput.readObject(ptr); }

  void readCharArray(char **value, int32_t &length) {
    m_dataInput.readCharArray(value, length);
  }

  void readString(char **value) { m_dataInput.readString(value); }

  void readWideString(wchar_t **value) { m_dataInput.readWideString(value); }

  void readStringArray(char ***strArray, int32_t &length) {
    m_dataInput.readStringArray(strArray, length);
  }

  void readWideStringArray(wchar_t ***strArray, int32_t &length) {
    m_dataInput.readWideStringArray(strArray, length);
  }

  void readArrayOfByteArrays(int8_t ***arrayofBytearr, int32_t &arrayLength,
                             int32_t **elementLength) {
    m_dataInput.readArrayOfByteArrays(arrayofBytearr, arrayLength,
                                      elementLength);
  }

  const uint8_t *currentBufferPosition() const {
    return m_dataInput.currentBufferPosition();
  }

  int32_t getBytesRead() const { return m_dataInput.getBytesRead(); }

  int32_t getBytesRemaining() const { return m_dataInput.getBytesRemaining(); }

  void advanceCursor(int32_t offset) { m_dataInput.advanceCursor(offset); }

  void rewindCursor(int32_t offset) { m_dataInput.rewindCursor(offset); }

  void reset() { m_dataInput.reset(); }

  void setBuffer() { m_dataInput.setBuffer(); }

  const char *getPoolName() { return m_dataInput.getPoolName(); }

  void setPoolName(const char *poolName) { m_dataInput.setPoolName(poolName); }

 private:
  ByteArray m_byteArray;

  DataInput m_dataInput;
};

class DataInputTest : public ::testing::Test, protected ByteArrayFixture {
 public:
  virtual ~DataInputTest() {
    // NOP
  }
};

TEST_F(DataInputTest, ThrowsWhenReadingInputWithSizeZero) {
  TestDataInput dataInput("");

  uint8_t aByte = 0U;
  ASSERT_THROW(dataInput.read(&aByte), gemfire::OutOfRangeException);
}

TEST_F(DataInputTest, ThrowsWhenReadingMoreBytesThanSizePassedToConstructor) {
  TestDataInput dataInput("01");

  uint8_t aByte = 0U;
  dataInput.read(&aByte);
  EXPECT_EQ(1U, aByte);

  aByte = 0U;
  ASSERT_THROW(dataInput.read(&aByte), gemfire::OutOfRangeException);
}

TEST_F(DataInputTest, CanReadUnsignedBytesFromInput) {
  TestDataInput dataInput("FF00");

  uint8_t aByte = 0U;
  dataInput.read(&aByte);
  EXPECT_EQ(aByte, 255);

  aByte = 0U;
  dataInput.read(&aByte);
  EXPECT_EQ(aByte, 0);
}

TEST_F(DataInputTest, CanReadSignedBytesFromInput) {
  TestDataInput dataInput("807F");

  int8_t aByte = 0U;
  dataInput.read(&aByte);
  EXPECT_EQ(aByte, -128);

  aByte = 0;
  dataInput.read(&aByte);
  EXPECT_EQ(aByte, 127);
}

TEST_F(DataInputTest, CanReadABooleanFromInput) {
  bool boolArray[2] = {true, false};
  DataInput dataInput(reinterpret_cast<uint8_t *>(boolArray), 2);

  bool aBool = false;
  dataInput.readBoolean(&aBool);
  EXPECT_EQ(aBool, true);

  aBool = true;
  dataInput.readBoolean(&aBool);
  EXPECT_EQ(aBool, false);
}

TEST_F(DataInputTest, CanReadAnArrayOfBytesFromInput) {
  TestDataInput dataInput("010203");

  uint8_t byteArrayCopy[4];
  dataInput.readBytesOnly(byteArrayCopy, 3);
  EXPECT_EQ(byteArrayCopy[0], 1);
  EXPECT_EQ(byteArrayCopy[1], 2);
  EXPECT_EQ(byteArrayCopy[2], 3);
}

TEST_F(DataInputTest,
       ThrowsWhenReadingMoreContinuousBytesThanSizePassedToConstructor) {
  TestDataInput dataInput("010203");

  // fails to read 4 bytes from 3 byte buffer
  uint8_t byteArrayCopy[4];
  ASSERT_THROW(dataInput.readBytesOnly(byteArrayCopy, 4),
               gemfire::OutOfRangeException);
}

TEST_F(DataInputTest, CanReadIntWithAMaxSizeUnsigned64BitIntInput) {
  uint64_t intArray[1] = {std::numeric_limits<uint64_t>::max()};
  DataInput dataInput(reinterpret_cast<uint8_t *>(intArray), sizeof(intArray));

  uint64_t aInt = 0UL;
  dataInput.readInt(&aInt);
  EXPECT_EQ(aInt, std::numeric_limits<uint64_t>::max());
}

TEST_F(DataInputTest, CanReadASCIIWithAnASCIIStringInput) {
  char *actualString;
  const char *expectedString = "foobar";
  DataOutput stream;
  stream.writeASCII(expectedString);

  DataInput dataInput(stream.getBufferCopy(), stream.getBufferLength());
  dataInput.readASCII(&actualString);

  EXPECT_TRUE(std::string(expectedString) == std::string(actualString));
}

TEST_F(DataInputTest, ThrowsWhenCallingReadStringWithAnASCIIStringInput) {
  char *actualString;
  const char *expectedString = "foobar";
  DataOutput stream;
  stream.writeASCII(expectedString);

  DataInput dataInput(stream.getBufferCopy(), stream.getBufferLength());

  // ASCII and non-ASCII: consider matching exception string
  ASSERT_THROW(dataInput.readString(&actualString),
               gemfire::IllegalArgumentException);
}

TEST_F(DataInputTest, CanReadASCIIWithAnUTFStringInput) {
  char *actualString;
  const char *expectedString = "foobar";
  DataOutput stream;
  stream.writeUTF(expectedString);

  DataInput dataInput(stream.getBufferCopy(), stream.getBufferLength());
  dataInput.readASCII(&actualString);

  EXPECT_TRUE(std::string(expectedString) == std::string(actualString));
}

TEST_F(DataInputTest, ThrowsWhenCallingReadStringWithAnUTFStringInput) {
  char *actualString;
  const char *expectedString = "foobar";
  DataOutput stream;
  stream.writeUTF(expectedString);

  DataInput dataInput(stream.getBufferCopy(), stream.getBufferLength());

  // UTF and non-UTF: consider matching exception string
  ASSERT_THROW(dataInput.readString(&actualString),
               gemfire::IllegalArgumentException);
}

TEST_F(DataInputTest, CanReadUTFWithAnUTFStringInput) {
  char *actualString;
  const char *expectedString = "foobar";
  DataOutput stream;
  stream.writeUTF(expectedString);

  DataInput dataInput(stream.getBufferCopy(), stream.getBufferLength());
  dataInput.readUTF(&actualString);

  EXPECT_TRUE(std::string(expectedString) == std::string(actualString));
}

TEST_F(DataInputTest, CanReadUTFWithAnASCIIStringInput) {
  char *actualString;
  const char *expectedString = "foobar";
  DataOutput stream;
  stream.writeASCII(expectedString);

  DataInput dataInput(stream.getBufferCopy(), stream.getBufferLength());
  dataInput.readUTF(&actualString);

  EXPECT_TRUE(std::string(expectedString) == std::string(actualString));
}

TEST_F(DataInputTest, InputResetCausesPristineRead) {
  TestDataInput dataInput("010203");

  // 1) read byte off buffer
  // 2) then reset buffer back
  uint8_t aByte = 0U;
  dataInput.read(&aByte);
  dataInput.reset();

  // 3) next byte read should be start byte
  aByte = 0U;
  dataInput.read(&aByte);
  EXPECT_EQ(aByte, 1);
}

TEST_F(DataInputTest, InputRewindCausesReplayedRead) {
  TestDataInput dataInput("010203");

  uint8_t aByte = 0U;
  dataInput.read(&aByte);
  dataInput.read(&aByte);
  dataInput.read(&aByte);

  dataInput.rewindCursor(1);

  dataInput.read(&aByte);
  EXPECT_EQ(aByte, 3);
}

TEST_F(DataInputTest, TestReadUint8) {
  TestDataInput dataInput("37");
  uint8_t value = 0U;
  dataInput.read(&value);
  EXPECT_EQ((uint8_t)55U, value) << "Correct uint8_t";
}

TEST_F(DataInputTest, TestReadInt8) {
  TestDataInput dataInput("37");
  int8_t value = 0;
  dataInput.read(&value);
  EXPECT_EQ((int8_t)55, value) << "Correct int8_t";
}

TEST_F(DataInputTest, TestReadBoolean) {
  TestDataInput dataInput("01");
  bool value = false;
  dataInput.readBoolean(&value);
  EXPECT_EQ(true, value) << "Correct bool";
}

TEST_F(DataInputTest, TestReadUint8_tBytesOnly) {
  TestDataInput dataInput("BABEFACE");
  uint8_t buffer[4];
  ::memset(buffer, 0U, 4 * sizeof(uint8_t));
  dataInput.readBytesOnly(buffer, 4);
  EXPECT_EQ((uint8_t)186U, buffer[0]) << "Correct zeroth uint8_t";
  EXPECT_EQ((uint8_t)190U, buffer[1]) << "Correct first uint8_t";
  EXPECT_EQ((uint8_t)250U, buffer[2]) << "Correct second uint8_t";
  EXPECT_EQ((uint8_t)206U, buffer[3]) << "Correct third uint8_t";
}

TEST_F(DataInputTest, TestReadInt8_tBytesOnly) {
  TestDataInput dataInput("DEADBEEF");
  int8_t buffer[4];
  ::memset(buffer, 0, 4 * sizeof(int8_t));
  dataInput.readBytesOnly(buffer, 4);
  EXPECT_EQ((int8_t)-34, buffer[0]) << "Correct zeroth int8_t";
  EXPECT_EQ((int8_t)-83, buffer[1]) << "Correct first int8_t";
  EXPECT_EQ((int8_t)-66, buffer[2]) << "Correct second int8_t";
  EXPECT_EQ((int8_t)-17, buffer[3]) << "Correct third int8_t";
}

TEST_F(DataInputTest, TestReadUint8_tBytes) {
  TestDataInput dataInput("04BABEFACE");
  uint8_t *buffer = NULL;
  int32_t len = 0;
  dataInput.readBytes(&buffer, &len);
  EXPECT_NE((uint8_t *)NULL, buffer) << "Non-null buffer";
  ASSERT_EQ(4, len) << "Correct length";
  EXPECT_EQ((uint8_t)186U, buffer[0]) << "Correct zeroth uint8_t";
  EXPECT_EQ((uint8_t)190U, buffer[1]) << "Correct first uint8_t";
  EXPECT_EQ((uint8_t)250U, buffer[2]) << "Correct second uint8_t";
  EXPECT_EQ((uint8_t)206U, buffer[3]) << "Correct third uint8_t";
  GF_SAFE_DELETE_ARRAY(buffer);
}

TEST_F(DataInputTest, TestReadInt8_tBytes) {
  TestDataInput dataInput("04DEADBEEF");
  int8_t *buffer = NULL;
  int32_t len = 0;
  dataInput.readBytes(&buffer, &len);
  EXPECT_NE((int8_t *)NULL, buffer) << "Non-null buffer";
  ASSERT_EQ(4, len) << "Correct length";
  EXPECT_EQ((int8_t)-34, buffer[0]) << "Correct zeroth int8_t";
  EXPECT_EQ((int8_t)-83, buffer[1]) << "Correct first int8_t";
  EXPECT_EQ((int8_t)-66, buffer[2]) << "Correct second int8_t";
  EXPECT_EQ((int8_t)-17, buffer[3]) << "Correct third int8_t";
  GF_SAFE_DELETE_ARRAY(buffer);
}

TEST_F(DataInputTest, TestReadIntUint16) {
  TestDataInput dataInput("123456789ABCDEF0");
  uint16_t value = 0U;
  dataInput.readInt(&value);
  EXPECT_EQ((uint16_t)4660U, value) << "Correct uint16_t";
}

TEST_F(DataInputTest, TestReadIntInt16) {
  TestDataInput dataInput("123456789ABCDEF0");
  int16_t value = 0;
  dataInput.readInt(&value);
  EXPECT_EQ((int16_t)4660, value) << "Correct int16_t";
}

TEST_F(DataInputTest, TestReadIntUint32) {
  TestDataInput dataInput("123456789ABCDEF0");
  uint32_t value = 0U;
  dataInput.readInt(&value);
  EXPECT_EQ((uint32_t)305419896U, value) << "Correct uint32_t";
}

TEST_F(DataInputTest, TestReadIntInt32) {
  TestDataInput dataInput("123456789ABCDEF0");
  int32_t value = 0;
  dataInput.readInt(&value);
  EXPECT_EQ((int32_t)305419896, value) << "Correct int32_t";
}

TEST_F(DataInputTest, TestReadIntUint64) {
  TestDataInput dataInput("123456789ABCDEF0");
  uint64_t value = 0U;
  dataInput.readInt(&value);
  EXPECT_EQ((uint64_t)1311768467463790320U, value) << "Correct uint64_t";
}

TEST_F(DataInputTest, TestReadIntInt64) {
  TestDataInput dataInput("123456789ABCDEF0");
  int64_t value = 0;
  dataInput.readInt(&value);
  EXPECT_EQ((int64_t)1311768467463790320, value) << "Correct int64_t";
}

TEST_F(DataInputTest, TestReadArrayLen) {
  int32_t len = 0;

  TestDataInput dataInput0("FF12345678");
  dataInput0.readArrayLen(&len);
  EXPECT_EQ(-1, len) << "Correct length for 0xFF";

  TestDataInput dataInput1("FE12345678");
  dataInput1.readArrayLen(&len);
  EXPECT_EQ(4660, len) << "Correct length for 0xFE";

  TestDataInput dataInput2("FD12345678");
  dataInput2.readArrayLen(&len);
  EXPECT_EQ(305419896, len) << "Correct length for 0xFD";

  TestDataInput dataInput3("FC12345678");
  dataInput3.readArrayLen(&len);
  EXPECT_EQ(252, len) << "Correct length for 0xFC";
}

TEST_F(DataInputTest, TestReadUnsignedVL) {
  // DataInput::readUnsignedVL() uses a variable-length encoding
  // that uses the top bit of a byte to indicate whether another
  // byte follows. Thus, the 64 bits must be split into 7-bit
  // groups and then the follow bit applied. Note that integer
  // is encoded byte-at-a-time from the smaller end.
  //
  //  0001  0010  0011  0100  0101  0110  0111  1000  1001  1010  1011  1100
  //  1101  1110  1111  0000
  //  0  0010010  0011010  0010101  1001111  0001001  1010101  1110011  0111101
  //  1110000
  // 00    12       1A       15       4F       09       55       73       3D 70
  // 00    92       9A       95       CF       89       D5       F3       BD F0
  TestDataInput dataInput("F0BDF3D589CF959A9200");
  int64_t value = 0;
  dataInput.readUnsignedVL(&value);
  EXPECT_EQ((int64_t)1311768467463790320, value) << "Correct int64_t";
}

TEST_F(DataInputTest, TestReadFloat) {
  TestDataInput dataInput("123456789ABCDEF0");
  float value = 0.F;
  dataInput.readFloat(&value);
  EXPECT_FLOAT_EQ(5.6904566e-28F, value) << "Correct float";
}

TEST_F(DataInputTest, TestReadDouble) {
  TestDataInput dataInput("123456789ABCDEF0");
  double value = 0.;
  dataInput.readDouble(&value);
  EXPECT_DOUBLE_EQ(5.626349274901198e-221, value) << "Correct double";
}

TEST_F(DataInputTest, TestReadASCII) {
  TestDataInput dataInput(
      "001B596F7520686164206D65206174206D65617420746F726E61646F2E");
  char *value = NULL;
  uint16_t len = 0U;
  dataInput.readASCII(&value, &len);
  ASSERT_EQ((uint16_t)27U, len) << "Correct length";
  EXPECT_STREQ("You had me at meat tornado.", value) << "Correct char *";
  DataInput::freeUTFMemory(value);
}

TEST_F(DataInputTest, TestReadASCIIHuge) {
  TestDataInput dataInput(
      "0000001B596F7520686164206D65206174206D65617420746F726E61646F2E");
  char *value = NULL;
  uint32_t len = 0U;
  dataInput.readASCIIHuge(&value, &len);
  ASSERT_EQ((uint32_t)27U, len) << "Correct length";
  EXPECT_STREQ("You had me at meat tornado.", value) << "Correct char *";
  DataInput::freeUTFMemory(value);
}

TEST_F(DataInputTest, TestReadUTFNarrow) {
  TestDataInput dataInput(
      "001B596F7520686164206D65206174206D65617420746F726E61646F2E");
  char *value = NULL;
  uint16_t len = 0U;
  dataInput.readUTF(&value, &len);
  ASSERT_EQ((uint16_t)27U, len) << "Correct length";
  EXPECT_STREQ("You had me at meat tornado.", value) << "Correct char *";
  DataInput::freeUTFMemory(value);
}

TEST_F(DataInputTest, TestReadUTFHugeNarrow) {
  TestDataInput dataInput(
      "0000001B0059006F007500200068006100640020006D00650020006100740020006D0065"
      "0061007400200074006F0072006E00610064006F002E");
  char *value = NULL;
  uint32_t len = 0U;
  dataInput.readUTFHuge(&value, &len);
  ASSERT_EQ((uint32_t)27U, len) << "Correct length";
  EXPECT_STREQ("You had me at meat tornado.", value) << "Correct char *";
  DataInput::freeUTFMemory(value);
}

TEST_F(DataInputTest, TestReadUTFNoLen) {
  TestDataInput dataInput(
      "596F7520686164206D65206174206D65617420746F726E61646F2E");
  wchar_t *value = NULL;
  dataInput.readUTFNoLen(&value, static_cast<uint16_t>(27U));
  EXPECT_STREQ(L"You had me at meat tornado.", value) << "Correct wchar_t *";
  DataInput::freeUTFMemory(value);
}

TEST_F(DataInputTest, TestReadUTFWide) {
  TestDataInput dataInput(
      "001B596F7520686164206D65206174206D65617420746F726E61646F2E");
  wchar_t *value = NULL;
  uint16_t len = 0U;
  dataInput.readUTF(&value, &len);
  ASSERT_EQ((uint16_t)27U, len) << "Correct length";
  EXPECT_STREQ(L"You had me at meat tornado.", value) << "Correct wchar_t *";
  DataInput::freeUTFMemory(value);
}

TEST_F(DataInputTest, TestReadUTFHugeWide) {
  TestDataInput dataInput(
      "0000001B0059006F007500200068006100640020006D00650020006100740020006D0065"
      "0061007400200074006F0072006E00610064006F002E");
  wchar_t *value = NULL;
  uint32_t len = 0U;
  dataInput.readUTFHuge(&value, &len);
  ASSERT_EQ((uint32_t)27U, len) << "Correct length";
  EXPECT_STREQ(L"You had me at meat tornado.", value) << "Correct wchar_t *";
  DataInput::freeUTFMemory(value);
}

TEST_F(DataInputTest, TestReadObjectSharedPtr) {
  TestDataInput dataInput(
      "57001B596F7520686164206D65206174206D65617420746F726E61646F2E");
  CacheableStringPtr objptr;
  dataInput.readObject(objptr);
  EXPECT_STREQ((const char *)"You had me at meat tornado.",
               (const char *)objptr->toString())
      << "Correct const char *";
}

TEST_F(DataInputTest, TestReadNativeBool) {
  TestDataInput dataInput("0001");
  const bool value = dataInput.readNativeBool();
  EXPECT_EQ(true, value) << "Correct bool";
}

TEST_F(DataInputTest, TestReadNativeInt32) {
  TestDataInput dataInput("0012345678");
  const int32_t value = dataInput.readNativeInt32();
  EXPECT_EQ((int32_t)305419896, value) << "Correct int32_t";
}

TEST_F(DataInputTest, TestReadNativeString) {
  TestDataInput dataInput(
      "57001B596F7520686164206D65206174206D65617420746F726E61646F2E");
  CacheableStringPtr objptr;
  ASSERT_EQ(true, dataInput.readNativeString(objptr)) << "Successful read";
  EXPECT_STREQ((const char *)"You had me at meat tornado.",
               (const char *)objptr->toString())
      << "Correct const char *";
}

TEST_F(DataInputTest, TestReadDirectObject) {
  TestDataInput dataInput(
      "57001B596F7520686164206D65206174206D65617420746F726E61646F2E");
  SerializablePtr objptr;
  dataInput.readDirectObject(objptr);
  EXPECT_STREQ(
      (const char *)"You had me at meat tornado.",
      (const char *)(dynCast<SharedPtr<CacheableString> >(objptr))->toString())
      << "Correct const char *";
}

TEST_F(DataInputTest, TestReadObjectSerializablePtr) {
  TestDataInput dataInput(
      "57001B596F7520686164206D65206174206D65617420746F726E61646F2E");
  SerializablePtr objptr;
  dataInput.readObject(objptr);
  EXPECT_STREQ(
      (const char *)"You had me at meat tornado.",
      (const char *)(dynCast<SharedPtr<CacheableString> >(objptr))->toString())
      << "Correct const char *";
}

TEST_F(DataInputTest, TestReadCharArray) {
  TestDataInput dataInput(
      "1C0059006F007500200068006100640020006D00650020006100740020006D0065006100"
      "7400200074006F0072006E00610064006F002E0000");
  char *value = NULL;
  int32_t length = 0;
  dataInput.readCharArray(&value, length);
  ASSERT_EQ((int32_t)28, length) << "Correct length";
  EXPECT_STREQ((const char *)"You had me at meat tornado.", value)
      << "Correct const char *";
}

TEST_F(DataInputTest, TestReadString) {
  TestDataInput dataInput(
      "57001B596F7520686164206D65206174206D65617420746F726E61646F2E");
  char *value = NULL;
  dataInput.readString(&value);
  EXPECT_STREQ("You had me at meat tornado.", value) << "Correct char *";
  DataInput::freeUTFMemory(value);
}

TEST_F(DataInputTest, TestReadWideString) {
  TestDataInput dataInput(
      "57001B596F7520686164206D65206174206D65617420746F726E61646F2E");
  wchar_t *value = NULL;
  dataInput.readWideString(&value);
  EXPECT_STREQ(L"You had me at meat tornado.", value) << "Correct wchar_t *";
  DataInput::freeUTFMemory(value);
}

TEST_F(DataInputTest, TestReadStringArray) {
  TestDataInput dataInput(
      "0157001B596F7520686164206D65206174206D65617420746F726E61646F2E");
  char **value = NULL;
  int32_t length = 0;
  dataInput.readStringArray(&value, length);
  ASSERT_EQ((int32_t)1, length) << "Correct length";
  EXPECT_STREQ("You had me at meat tornado.", value[0]) << "Correct char *";
  DataInput::freeUTFMemory(value[0]);
  GF_SAFE_DELETE_ARRAY(value);
}

TEST_F(DataInputTest, TestReadWideStringArray) {
  TestDataInput dataInput(
      "0157001B596F7520686164206D65206174206D65617420746F726E61646F2E");
  wchar_t **value = NULL;
  int32_t length = 0;
  dataInput.readWideStringArray(&value, length);
  ASSERT_EQ((int32_t)1, length) << "Correct length";
  EXPECT_STREQ(L"You had me at meat tornado.", value[0]) << "Correct wchar_t *";
  DataInput::freeUTFMemory(value[0]);
  GF_SAFE_DELETE_ARRAY(value);
}

TEST_F(DataInputTest, TestReadArrayOfByteArrays) {
  TestDataInput dataInput("0104DEADBEEF");
  int8_t **arrayOfByteArrays = NULL;
  int32_t arrayLength = 0;
  int32_t *elementLength = NULL;
  dataInput.readArrayOfByteArrays(&arrayOfByteArrays, arrayLength,
                                  &elementLength);
  EXPECT_NE((int8_t **)NULL, arrayOfByteArrays)
      << "Non-null array of byte arrays";
  ASSERT_EQ((int32_t)1, arrayLength) << "Correct array length";
  EXPECT_NE((int8_t *)NULL, arrayOfByteArrays[0])
      << "Non-null first byte array";
  ASSERT_EQ(4, elementLength[0]) << "Correct length";
  EXPECT_EQ((int8_t)-34, arrayOfByteArrays[0][0]) << "Correct zeroth int8_t";
  EXPECT_EQ((int8_t)-83, arrayOfByteArrays[0][1]) << "Correct first int8_t";
  EXPECT_EQ((int8_t)-66, arrayOfByteArrays[0][2]) << "Correct second int8_t";
  EXPECT_EQ((int8_t)-17, arrayOfByteArrays[0][3]) << "Correct third int8_t";
  GF_SAFE_DELETE_ARRAY(elementLength);
  GF_SAFE_DELETE_ARRAY(arrayOfByteArrays);
}

TEST_F(DataInputTest, TestGetBytesRead) {
  TestDataInput dataInput("123456789ABCDEF0");
  EXPECT_EQ((int32_t)0, dataInput.getBytesRead())
      << "Correct bytes read before any reads";
  uint8_t value = 0U;
  dataInput.read(&value);
  dataInput.read(&value);
  dataInput.read(&value);
  dataInput.read(&value);
  EXPECT_EQ((int32_t)4, dataInput.getBytesRead())
      << "Correct bytes read after half of the reads";
  dataInput.read(&value);
  dataInput.read(&value);
  dataInput.read(&value);
  dataInput.read(&value);
  EXPECT_EQ((int32_t)8, dataInput.getBytesRead())
      << "Correct bytes read after all of the reads";
}

TEST_F(DataInputTest, TestGetBytesRemaining) {
  TestDataInput dataInput("123456789ABCDEF0");
  EXPECT_EQ((int32_t)8, dataInput.getBytesRemaining())
      << "Correct bytes remaining before any reads";
  uint8_t value = 0U;
  dataInput.read(&value);
  dataInput.read(&value);
  dataInput.read(&value);
  dataInput.read(&value);
  EXPECT_EQ((int32_t)4, dataInput.getBytesRemaining())
      << "Correct bytes remaining after half of the reads";
  dataInput.read(&value);
  dataInput.read(&value);
  dataInput.read(&value);
  dataInput.read(&value);
  EXPECT_EQ((int32_t)0, dataInput.getBytesRemaining())
      << "Correct bytes remaining after all of the reads";
}

TEST_F(DataInputTest, TestAdvanceCursor) {
  TestDataInput dataInput("123456789ABCDEF0");
  EXPECT_EQ((int32_t)0, dataInput.getBytesRead())
      << "Correct bytes read before any advancement";
  EXPECT_EQ((int32_t)8, dataInput.getBytesRemaining())
      << "Correct bytes remaining before any advancement";
  dataInput.advanceCursor(5);
  EXPECT_EQ((int32_t)5, dataInput.getBytesRead())
      << "Correct bytes read after forward advancement";
  EXPECT_EQ((int32_t)3, dataInput.getBytesRemaining())
      << "Correct bytes remaining after forward advancement";
  dataInput.advanceCursor(-3);
  EXPECT_EQ((int32_t)2, dataInput.getBytesRead())
      << "Correct bytes read after rearward advancement";
  EXPECT_EQ((int32_t)6, dataInput.getBytesRemaining())
      << "Correct bytes remaining after rearward advancement";
}

TEST_F(DataInputTest, TestRewindCursor) {
  TestDataInput dataInput("123456789ABCDEF0");
  EXPECT_EQ((int32_t)0, dataInput.getBytesRead())
      << "Correct bytes read before any rewinding";
  EXPECT_EQ((int32_t)8, dataInput.getBytesRemaining())
      << "Correct bytes remaining before any rewinding";
  dataInput.rewindCursor(-5);
  EXPECT_EQ((int32_t)5, dataInput.getBytesRead())
      << "Correct bytes read after forward rewinding";
  EXPECT_EQ((int32_t)3, dataInput.getBytesRemaining())
      << "Correct bytes remaining after forward rewinding";
  dataInput.rewindCursor(3);
  EXPECT_EQ((int32_t)2, dataInput.getBytesRead())
      << "Correct bytes read after rearward rewinding";
  EXPECT_EQ((int32_t)6, dataInput.getBytesRemaining())
      << "Correct bytes remaining after rearward rewinding";
}

TEST_F(DataInputTest, TestReset) {
  TestDataInput dataInput("123456789ABCDEF0");
  EXPECT_EQ((int32_t)0, dataInput.getBytesRead())
      << "Correct bytes read before any reads";
  EXPECT_EQ((int32_t)8, dataInput.getBytesRemaining())
      << "Correct bytes remaining before any reads";
  uint8_t value = 0U;
  dataInput.read(&value);
  dataInput.read(&value);
  dataInput.read(&value);
  dataInput.read(&value);
  EXPECT_EQ((int32_t)4, dataInput.getBytesRead())
      << "Correct bytes read after the reads";
  EXPECT_EQ((int32_t)4, dataInput.getBytesRemaining())
      << "Correct bytes remaining after the reads";
  dataInput.reset();
  EXPECT_EQ((int32_t)0, dataInput.getBytesRead())
      << "Correct bytes read after the reset";
  EXPECT_EQ((int32_t)8, dataInput.getBytesRemaining())
      << "Correct bytes remaining after the reset";
}

TEST_F(DataInputTest, TestSetBuffer) {
  TestDataInput dataInput("123456789ABCDEF0");
  EXPECT_EQ((int32_t)0, dataInput.getBytesRead())
      << "Correct bytes read before any reads";
  EXPECT_EQ((int32_t)8, dataInput.getBytesRemaining())
      << "Correct bytes remaining before any reads";
  uint8_t value = 0U;
  dataInput.read(&value);
  dataInput.read(&value);
  dataInput.read(&value);
  dataInput.read(&value);
  EXPECT_EQ((int32_t)4, dataInput.getBytesRead())
      << "Correct bytes read after the reads";
  EXPECT_EQ((int32_t)4, dataInput.getBytesRemaining())
      << "Correct bytes remaining after the reads";
  dataInput.setBuffer();
  EXPECT_EQ((int32_t)4, dataInput.getBytesRead())
      << "Correct bytes read after the setting";
  EXPECT_EQ((int32_t)0, dataInput.getBytesRemaining())
      << "Correct bytes remaining after the setting";
}

TEST_F(DataInputTest, TestSetPoolName) {
  static const char *poolName = "Das Schwimmbad";
  TestDataInput dataInput("123456789ABCDEF0");
  EXPECT_EQ((const char *)NULL, dataInput.getPoolName())
      << "Null pool name before setting";
  dataInput.setPoolName(poolName);
  EXPECT_NE((const char *)NULL, dataInput.getPoolName())
      << "Non-null pool name after setting";
  EXPECT_STREQ(poolName, dataInput.getPoolName())
      << "Correct pool name after setting";
}

#include <stdint.h>
#include <limits>
#include <random>

#include <gtest/gtest.h>

#include <gfcpp/DataOutput.hpp>
#include "ByteArrayFixture.hpp"

using namespace gemfire;

class TestDataOutput : public DataOutput {
 public:
  TestDataOutput() : m_byteArray(NULL) {
    // NOP
  }

  virtual ~TestDataOutput() {
    delete m_byteArray;
    m_byteArray = NULL;
  }

  const ByteArray& getByteArray() const {
    if (!m_byteArray) {
      m_byteArray = new ByteArray(getBuffer(), getBufferLength());
    }
    return *m_byteArray;
  }

 private:
  mutable ByteArray* m_byteArray;
};

class DataOutputTest : public ::testing::Test, public ByteArrayFixture {
 public:
  DataOutputTest() : m_mersennesTwister(m_randomDevice()) {
    // NOP
  }

  virtual ~DataOutputTest() {
    // NOP
  }

 protected:
  std::random_device m_randomDevice;
  std::mt19937 m_mersennesTwister;

  int32_t getRandomSequenceNumber() {
    // One would normally just use std::uniform_int_distribution but gcc 4.4.7
    // is lacking.
    const std::mt19937::result_type upperLimit =
        static_cast<std::mt19937::result_type>(
            std::numeric_limits<int32_t>::max());
    std::mt19937::result_type result;
    while (upperLimit < (result = m_mersennesTwister())) {
      // Try again.
    }
    return static_cast<int32_t>(result);
  }
};

TEST_F(DataOutputTest, TestWriteUint8) {
  TestDataOutput dataOutput;
  dataOutput.write(static_cast<uint8_t>(55U));
  dataOutput.write(static_cast<uint8_t>(66U));
  EXPECT_BYTEARRAY_EQ("3742", dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteInt8) {
  TestDataOutput dataOutput;
  dataOutput.write(static_cast<int8_t>(66));
  dataOutput.write(static_cast<int8_t>(55));
  EXPECT_BYTEARRAY_EQ("4237", dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteSequenceNumber) {
  TestDataOutput dataOutput;
  dataOutput.writeInt((int32_t)55);
  dataOutput.writeInt((int32_t)17);
  dataOutput.writeInt((int32_t)0);
  dataOutput.writeInt(getRandomSequenceNumber());
  dataOutput.write(static_cast<uint8_t>(0U));
  EXPECT_BYTEARRAY_EQ("000000370000001100000000\\h{8}00",
                      dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteBoolean) {
  TestDataOutput dataOutput;
  dataOutput.writeBoolean(true);
  dataOutput.writeBoolean(false);
  EXPECT_BYTEARRAY_EQ("0100", dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteBytesSigned) {
  int8_t bytes[] = {0, 1, 2, 3, 4, 5, -4, -3, -2, -1, 0};

  TestDataOutput dataOutput;
  dataOutput.writeBytes(bytes, 11);
  EXPECT_BYTEARRAY_EQ("0B000102030405FCFDFEFF00", dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteBytesOnlyUnsigned) {
  uint8_t bytes[] = {0, 1, 2, 3, 4, 5, 4, 3, 2, 1, 0};

  TestDataOutput dataOutput;
  dataOutput.writeBytesOnly(bytes, 11);
  EXPECT_BYTEARRAY_EQ("0001020304050403020100", dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteBytesOnlySigned) {
  int8_t bytes[] = {0, 1, 2, 3, 4, 5, -4, -3, -2, -1, 0};

  TestDataOutput dataOutput;
  dataOutput.writeBytesOnly(bytes, 11);
  EXPECT_BYTEARRAY_EQ("000102030405FCFDFEFF00", dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteIntUInt16) {
  TestDataOutput dataOutput;
  dataOutput.writeInt(static_cast<uint16_t>(66));
  dataOutput.writeInt(static_cast<uint16_t>(55));
  dataOutput.writeInt(static_cast<uint16_t>(3333));
  EXPECT_BYTEARRAY_EQ("004200370D05", dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteCharUInt16) {
  TestDataOutput dataOutput;
  dataOutput.writeChar(static_cast<uint16_t>(66));
  dataOutput.writeChar(static_cast<uint16_t>(55));
  dataOutput.writeChar(static_cast<uint16_t>(3333));
  EXPECT_BYTEARRAY_EQ("004200370D05", dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteIntUInt32) {
  TestDataOutput dataOutput;
  dataOutput.writeInt(static_cast<uint32_t>(3435973836));
  EXPECT_BYTEARRAY_EQ("CCCCCCCC", dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteIntUInt64) {
  TestDataOutput dataOutput;
  uint64_t big = 13455272147882261178U;
  dataOutput.writeInt(big);
  EXPECT_BYTEARRAY_EQ("BABABABABABABABA", dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteIntInt16) {
  TestDataOutput dataOutput;
  dataOutput.writeInt(static_cast<int16_t>(66));
  dataOutput.writeInt(static_cast<int16_t>(55));
  dataOutput.writeInt(static_cast<int16_t>(3333));
  EXPECT_BYTEARRAY_EQ("004200370D05", dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteIntInt32) {
  TestDataOutput dataOutput;
  dataOutput.writeInt(static_cast<int32_t>(3435973836));
  EXPECT_BYTEARRAY_EQ("CCCCCCCC", dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteIntInt64) {
  TestDataOutput dataOutput;
  int64_t big = 773738426788457421;
  dataOutput.writeInt(big);
  EXPECT_BYTEARRAY_EQ("0ABCDEFFEDCBABCD", dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteArrayLength) {
  TestDataOutput dataOutput;
  dataOutput.writeArrayLen(static_cast<int32_t>(3435973836));
  EXPECT_BYTEARRAY_EQ("CC", dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteFloat) {
  TestDataOutput dataOutput;
  float pi = 3.14;
  dataOutput.writeFloat(pi);
  EXPECT_BYTEARRAY_EQ("4048F5C3", dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteDouble) {
  TestDataOutput dataOutput;
  double pi = 3.14159265359;
  dataOutput.writeDouble(pi);
  EXPECT_BYTEARRAY_EQ("400921FB54442EEA", dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteASCII) {
  TestDataOutput dataOutput;
  dataOutput.writeASCII("You had me at meat tornado.");
  EXPECT_BYTEARRAY_EQ(
      "001B596F7520686164206D65206174206D65617420746F726E61646F2E",
      dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteNativeString) {
  TestDataOutput dataOutput;
  dataOutput.writeNativeString("You had me at meat tornado.");
  EXPECT_BYTEARRAY_EQ(
      "57001B596F7520686164206D65206174206D65617420746F726E61646F2E",
      dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteASCIIHuge) {
  TestDataOutput dataOutput;
  dataOutput.writeASCIIHuge("You had me at meat tornado.");
  EXPECT_BYTEARRAY_EQ(
      "0000001B596F7520686164206D65206174206D65617420746F726E61646F2E",
      dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteFullUTF) {
  TestDataOutput dataOutput;
  dataOutput.writeFullUTF("You had me at meat tornado.");
  EXPECT_BYTEARRAY_EQ(
      "0000001B00596F7520686164206D65206174206D65617420746F726E61646F2E",
      dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteUTF) {
  TestDataOutput dataOutput;
  dataOutput.writeUTF("You had me at meat tornado.");
  EXPECT_BYTEARRAY_EQ(
      "001B596F7520686164206D65206174206D65617420746F726E61646F2E",
      dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteUTFHuge) {
  TestDataOutput dataOutput;
  dataOutput.writeUTFHuge("You had me at meat tornado.");
  EXPECT_BYTEARRAY_EQ(
      "0000001B0059006F007500200068006100640020006D00650020006100740020006D0065"
      "0061007400200074006F0072006E00610064006F002E",
      dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteUTFWide) {
  TestDataOutput dataOutput;
  dataOutput.writeUTF(L"You had me at meat tornado!");
  EXPECT_BYTEARRAY_EQ(
      "001B596F7520686164206D65206174206D65617420746F726E61646F21",
      dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteUTFHugeWide) {
  TestDataOutput dataOutput;
  dataOutput.writeUTFHuge(L"You had me at meat tornado.");
  EXPECT_BYTEARRAY_EQ(
      "0000001B0059006F007500200068006100640020006D00650020006100740020006D0065"
      "0061007400200074006F0072006E00610064006F002E",
      dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestEncodedLength) {
  TestDataOutput dataOutput;
  EXPECT_EQ(27, dataOutput.getEncodedLength("You had me at meat tornado!"));
}

TEST_F(DataOutputTest, TestEncodedLengthWide) {
  TestDataOutput dataOutput;
  EXPECT_EQ(27, dataOutput.getEncodedLength(L"You had me at meat tornado."));
}

TEST_F(DataOutputTest, TestWriteObjectSharedPtr) {
  TestDataOutput dataOutput;
  SharedPtr<CacheableString> objptr =
      CacheableString::create("You had me at meat tornado.");
  dataOutput.writeObject(objptr);
  EXPECT_BYTEARRAY_EQ(
      "57001B596F7520686164206D65206174206D65617420746F726E61646F2E",
      dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestWriteObjectCacheableString) {
  TestDataOutput dataOutput;
  CacheableStringPtr objptr =
      CacheableString::create("You had me at meat tornado.");
  dataOutput.writeObject(objptr);
  EXPECT_BYTEARRAY_EQ(
      "57001B596F7520686164206D65206174206D65617420746F726E61646F2E",
      dataOutput.getByteArray());
}

TEST_F(DataOutputTest, TestCursorAdvance) {
  TestDataOutput dataOutput;
  dataOutput.writeUTF("You had me at meat tornado.");
  EXPECT_BYTEARRAY_EQ(
      "001B596F7520686164206D65206174206D65617420746F726E61646F2E",
      dataOutput.getByteArray());

  EXPECT_EQ((2 + 27), dataOutput.getBufferLength());

  // buffers are pre-allocated 8k and have 2 bytes to hold the data length
  EXPECT_EQ(((8 * 1024) - (2 + 27)), dataOutput.getRemainingBufferLength());

  dataOutput.advanceCursor(2);
  EXPECT_EQ((2 + 27 + 2), dataOutput.getBufferLength());

  EXPECT_EQ(((8 * 1024) - (2 + 27 + 2)), dataOutput.getRemainingBufferLength());
}

TEST_F(DataOutputTest, TestCursorNegativeAdvance) {
  TestDataOutput dataOutput;
  dataOutput.writeUTF("You had me at meat tornado.");
  EXPECT_BYTEARRAY_EQ(
      "001B596F7520686164206D65206174206D65617420746F726E61646F2E",
      dataOutput.getByteArray());

  EXPECT_EQ((2 + 27), dataOutput.getBufferLength());

  // buffers are pre-allocated 8k and have 2 bytes to hold the data length
  EXPECT_EQ(((8 * 1024) - (2 + 27)), dataOutput.getRemainingBufferLength());

  dataOutput.advanceCursor(-2);
  EXPECT_EQ((2 + 27 - 2), dataOutput.getBufferLength());

  EXPECT_EQ(((8 * 1024) - (2 + 27 - 2)), dataOutput.getRemainingBufferLength());
}

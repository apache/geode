#include "ByteArrayFixture.hpp"

using namespace gemfire;

TEST(ByteArrayTest, TestNoArgConstructor) {
  const ByteArray ba;
  EXPECT_EQ(0U, ba.size()) << "Zero size for no-arg constructor";
  EXPECT_EQ((const uint8_t *)NULL, (const uint8_t *)ba)
      << "Null pointer for no-arg constructor";
}

TEST(ByteArrayTest, TestTwoArgConstructor) {
  const uint8_t bytes[] = {0xDE, 0xAD, 0xBE, 0xEF};
  const ByteArray ba(bytes, 4);
  EXPECT_EQ(4U, ba.size()) << "Correct size for two-arg constructor";
  EXPECT_NE((const uint8_t *)NULL, (const uint8_t *)ba)
      << "Non-null pointer for two-arg constructor";
  EXPECT_EQ(0xDE, ba[0]) << "Correct zeroth byte for two-arg constructor";
  EXPECT_EQ(0xAD, ba[1]) << "Correct first byte for two-arg constructor";
  EXPECT_EQ(0xBE, ba[2]) << "Correct second byte for two-arg constructor";
  EXPECT_EQ(0xEF, ba[3]) << "Correct third byte for two-arg constructor";
}

TEST(ByteArrayTest, TestCopyConstructor) {
  const uint8_t bytes[] = {0xDE, 0xAD, 0xBE, 0xEF};
  const ByteArray ba1(bytes, 4U);
  const ByteArray ba2(ba1);
  EXPECT_EQ(4U, ba2.size()) << "Correct size for copy constructor";
  EXPECT_NE((const uint8_t *)NULL, (const uint8_t *)ba2)
      << "Non-null pointer for copy constructor";
  EXPECT_EQ(0xDE, ba2[0]) << "Correct zeroth byte for copy constructor";
  EXPECT_EQ(0xAD, ba2[1]) << "Correct first byte for copy constructor";
  EXPECT_EQ(0xBE, ba2[2]) << "Correct second byte for copy constructor";
  EXPECT_EQ(0xEF, ba2[3]) << "Correct third byte for copy constructor";
}

TEST(ByteArrayTest, TestAssignmentOperator) {
  ByteArray ba2;
  const uint8_t bytes[] = {0xDE, 0xAD, 0xBE, 0xEF};
  const ByteArray ba1(bytes, 4U);
  ba2 = ba1;
  EXPECT_EQ(4U, ba2.size()) << "Correct size for assignment operator";
  EXPECT_NE((const uint8_t *)NULL, (const uint8_t *)ba2)
      << "Non-null pointer for assignment operator";
  EXPECT_EQ(0xDE, ba2[0]) << "Correct zeroth byte for assignment operator";
  EXPECT_EQ(0xAD, ba2[1]) << "Correct first byte for assignment operator";
  EXPECT_EQ(0xBE, ba2[2]) << "Correct second byte for assignment operator";
  EXPECT_EQ(0xEF, ba2[3]) << "Correct third byte for assignment operator";
}

TEST(ByteArrayTest, TestFromStringForEmpty) {
  const std::string empty;
  const ByteArray ba(ByteArray::fromString(empty));
  EXPECT_EQ(0U, ba.size()) << "Zero size for empty string";
  EXPECT_EQ((const uint8_t *)NULL, (const uint8_t *)ba)
      << "Null pointer for empty string";
}

TEST(ByteArrayTest, TestFromStringForOneCharacter) {
  const std::string one("A");
  const ByteArray ba(ByteArray::fromString(one));
  EXPECT_EQ(1U, ba.size()) << "Correct size for one character";
  EXPECT_NE((const uint8_t *)NULL, (const uint8_t *)ba)
      << "Non-null pointer for one character";
  EXPECT_EQ(0xA0, ba[0]) << "Correct zeroth byte for one character";
}

TEST(ByteArrayTest, TestFromStringForEightCharacters) {
  const std::string eight("BABEFACE");
  const ByteArray ba(ByteArray::fromString(eight));
  EXPECT_EQ(4U, ba.size()) << "Correct size for eight characters";
  EXPECT_NE((const uint8_t *)NULL, (const uint8_t *)ba)
      << "Non-null pointer for eight characters";
  EXPECT_EQ(0xBA, ba[0]) << "Correct zeroth byte for eight characters";
  EXPECT_EQ(0xBE, ba[1]) << "Correct first byte for eight characters";
  EXPECT_EQ(0xFA, ba[2]) << "Correct second byte for eight characters";
  EXPECT_EQ(0xCE, ba[3]) << "Correct third byte for eight characters";
}

TEST(ByteArrayTest, TestToStringForEmpty) {
  const ByteArray ba;
  EXPECT_EQ(std::string(), ByteArray::toString(ba))
      << "Empty string for empty byte array";
}

TEST(ByteArrayTest, TestToStringForOneByte) {
  const uint8_t bytes[] = {0x25};
  const ByteArray ba(bytes, 1U);
  EXPECT_EQ(std::string("25"), ByteArray::toString(ba))
      << "Correct string for one byte";
}

TEST(ByteArrayTest, TestToStringForFourBytes) {
  const uint8_t bytes[] = {0xBA, 0xBE, 0xFA, 0xCE};
  const ByteArray ba(bytes, 4U);
  EXPECT_EQ(std::string("BABEFACE"), ByteArray::toString(ba))
      << "Correct string for four bytes";
}

/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testDataOutput"

#include "fw_helper.hpp"
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/DataInput.hpp>

using namespace gemfire;
using namespace test;

void dumpnbytes(const uint8_t* buf, uint32_t length) {
  for (uint32_t i = 0; i < length; i++) {
    cout << "buf[" << i << "] = " << hex << static_cast<int16_t>(buf[i]) << dec
         << " " << static_cast<char>(buf[i]) << endl;
  }
}
void dumpnshorts(const uint16_t* buf, uint32_t length) {
  for (uint32_t i = 0; i < length; i++) {
    cout << "buf[" << i << "] = " << hex << static_cast<uint16_t>(buf[i]) << dec
         << endl;
  }
}
void dumpnwords(const uint32_t* buf, uint32_t length) {
  for (uint32_t i = 0; i < length; i++) {
    cout << "buf[" << i << "] = " << hex << static_cast<uint32_t>(buf[i]) << dec
         << endl;
  }
}

BEGIN_TEST(Byte)
  {
    DataOutput dataOutput;

    dataOutput.write(static_cast<uint8_t>(0x11));
    const uint8_t* buffer = dataOutput.getBuffer();

    ASSERT(buffer[0] == (uint8_t)0x11, "expected 0x11.");

    int8_t result;
    DataInput dataInput(buffer, dataOutput.getBufferLength());
    dataInput.read(&result);
    ASSERT(result == (uint8_t)0x11, "expected 0x11");
  }
END_TEST(Byte)

BEGIN_TEST(Boolean)
  {
    DataOutput dataOutput;

    dataOutput.writeBoolean(true);
    dataOutput.writeBoolean(false);
    const uint8_t* buffer = dataOutput.getBuffer();

    ASSERT(buffer[0] == (uint8_t)0x1, "expected 0x1.");
    ASSERT(buffer[1] == (uint8_t)0x0, "expected 0x0.");

    bool result;
    DataInput dataInput(buffer, dataOutput.getBufferLength());
    dataInput.readBoolean(&result);
    ASSERT(result, "expected true");
    dataInput.readBoolean(&result);
    ASSERT(result == false, "expected false");
  }
END_TEST(Boolean)

BEGIN_TEST(Short)
  {
    DataOutput dataOutput;

    dataOutput.writeInt(static_cast<int16_t>(0x1122));
    const uint8_t* buffer = dataOutput.getBuffer();
    ASSERT(buffer[0] == (uint8_t)0x11, "expected 0x11.");
    ASSERT(buffer[1] == (uint8_t)0x22, "expected 0x11.");

    int16_t result;
    DataInput dataInput(buffer, dataOutput.getBufferLength());
    dataInput.readInt(&result);
    ASSERT(result == 0x1122, "expected 0x1122");
  }
END_TEST(Short)

BEGIN_TEST(int_t)
  {
    DataOutput dataOutput;

    dataOutput.writeInt((int32_t)0x11223344);
    const uint8_t* buffer = dataOutput.getBuffer();
    dumpnbytes(buffer, 4);
    ASSERT(buffer[0] == (uint8_t)0x11, "expected 0x11.");
    ASSERT(buffer[1] == (uint8_t)0x22, "expected 0x22.");
    ASSERT(buffer[2] == (uint8_t)0x33, "expected 0x33.");
    ASSERT(buffer[3] == (uint8_t)0x44, "expected 0x44.");

    DataInput dataInput(buffer, dataOutput.getBufferLength());
    int32_t result;
    dataInput.readInt(&result);
    ASSERT(result == 0x11223344, "expected 0x11223344");
  }
END_TEST(int_t)

BEGIN_TEST(Long)
  {
    DataOutput dataOutput;

    int64_t value = ((static_cast<int64_t>(0x11223344)) << 32) | 0x55667788;
    dataOutput.writeInt(value);
    const uint8_t* buffer = dataOutput.getBuffer();
    ASSERT(buffer[0] == (uint8_t)0x11, "expected 0x11.");
    ASSERT(buffer[1] == (uint8_t)0x22, "expected 0x22.");
    ASSERT(buffer[2] == (uint8_t)0x33, "expected 0x33.");
    ASSERT(buffer[3] == (uint8_t)0x44, "expected 0x44.");
    ASSERT(buffer[4] == (uint8_t)0x55, "expected 0x55.");
    ASSERT(buffer[5] == (uint8_t)0x66, "expected 0x66.");
    ASSERT(buffer[6] == (uint8_t)0x77, "expected 0x77.");
    ASSERT(buffer[7] == (uint8_t)0x88, "expected 0x88.");

    DataInput dataInput(buffer, dataOutput.getBufferLength());
    int64_t result;
    dataInput.readInt(&result);
    ASSERT(result == value, "expected 0x1122334455667788");
  }
END_TEST(Long)

BEGIN_TEST(Float)
  {
    DataOutput dataOutput;

    dataOutput.writeFloat(1.2f);
    const uint8_t* buffer = dataOutput.getBuffer();
    ASSERT(buffer[0] == (uint8_t)0x3f, "expected 0x3f.");
    ASSERT(buffer[1] == (uint8_t)0x99, "expected 0x99.");
    ASSERT(buffer[2] == (uint8_t)0x99, "expected 0x99.");
    ASSERT(buffer[3] == (uint8_t)0x9a, "expected 0x9a.");

    DataInput dataInput(buffer, dataOutput.getBufferLength());
    float result;
    dataInput.readFloat(&result);
    ASSERT(result == 1.2f, "expected 1.2f");
  }
END_TEST(Float)

BEGIN_TEST(Double)
  {
    DataOutput dataOutput;

    dataOutput.writeDouble(1.2);
    const uint8_t* buffer = dataOutput.getBuffer();
    ASSERT(buffer[0] == (uint8_t)0x3f, "expected 0x3f.");
    ASSERT(buffer[1] == (uint8_t)0xf3, "expected 0xf3.");
    ASSERT(buffer[2] == (uint8_t)0x33, "expected 0x33.");
    ASSERT(buffer[3] == (uint8_t)0x33, "expected 0x33.");
    ASSERT(buffer[4] == (uint8_t)0x33, "expected 0x33.");
    ASSERT(buffer[5] == (uint8_t)0x33, "expected 0x33.");
    ASSERT(buffer[6] == (uint8_t)0x33, "expected 0x33.");
    ASSERT(buffer[7] == (uint8_t)0x33, "expected 0x33.");

    DataInput dataInput(buffer, dataOutput.getBufferLength());
    double result;
    dataInput.readDouble(&result);
    ASSERT(result == 1.2, "expected 1.2");
  }
END_TEST(Double)

// Test data output numbers.
BEGIN_TEST(Numbers)
  {
    DataOutput dataOutput;

    dataOutput.write(static_cast<uint8_t>(0x11));
    dataOutput.write(static_cast<uint8_t>(0xAA));
    dataOutput.writeInt(static_cast<int16_t>(0x1122));
    dataOutput.write(static_cast<uint8_t>(0xAA));
    dataOutput.writeInt(0x11223344);
    dataOutput.write(static_cast<uint8_t>(0xAA));
    dataOutput.writeInt(((static_cast<int64_t>(0x11223344)) << 32) |
                        0x55667788);
    dataOutput.write(static_cast<uint8_t>(0xAA));
    dataOutput.writeFloat(1.2f);
    dataOutput.write(static_cast<uint8_t>(0xAA));
    dataOutput.writeDouble(1.2);
    dataOutput.write(static_cast<uint8_t>(0xAA));

    // test data
  }
END_TEST(Numbers)

BEGIN_TEST(NarrowStrings)
  {
    DataOutput dataOutput;

    const char* strOrig = "This is fun.";
    dataOutput.writeASCII(strOrig);

    const uint8_t* buffer = dataOutput.getBuffer();
    cout << "Wrote to buffer..." << endl;
    dumpnbytes(buffer, 14);

    ASSERT(buffer[0] == 0x00, "wrong utf encoding.");
    ASSERT(buffer[1] == 0x0c, "wrong utf encoding.");
    ASSERT(buffer[2] == 'T', "wrong utf encoding.");
    ASSERT(buffer[3] == 'h', "wrong utf encoding.");
    ASSERT(buffer[4] == 'i', "wrong utf encoding.");
    ASSERT(buffer[5] == 's', "wrong utf encoding.");
    ASSERT(buffer[6] == ' ', "wrong utf encoding.");
    ASSERT(buffer[7] == 'i', "wrong utf encoding.");
    ASSERT(buffer[8] == 's', "wrong utf encoding.");
    ASSERT(buffer[9] == ' ', "wrong utf encoding.");
    ASSERT(buffer[10] == 'f', "wrong utf encoding.");
    ASSERT(buffer[11] == 'u', "wrong utf encoding.");
    ASSERT(buffer[12] == 'n', "wrong utf encoding.");
    ASSERT(buffer[13] == '.', "wrong utf encoding.");

    DataInput dataInput(buffer, dataOutput.getBufferLength());
    char* str = NULL;
    uint16_t res_length;
    dataInput.readASCII(&str, &res_length);
    cout << "Read from buffer..." << endl;
    ASSERT(str != NULL, "expected non-null str");
    ASSERT(res_length == 12, "expected length 12.");
    dumpnbytes(reinterpret_cast<uint8_t*>(str), 12);
    int res = strncmp(str, strOrig, 12);
    ASSERT(res == 0, "expected a match.");
    dataInput.freeUTFMemory(str);
  }
END_TEST(NarrowStrings)

BEGIN_TEST(WideStrings)
  {
    DataOutput dataOutput;

    wchar_t* strOrig = new wchar_t[40];
    strOrig[0] = 0;
    strOrig[1] = 0x7f;
    strOrig[2] = 0x80;
    strOrig[3] = 0x81;
    strOrig[4] = 0xffff;

    dumpnshorts(reinterpret_cast<uint16_t*>(strOrig), 5);
    dataOutput.writeUTF(strOrig, 5);

    const uint8_t* buffer = dataOutput.getBuffer();
    cout << "Wrote to buffer..." << endl;
    dumpnbytes(buffer, 12);

    ASSERT(buffer[0] == 0x00, "wrong utf encoding.");
    ASSERT(buffer[1] == 0x0a, "wrong utf encoding.");
    ASSERT(buffer[2] == 0xc0, "wrong utf encoding.");
    ASSERT(buffer[3] == 0x80, "wrong utf encoding.");
    ASSERT(buffer[4] == 0x7f, "wrong utf encoding.");
    ASSERT(buffer[5] == 0xc2, "wrong utf encoding.");
    ASSERT(buffer[6] == 0x80, "wrong utf encoding.");
    ASSERT(buffer[7] == 0xc2, "wrong utf encoding.");
    ASSERT(buffer[8] == 0x81, "wrong utf encoding.");
    ASSERT(buffer[9] == 0xef, "wrong utf encoding.");
    ASSERT(buffer[10] == 0xbf, "wrong utf encoding.");
    ASSERT(buffer[11] == 0xbf, "wrong utf encoding.");
    cout << "sizeof wchar_t " << sizeof(wchar_t) << endl;
    DataInput dataInput(buffer, dataOutput.getBufferLength());
    wchar_t* str = NULL;
    uint16_t res_length;
    dataInput.readUTF(&str, &res_length);
    ASSERT(str != NULL, "expected non-null str");
    ASSERT(res_length == 5, "expected length 5.");
    cout << "Read from buffer..." << endl;
    dumpnshorts(reinterpret_cast<uint16_t*>(str), 5);

    ASSERT(str[0] == 0x00, "wrong decoded value");
    ASSERT(str[1] == 0x7f, "wrong decoded value");
    ASSERT(str[2] == 0x80, "wrong decoded value");
    ASSERT(str[3] == 0x81, "wrong decoded value");
    ASSERT(str[4] == 0xffff, "wrong decoded value");

    dataInput.freeUTFMemory(str);
    delete[] strOrig;
  }
END_TEST(WideStrings)

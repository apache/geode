/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
* PdxObject.cpp
*
*  Created on: Sep 29, 2011
*      Author: npatel
*/

#include "InvalidPdxUsage.hpp"
//#include "../cppcache/fw_dunit.hpp"

using namespace gemfire;
using namespace PdxTests;

template <typename T1, typename T2>
bool InvalidPdxUsage::genericValCompare(T1 value1, T2 value2) const {
  if (value1 != value2) return false;
  LOGINFO("PdxObject::genericValCompare Line_19");
  return true;
}

template <typename T1, typename T2>
bool InvalidPdxUsage::genericCompare(T1* value1, T2* value2, int length) const {
  int i = 0;
  while (i < length) {
    if (value1[i] != value2[i]) {
      return false;
    } else {
      i++;
    }
  }
  LOGINFO("PdxObject::genericCompare Line_34");
  return true;
}

template <typename T1, typename T2>
bool InvalidPdxUsage::generic2DCompare(T1** value1, T2** value2, int length,
                                       int* arrLengths) const {
  LOGINFO("generic2DCompare length = %d ", length);
  LOGINFO("generic2DCompare value1 = %d \t value2", value1[0][0], value2[0][0]);
  LOGINFO("generic2DCompare value1 = %d \t value2", value1[1][0], value2[1][0]);
  LOGINFO("generic2DCompare value1 = %d \t value2", value1[1][1], value2[1][1]);
  for (int j = 0; j < length; j++) {
    LOGINFO("generic2DCompare arrlength0 = %d ", arrLengths[j]);
    for (int k = 0; k < arrLengths[j]; k++) {
      LOGINFO("generic2DCompare arrlength = %d ", arrLengths[j]);
      LOGINFO("generic2DCompare value1 = %d \t value2 = %d ", value1[j][k],
              value2[j][k]);
      if (value1[j][k] != value2[j][k]) return false;
    }
  }
  LOGINFO("PdxObject::genericCompare Line_34");
  return true;
}

// InvalidPdxUsage::~PdxObject() {
//}

void InvalidPdxUsage::toData(PdxWriterPtr pw) /*const*/ {
  // TODO:delete it later
  LOGINFO(" NILKANTH InvalidPdxUsage::toData() Start exceptionCounter = %d ",
          toDataexceptionCounter);

  int* lengthArr = new int[2];

  lengthArr[0] = 1;
  lengthArr[1] = 2;

  // TestCase: writeArrayOfByteArrays with empty field name,
  // IllegalStateException is expected
  bool excptExpected ATTR_UNUSED = false;

  try {
    pw->writeArrayOfByteArrays("", m_byteByteArray, 2, lengthArr);
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeArrayOfByteArrays():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeWideChar("", m_char);
    pw->markIdentityField("m_char");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeWideChar():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeBoolean("", m_bool);  // 1
    pw->markIdentityField("m_bool");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeBoolean():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeBooleanArray("", m_boolArray, 3);
    pw->markIdentityField("m_boolArray");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeBooleanArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeByte("", m_byte);
    pw->markIdentityField("m_byte");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeByte():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    pw->writeByteArray("", m_byteArray, 2);
    pw->markIdentityField("m_byteArray");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeByteArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeWideCharArray("", m_charArray, 2);
    pw->markIdentityField("m_charArray");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeWideCharArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeObject("", m_arraylist);
    pw->markIdentityField("m_arraylist");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeObject() for ArrayList:: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeObject("", m_map);
    pw->markIdentityField("m_map");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeObject() for Map:: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeObject("", m_hashtable);
    pw->markIdentityField("m_hashtable");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeObject() for HashTable:: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeObject("", m_vector);
    pw->markIdentityField("m_vector");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeObject() for Vector:: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeObject("", m_chs);
    pw->markIdentityField("m_chs");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO(
        "writeObject() for CacheableHashSet:: Got expected Exception :: %s ",
        excpt.getMessage());
  }

  try {
    pw->writeObject("", m_clhs);
    pw->markIdentityField("m_clhs");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO(
        "writeObject() for CacheableLinkedHashSet:: Got expected Exception :: "
        "%s ",
        excpt.getMessage());
  }

  try {
    pw->writeString("", m_string);
    pw->markIdentityField("m_string");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeString():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeDate("", m_date);
    pw->markIdentityField("m_dateTime");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeDate():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    pw->writeDouble("", m_double);
    pw->markIdentityField("m_double");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeDouble():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeDoubleArray("", m_doubleArray, 2);
    pw->markIdentityField("m_doubleArray");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeDoubleArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeFloat("", m_float);
    pw->markIdentityField("m_float");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeFloat():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    pw->writeFloatArray("", m_floatArray, 2);
    pw->markIdentityField("m_floatArray");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeFloatArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeShort("", m_int16);
    pw->markIdentityField("m_int16");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeShort():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    pw->writeInt("", m_int32);
    pw->markIdentityField("m_int32");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeInt():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    pw->writeLong("", m_long);
    pw->markIdentityField("m_long");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeLong():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    pw->writeIntArray("", m_int32Array, 4);
    pw->markIdentityField("m_int32Array");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeIntArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeLongArray("", m_longArray, 2);
    pw->markIdentityField("m_longArray");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeLongArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeShortArray("", m_int16Array, 2);
    pw->markIdentityField("m_int16Array");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeShortArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeByte("", m_sbyte);
    pw->markIdentityField("m_sbyte");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeByte():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    pw->writeByteArray("", m_sbyteArray, 2);
    pw->markIdentityField("m_sbyteArray");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeByteArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeStringArray("", m_stringArray, 2);
    pw->markIdentityField("m_stringArray");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeStringArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeShort("", m_uint16);
    pw->markIdentityField("m_uint16");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeShort():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    pw->writeInt("", m_uint32);
    pw->markIdentityField("m_uint32");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeInt():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    pw->writeLong("", m_ulong);
    pw->markIdentityField("m_ulong");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeLong():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    pw->writeIntArray("", m_uint32Array, 4);
    pw->markIdentityField("m_uint32Array");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeIntArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeLongArray("", m_ulongArray, 2);
    pw->markIdentityField("m_ulongArray");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeLongArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeShortArray("", m_uint16Array, 2);
    pw->markIdentityField("m_uint16Array");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeShortArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeByteArray("", m_byte252, 252);
    pw->markIdentityField("m_byte252");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeByteArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeByteArray("", m_byte253, 253);
    pw->markIdentityField("m_byte253");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeByteArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeByteArray("", m_byte65535, 65535);
    pw->markIdentityField("m_byte65535");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeByteArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeByteArray("", m_byte65536, 65536);
    pw->markIdentityField("m_byte65536");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeByteArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeObject("", m_pdxEnum);
    pw->markIdentityField("m_pdxEnum");
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeObject() for Enum:: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeObject("", m_objectArray);
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO(
        "writeObject() for Custom Object Address:: Got expected Exception :: "
        "%s ",
        excpt.getMessage());
  }

  try {
    pw->writeObjectArray("", m_objectArray);
  } catch (IllegalStateException& excpt) {
    toDataexceptionCounter++;
    LOGINFO("writeObjectArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    pw->writeInt("toDataexceptionCounter", toDataexceptionCounter);
    pw->writeInt("fromDataexceptionCounter", fromDataexceptionCounter);
  } catch (IllegalStateException& excpt) {
    LOGINFO("writeInt():: Got expected Exception :: %s ", excpt.getMessage());
  }

  LOGDEBUG("PdxObject::toData() Done......");
}

void InvalidPdxUsage::fromData(PdxReaderPtr pr) {
  // TODO:temp added, delete later
  LOGINFO(
      " NILKANTH InvalidPdxUsage::fromData() Start fromDataexceptionCounter = "
      "%d ",
      fromDataexceptionCounter);

  int32_t* Lengtharr;
  GF_NEW(Lengtharr, int32_t[2]);
  int32_t arrLen = 0;
  int exceptionCounter = 0;
  try {
    m_byteByteArray = pr->readArrayOfByteArrays("", arrLen, &Lengtharr);
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readArrayOfByteArrays():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    m_char = pr->readWideChar("");
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readWideChar():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    m_bool = pr->readBoolean("");
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readBoolean():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    m_boolArray = pr->readBooleanArray("", boolArrayLen);
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readBooleanArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    m_byte = pr->readByte("");
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readByte():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    m_byteArray = pr->readByteArray("", byteArrayLen);
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readByteArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    m_charArray = pr->readWideCharArray("", charArrayLen);
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readWideCharArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    m_arraylist = pr->readObject("");
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readObject():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    m_map = dynCast<CacheableHashMapPtr>(pr->readObject(""));
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readObject():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    m_hashtable = pr->readObject("");
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readObject():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    m_vector = pr->readObject("");
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readObject():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    m_chs = pr->readObject("");
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readObject():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    m_clhs = pr->readObject("");
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readObject():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    m_string = pr->readString("");  // GenericValCompare
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readString():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    m_date = pr->readDate("");
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readDate():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    m_double = pr->readDouble("");
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readDouble():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    m_doubleArray = pr->readDoubleArray("", doubleArrayLen);
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readDoubleArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    m_float = pr->readFloat("");
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readFloat():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    m_floatArray = pr->readFloatArray("", floatArrayLen);
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readFloatArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    m_int16 = pr->readShort("");
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readShort():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    m_int32 = pr->readInt("");
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readInt():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    m_long = pr->readLong("");
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readLong():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    m_int32Array = pr->readIntArray("", intArrayLen);
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readIntArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    m_longArray = pr->readLongArray("", longArrayLen);
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readLongArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    m_int16Array = pr->readShortArray("", shortArrayLen);
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readShortArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    m_sbyte = pr->readByte("");
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readByte():: Got expected Exception :: %s ", excpt.getMessage());
  }
  try {
    m_sbyteArray = pr->readByteArray("", byteArrayLen);
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readByteArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    m_stringArray = pr->readStringArray("", strLenArray);
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readStringArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    m_uint16 = pr->readShort("");
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readShort():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    m_uint32 = pr->readInt("");
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readInt():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    m_ulong = pr->readLong("");
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readLong():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    m_uint32Array = pr->readIntArray("", intArrayLen);
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readIntArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    m_ulongArray = pr->readLongArray("", longArrayLen);
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readLongArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    m_uint16Array = pr->readShortArray("", shortArrayLen);
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readShortArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    m_byte252 = pr->readByteArray("", m_byte252Len);
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readByteArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }
  try {
    m_byte253 = pr->readByteArray("", m_byte253Len);
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readByteArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    m_byte65535 = pr->readByteArray("", m_byte65535Len);
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readByteArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    m_byte65536 = pr->readByteArray("", m_byte65536Len);
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readByteArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    m_pdxEnum = pr->readObject("");
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readObject():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    m_address = pr->readObject("");
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readObject():: Got expected Exception :: %s ", excpt.getMessage());
  }

  try {
    m_objectArray = pr->readObjectArray("");
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readObjectArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  try {
    toDataexceptionCounter = pr->readInt("toDataexceptionCounter");
    fromDataexceptionCounter = pr->readInt("fromDataexceptionCounter");
  } catch (IllegalStateException& excpt) {
    exceptionCounter++;
    LOGINFO("readObjectArray():: Got expected Exception :: %s ",
            excpt.getMessage());
  }

  this->fromDataexceptionCounter = exceptionCounter;

  LOGINFO(
      "InvalidPdxUsage::fromData() competed...fromDataexceptionCounter = %d "
      "and exceptionCounter=%d ",
      fromDataexceptionCounter, exceptionCounter);
}

CacheableStringPtr InvalidPdxUsage::toString() const {
  char idbuf[1024];
  // sprintf(idbuf,"PdxObject: [ m_bool=%d ] [m_byte=%d] [m_int16=%d]
  // [m_int32=%d] [m_float=%f] [m_double=%lf] [ m_string=%s ]",m_bool, m_byte,
  // m_int16, m_int32, m_float, m_double, m_string);
  sprintf(idbuf, "PdxObject:[m_int32=%d]", m_int32);
  return CacheableString::create(idbuf);
}

bool InvalidPdxUsage::equals(PdxTests::InvalidPdxUsage& other,
                             bool isPdxReadSerialized) const {
  InvalidPdxUsage* ot = dynamic_cast<InvalidPdxUsage*>(&other);
  if (ot == NULL) {
    return false;
  }
  if (ot == this) {
    return true;
  }
  genericValCompare(ot->m_int32, m_int32);
  genericValCompare(ot->m_bool, m_bool);
  genericValCompare(ot->m_byte, m_byte);
  genericValCompare(ot->m_int16, m_int16);
  genericValCompare(ot->m_long, m_long);
  genericValCompare(ot->m_float, m_float);
  genericValCompare(ot->m_double, m_double);
  genericValCompare(ot->m_sbyte, m_sbyte);
  genericValCompare(ot->m_uint16, m_uint16);
  genericValCompare(ot->m_uint32, m_uint32);
  genericValCompare(ot->m_ulong, m_ulong);
  genericValCompare(ot->m_char, m_char);
  if (strcmp(ot->m_string, m_string) != 0) {
    return false;
  }
  genericCompare(ot->m_byteArray, m_byteArray, byteArrayLen);
  genericCompare(ot->m_int16Array, m_int16Array, shortArrayLen);
  genericCompare(ot->m_int32Array, m_int32Array, intArrayLen);
  genericCompare(ot->m_longArray, m_longArray, longArrayLen);
  genericCompare(ot->m_doubleArray, m_doubleArray, doubleArrayLen);
  genericCompare(ot->m_floatArray, m_floatArray, floatArrayLen);
  genericCompare(ot->m_uint32Array, m_uint32Array, intArrayLen);
  genericCompare(ot->m_ulongArray, m_ulongArray, longArrayLen);
  genericCompare(ot->m_uint16Array, m_uint16Array, shortArrayLen);
  genericCompare(ot->m_sbyteArray, m_sbyteArray, shortArrayLen);
  genericCompare(ot->m_charArray, m_charArray, charArrayLen);
  // generic2DCompare(ot->m_byteByteArray, m_byteByteArray, byteByteArrayLen,
  // lengthArr);

  if (!isPdxReadSerialized) {
    for (int i = 0; i < m_objectArray->size(); i++) {
      AddressWithInvalidAPIUsage* otherAddr1 =
          dynamic_cast<AddressWithInvalidAPIUsage*>(
              ot->m_objectArray->at(i).ptr());
      AddressWithInvalidAPIUsage* myAddr1 =
          dynamic_cast<AddressWithInvalidAPIUsage*>(m_objectArray->at(i).ptr());
      if (!otherAddr1->equals(*myAddr1)) return false;
    }
    LOGINFO("PdxObject::equals isPdxReadSerialized = %d", isPdxReadSerialized);
  }

  CacheableEnumPtr myenum = dynCast<CacheableEnumPtr>(m_pdxEnum);
  CacheableEnumPtr otenum = dynCast<CacheableEnumPtr>(ot->m_pdxEnum);
  if (myenum->getEnumOrdinal() != otenum->getEnumOrdinal()) return false;
  if (strcmp(myenum->getEnumClassName(), otenum->getEnumClassName()) != 0) {
    return false;
  }
  if (strcmp(myenum->getEnumName(), otenum->getEnumName()) != 0) return false;

  genericValCompare(ot->m_arraylist->size(), m_arraylist->size());
  for (int k = 0; k < m_arraylist->size(); k++) {
    genericValCompare(ot->m_arraylist->at(k), m_arraylist->at(k));
  }

  genericValCompare(ot->m_vector->size(), m_vector->size());
  for (int j = 0; j < m_vector->size(); j++) {
    genericValCompare(ot->m_vector->at(j), m_vector->at(j));
  }

  LOGINFO("PdxObject::equals DOne Line_201");
  return true;
}

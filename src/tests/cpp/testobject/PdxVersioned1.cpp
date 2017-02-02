/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
* PdxObject.cpp
*
*  Created on: Sep 29, 2011
*      Author: npatel
*/

#include "PdxVersioned1.hpp"

using namespace apache::geode::client;
using namespace PdxTests;

template <typename T1, typename T2>
bool PdxTests::PdxVersioned1::genericValCompare(T1 value1, T2 value2) const {
  if (value1 != value2) return false;
  LOGINFO("PdxObject::genericValCompare Line_19");
  return true;
}

template <typename T1, typename T2>
bool PdxTests::PdxVersioned1::genericCompare(T1* value1, T2* value2,
                                             int length) const {
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
bool PdxTests::PdxVersioned1::generic2DCompare(T1** value1, T2** value2,
                                               int length,
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
/*void PdxVersioned1::checkNullAndDelete(void *data)
{
        if(data != NULL)
                delete[] data;
}
PdxVersioned1::~PdxVersioned1() {
         LOGINFO("~PdxVersioned1 - 1");
                checkNullAndDelete( m_string);
                LOGINFO("~PdxVersioned1 - 2");
                checkNullAndDelete( m_byteArray);
                LOGINFO("~PdxVersioned1 - 3");
                checkNullAndDelete( m_boolArray);
                LOGINFO("~PdxVersioned1 - 4");
                checkNullAndDelete( m_sbyteArray);
                LOGINFO("~PdxVersioned1 - 5");
                checkNullAndDelete( m_charArray);
                LOGINFO("~PdxVersioned1 - 6");
                checkNullAndDelete( m_uint16Array);
                LOGINFO("~PdxVersioned1 - 7");
                checkNullAndDelete( m_uint16Array);
                LOGINFO("~PdxVersioned1 - 8");
                checkNullAndDelete( m_int32Array);
                LOGINFO("~PdxVersioned1 - 9");
                checkNullAndDelete( m_uint32Array);
                LOGINFO("~PdxVersioned1 - 10");
                checkNullAndDelete( m_longArray);
                LOGINFO("~PdxVersioned1 - 11");
                checkNullAndDelete( m_ulongArray);
                LOGINFO("~PdxVersioned1 - 12");
                checkNullAndDelete( m_floatArray);
                LOGINFO("~PdxVersioned1 - 13");
                checkNullAndDelete( m_doubleArray);
                LOGINFO("~PdxVersioned1 - 14");
                checkNullAndDelete( m_byteByteArray);
                LOGINFO("~PdxVersioned1 - 15");
                checkNullAndDelete( m_stringArray);
                LOGINFO("~PdxVersioned1 - 16");
                checkNullAndDelete( m_byte252);
                LOGINFO("~PdxVersioned1 -17 ");
                checkNullAndDelete( m_byte253);
                LOGINFO("~PdxVersioned1 - 18");
                checkNullAndDelete( m_byte65535);
                LOGINFO("~PdxVersioned1 - 19");
                checkNullAndDelete( m_byte65536);
                LOGINFO("~PdxVersioned1 -20 ");
}*/
PdxVersioned1::PdxVersioned1(const char* key) { init(key); }

void PdxVersioned1::init(const char* key) {
  m_char = 'C';
  m_bool = true;
  m_byte = 0x74;
  m_sbyte = 0x67;
  m_int16 = 0xab;
  m_uint16 = 0x2dd5;
  m_int32 = 0x2345abdc;
  // m_uint32 = 0x2a65c434;
  // m_long = 324897980;
  m_ulong = 238749898;
  m_float = 23324.324f;
  m_double = 3243298498.00;
  size_t len = strlen("PdxVersioned ") + strlen(key) + 1;
  m_string = new char[len];
  strcpy(m_string, "PdxVersioned ");
  strcat(m_string, key);
  m_boolArray = new bool[3];
  m_boolArray[0] = true;
  m_boolArray[1] = false;
  m_boolArray[2] = true;
  /*for(int i=0; i<3; i++){
    m_boolArray[i] = true;
  };*/

  m_byteArray = new int8_t[2];
  m_byteArray[0] = 0x34;
  m_byteArray[1] = 0x64;

  m_sbyteArray = new int8_t[2];
  m_sbyteArray[0] = 0x34;
  m_sbyteArray[1] = 0x64;

  m_charArray = new wchar_t[2];
  m_charArray[0] = L'c';
  m_charArray[1] = L'v';

  int64_t d = 1310447869154L;
  m_date = CacheableDate::create(CacheableDate::duration(d));

  m_int16Array = new int16_t[2];
  m_int16Array[0] = 0x2332;
  m_int16Array[1] = 0x4545;

  m_uint16Array = new int16_t[2];
  m_uint16Array[0] = 0x3243;
  m_uint16Array[1] = 0x3232;

  m_int32Array = new int32_t[4];
  m_int32Array[0] = 23;
  m_int32Array[1] = 676868;
  m_int32Array[2] = 34343;
  m_int32Array[3] = 2323;

  m_uint32Array = new int32_t[4];
  m_uint32Array[0] = 435;
  m_uint32Array[1] = 234324;
  m_uint32Array[2] = 324324;
  m_uint32Array[3] = 23432432;

  m_longArray = new int64_t[2];
  m_longArray[0] = 324324L;
  m_longArray[1] = 23434545L;

  m_ulongArray = new int64_t[2];
  m_ulongArray[0] = 3245435;
  m_ulongArray[1] = 3425435;

  m_floatArray = new float[2];
  m_floatArray[0] = 232.565f;
  m_floatArray[1] = 2343254.67f;

  m_doubleArray = new double[2];
  m_doubleArray[0] = 23423432;
  m_doubleArray[1] = 4324235435.00;

  m_byteByteArray = new int8_t*[2];
  // for(int i=0; i<2; i++){
  //  m_byteByteArray[i] = new int8_t[1];
  //}
  m_byteByteArray[0] = new int8_t[1];
  m_byteByteArray[1] = new int8_t[2];
  m_byteByteArray[0][0] = 0x23;
  m_byteByteArray[1][0] = 0x34;
  m_byteByteArray[1][1] = 0x55;

  m_stringArray = new char*[2];
  const char* str1 = "one";
  const char* str2 = "two";

  int size = static_cast<int>(strlen(str1));
  for (int i = 0; i < 2; i++) {
    m_stringArray[i] = new char[size];
  }
  m_stringArray[0] = const_cast<char*>(str1);
  m_stringArray[1] = const_cast<char*>(str2);

  m_arraylist = CacheableArrayList::create();
  m_arraylist->push_back(CacheableInt32::create(1));
  m_arraylist->push_back(CacheableInt32::create(2));

  m_map = CacheableHashMap::create();
  m_map->insert(CacheableInt32::create(1), CacheableInt32::create(1));
  m_map->insert(CacheableInt32::create(2), CacheableInt32::create(2));
  m_pdxEnum = CacheableEnum::create("pdxEnumTest", "pdx2", pdx2);
  m_byte252 = new int8_t[252];
  for (int i = 0; i < 252; i++) {
    m_byte252[i] = 0;
  }

  m_byte253 = new int8_t[253];
  for (int i = 0; i < 253; i++) {
    m_byte253[i] = 0;
  }

  m_byte65535 = new int8_t[65535];
  for (int i = 0; i < 65535; i++) {
    m_byte65535[i] = 0;
  }

  m_byte65536 = new int8_t[65536];
  for (int i = 0; i < 65536; i++) {
    m_byte65536[i] = 0;
  }

  /*for (int32_t index = 0; index <3; ++index) {
    m_objectArray->push_back(objectArray[index]);
  }*/
  /*
  if (keys.size() > 0) {
    m_objectArray = CacheableObjectArray::create();
    for (int32_t index = 0; index < keys.size(); ++index) {
      m_objectArray->push_back(keys.operator[](index));
    }
  }*/

  boolArrayLen = 3;
  byteArrayLen = 2;
  shortArrayLen = 2;
  intArrayLen = 4;
  longArrayLen = 2;
  doubleArrayLen = 2;
  floatArrayLen = 2;
  strLenArray = 2;
  charArrayLen = 2;
  byteByteArrayLen = 2;

  lengthArr = new int[2];

  lengthArr[0] = 1;
  lengthArr[1] = 2;
}

void PdxTests::PdxVersioned1::toData(PdxWriterPtr pw) /*const*/ {
  // TODO:delete it later

  int* lengthArr = new int[2];

  lengthArr[0] = 1;
  lengthArr[1] = 2;
  pw->writeArrayOfByteArrays("m_byteByteArray", m_byteByteArray, 2, lengthArr);
  pw->writeWideChar("m_char", m_char);
  pw->writeBoolean("m_bool", m_bool);  // 1
  pw->writeBooleanArray("m_boolArray", m_boolArray, 3);
  pw->writeByte("m_byte", m_byte);
  pw->writeByteArray("m_byteArray", m_byteArray, 2);
  pw->writeWideCharArray("m_charArray", m_charArray, 2);
  pw->writeObject("m_arraylist", m_arraylist);
  pw->writeObject("m_map", m_map);
  pw->writeString("m_string", m_string);
  pw->writeDate("m_dateTime", m_date);
  pw->writeDouble("m_double", m_double);
  pw->writeDoubleArray("m_doubleArray", m_doubleArray, 2);
  pw->writeFloat("m_float", m_float);
  pw->writeFloatArray("m_floatArray", m_floatArray, 2);
  pw->writeShort("m_int16", m_int16);
  pw->writeInt("m_int32", m_int32);
  // pw->writeLong("m_long", m_long);
  pw->writeIntArray("m_int32Array", m_int32Array, 4);
  pw->writeLongArray("m_longArray", m_longArray, 2);
  pw->writeShortArray("m_int16Array", m_int16Array, 2);
  pw->writeByte("m_sbyte", m_sbyte);
  pw->writeByteArray("m_sbyteArray", m_sbyteArray, 2);
  // int* strlengthArr = new int[2];

  // strlengthArr[0] = 5;
  // strlengthArr[1] = 5;
  pw->writeStringArray("m_stringArray", m_stringArray, 2);
  pw->writeShort("m_uint16", m_uint16);
  // pw->writeInt("m_uint32", m_uint32);
  pw->writeLong("m_ulong", m_ulong);
  pw->writeIntArray("m_uint32Array", m_uint32Array, 4);
  pw->writeLongArray("m_ulongArray", m_ulongArray, 2);
  pw->writeShortArray("m_uint16Array", m_uint16Array, 2);
  pw->writeByteArray("m_byte252", m_byte252, 252);
  pw->writeByteArray("m_byte253", m_byte253, 253);
  pw->writeByteArray("m_byte65535", m_byte65535, 65535);
  pw->writeByteArray("m_byte65536", m_byte65536, 65536);
  pw->writeObject("m_pdxEnum", m_pdxEnum);

  // TODO:delete it later
}

void PdxTests::PdxVersioned1::fromData(PdxReaderPtr pr) {
  // TODO:temp added, delete later

  int32_t* Lengtharr;
  GF_NEW(Lengtharr, int32_t[2]);
  int32_t arrLen = 0;
  m_byteByteArray =
      pr->readArrayOfByteArrays("m_byteByteArray", arrLen, &Lengtharr);
  // TODO::need to write compareByteByteArray() and check for m_byteByteArray
  // elements
  m_char = pr->readWideChar("m_char");
  // GenericValCompare
  m_bool = pr->readBoolean("m_bool");
  // GenericValCompare
  m_boolArray = pr->readBooleanArray("m_boolArray", boolArrayLen);
  m_byte = pr->readByte("m_byte");
  m_byteArray = pr->readByteArray("m_byteArray", byteArrayLen);
  m_charArray = pr->readWideCharArray("m_charArray", charArrayLen);
  m_arraylist = dynCast<CacheableArrayListPtr>(pr->readObject("m_arraylist"));
  m_map = dynCast<CacheableHashMapPtr>(pr->readObject("m_map"));
  // TODO:Check for the size
  m_string = pr->readString("m_string");  // GenericValCompare
  m_date = pr->readDate("m_dateTime");    // compareData
  m_double = pr->readDouble("m_double");
  m_doubleArray = pr->readDoubleArray("m_doubleArray", doubleArrayLen);
  m_float = pr->readFloat("m_float");
  m_floatArray = pr->readFloatArray("m_floatArray", floatArrayLen);
  m_int16 = pr->readShort("m_int16");
  m_int32 = pr->readInt("m_int32");
  // m_long = pr->readLong("m_long");
  m_int32Array = pr->readIntArray("m_int32Array", intArrayLen);
  m_longArray = pr->readLongArray("m_longArray", longArrayLen);
  m_int16Array = pr->readShortArray("m_int16Array", shortArrayLen);
  m_sbyte = pr->readByte("m_sbyte");
  m_sbyteArray = pr->readByteArray("m_sbyteArray", byteArrayLen);
  m_stringArray = pr->readStringArray("m_stringArray", strLenArray);
  m_uint16 = pr->readShort("m_uint16");
  // m_uint32 = pr->readInt("m_uint32");
  m_ulong = pr->readLong("m_ulong");
  m_uint32Array = pr->readIntArray("m_uint32Array", intArrayLen);
  m_ulongArray = pr->readLongArray("m_ulongArray", longArrayLen);
  m_uint16Array = pr->readShortArray("m_uint16Array", shortArrayLen);
  // LOGINFO("PdxVersioned1::readInt() start...");

  m_byte252 = pr->readByteArray("m_byte252", m_byte252Len);
  m_byte253 = pr->readByteArray("m_byte253", m_byte253Len);
  m_byte65535 = pr->readByteArray("m_byte65535", m_byte65535Len);
  m_byte65536 = pr->readByteArray("m_byte65536", m_byte65536Len);
  // TODO:Check for size
  m_pdxEnum = pr->readObject("m_pdxEnum");
}

CacheableStringPtr PdxTests::PdxVersioned1::toString() const {
  char idbuf[1024];
  // sprintf(idbuf,"PdxObject: [ m_bool=%d ] [m_byte=%d] [m_int16=%d]
  // [m_int32=%d] [m_float=%f] [m_double=%lf] [ m_string=%s ]",m_bool, m_byte,
  // m_int16, m_int32, m_float, m_double, m_string);
  // sprintf(idbuf,"PdxObject testObject:[m_int32=%d] string = %s",
  // m_int32,m_string);
  sprintf(idbuf, "PdxVersioned 1 : %s", m_string);
  return CacheableString::create(idbuf);
}

bool PdxTests::PdxVersioned1::equals(PdxTests::PdxVersioned1& other,
                                     bool isPdxReadSerialized) const {
  PdxVersioned1* ot = dynamic_cast<PdxVersioned1*>(&other);
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
  // genericValCompare(ot->m_long, m_long);
  genericValCompare(ot->m_float, m_float);
  genericValCompare(ot->m_double, m_double);
  genericValCompare(ot->m_sbyte, m_sbyte);
  genericValCompare(ot->m_uint16, m_uint16);
  // genericValCompare(ot->m_uint32, m_uint32);
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

  // CacheableEnumPtr myenum = dynCast<CacheableEnumPtr>(m_pdxEnum);
  // CacheableEnumPtr otenum = dynCast<CacheableEnumPtr>(ot->m_pdxEnum);
  // if (myenum->getEnumOrdinal() != otenum->getEnumOrdinal()) return false;
  // if (strcmp(myenum->getEnumClassName(), otenum->getEnumClassName()) != 0)
  // return false;
  // if (strcmp(myenum->getEnumName(), otenum->getEnumName()) != 0) return
  // false;

  genericValCompare(ot->m_arraylist->size(), m_arraylist->size());
  for (int k = 0; k < m_arraylist->size(); k++) {
    genericValCompare(ot->m_arraylist->at(k), m_arraylist->at(k));
  }

  return true;
}

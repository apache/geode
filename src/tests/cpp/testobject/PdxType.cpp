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

#include "PdxType.hpp"

using namespace apache::geode::client;
using namespace PdxTests;

template <typename T1, typename T2>
bool PdxTests::PdxType::genericValCompare(T1 value1, T2 value2) const {
  if (value1 != value2) return false;
  LOGINFO("PdxObject::genericValCompare Line_19");
  return true;
}

template <typename T1, typename T2>
bool PdxTests::PdxType::genericCompare(T1* value1, T2* value2,
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
bool PdxTests::PdxType::generic2DCompare(T1** value1, T2** value2, int length,
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

// PdxType::~PdxObject() {
//}

void PdxTests::PdxType::toData(PdxWriterPtr pw) /*const*/ {
  // TODO:delete it later

  int* lengthArr = new int[2];

  lengthArr[0] = 1;
  lengthArr[1] = 2;
  pw->writeArrayOfByteArrays("m_byteByteArray", m_byteByteArray, 2, lengthArr);
  pw->writeWideChar("m_char", m_char);
  pw->markIdentityField("m_char");
  pw->writeBoolean("m_bool", m_bool);  // 1
  pw->markIdentityField("m_bool");
  pw->writeBooleanArray("m_boolArray", m_boolArray, 3);
  pw->markIdentityField("m_boolArray");
  pw->writeByte("m_byte", m_byte);
  pw->markIdentityField("m_byte");
  pw->writeByteArray("m_byteArray", m_byteArray, 2);
  pw->markIdentityField("m_byteArray");
  pw->writeWideCharArray("m_charArray", m_charArray, 2);
  pw->markIdentityField("m_charArray");
  pw->writeObject("m_arraylist", m_arraylist);
  pw->writeObject("m_linkedlist", m_linkedlist);
  pw->markIdentityField("m_arraylist");
  pw->writeObject("m_map", m_map);
  pw->markIdentityField("m_map");
  pw->writeObject("m_hashtable", m_hashtable);
  pw->markIdentityField("m_hashtable");
  pw->writeObject("m_vector", m_vector);
  pw->markIdentityField("m_vector");
  pw->writeObject("m_chs", m_chs);
  pw->markIdentityField("m_chs");
  pw->writeObject("m_clhs", m_clhs);
  pw->markIdentityField("m_clhs");
  pw->writeString("m_string", m_string);
  pw->markIdentityField("m_string");
  pw->writeDate("m_dateTime", m_date);
  pw->markIdentityField("m_dateTime");
  pw->writeDouble("m_double", m_double);
  pw->markIdentityField("m_double");
  pw->writeDoubleArray("m_doubleArray", m_doubleArray, 2);
  pw->markIdentityField("m_doubleArray");
  pw->writeFloat("m_float", m_float);
  pw->markIdentityField("m_float");
  pw->writeFloatArray("m_floatArray", m_floatArray, 2);
  pw->markIdentityField("m_floatArray");
  pw->writeShort("m_int16", m_int16);
  pw->markIdentityField("m_int16");
  pw->writeInt("m_int32", m_int32);
  pw->markIdentityField("m_int32");
  pw->writeLong("m_long", m_long);
  pw->markIdentityField("m_long");
  pw->writeIntArray("m_int32Array", m_int32Array, 4);
  pw->markIdentityField("m_int32Array");
  pw->writeLongArray("m_longArray", m_longArray, 2);
  pw->markIdentityField("m_longArray");
  pw->writeShortArray("m_int16Array", m_int16Array, 2);
  pw->markIdentityField("m_int16Array");
  pw->writeByte("m_sbyte", m_sbyte);
  pw->markIdentityField("m_sbyte");
  pw->writeByteArray("m_sbyteArray", m_sbyteArray, 2);
  pw->markIdentityField("m_sbyteArray");

  // int* strlengthArr = new int[2];

  // strlengthArr[0] = 5;
  // strlengthArr[1] = 5;
  pw->writeStringArray("m_stringArray", m_stringArray, 2);
  pw->markIdentityField("m_stringArray");
  pw->writeShort("m_uint16", m_uint16);
  pw->markIdentityField("m_uint16");
  pw->writeInt("m_uint32", m_uint32);
  pw->markIdentityField("m_uint32");
  pw->writeLong("m_ulong", m_ulong);
  pw->markIdentityField("m_ulong");
  pw->writeIntArray("m_uint32Array", m_uint32Array, 4);
  pw->markIdentityField("m_uint32Array");
  pw->writeLongArray("m_ulongArray", m_ulongArray, 2);
  pw->markIdentityField("m_ulongArray");
  pw->writeShortArray("m_uint16Array", m_uint16Array, 2);
  pw->markIdentityField("m_uint16Array");

  pw->writeByteArray("m_byte252", m_byte252, 252);
  pw->markIdentityField("m_byte252");
  pw->writeByteArray("m_byte253", m_byte253, 253);
  pw->markIdentityField("m_byte253");
  pw->writeByteArray("m_byte65535", m_byte65535, 65535);
  pw->markIdentityField("m_byte65535");
  pw->writeByteArray("m_byte65536", m_byte65536, 65536);
  pw->markIdentityField("m_byte65536");

  pw->writeObject("m_pdxEnum", m_pdxEnum);
  pw->markIdentityField("m_pdxEnum");
  pw->writeObject("m_address", m_objectArray);

  pw->writeObjectArray("m_objectArray", m_objectArray);
  pw->writeObjectArray("", m_objectArrayEmptyPdxFieldName);

  LOGDEBUG("PdxObject::writeObject() for enum Done......");

  LOGDEBUG("PdxObject::toData() Done......");
  // TODO:delete it later
}

void PdxTests::PdxType::fromData(PdxReaderPtr pr) {
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

  m_arraylist = pr->readObject("m_arraylist");
  m_linkedlist =
      dynCast<CacheableLinkedListPtr>(pr->readObject("m_linkedlist"));
  m_map = dynCast<CacheableHashMapPtr>(pr->readObject("m_map"));
  // TODO:Check for the size

  m_hashtable = pr->readObject("m_hashtable");
  // TODO:Check for the size

  m_vector = pr->readObject("m_vector");
  // TODO::Check for size

  m_chs = pr->readObject("m_chs");
  // TODO::Size check

  m_clhs = pr->readObject("m_clhs");
  // TODO:Size check

  m_string = pr->readString("m_string");  // GenericValCompare
  m_date = pr->readDate("m_dateTime");    // compareData

  m_double = pr->readDouble("m_double");

  m_doubleArray = pr->readDoubleArray("m_doubleArray", doubleArrayLen);
  m_float = pr->readFloat("m_float");
  m_floatArray = pr->readFloatArray("m_floatArray", floatArrayLen);
  m_int16 = pr->readShort("m_int16");
  m_int32 = pr->readInt("m_int32");
  m_long = pr->readLong("m_long");
  m_int32Array = pr->readIntArray("m_int32Array", intArrayLen);
  m_longArray = pr->readLongArray("m_longArray", longArrayLen);
  m_int16Array = pr->readShortArray("m_int16Array", shortArrayLen);
  m_sbyte = pr->readByte("m_sbyte");
  m_sbyteArray = pr->readByteArray("m_sbyteArray", byteArrayLen);
  m_stringArray = pr->readStringArray("m_stringArray", strLenArray);
  m_uint16 = pr->readShort("m_uint16");
  m_uint32 = pr->readInt("m_uint32");
  m_ulong = pr->readLong("m_ulong");
  m_uint32Array = pr->readIntArray("m_uint32Array", intArrayLen);
  m_ulongArray = pr->readLongArray("m_ulongArray", longArrayLen);
  m_uint16Array = pr->readShortArray("m_uint16Array", shortArrayLen);
  // LOGINFO("PdxType::readInt() start...");

  m_byte252 = pr->readByteArray("m_byte252", m_byte252Len);
  m_byte253 = pr->readByteArray("m_byte253", m_byte253Len);
  m_byte65535 = pr->readByteArray("m_byte65535", m_byte65535Len);
  m_byte65536 = pr->readByteArray("m_byte65536", m_byte65536Len);
  // TODO:Check for size

  m_pdxEnum = pr->readObject("m_pdxEnum");

  m_address = pr->readObject("m_address");
  // size chaeck

  m_objectArray = pr->readObjectArray("m_objectArray");
  m_objectArrayEmptyPdxFieldName = pr->readObjectArray("");

  // Check for individual elements

  // TODO:temp added delete it later

  LOGINFO("PdxObject::readObject() for enum Done...");
}

CacheableStringPtr PdxTests::PdxType::toString() const {
  char idbuf[1024];
  // sprintf(idbuf,"PdxObject: [ m_bool=%d ] [m_byte=%d] [m_int16=%d]
  // [m_int32=%d] [m_float=%f] [m_double=%lf] [ m_string=%s ]",m_bool, m_byte,
  // m_int16, m_int32, m_float, m_double, m_string);
  sprintf(idbuf, "PdxObject:[m_int32=%d]", m_int32);
  return CacheableString::create(idbuf);
}

bool PdxTests::PdxType::equals(PdxTests::PdxType& other,
                               bool isPdxReadSerialized) const {
  PdxType* ot = dynamic_cast<PdxType*>(&other);
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
      Address* otherAddr1 =
          dynamic_cast<Address*>(ot->m_objectArray->at(i).ptr());
      Address* myAddr1 = dynamic_cast<Address*>(m_objectArray->at(i).ptr());
      if (!otherAddr1->equals(*myAddr1)) return false;
    }
    LOGINFO("PdxObject::equals isPdxReadSerialized = %d", isPdxReadSerialized);
  }

  // m_objectArrayEmptyPdxFieldName
  if (!isPdxReadSerialized) {
    for (int i = 0; i < m_objectArrayEmptyPdxFieldName->size(); i++) {
      Address* otherAddr1 =
          dynamic_cast<Address*>(ot->m_objectArray->at(i).ptr());
      Address* myAddr1 = dynamic_cast<Address*>(m_objectArray->at(i).ptr());
      if (!otherAddr1->equals(*myAddr1)) return false;
    }
    LOGINFO("PdxObject::equals Empty Field Name isPdxReadSerialized = %d",
            isPdxReadSerialized);
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

  LOGINFO("Equals Linked List Starts");
  genericValCompare(ot->m_linkedlist->size(), m_linkedlist->size());
  for (int k = 0; k < m_linkedlist->size(); k++) {
    genericValCompare(ot->m_linkedlist->at(k), m_linkedlist->at(k));
  }
  LOGINFO("Equals Linked List Finished");

  genericValCompare(ot->m_vector->size(), m_vector->size());
  for (int j = 0; j < m_vector->size(); j++) {
    genericValCompare(ot->m_vector->at(j), m_vector->at(j));
  }

  LOGINFO("PdxObject::equals DOne Line_201");
  return true;
}

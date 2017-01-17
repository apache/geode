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
* NonPdxType.cpp
*
*  Created on: Apr 30, 2012
*      Author: vrao
*/

#include "NonPdxType.hpp"

using namespace gemfire;
using namespace PdxTests;

template <typename T1, typename T2>
bool PdxTests::NonPdxType::genericValCompare(T1 value1, T2 value2) const {
  if (value1 != value2) return false;
  LOGINFO("NonPdxType::genericValCompare");
  return true;
}

template <typename T1, typename T2>
bool PdxTests::NonPdxType::genericCompare(T1* value1, T2* value2,
                                          int length) const {
  int i = 0;
  while (i < length) {
    if (value1[i] != value2[i]) {
      return false;
    } else {
      i++;
    }
  }
  LOGINFO("NonPdxType::genericCompareArray");
  return true;
}

template <typename T1, typename T2>
bool PdxTests::NonPdxType::generic2DCompare(T1** value1, T2** value2,
                                            int length, int* arrLengths) const {
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
  LOGINFO("NonPdxType::generic2DCompare");
  return true;
}

bool PdxTests::NonPdxType::selfCheck() { return false; }

bool PdxTests::NonPdxType::equals(PdxTests::NonPdxType& other,
                                  bool isPdxReadSerialized) const {
  NonPdxType* ot = dynamic_cast<NonPdxType*>(&other);
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

  LOGINFO("NonPdxType::equals isPdxReadSerialized = %d", isPdxReadSerialized);

  if (!isPdxReadSerialized) {
    for (int i = 0; i < m_objectArray->size(); i++) {
      PdxWrapperPtr wrapper1 = dynCast<PdxWrapperPtr>(ot->m_objectArray->at(i));
      NonPdxAddress* otherAddr1 =
          reinterpret_cast<NonPdxAddress*>(wrapper1->getObject());
      PdxWrapperPtr wrapper2 = dynCast<PdxWrapperPtr>(m_objectArray->at(i));
      NonPdxAddress* myAddr1 =
          reinterpret_cast<NonPdxAddress*>(wrapper2->getObject());
      if (!otherAddr1->equals(*myAddr1)) return false;
    }
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

  LOGINFO("NonPdxType::equals done");
  return true;
}

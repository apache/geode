#pragma once

#ifndef GEODE_PDXHELPER_H_
#define GEODE_PDXHELPER_H_

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
 * PdxHelper.hpp
 *
 *  Created on: Dec 13, 2011
 *      Author: npatel
 */


#include <gfcpp/DataOutput.hpp>
#include "EnumInfo.hpp"
#include "PdxType.hpp"
#include "CacheImpl.hpp"

namespace apache {
namespace geode {
namespace client {

class PdxHelper {
 private:
  static void createMergedType(PdxTypePtr localType, PdxTypePtr remoteType,
                               DataInput& dataInput);

 public:
  static uint8_t PdxHeader;

  PdxHelper();

  virtual ~PdxHelper();

  static void serializePdx(DataOutput& output,
                           const PdxSerializable& pdxObject);

  static PdxSerializablePtr deserializePdx(DataInput& dataInput,
                                           bool forceDeserialize);

  static PdxSerializablePtr deserializePdx(DataInput& dataInput,
                                           bool forceDeserialize,
                                           int32_t typeId, int32_t length);

  static int32 readInt16(uint8_t* offsetPosition);

  static int32 readUInt16(uint8_t* offsetPosition);

  static int32 readByte(uint8_t* offsetPosition);

  static int32 readInt32(uint8_t* offsetPosition);

  static void writeInt32(uint8_t* offsetPosition, int32 value);

  static void writeInt16(uint8_t* offsetPosition, int32 value);

  static void writeByte(uint8_t* offsetPosition, int32 value);

  static int32 readInt(uint8_t* offsetPosition, int size);

  static int32 getEnumValue(const char* enumClassName, const char* enumName,
                            int hashcode);

  static EnumInfoPtr getEnum(int enumId);

  static CacheImpl* getCacheImpl();
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_PDXHELPER_H_

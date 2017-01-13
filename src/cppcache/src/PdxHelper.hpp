/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * PdxHelper.hpp
 *
 *  Created on: Dec 13, 2011
 *      Author: npatel
 */

#ifndef PDXHELPER_HPP_
#define PDXHELPER_HPP_

#include <gfcpp/DataOutput.hpp>
#include "EnumInfo.hpp"
#include "PdxType.hpp"
#include "CacheImpl.hpp"

namespace gemfire {

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
}
#endif /* PDXHELPER_HPP_ */

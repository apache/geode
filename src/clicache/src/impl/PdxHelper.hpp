/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once
//#include "../DataOutput.hpp"
#include <gfcpp/DataOutput.hpp>
#include "../IPdxSerializable.hpp"
using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {
				ref class DataOutput;
      ref class DataInput;
      namespace Internal
      {
        ref class PdxType;
        public ref class PdxHelper
        {
        public:

          static void SerializePdx(DataOutput^ dataOutput, IPdxSerializable^ pdxObject);

          static IPdxSerializable^ DeserializePdx(DataInput^ dataOutput, bool forceDeserialize);

          static IPdxSerializable^ PdxHelper::DeserializePdx(DataInput^ dataInput, bool forceDeserialize, int typeId, int length );

          literal Byte PdxHeader = 8;

          static Int32 ReadInt32(uint8_t* offsetPosition);

          static Int32 ReadInt16(uint8_t* offsetPosition);

					static Int32 PdxHelper::ReadUInt16(uint8_t* offsetPosition);

          static Int32 ReadByte(uint8_t* offsetPosition);

          static void WriteInt32(uint8_t* offsetPosition, Int32 value);

          static void WriteInt16(uint8_t* offsetPosition, Int32 value);

          static void WriteByte(uint8_t* offsetPosition, Int32 value);

          static Int32 ReadInt(uint8_t* offsetPosition, int size);

          static Int32 GetEnumValue(String^ enumClassName, String^ enumName, int hashcode);

          static Object^ GetEnum(int enumId);

        private:
          static void CreateMergedType(PdxType^ localType, PdxType^ remoteType, DataInput^ dataInput);
        };
      }
			}
    }
  }
}
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */



//#include "gf_includes.hpp"
#include <GemfireTypeIdsImpl.hpp>
#include "CacheableObjectArray.hpp"
#include "DataOutput.hpp"
#include "DataInput.hpp"
#include "ExceptionTypes.hpp"
#include "impl/SafeConvert.hpp"
#include "impl/PdxTypeRegistry.hpp"

using namespace System;
using namespace System::Collections::Generic;


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      // Region: IGFSerializable Members

      void CacheableObjectArray::ToData(DataOutput^ output)
      {
        output->WriteArrayLen((int32_t)Count);
        output->WriteByte((int8_t)gemfire::GemfireTypeIdsImpl::Class);
        output->WriteByte((int8_t)gemfire::GemfireTypeIds::CacheableASCIIString);
        output->WriteUTF("java.lang.Object");

        for each (Object^ obj in this) {
					//TODO::split
          output->WriteObject(obj);
        }

        /*_GF_MG_EXCEPTION_TRY

          gemfire::DataOutput& nativeOutput = *(output->_NativePtr);
          nativeOutput.writeArrayLen((int32_t)Count);
          nativeOutput.write((int8_t)gemfire::GemfireTypeIdsImpl::Class);
          nativeOutput.write((int8_t)gemfire::GemfireTypeIds::CacheableASCIIString);
          nativeOutput.writeASCII("java.lang.Object");
          for each (IGFSerializable^ obj in this) {
            gemfire::SerializablePtr objPtr(SafeMSerializableConvert(obj));
            nativeOutput.writeObject(objPtr);
          }

        _GF_MG_EXCEPTION_CATCH_ALL*/
      }

      IGFSerializable^ CacheableObjectArray::FromData(DataInput^ input)
      {
        int len = input->ReadArrayLen();
        if (len >= 0) {
          //int8_t typeCode;
          input->ReadByte(); // ignore CLASS typeid
          input->ReadByte(); // ignore string typeid
          unsigned short classLen;
          classLen = input->ReadInt16();
          input->AdvanceCursor(classLen);
          //nativeInput.readInt(&classLen);
          //nativeInput.advanceCursor(classLen);
        }
        for (int32_t index = 0; index < len; ++index) {
          Add(input->ReadObject());
        }
        return this;
        /*_GF_MG_EXCEPTION_TRY

          gemfire::DataInput& nativeInput = *(input->_NativePtr);
          int32_t len;
          nativeInput.readArrayLen(&len);
          if (len >= 0) {
            int8_t typeCode;
            nativeInput.read(&typeCode); // ignore CLASS typeid
            nativeInput.read(&typeCode); // ignore string typeid
            uint16_t classLen;
            nativeInput.readInt(&classLen);
            nativeInput.advanceCursor(classLen);
          }
          gemfire::CacheablePtr value;
          for (int32_t index = 0; index < len; ++index) {
            nativeInput.readObject(value);
            Add(SafeUMSerializableConvert(value.ptr()));
          }

        _GF_MG_EXCEPTION_CATCH_ALL
        return this;*/
      }

      uint32_t CacheableObjectArray::ObjectSize::get()
      { 
       /* uint32_t size = static_cast<uint32_t> (sizeof(CacheableObjectArray^));
        for each (IGFSerializable^ val in this) {
          if (val != nullptr) {
            size += val->ObjectSize;
          }
        }*/
        return Count;
      }      
      // End Region: IGFSerializable Members
    }
  }
}
 } //namespace 

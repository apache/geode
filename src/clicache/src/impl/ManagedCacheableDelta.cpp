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

//#include "../gf_includes.hpp"
#include "ManagedCacheableDelta.hpp"
#include "../DataInput.hpp"
#include "../DataOutput.hpp"
#include "../CacheableString.hpp"
#include <GeodeTypeIdsImpl.hpp>
#include "../ExceptionTypes.hpp"
#include "SafeConvert.hpp"


using namespace System;

namespace apache
{
  namespace geode
  {
    namespace client
    {

      void ManagedCacheableDeltaGeneric::toData(DataOutput& output) const
      {
        try {
          uint32 pos = (int)output.getBufferLength();
          Apache::Geode::Client::DataOutput mg_output(&output, true);
          m_managedSerializableptr->ToData(%mg_output);
          //this will move the cursor in c++ layer
          mg_output.WriteBytesToUMDataOutput();
          ManagedCacheableDeltaGeneric* tmp = const_cast<ManagedCacheableDeltaGeneric*>(this);
          tmp->m_objectSize = (int)(output.getBufferLength() - pos);
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
      }

      Serializable* ManagedCacheableDeltaGeneric::fromData(DataInput& input)
      {
        try {
          int pos = input.getBytesRead();
          Apache::Geode::Client::DataInput mg_input(&input, true);
          m_managedSerializableptr->FromData(%mg_input);

          //this will move the cursor in c++ layer
          input.advanceCursor(mg_input.BytesReadInternally);

          m_objectSize = input.getBytesRead() - pos;

          if (m_hashcode == 0)
            m_hashcode = m_managedptr->GetHashCode();

        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        return this;
      }

      uint32_t ManagedCacheableDeltaGeneric::objectSize() const
      {
        try {
          int ret = m_managedSerializableptr->ObjectSize;
          if (ret > m_objectSize)
            return ret;
          else
            return m_objectSize;
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        return 0;
      }

      int32_t ManagedCacheableDeltaGeneric::classId() const
      {
        uint32_t classId;
        try {
          classId = m_managedSerializableptr->ClassId;
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        return (classId >= 0x80000000 ? 0 : classId);
      }

      int8_t ManagedCacheableDeltaGeneric::typeId() const
      {
        try {
          uint32_t classId = m_classId;
          if (classId >= 0x80000000) {
            return (int8_t)((classId - 0x80000000) % 0x20000000);
          }
          else if (classId <= 0x7F) {
            return (int8_t)GeodeTypeIdsImpl::CacheableUserData;
          }
          else if (classId <= 0x7FFF) {
            return (int8_t)GeodeTypeIdsImpl::CacheableUserData2;
          }
          else {
            return (int8_t)GeodeTypeIdsImpl::CacheableUserData4;
          }
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        return 0;
      }

      int8_t ManagedCacheableDeltaGeneric::DSFID() const
      {
        // convention that [0x8000000, 0xa0000000) is for FixedIDDefault,
        // [0xa000000, 0xc0000000) is for FixedIDByte,
        // [0xc0000000, 0xe0000000) is for FixedIDShort
        // and [0xe0000000, 0xffffffff] is for FixedIDInt
        // Note: depends on fact that FixedIDByte is 1, FixedIDShort is 2
        // and FixedIDInt is 3; if this changes then correct this accordingly
        uint32_t classId = m_managedSerializableptr->ClassId;
        if (classId >= 0x80000000) {
          return (int8_t)((classId - 0x80000000) / 0x20000000);
        }
        return 0;
      }

      bool ManagedCacheableDeltaGeneric::hasDelta()
      {
        return m_managedptr->HasDelta();
      }

      void ManagedCacheableDeltaGeneric::toDelta(DataOutput& output) const
      {
        try {
          Apache::Geode::Client::DataOutput mg_output(&output, true);
          m_managedptr->ToDelta(%mg_output);
          //this will move the cursor in c++ layer
          mg_output.WriteBytesToUMDataOutput();
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
      }

      void ManagedCacheableDeltaGeneric::fromDelta(DataInput& input)
      {
        try {
          Apache::Geode::Client::DataInput mg_input(&input, true);
          m_managedptr->FromDelta(%mg_input);

          //this will move the cursor in c++ layer
          input.advanceCursor(mg_input.BytesReadInternally);

          m_hashcode = m_managedptr->GetHashCode();
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
      }

      DeltaPtr ManagedCacheableDeltaGeneric::clone()
      {
        try {
          ICloneable^ cloneable = dynamic_cast<ICloneable^>((
            Apache::Geode::Client::IGFDelta^) m_managedptr);
          if (cloneable) {
            Apache::Geode::Client::IGFSerializable^ Mclone =
              dynamic_cast<Apache::Geode::Client::IGFSerializable^>(cloneable->Clone());
            return DeltaPtr(static_cast<ManagedCacheableDeltaGeneric*>(
              SafeMSerializableConvertGeneric(Mclone)));
          }
          else {
            return Delta::clone();
          }
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        return NULLPTR;
      }

      bool ManagedCacheableDeltaGeneric::operator ==(const apache::geode::client::CacheableKey& other) const
      {
        try {
          // now checking classId(), typeId(), DSFID() etc. will be much more
          // expensive than just a dynamic_cast
          const ManagedCacheableDeltaGeneric* p_other =
            dynamic_cast<const ManagedCacheableDeltaGeneric*>(&other);
          if (p_other != NULL) {
            return m_managedptr->Equals(p_other->ptr());
          }
          return false;
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        return false;
      }

      bool ManagedCacheableDeltaGeneric::operator == (const ManagedCacheableDeltaGeneric& other) const
      {
        try {
          return m_managedptr->Equals(other.ptr());
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        return false;

      }

      uint32_t ManagedCacheableDeltaGeneric::hashcode() const
      {
        throw gcnew System::NotSupportedException;
      }

      size_t ManagedCacheableDeltaGeneric::logString(char* buffer, size_t maxLength) const
      {
        try {
          if (maxLength > 0) {
            String^ logstr = m_managedptr->GetType()->Name + '(' +
              m_managedptr->ToString() + ')';
            Apache::Geode::Client::ManagedString mg_str(logstr);
            return snprintf(buffer, maxLength, "%s", mg_str.CharPtr);
          }
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        return 0;
      }
    }  // namespace client
  }  // namespace geode
}  // namespace apache

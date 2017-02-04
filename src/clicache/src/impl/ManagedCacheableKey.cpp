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
#include "../ICacheableKey.hpp"
#include "ManagedCacheableKey.hpp"
#include "../DataInput.hpp"
#include "../DataOutput.hpp"
#include "../CacheableString.hpp"
#include <GeodeTypeIdsImpl.hpp>
#include "../ExceptionTypes.hpp"
#include "../Log.hpp"

using namespace System;

namespace apache
{
  namespace geode
  {
    namespace client
    {

      void ManagedCacheableKeyGeneric::toData(apache::geode::client::DataOutput& output) const
      {
        try {
          uint32 pos = (int)output.getBufferLength();
          //Apache::Geode::Client::Log::Debug("ManagedCacheableKeyGeneric::toData");      
          Apache::Geode::Client::DataOutput mg_output(&output, true);
          m_managedptr->ToData(%mg_output);
          //this will move the cursor in c++ layer
          mg_output.WriteBytesToUMDataOutput();

          ManagedCacheableKeyGeneric* tmp = const_cast<ManagedCacheableKeyGeneric*>(this);
          tmp->m_objectSize = (int)(output.getBufferLength() - pos);
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
      }

      apache::geode::client::Serializable* ManagedCacheableKeyGeneric::fromData(apache::geode::client::DataInput& input)
      {
        try {
          int pos = input.getBytesRead();
          //Apache::Geode::Client::Log::Debug("ManagedCacheableKeyGeneric::fromData");      
          Apache::Geode::Client::DataInput mg_input(&input, true);
          m_managedptr = m_managedptr->FromData(%mg_input);

          //this will move the cursor in c++ layer
          input.advanceCursor(mg_input.BytesReadInternally);
          m_objectSize = input.getBytesRead() - pos;
          //if(m_hashcode == 0)
          //m_hashcode = m_managedptr->GetHashCode();


        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        return this;
      }

      uint32_t ManagedCacheableKeyGeneric::objectSize() const
      {
        try {
          int ret = m_managedptr->ObjectSize;
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

      int32_t ManagedCacheableKeyGeneric::classId() const
      {
        //Apache::Geode::Client::Log::Debug("ManagedCacheableKeyGeneric::classid " + m_classId);
        /*uint32_t classId;
        try {
        classId = m_managedptr->ClassId;
        }
        catch (GeodeException^ ex) {
        ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
        GeodeException::ThrowNative(ex);
        }*/
        return (m_classId >= 0x80000000 ? 0 : m_classId);
      }

      int8_t ManagedCacheableKeyGeneric::typeId() const
      {
        //Apache::Geode::Client::Log::Debug("ManagedCacheableKeyGeneric::typeId " + m_classId);
        if (m_classId >= 0x80000000) {
          return (int8_t)((m_classId - 0x80000000) % 0x20000000);
        }
        else if (m_classId <= 0x7F) {
          //Apache::Geode::Client::Log::Debug("ManagedCacheableKeyGeneric::typeId inin"); 
          return (int8_t)GeodeTypeIdsImpl::CacheableUserData;
        }
        else if (m_classId <= 0x7FFF) {
          return (int8_t)GeodeTypeIdsImpl::CacheableUserData2;
        }
        else {
          return (int8_t)GeodeTypeIdsImpl::CacheableUserData4;
        }
      }

      int8_t ManagedCacheableKeyGeneric::DSFID() const
      {
        // convention that [0x8000000, 0xa0000000) is for FixedIDDefault,
        // [0xa000000, 0xc0000000) is for FixedIDByte,
        // [0xc0000000, 0xe0000000) is for FixedIDShort
        // and [0xe0000000, 0xffffffff] is for FixedIDInt
        // Note: depends on fact that FixedIDByte is 1, FixedIDShort is 2
        // and FixedIDInt is 3; if this changes then correct this accordingly
        if (m_classId >= 0x80000000) {
          return (int8_t)((m_classId - 0x80000000) / 0x20000000);
        }
        return 0;
      }

      apache::geode::client::CacheableStringPtr ManagedCacheableKeyGeneric::toString() const
      {
        try {
          apache::geode::client::CacheableStringPtr cStr;
          Apache::Geode::Client::CacheableString::GetCacheableString(
            m_managedptr->ToString(), cStr);
          return cStr;
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        return NULLPTR;
      }

      bool ManagedCacheableKeyGeneric::operator ==(const apache::geode::client::CacheableKey& other) const
      {
        try {
          // now checking classId(), typeId(), DSFID() etc. will be much more
          // expensive than just a dynamic_cast
          const ManagedCacheableKeyGeneric* p_other =
            dynamic_cast<const ManagedCacheableKeyGeneric*>(&other);
          if (p_other != NULL) {
            return static_cast<Apache::Geode::Client::ICacheableKey^>(
              (static_cast<Apache::Geode::Client::IGFSerializable^>((Apache::Geode::Client::IGFSerializable^)m_managedptr)))->Equals(
              static_cast<Apache::Geode::Client::ICacheableKey^>(p_other->ptr()));
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

      bool ManagedCacheableKeyGeneric::operator ==(const ManagedCacheableKeyGeneric& other) const
      {
        try {
          return static_cast<Apache::Geode::Client::ICacheableKey^>(
            (Apache::Geode::Client::IGFSerializable^)(Apache::Geode::Client::IGFSerializable^)m_managedptr)->Equals(
            static_cast<Apache::Geode::Client::ICacheableKey^>(other.ptr()));
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        return false;
      }

      uint32_t ManagedCacheableKeyGeneric::hashcode() const
      {
        if (m_hashcode != 0)
          return m_hashcode;
        try {

          ManagedCacheableKeyGeneric* tmp = const_cast<ManagedCacheableKeyGeneric*>(this);
          tmp->m_hashcode = ((Apache::Geode::Client::ICacheableKey^)
                             (Apache::Geode::Client::IGFSerializable^)m_managedptr)
                             ->GetHashCode();
          return m_hashcode;
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        return 0;
      }

      size_t ManagedCacheableKeyGeneric::logString(char* buffer, size_t maxLength) const
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

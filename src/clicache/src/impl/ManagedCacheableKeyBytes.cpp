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
#include "ManagedCacheableKeyBytes.hpp"
#include "../DataInput.hpp"
#include "../DataOutput.hpp"
#include "../Serializable.hpp"
#include "../CacheableString.hpp"
#include <GeodeTypeIdsImpl.hpp>
#include "../ExceptionTypes.hpp"
#include "ManagedString.hpp"


using namespace System;

namespace apache
{
  namespace geode
  {
    namespace client
    {
      void ManagedCacheableKeyBytesGeneric::toData(apache::geode::client::DataOutput& output) const
      {
        Apache::Geode::Client::Log::Debug("ManagedCacheableKeyBytesGeneric::toData: current domain ID: " + System::Threading::Thread::GetDomainID() + " for object: " + System::Convert::ToString((int)this) + " with its domain ID: " + m_domainId);
        try {
          //TODO: I think this should work as it is
          output.writeBytesOnly(m_bytes, m_size);
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
      }

      apache::geode::client::Serializable* ManagedCacheableKeyBytesGeneric::fromData(apache::geode::client::DataInput& input)
      {
        try {

          Apache::Geode::Client::Log::Debug("ManagedCacheableKeyBytesGeneric::fromData: classid " + m_classId + "aid = " + +System::Threading::Thread::GetDomainID());
          Apache::Geode::Client::DataInput mg_input(&input, true);
          const uint8_t* objStartPos = input.currentBufferPosition();

          Apache::Geode::Client::IGFSerializable^ obj = Apache::Geode::Client::Serializable::GetTypeFactoryMethodGeneric(m_classId)();
          obj->FromData(%mg_input);

          input.advanceCursor(mg_input.BytesReadInternally);

          m_hashCode = obj->GetHashCode();

          const uint8_t* objEndPos = input.currentBufferPosition();

          //m_size = mg_input.BytesRead;
          m_size = (uint32_t)(objEndPos - objStartPos);
          Apache::Geode::Client::Log::Debug("ManagedCacheableKeyBytesGeneric::fromData: objectSize = " + m_size + " m_hashCode = " + m_hashCode);
          m_bytes = input.getBufferCopyFrom(objStartPos, m_size);

        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        return this;
      }

      uint32_t ManagedCacheableKeyBytesGeneric::objectSize() const
      {
        try {
          //return m_managedptr->ObjectSize;
          return m_size;
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        return 0;
      }

      int32_t ManagedCacheableKeyBytesGeneric::classId() const
      {
        uint32_t classId;
        try {
          //classId = m_managedptr->ClassId;
          classId = m_classId;
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        return (classId >= 0x80000000 ? 0 : classId);
      }

      int8_t ManagedCacheableKeyBytesGeneric::typeId() const
      {
        try {
          //uint32_t classId = m_managedptr->ClassId;
          uint32_t classId = m_classId;
          if (classId >= 0x80000000) {
            return (int8_t)((classId - 0x80000000) % 0x20000000);
          }
          else if (classId <= 0x7F) {
            return (int8_t)apache::geode::client::GeodeTypeIdsImpl::CacheableUserData;
          }
          else if (classId <= 0x7FFF) {
            return (int8_t)apache::geode::client::GeodeTypeIdsImpl::CacheableUserData2;
          }
          else {
            return (int8_t)apache::geode::client::GeodeTypeIdsImpl::CacheableUserData4;
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

      int8_t ManagedCacheableKeyBytesGeneric::DSFID() const
      {
        // convention that [0x8000000, 0xa0000000) is for FixedIDDefault,
        // [0xa000000, 0xc0000000) is for FixedIDByte,
        // [0xc0000000, 0xe0000000) is for FixedIDShort
        // and [0xe0000000, 0xffffffff] is for FixedIDInt
        // Note: depends on fact that FixedIDByte is 1, FixedIDShort is 2
        // and FixedIDInt is 3; if this changes then correct this accordingly
        //uint32_t classId = m_managedptr->ClassId;
        uint32_t classId = m_classId;
        if (classId >= 0x80000000) {
          return (int8_t)((classId - 0x80000000) / 0x20000000);
        }
        return 0;
      }

      apache::geode::client::CacheableStringPtr ManagedCacheableKeyBytesGeneric::toString() const
      {
        try {
          Apache::Geode::Client::IGFSerializable^ manageObject = getManagedObject();
          if (manageObject != nullptr)
          {
            apache::geode::client::CacheableStringPtr cStr;
            Apache::Geode::Client::CacheableString::GetCacheableString(
              manageObject->ToString(), cStr);
            return cStr;
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

      bool ManagedCacheableKeyBytesGeneric::operator ==(const apache::geode::client::CacheableKey& other) const
      {
        try {
          Apache::Geode::Client::Log::Debug("ManagedCacheableKeyBytesGeneric::equal");
          // now checking classId(), typeId(), DSFID() etc. will be much more
          // expensive than just a dynamic_cast
          const ManagedCacheableKeyBytesGeneric* p_other =
            dynamic_cast<const ManagedCacheableKeyBytesGeneric*>(&other);
          if (p_other != NULL) {
            apache::geode::client::DataInput di(m_bytes, m_size);
            Apache::Geode::Client::DataInput mg_input(&di, true);
            Apache::Geode::Client::IGFSerializable^ obj =
              Apache::Geode::Client::Serializable::GetTypeFactoryMethodGeneric(m_classId)();
            obj->FromData(%mg_input);
            bool ret = obj->Equals(p_other->ptr());
            Apache::Geode::Client::Log::Debug("ManagedCacheableKeyBytesGeneric::equal return VAL = " + ret);
            return ret;
          }
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        Apache::Geode::Client::Log::Debug("ManagedCacheableKeyBytesGeneric::equal returns false");
        return false;
      }

      bool ManagedCacheableKeyBytesGeneric::operator ==(const ManagedCacheableKeyBytesGeneric& other) const
      {
        try {
          Apache::Geode::Client::Log::Debug("ManagedCacheableKeyBytesGeneric::equal. ");
          apache::geode::client::DataInput di(m_bytes, m_size);
          Apache::Geode::Client::DataInput mg_input(&di, true);
          Apache::Geode::Client::IGFSerializable^ obj =
            Apache::Geode::Client::Serializable::GetTypeFactoryMethodGeneric(m_classId)();
          obj->FromData(%mg_input);
          bool ret = obj->Equals(other.ptr());
          Apache::Geode::Client::Log::Debug("ManagedCacheableKeyBytesGeneric::equal return VAL = " + ret);
          return ret;
          //return obj->Equals(other.ptr());
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        Apache::Geode::Client::Log::Debug("ManagedCacheableKeyBytesGeneric::equal return false");
        return false;
      }

      uint32_t ManagedCacheableKeyBytesGeneric::hashcode() const
      {
        return m_hashCode;
      }

      size_t ManagedCacheableKeyBytesGeneric::logString(char* buffer, size_t maxLength) const
      {
        try {
          Apache::Geode::Client::IGFSerializable^ manageObject = getManagedObject();
          if (manageObject != nullptr)
          {
            if (maxLength > 0) {
              String^ logstr = manageObject->GetType()->Name + '(' +
                manageObject->ToString() + ')';
              Apache::Geode::Client::ManagedString mg_str(logstr);
              return snprintf(buffer, maxLength, "%s", mg_str.CharPtr);
            }
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

      Apache::Geode::Client::IGFSerializable^
        ManagedCacheableKeyBytesGeneric::getManagedObject() const
      {

        Apache::Geode::Client::Log::Debug("ManagedCacheableKeyBytesGeneric::getManagedObject " + m_size);

        //System::Text::StringBuilder^ sb = gcnew System::Text::StringBuilder(2000);
        //for(uint32_t i = 0; i<m_size; i++)
        //{
        //	if(m_bytes[i] != 0)
        //		sb->Append(System::Convert::ToChar( m_bytes[i]));
        //	//sb->Append(' ');
        //}

        //  Apache::Geode::Client::Log::Debug("ManagedCacheableKeyBytesGeneric::getManagedObject " + sb);
        apache::geode::client::DataInput dinp(m_bytes, m_size);
        Apache::Geode::Client::DataInput mg_dinp(&dinp, true);
        Apache::Geode::Client::TypeFactoryMethodGeneric^ creationMethod =
          Apache::Geode::Client::Serializable::GetTypeFactoryMethodGeneric(m_classId);
        Apache::Geode::Client::IGFSerializable^ newObj = creationMethod();
        return newObj->FromData(%mg_dinp);
      }
    }  // namespace client
  }  // namespace geode
}  // namespace apache

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
#include "PdxManagedCacheableKeyBytes.hpp"
#include "../DataInput.hpp"
#include "../DataOutput.hpp"
#include "../Serializable.hpp"
#include "../CacheableString.hpp"
#include <GeodeTypeIdsImpl.hpp>
#include "../ExceptionTypes.hpp"
#include "ManagedString.hpp"
#include "SafeConvert.hpp"

using namespace System;

namespace apache
{
  namespace geode
  {
    namespace client
    {
      void PdxManagedCacheableKeyBytes::toData(apache::geode::client::DataOutput& output) const
      {
        // Apache::Geode::Client::Log::Debug("PdxManagedCacheableKeyBytes::toData: current domain ID: " + System::Threading::Thread::GetDomainID() + " for object: " + System::Convert::ToString((int)this) + " with its domain ID: " + m_domainId );
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

      apache::geode::client::Serializable* PdxManagedCacheableKeyBytes::fromData(apache::geode::client::DataInput& input)
      {
        try {

          //Apache::Geode::Client::Log::Debug("PdxManagedCacheableKeyBytes::fromData: classid " + m_classId);
          Apache::Geode::Client::DataInput mg_input(&input, true);
          const uint8_t* objStartPos = input.currentBufferPosition();

          Apache::Geode::Client::IPdxSerializable^ obj = Apache::Geode::Client::Internal::PdxHelper::DeserializePdx(%mg_input, false);

          //Apache::Geode::Client::IGFSerializable^ obj = Apache::Geode::Client::Serializable::GetTypeFactoryMethodGeneric(m_classId)();
          //obj->FromData(%mg_input);

          input.advanceCursor(mg_input.BytesReadInternally);

          m_hashCode = obj->GetHashCode();

          const uint8_t* objEndPos = input.currentBufferPosition();

          //m_size = mg_input.BytesRead;
          m_size = (uint32_t)(objEndPos - objStartPos);
          // Apache::Geode::Client::Log::Debug("PdxManagedCacheableKeyBytes::fromData: objectSize = " + m_size + " m_hashCode = " + m_hashCode);
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

      uint32_t PdxManagedCacheableKeyBytes::objectSize() const
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

      int32_t PdxManagedCacheableKeyBytes::classId() const
      {
        //uint32_t classId;
        //try {
        //  //classId = m_managedptr->ClassId;
        //  classId = m_classId;
        //}
        //catch (Apache::Geode::Client::GeodeException^ ex) {
        //  ex->ThrowNative();
        //}
        //catch (System::Exception^ ex) {
        //  Apache::Geode::Client::GeodeException::ThrowNative(ex);
        //}
        //return (classId >= 0x80000000 ? 0 : classId);
        return 0;
      }

      int8_t PdxManagedCacheableKeyBytes::typeId() const
      {
        //try {
        //  //uint32_t classId = m_managedptr->ClassId;
        //  uint32_t classId = m_classId;
        //  if (classId >= 0x80000000) {
        //    return (int8_t)((classId - 0x80000000) % 0x20000000);
        //  }
        //  else if (classId <= 0x7F) {
        //    return (int8_t)apache::geode::client::GeodeTypeIdsImpl::CacheableUserData;
        //  }
        //  else if (classId <= 0x7FFF) {
        //    return (int8_t)apache::geode::client::GeodeTypeIdsImpl::CacheableUserData2;
        //  }
        //  else {
        //    return (int8_t)apache::geode::client::GeodeTypeIdsImpl::CacheableUserData4;
        //  }
        //}
        //catch (Apache::Geode::Client::GeodeException^ ex) {
        //  ex->ThrowNative();
        //}
        //catch (System::Exception^ ex) {
        //  Apache::Geode::Client::GeodeException::ThrowNative(ex);
        //}
        return (int8_t)GeodeTypeIdsImpl::PDX;
      }

      int8_t PdxManagedCacheableKeyBytes::DSFID() const
      {
        // convention that [0x8000000, 0xa0000000) is for FixedIDDefault,
        // [0xa000000, 0xc0000000) is for FixedIDByte,
        // [0xc0000000, 0xe0000000) is for FixedIDShort
        // and [0xe0000000, 0xffffffff] is for FixedIDInt
        // Note: depends on fact that FixedIDByte is 1, FixedIDShort is 2
        // and FixedIDInt is 3; if this changes then correct this accordingly
        //uint32_t classId = m_managedptr->ClassId;
        /*  uint32_t classId = m_classId;
          if (classId >= 0x80000000) {
          return (int8_t)((classId - 0x80000000) / 0x20000000);
          }*/
        return 0;
      }

      apache::geode::client::CacheableStringPtr PdxManagedCacheableKeyBytes::toString() const
      {
        try {
          Apache::Geode::Client::IPdxSerializable^ manageObject = getManagedObject();
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

      bool PdxManagedCacheableKeyBytes::operator ==(const apache::geode::client::CacheableKey& other) const
      {
        try {
          //  Apache::Geode::Client::Log::Debug("PdxManagedCacheableKeyBytes::equal");
          // now checking classId(), typeId(), DSFID() etc. will be much more
          // expensive than just a dynamic_cast
          const PdxManagedCacheableKeyBytes* p_other =
            dynamic_cast<const PdxManagedCacheableKeyBytes*>(&other);
          if (p_other != NULL) {
            apache::geode::client::DataInput di(m_bytes, m_size);
            Apache::Geode::Client::DataInput mg_input(&di, true);
            /* Apache::Geode::Client::IGFSerializable^ obj =
               Apache::Geode::Client::Serializable::GetTypeFactoryMethodGeneric(m_classId)();
               obj->FromData(%mg_input);*/
            Apache::Geode::Client::IPdxSerializable^ obj = getManagedObject();
            bool ret = obj->Equals(p_other->ptr());
            // Apache::Geode::Client::Log::Debug("PdxManagedCacheableKeyBytes::equal return VAL = " + ret);
            return ret;
          }
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        // Apache::Geode::Client::Log::Debug("PdxManagedCacheableKeyBytes::equal returns false");
        return false;
      }

      bool PdxManagedCacheableKeyBytes::operator ==(const PdxManagedCacheableKeyBytes& other) const
      {
        try {
          //Apache::Geode::Client::Log::Debug("PdxManagedCacheableKeyBytes::equal. ");
          apache::geode::client::DataInput di(m_bytes, m_size);
          Apache::Geode::Client::DataInput mg_input(&di, true);
          /*Apache::Geode::Client::IGFSerializable^ obj =
            Apache::Geode::Client::Serializable::GetTypeFactoryMethodGeneric(m_classId)();
            obj->FromData(%mg_input);*/
          Apache::Geode::Client::IPdxSerializable^ obj = getManagedObject();
          bool ret = obj->Equals(other.ptr());
          // Apache::Geode::Client::Log::Debug("PdxManagedCacheableKeyBytes::equal return VAL = " + ret);
          return ret;
          //return obj->Equals(other.ptr());
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        //  Apache::Geode::Client::Log::Debug("PdxManagedCacheableKeyBytes::equal return false");
        return false;
      }

      uint32_t PdxManagedCacheableKeyBytes::hashcode() const
      {
        return m_hashCode;
      }

      size_t PdxManagedCacheableKeyBytes::logString(char* buffer, size_t maxLength) const
      {
        try {
          Apache::Geode::Client::IPdxSerializable^ manageObject = getManagedObject();
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

      Apache::Geode::Client::IPdxSerializable^
        PdxManagedCacheableKeyBytes::getManagedObject() const
      {

        // Apache::Geode::Client::Log::Debug("PdxManagedCacheableKeyBytes::getManagedObject " + m_size);

        apache::geode::client::DataInput dinp(m_bytes, m_size);
        Apache::Geode::Client::DataInput mg_dinp(&dinp, true);
        /*TypeFactoryMethodGeneric^ creationMethod =
          Apache::Geode::Client::Serializable::GetTypeFactoryMethodGeneric(m_classId);
          Apache::Geode::Client::IGFSerializable^ newObj = creationMethod();
          return newObj->FromData(%mg_dinp);*/
        return  Apache::Geode::Client::Internal::PdxHelper::DeserializePdx(%mg_dinp, false);
      }

      bool PdxManagedCacheableKeyBytes::hasDelta()
      {
        /* Apache::Geode::Client::IGFDelta^ deltaObj = dynamic_cast<Apache::Geode::Client::IGFDelta^>(this->getManagedObject());

         if(deltaObj)
         return deltaObj->HasDelta();*/
        return m_hasDelta;
      }

      void PdxManagedCacheableKeyBytes::toDelta(DataOutput& output) const
      {
        try {
          Apache::Geode::Client::Log::Debug("PdxManagedCacheableKeyBytes::toDelta: current domain ID: " + System::Threading::Thread::GetDomainID() + " for object: " + System::Convert::ToString((int)this) + " with its domain ID: " + m_domainId);
          Apache::Geode::Client::IGFDelta^ deltaObj = dynamic_cast<Apache::Geode::Client::IGFDelta^>(this->getManagedObject());
          Apache::Geode::Client::DataOutput mg_output(&output, true);
          deltaObj->ToDelta(%mg_output);
          mg_output.WriteBytesToUMDataOutput();
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
      }

      void PdxManagedCacheableKeyBytes::fromDelta(DataInput& input)
      {
        try {
          Apache::Geode::Client::Log::Debug("PdxManagedCacheableKeyBytes::fromDelta:");
          Apache::Geode::Client::IGFDelta^ deltaObj = dynamic_cast<Apache::Geode::Client::IGFDelta^>(this->getManagedObject());
          Apache::Geode::Client::DataInput mg_input(&input, true);
          deltaObj->FromDelta(%mg_input);

          Apache::Geode::Client::IPdxSerializable^ managedptr =
            dynamic_cast <Apache::Geode::Client::IPdxSerializable^> (deltaObj);
          // if(managedptr != nullptr)
          {
            Apache::Geode::Client::Log::Debug("PdxManagedCacheableKeyBytes::fromDelta: current domain ID: " + System::Threading::Thread::GetDomainID() + " for object: " + System::Convert::ToString((int)this) + " with its domain ID: " + m_domainId);
            //Apache::Geode::Client::Log::Debug("PdxManagedCacheableKeyBytes::fromDelta: classid " + managedptr->ClassId + " : " + managedptr->ToString());
            apache::geode::client::DataOutput dataOut;
            Apache::Geode::Client::DataOutput mg_output(&dataOut, true);
            //managedptr->ToData( %mg_output );
            Apache::Geode::Client::Internal::PdxHelper::SerializePdx(%mg_output, managedptr);
            //m_managedptr->ToData( %mg_output );
            //this will move the cursor in c++ layer
            mg_output.WriteBytesToUMDataOutput();

            //move cursor
            // dataOut.advanceCursor(mg_output.BufferLength);

            GF_SAFE_DELETE(m_bytes);
            m_bytes = dataOut.getBufferCopy();
            m_size = dataOut.getBufferLength();
            Apache::Geode::Client::Log::Debug("PdxManagedCacheableKeyBytes::fromDelta objectSize = " + m_size + " m_hashCode = " + m_hashCode);
            m_hashCode = managedptr->GetHashCode();
          }
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
      }

      DeltaPtr PdxManagedCacheableKeyBytes::clone()
      {
        try {
          Apache::Geode::Client::IGFDelta^ deltaObj = dynamic_cast<Apache::Geode::Client::IGFDelta^>(this->getManagedObject());
          ICloneable^ cloneable = dynamic_cast<ICloneable^>((Apache::Geode::Client::IGFDelta^) deltaObj);
          if (cloneable) {
            Apache::Geode::Client::IPdxSerializable^ Mclone =
              dynamic_cast<Apache::Geode::Client::IPdxSerializable^>(cloneable->Clone());
            return DeltaPtr(static_cast<PdxManagedCacheableKeyBytes*>(
              SafeGenericM2UMConvert(Mclone)));
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

    }  // namespace client
  }  // namespace geode
}  // namespace apache

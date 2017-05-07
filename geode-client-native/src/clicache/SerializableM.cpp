/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "cppcache/impl/SerializationRegistry.hpp"

#include "gf_includes.hpp"
#include "SerializableM.hpp"
#include "impl/DelegateWrapper.hpp"
#include "com/vmware/impl/DelegateWrapperN.hpp"
#include "DataOutputM.hpp"
#include "DataInputM.hpp"
#include "CacheableStringM.hpp"
#include "CacheableStringArrayM.hpp"

#include "LogM.hpp"

#include "CacheableBuiltinsM.hpp"
#include "ExceptionTypesM.hpp"
#include "impl/SafeConvert.hpp"
#include <cppcache/impl/GemfireTypeIdsImpl.hpp>
#include "com/vmware/CacheableHashMapMN.hpp"
#include "com/vmware/CacheableHashTableMN.hpp"
#include "CacheableArrayListM.hpp" 
#include "PropertiesM.hpp"

using namespace System;
//using namespace System::Collections::Generic;

using namespace gemfire;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      void GemStone::GemFire::Cache::Serializable::ToData(GemStone::GemFire::Cache::DataOutput^ output)
      {
        gemfire::DataOutput* nativeOutput =
          GetNativePtr<gemfire::DataOutput>(output);
        NativePtr->toData(*nativeOutput);        
      }
      GemStone::GemFire::Cache::IGFSerializable^ GemStone::GemFire::Cache::Serializable::FromData(GemStone::GemFire::Cache::DataInput^ input)
      {
        gemfire::DataInput* nativeInput =
          GetNativePtr<gemfire::DataInput>(input);
        AssignSP(NativePtr->fromData(*nativeInput));
        return this;
      }

      uint32_t GemStone::GemFire::Cache::Serializable::ObjectSize::get()
      {
        return NativePtr->objectSize();
      }

      uint32_t GemStone::GemFire::Cache::Serializable::ClassId::get()
      {
        int8_t typeId = NativePtr->typeId();
        if (typeId == gemfire::GemfireTypeIdsImpl::CacheableUserData ||
             typeId == gemfire::GemfireTypeIdsImpl::CacheableUserData2 ||
             typeId == gemfire::GemfireTypeIdsImpl::CacheableUserData4) {
          return NativePtr->classId();
        } else {
          return typeId + 0x80000000 + (0x20000000 * NativePtr->DSFID());
        }
      }

      String^ GemStone::GemFire::Cache::Serializable::ToString()
      {
        gemfire::CacheableStringPtr& cStr = NativePtr->toString();
        if (cStr->isWideString()) {
          return ManagedString::Get(cStr->asWChar());
        } else {
          return ManagedString::Get(cStr->asChar());
        }
      }

      GemStone::GemFire::Cache::Serializable::operator GemStone::GemFire::Cache::Serializable^ (Byte value)
      {
        return (GemStone::GemFire::Cache::Serializable^) GemStone::GemFire::Cache::CacheableByte::Create(value);
      }

      GemStone::GemFire::Cache::Serializable::operator GemStone::GemFire::Cache::Serializable^ (bool value)
      {
        return (GemStone::GemFire::Cache::Serializable^)GemStone::GemFire::Cache::CacheableBoolean::Create(value);
      }

      GemStone::GemFire::Cache::Serializable::operator GemStone::GemFire::Cache::Serializable^ (array<bool>^ value)
      {
        return (GemStone::GemFire::Cache::Serializable^)GemStone::GemFire::Cache::BooleanArray::Create(value);
      }

      GemStone::GemFire::Cache::Serializable::operator GemStone::GemFire::Cache::Serializable^ (array<Byte>^ value)
      {
        return (GemStone::GemFire::Cache::Serializable^)GemStone::GemFire::Cache::CacheableBytes::Create(value);
      }

      GemStone::GemFire::Cache::Serializable::operator GemStone::GemFire::Cache::Serializable^ (Char value)
      {
        return (GemStone::GemFire::Cache::Serializable^)CacheableCharacter::Create(value);
      }

      GemStone::GemFire::Cache::Serializable::operator GemStone::GemFire::Cache::Serializable^ (array<Char>^ value)
      {
        return (GemStone::GemFire::Cache::Serializable^)GemStone::GemFire::Cache::CharArray::Create(value);
      }

      GemStone::GemFire::Cache::Serializable::operator GemStone::GemFire::Cache::Serializable^ (Double value)
      {
        return (GemStone::GemFire::Cache::Serializable^)GemStone::GemFire::Cache::CacheableDouble::Create(value);
      }

      GemStone::GemFire::Cache::Serializable::operator GemStone::GemFire::Cache::Serializable^ (array<Double>^ value)
      {
        return (GemStone::GemFire::Cache::Serializable^)GemStone::GemFire::Cache::CacheableDoubleArray::Create(value);
      }

      GemStone::GemFire::Cache::Serializable::operator GemStone::GemFire::Cache::Serializable^ (Single value)
      {
        return (GemStone::GemFire::Cache::Serializable^)GemStone::GemFire::Cache::CacheableFloat::Create(value);
      }

      GemStone::GemFire::Cache::Serializable::operator GemStone::GemFire::Cache::Serializable^ (array<Single>^ value)
      {
        return (GemStone::GemFire::Cache::Serializable^)GemStone::GemFire::Cache::CacheableFloatArray::Create(value);
      }

      GemStone::GemFire::Cache::Serializable::operator GemStone::GemFire::Cache::Serializable^ (int16_t value)
      {
        return (GemStone::GemFire::Cache::Serializable^)GemStone::GemFire::Cache::CacheableInt16::Create(value);
      }

      GemStone::GemFire::Cache::Serializable::operator GemStone::GemFire::Cache::Serializable^ (array<int16_t>^ value)
      {
        return (GemStone::GemFire::Cache::Serializable^)GemStone::GemFire::Cache::CacheableInt16Array::Create(value);
      }

      GemStone::GemFire::Cache::Serializable::operator GemStone::GemFire::Cache::Serializable^ (int32_t value)
      {
        return (GemStone::GemFire::Cache::Serializable^)CacheableInt32::Create(value);
      }

      GemStone::GemFire::Cache::Serializable::operator GemStone::GemFire::Cache::Serializable^ (array<int32_t>^ value)
      {
        return (GemStone::GemFire::Cache::Serializable^)GemStone::GemFire::Cache::CacheableInt32Array::Create(value);
      }

      GemStone::GemFire::Cache::Serializable::operator GemStone::GemFire::Cache::Serializable^ (int64_t value)
      {
        return (GemStone::GemFire::Cache::Serializable^)GemStone::GemFire::Cache::CacheableInt64::Create(value);
      }

      /*GemStone::GemFire::Cache::*/Serializable::operator /*GemStone::GemFire::Cache::*/Serializable^ (array<int64_t>^ value)
      {
        return (GemStone::GemFire::Cache::Serializable^)GemStone::GemFire::Cache::CacheableInt64Array::Create(value);
      }

      GemStone::GemFire::Cache::Serializable::operator GemStone::GemFire::Cache::Serializable^ (String^ value)
      {
        return (GemStone::GemFire::Cache::Serializable^)GemStone::GemFire::Cache::CacheableString::Create(value);
      }

      GemStone::GemFire::Cache::Serializable::operator GemStone::GemFire::Cache::Serializable^ (array<String^>^ value)
      {
        return (GemStone::GemFire::Cache::Serializable^)GemStone::GemFire::Cache::CacheableStringArray::Create(value);
      }

      void GemStone::GemFire::Cache::Serializable::RegisterType(TypeFactoryMethod^ creationMethod)
      {
        if (creationMethod == nullptr) {
          throw gcnew IllegalArgumentException("Serializable.RegisterType(): "
            "null TypeFactoryMethod delegate passed");
        }

        //--------------------------------------------------------------
        
        //adding user type as well in global builtin hashmap
        int64_t classId = ((int64_t)creationMethod()->ClassId);
        if (!ManagedDelegates->ContainsKey(classId))
          ManagedDelegates->Add(classId, creationMethod);

        DelegateWrapper^ delegateObj = gcnew DelegateWrapper(creationMethod);
        TypeFactoryNativeMethod^ nativeDelegate =
          gcnew TypeFactoryNativeMethod(delegateObj,
          &DelegateWrapper::NativeDelegate);

        // this is avoid object being Gced
        NativeDelegates->Add(nativeDelegate);
        
        // register the type in the DelegateMap, this is pure c# for create domain object 
        IGFSerializable^ tmp = creationMethod();
        Log::Fine("Registering serializable class ID " + tmp->ClassId +
          ", AppDomain ID " + System::Threading::Thread::GetDomainID());
        DelegateMap[tmp->ClassId] = creationMethod;

        _GF_MG_EXCEPTION_TRY

          gemfire::Serializable::registerType((gemfire::TypeFactoryMethod)
            System::Runtime::InteropServices::Marshal::
            GetFunctionPointerForDelegate(nativeDelegate).ToPointer());

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void GemStone::GemFire::Cache::Serializable::RegisterType(Byte typeId,
        TypeFactoryMethod^ creationMethod)
      {
        if (creationMethod == nullptr) {
          throw gcnew IllegalArgumentException("Serializable.RegisterType(): "
            "null TypeFactoryMethod delegate passed");
        }
        DelegateWrapper^ delegateObj = gcnew DelegateWrapper(creationMethod);
        TypeFactoryNativeMethod^ nativeDelegate =
          gcnew TypeFactoryNativeMethod(delegateObj,
          &DelegateWrapper::NativeDelegate);

        BuiltInDelegates[typeId] = nativeDelegate;

          //This is hashmap for manged builtin objects
        if (!ManagedDelegates->ContainsKey(typeId + 0x80000000))
          ManagedDelegates->Add(typeId + 0x80000000, creationMethod);

        // register the type in the DelegateMap
        IGFSerializable^ tmp = creationMethod();
        Log::Finer("Registering(,) serializable class ID " + tmp->ClassId +
          ", AppDomain ID " + System::Threading::Thread::GetDomainID());
        DelegateMap[tmp->ClassId] = creationMethod;

        try
        {
          gemfire::SerializationRegistry::addType(typeId,
            (gemfire::TypeFactoryMethod)System::Runtime::InteropServices::
            Marshal::GetFunctionPointerForDelegate(
            nativeDelegate).ToPointer());

        }catch(gemfire::IllegalStateException&)
        {
          //ignore it as this is internal only
        }
      }

      void GemStone::GemFire::Cache::Serializable::UnregisterType(Byte typeId)
      {
        BuiltInDelegates->Remove(typeId);
        _GF_MG_EXCEPTION_TRY

          gemfire::SerializationRegistry::removeType(typeId);

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void GemStone::GemFire::Cache::Serializable::RegisterWrapper(WrapperDelegate^ wrapperMethod,
        Byte typeId)
      {
        if (typeId < 0 || typeId > WrapperEnd)
        {
          throw gcnew GemFireException("The TypeID (" + typeId + ") being "
            "registered is beyond acceptable range of 0-" + WrapperEnd);
        }
        NativeWrappers[typeId] = wrapperMethod;
      }

      void GemStone::GemFire::Cache::Serializable::UnregisterNatives()
      {
        BuiltInDelegates->Clear();
        for (Byte typeId = 0; typeId <= WrapperEnd; ++typeId) {
          NativeWrappers[typeId] = nullptr;
        }
         //TODO::Hitesh unregister from managed hashmap as well.
      //  ManagedDelegates->Clear();
      }
    }
  }
}

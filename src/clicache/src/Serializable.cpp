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

#include <SerializationRegistry.hpp>

//#include "gf_includes.hpp"
#include "Serializable.hpp"
#include "impl/DelegateWrapper.hpp"
#include "DataOutput.hpp"
#include "DataInput.hpp"
//#include "CacheableString.hpp"
#include "CacheableStringArray.hpp"

//#include "Log.hpp"
//
#include "CacheableBuiltins.hpp"
//#include "ExceptionTypes.hpp"
#include "impl/SafeConvert.hpp"
//#include <GemfireTypeIdsImpl.hpp>
//#include "CacheableHashMap.hpp"
#include "CacheableHashTable.hpp"
#include "Struct.hpp"
#include "CacheableUndefined.hpp"
#include "CacheableObject.hpp"
#include "CacheableStack.hpp"
#include "CacheableObjectXml.hpp"
#include "CacheableHashSet.hpp"
#include "CacheableObjectArray.hpp"
#include "CacheableLinkedList.hpp"
#include "CacheableFileName.hpp"
#include "CacheableIdentityHashMap.hpp"
#include "IPdxSerializer.hpp"
#include "impl/DotNetTypes.hpp"
#pragma warning(disable:4091)
#include <msclr/lock.h>
using namespace System::Reflection;
using namespace System::Reflection::Emit;
//#include "CacheableArrayList.hpp" 
//#include "CacheableVector.hpp"
//#include "impl/PdxManagedCacheableKey.hpp"

using namespace System;
using namespace System::Collections::Generic;
using namespace Runtime::InteropServices;
using namespace apache::geode::client;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {
      void GemStone::GemFire::Cache::Generic::Serializable::ToData(
        GemStone::GemFire::Cache::Generic::DataOutput^ output)
      {
        if (output->IsManagedObject()) {
          output->WriteBytesToUMDataOutput();          
        }
        apache::geode::client::DataOutput* nativeOutput =
          GetNativePtr<apache::geode::client::DataOutput>(output);
        NativePtr->toData(*nativeOutput);
        if (output->IsManagedObject()) {
          output->SetBuffer();          
        }
      }
      GemStone::GemFire::Cache::Generic::IGFSerializable^
        GemStone::GemFire::Cache::Generic::Serializable::FromData(
        GemStone::GemFire::Cache::Generic::DataInput^ input)
      {
         if(input->IsManagedObject()) {
          input->AdvanceUMCursor();
        }
        apache::geode::client::DataInput* nativeInput =
          GetNativePtr<apache::geode::client::DataInput>(input);
        AssignSP(NativePtr->fromData(*nativeInput));
        if(input->IsManagedObject()) {
          input->SetBuffer();
        }
        return this;
      }

      uint32_t GemStone::GemFire::Cache::Generic::Serializable::ObjectSize::get()
      {
        return NativePtr->objectSize();
      }

      uint32_t GemStone::GemFire::Cache::Generic::Serializable::ClassId::get()
      {
        int8_t typeId = NativePtr->typeId();
        if (typeId == apache::geode::client::GemfireTypeIdsImpl::CacheableUserData ||
             typeId == apache::geode::client::GemfireTypeIdsImpl::CacheableUserData2 ||
             typeId == apache::geode::client::GemfireTypeIdsImpl::CacheableUserData4) {
          return NativePtr->classId();
        } else {
          return typeId + 0x80000000 + (0x20000000 * NativePtr->DSFID());
        }
      }

      String^ GemStone::GemFire::Cache::Generic::Serializable::ToString()
      {
        apache::geode::client::CacheableStringPtr& cStr = NativePtr->toString();
        if (cStr->isWideString()) {
          return ManagedString::Get(cStr->asWChar());
        } else {
          return ManagedString::Get(cStr->asChar());
        }
      }

      GemStone::GemFire::Cache::Generic::Serializable::operator GemStone::GemFire::Cache::Generic::Serializable^ (Byte value)
      {
        return (GemStone::GemFire::Cache::Generic::Serializable^) CacheableByte::Create(value);
      }

      GemStone::GemFire::Cache::Generic::Serializable::operator GemStone::GemFire::Cache::Generic::Serializable^ (bool value)
      {
        return (GemStone::GemFire::Cache::Generic::Serializable^)CacheableBoolean::Create(value);
      }

      GemStone::GemFire::Cache::Generic::Serializable::operator GemStone::GemFire::Cache::Generic::Serializable^ (array<bool>^ value)
      {
       // return (GemStone::GemFire::Cache::Generic::Serializable^)GemStone::GemFire::Cache::Generic::CacheableBooleanArray::Create(value);
				//TODO:split
				return nullptr;
      }

      GemStone::GemFire::Cache::Generic::Serializable::operator GemStone::GemFire::Cache::Generic::Serializable^ (array<Byte>^ value)
      {
        return (GemStone::GemFire::Cache::Generic::Serializable^)CacheableBytes::Create(value);
      }

      GemStone::GemFire::Cache::Generic::Serializable::operator GemStone::GemFire::Cache::Generic::Serializable^ (Char value)
      {
        return (GemStone::GemFire::Cache::Generic::Serializable^)CacheableCharacter::Create(value);
      }

      GemStone::GemFire::Cache::Generic::Serializable::operator GemStone::GemFire::Cache::Generic::Serializable^ (array<Char>^ value)
      {
        //return (GemStone::GemFire::Cache::Generic::Serializable^)GemStone::GemFire::Cache::Generic::CacheableCharArray::Create(value);
				//TODO:split
				return nullptr;

      }

      GemStone::GemFire::Cache::Generic::Serializable::operator GemStone::GemFire::Cache::Generic::Serializable^ (Double value)
      {
        return (GemStone::GemFire::Cache::Generic::Serializable^)CacheableDouble::Create(value);
      }

      GemStone::GemFire::Cache::Generic::Serializable::operator GemStone::GemFire::Cache::Generic::Serializable^ (array<Double>^ value)
      {
        return (GemStone::GemFire::Cache::Generic::Serializable^)CacheableDoubleArray::Create(value);
      }

      GemStone::GemFire::Cache::Generic::Serializable::operator GemStone::GemFire::Cache::Generic::Serializable^ (Single value)
      {
        return (GemStone::GemFire::Cache::Generic::Serializable^)CacheableFloat::Create(value);
      }

      GemStone::GemFire::Cache::Generic::Serializable::operator GemStone::GemFire::Cache::Generic::Serializable^ (array<Single>^ value)
      {
        return (GemStone::GemFire::Cache::Generic::Serializable^)CacheableFloatArray::Create(value);
      }

      GemStone::GemFire::Cache::Generic::Serializable::operator GemStone::GemFire::Cache::Generic::Serializable^ (int16_t value)
      {
        return (GemStone::GemFire::Cache::Generic::Serializable^)CacheableInt16::Create(value);
      }

      GemStone::GemFire::Cache::Generic::Serializable::operator GemStone::GemFire::Cache::Generic::Serializable^ (array<int16_t>^ value)
      {
        return (GemStone::GemFire::Cache::Generic::Serializable^)CacheableInt16Array::Create(value);
      }

      GemStone::GemFire::Cache::Generic::Serializable::operator GemStone::GemFire::Cache::Generic::Serializable^ (int32_t value)
      {
        return (GemStone::GemFire::Cache::Generic::Serializable^)CacheableInt32::Create(value);
      }

      GemStone::GemFire::Cache::Generic::Serializable::operator GemStone::GemFire::Cache::Generic::Serializable^ (array<int32_t>^ value)
      {
        return (GemStone::GemFire::Cache::Generic::Serializable^)CacheableInt32Array::Create(value);
      }

      GemStone::GemFire::Cache::Generic::Serializable::operator GemStone::GemFire::Cache::Generic::Serializable^ (int64_t value)
      {
        return (GemStone::GemFire::Cache::Generic::Serializable^)CacheableInt64::Create(value);
      }

      /*GemStone::GemFire::Cache::*/Serializable::operator /*GemStone::GemFire::Cache::*/Serializable^ (array<int64_t>^ value)
      {
				return (GemStone::GemFire::Cache::Generic::Serializable^)GemStone::GemFire::Cache::Generic::CacheableInt64Array::Create(value);
      }

      GemStone::GemFire::Cache::Generic::Serializable::operator GemStone::GemFire::Cache::Generic::Serializable^ (String^ value)
      {
        return (GemStone::GemFire::Cache::Generic::Serializable^)CacheableString::Create(value);
      }

      GemStone::GemFire::Cache::Generic::Serializable::operator GemStone::GemFire::Cache::Generic::Serializable^ (array<String^>^ value)
      {
        return (GemStone::GemFire::Cache::Generic::Serializable^)CacheableStringArray::Create(value);
      }

			int32 Serializable::GetPDXIdForType(const char* poolName, IGFSerializable^ pdxType)
      {
				apache::geode::client::CacheablePtr kPtr(SafeMSerializableConvertGeneric(pdxType));
        return apache::geode::client::SerializationRegistry::GetPDXIdForType(poolName, kPtr);
			}

      IGFSerializable^ Serializable::GetPDXTypeById(const char* poolName, int32 typeId)
      {
				SerializablePtr sPtr = apache::geode::client::SerializationRegistry::GetPDXTypeById(poolName, typeId);
        return SafeUMSerializableConvertGeneric(sPtr.ptr());
			}

      int Serializable::GetEnumValue(Internal::EnumInfo^ ei)
      {
        apache::geode::client::CacheablePtr kPtr(SafeMSerializableConvertGeneric(ei));
        return apache::geode::client::SerializationRegistry::GetEnumValue(kPtr);
      }
      
      Internal::EnumInfo^ Serializable::GetEnum(int val)
      {
        SerializablePtr sPtr = apache::geode::client::SerializationRegistry::GetEnum(val);
        return (Internal::EnumInfo^)SafeUMSerializableConvertGeneric(sPtr.ptr());
      }

      void Serializable::RegisterPdxType(PdxTypeFactoryMethod^ creationMethod)
      {
				if (creationMethod == nullptr) {
          throw gcnew IllegalArgumentException("Serializable.RegisterPdxType(): "
            "null PdxTypeFactoryMethod delegate passed");
        }
        IPdxSerializable^ obj = creationMethod();
        PdxDelegateMap[obj->GetType()->FullName] = creationMethod;
        Log::Debug("RegisterPdxType: class registered: " + obj->GetType()->FullName);
      }

      Object^ Serializable::CreateObject(String^ className)
      {
        
        Object^ retVal = CreateObjectEx(className);

        if(retVal == nullptr)
        {
          Type^ t = GetType(className);
          if(t)
          {
            retVal = t->GetConstructor(Type::EmptyTypes)->Invoke(nullptr);
            return retVal;
          }
        }
        return retVal;
      }

      Object^ Serializable::CreateObjectEx(String^ className)
      {
        CreateNewObjectDelegate^ del = nullptr;
        Dictionary<String^, CreateNewObjectDelegate^>^ tmp = ClassNameVsCreateNewObjectDelegate;

        tmp->TryGetValue(className, del);

        if(del != nullptr)
        {
          return del();
        }

        Type^ t = GetType(className);
        if(t)
        {
          msclr::lock lockInstance(ClassNameVsTypeLockObj);
          {
            tmp = ClassNameVsCreateNewObjectDelegate;
            tmp->TryGetValue(className, del);
            if(del != nullptr)
              return del();
            del = CreateNewObjectDelegateF(t);
            tmp = gcnew Dictionary<String^, CreateNewObjectDelegate^>(ClassNameVsCreateNewObjectDelegate);
            tmp[className] = del;
            ClassNameVsCreateNewObjectDelegate = tmp;
            return del();
          }
        }
        return nullptr;
      }

      Object^ Serializable::GetArrayObject(String^ className, int len)
      {
        Object^ retArr = GetArrayObjectEx(className, len);
        if(retArr == nullptr)
        {
          Type^ t = GetType(className);
          if(t)
          {
            retArr = t->MakeArrayType()->GetConstructor(singleIntType)->Invoke(gcnew array<Object^>(1) { len });
            return retArr;
          }              
        }        
        return retArr;
      }

      Object^ Serializable::GetArrayObjectEx(String^ className, int len)
      {
        CreateNewObjectArrayDelegate^ del = nullptr;
        Dictionary<String^, CreateNewObjectArrayDelegate^>^ tmp = ClassNameVsCreateNewObjectArrayDelegate;

        tmp->TryGetValue(className, del);

        if(del != nullptr)
        {
          return del(len);
        }

        Type^ t = GetType(className);
        if(t)
        {
          msclr::lock lockInstance(ClassNameVsTypeLockObj);
          {
            tmp = ClassNameVsCreateNewObjectArrayDelegate;
            tmp->TryGetValue(className, del);
            if(del != nullptr)
              return del(len);
            del = CreateNewObjectArrayDelegateF(t);
            tmp = gcnew Dictionary<String^, CreateNewObjectArrayDelegate^>(ClassNameVsCreateNewObjectArrayDelegate);
            tmp[className] = del;
            ClassNameVsCreateNewObjectArrayDelegate = tmp;
            return del(len);
          }
        }
        return nullptr;
      }

      //delegate Object^ CreateNewObject();
       //static CreateNewObjectDelegate^ CreateNewObjectDelegateF(Type^ type);
      Serializable::CreateNewObjectDelegate^ Serializable::CreateNewObjectDelegateF(Type^ type)
      {
        DynamicMethod^ dynam = gcnew DynamicMethod("", Internal::DotNetTypes::ObjectType, Type::EmptyTypes, type, true);
        ILGenerator^ il = dynam->GetILGenerator();
        
        ConstructorInfo^ ctorInfo = type->GetConstructor(Type::EmptyTypes);
        if ( ctorInfo == nullptr ) {
          Log::Error("Object missing public no arg constructor");
          throw gcnew IllegalStateException("Object missing public no arg constructor");
        }

        il->Emit(OpCodes::Newobj, ctorInfo);
        il->Emit(OpCodes::Ret);

        return (Serializable::CreateNewObjectDelegate^)dynam->CreateDelegate(createNewObjectDelegateType);
      }
      
      //delegate Object^ CreateNewObjectArray(int len);
      Serializable::CreateNewObjectArrayDelegate^ Serializable::CreateNewObjectArrayDelegateF(Type^ type)
      {
        DynamicMethod^ dynam = gcnew DynamicMethod("", Internal::DotNetTypes::ObjectType, singleIntTypeA, type, true);
        ILGenerator^ il = dynam->GetILGenerator();

        il->Emit(OpCodes::Ldarg_0);

        il->Emit(OpCodes::Newarr, type);
        il->Emit(OpCodes::Ret);

        return (Serializable::CreateNewObjectArrayDelegate^)dynam->CreateDelegate(createNewObjectArrayDelegateType);
      }

      Type^ Serializable::getTypeFromRefrencedAssemblies(String^ className, Dictionary<Assembly^, bool>^ referedAssembly, Assembly^ currentAsm)
      {
        Type^ t = currentAsm->GetType(className);
        if( t != nullptr)
        {
          Dictionary<String^, Type^>^ tmp = gcnew Dictionary<String^, Type^>(ClassNameVsType);
          tmp[className] = t;
          ClassNameVsType = tmp;
          return t;
        }
        //already touched
        if(referedAssembly->ContainsKey(currentAsm))
          return nullptr;
        referedAssembly[currentAsm] = true;

        //get all refrenced assembly
        array<AssemblyName^>^ ReferencedAssemblies = currentAsm->GetReferencedAssemblies();
        for each(AssemblyName^ tmpAsm in ReferencedAssemblies)
        {
          try
          {
            Assembly^ la = Assembly::Load(tmpAsm);
            if(la != nullptr && (!referedAssembly->ContainsKey(la)))
            {
              t = getTypeFromRefrencedAssemblies(className, referedAssembly, la);
              if(!t)
                return t;
            }
          }catch(System::Exception^ ){//ignore
          }
        }
        return nullptr;
       }

      Type^ Serializable::GetType(String^ className)
      {
        Type^ retVal = nullptr;
        Dictionary<String^, Type^>^ tmp = ClassNameVsType;
        tmp->TryGetValue(className, retVal);

        if(retVal != nullptr)
          return retVal;
        msclr::lock lockInstance(ClassNameVsTypeLockObj);
        {
          tmp = ClassNameVsType;
          tmp->TryGetValue(className, retVal);

          if(retVal != nullptr)
            return retVal;

          Dictionary<Assembly^, bool>^ referedAssembly = gcnew Dictionary<Assembly^, bool>();
          AppDomain^ MyDomain = AppDomain::CurrentDomain;
          array<Assembly^>^ AssembliesLoaded = MyDomain->GetAssemblies();
          for each(Assembly^ tmpAsm in AssembliesLoaded)
          {
            retVal = getTypeFromRefrencedAssemblies(className, referedAssembly, tmpAsm);
            if(retVal)
                return retVal;
          }
        }
        return retVal;
      }

      IPdxSerializable^ Serializable::GetPdxType(String^ className)
      {	
        PdxTypeFactoryMethod^ retVal = nullptr;
			  PdxDelegateMap->TryGetValue(className, retVal);

				if(retVal == nullptr){
          
          if(PdxSerializer != nullptr )
          {
            return gcnew PdxWrapper(className);            
          }
          try
          {
            Object^ retObj = CreateObject(className);
            
            IPdxSerializable^ retPdx = dynamic_cast<IPdxSerializable^>(retObj);
            if(retPdx != nullptr)
              return retPdx;
          }catch(System::Exception^ ex)
          {
            Log::Error("Unable to create object usqing reflection for class: " + className + " : " + ex->Message);
          }         
          throw gcnew IllegalStateException("Pdx factory method (or PdxSerializer ) not registered (or don't have zero arg constructor)"
            " to create default instance for class: " + className );
        }

        return retVal();
      }

			void Serializable::RegisterPDXManagedCacheableKey(bool appDomainEnable)
      {
				if(!appDomainEnable)
				{
					apache::geode::client::SerializationRegistry::addType(apache::geode::client::GemfireTypeIdsImpl::PDX,
						&apache::geode::client::PdxManagedCacheableKey::CreateDeserializable );
				}
				else
				{
					apache::geode::client::SerializationRegistry::addType(apache::geode::client::GemfireTypeIdsImpl::PDX,
						&apache::geode::client::PdxManagedCacheableKeyBytes::CreateDeserializable );
				}
      }

      void GemStone::GemFire::Cache::Generic::Serializable::RegisterTypeGeneric(TypeFactoryMethodGeneric^ creationMethod)
      {
        if (creationMethod == nullptr) {
          throw gcnew IllegalArgumentException("Serializable.RegisterType(): "
            "null TypeFactoryMethod delegate passed");
        }

        //--------------------------------------------------------------
        
        //adding user type as well in global builtin hashmap
        int64_t classId = ((int64_t)creationMethod()->ClassId);
        if (!ManagedDelegatesGeneric->ContainsKey(classId))
          ManagedDelegatesGeneric->Add(classId, creationMethod);

        DelegateWrapperGeneric^ delegateObj = gcnew DelegateWrapperGeneric(creationMethod);
        TypeFactoryNativeMethodGeneric^ nativeDelegate =
          gcnew TypeFactoryNativeMethodGeneric(delegateObj,
          &DelegateWrapperGeneric::NativeDelegateGeneric);

        // this is avoid object being Gced
        NativeDelegatesGeneric->Add(nativeDelegate);
        
        // register the type in the DelegateMap, this is pure c# for create domain object 
        IGFSerializable^ tmp = creationMethod();
        Log::Fine("Registering serializable class ID " + tmp->ClassId +
          ", AppDomain ID " + System::Threading::Thread::GetDomainID());
        DelegateMapGeneric[tmp->ClassId] = creationMethod;

        _GF_MG_EXCEPTION_TRY2

          apache::geode::client::Serializable::registerType((apache::geode::client::TypeFactoryMethod)
            System::Runtime::InteropServices::Marshal::
            GetFunctionPointerForDelegate(nativeDelegate).ToPointer());

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      void GemStone::GemFire::Cache::Generic::Serializable::RegisterTypeGeneric(Byte typeId,
        TypeFactoryMethodGeneric^ creationMethod, Type^ type)
      {
        if (creationMethod == nullptr) {
          throw gcnew IllegalArgumentException("Serializable.RegisterType(): "
            "null TypeFactoryMethod delegate passed");
        }
        DelegateWrapperGeneric^ delegateObj = gcnew DelegateWrapperGeneric(creationMethod);
        TypeFactoryNativeMethodGeneric^ nativeDelegate =
          gcnew TypeFactoryNativeMethodGeneric(delegateObj,
          &DelegateWrapperGeneric::NativeDelegateGeneric);

        BuiltInDelegatesGeneric[typeId] = nativeDelegate;

				if(type != nullptr)
					ManagedTypeMappingGeneric[type] = typeId;

          //This is hashmap for manged builtin objects
        if (!ManagedDelegatesGeneric->ContainsKey(typeId + 0x80000000))
          ManagedDelegatesGeneric->Add(typeId + 0x80000000, creationMethod);

        // register the type in the DelegateMap
        IGFSerializable^ tmp = creationMethod();
        Log::Finer("Registering(,) serializable class ID " + tmp->ClassId +
          ", AppDomain ID " + System::Threading::Thread::GetDomainID());
        DelegateMapGeneric[tmp->ClassId] = creationMethod;

        try
        {
					if(tmp->ClassId < 0xa0000000)
					{
						apache::geode::client::SerializationRegistry::addType(typeId,
							(apache::geode::client::TypeFactoryMethod)System::Runtime::InteropServices::
							Marshal::GetFunctionPointerForDelegate(
							nativeDelegate).ToPointer());
					}
					else
					{//special case for CacheableUndefined type
							apache::geode::client::SerializationRegistry::addType2(typeId,
							(apache::geode::client::TypeFactoryMethod)System::Runtime::InteropServices::
							Marshal::GetFunctionPointerForDelegate(
							nativeDelegate).ToPointer());
					}

        }catch(apache::geode::client::IllegalStateException&)
        {
          //ignore it as this is internal only
        }
      }

      void GemStone::GemFire::Cache::Generic::Serializable::UnregisterTypeGeneric(Byte typeId)
      {
        BuiltInDelegatesGeneric->Remove(typeId);
        _GF_MG_EXCEPTION_TRY2

          apache::geode::client::SerializationRegistry::removeType(typeId);

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      void GemStone::GemFire::Cache::Generic::Serializable::RegisterWrapperGeneric(
        WrapperDelegateGeneric^ wrapperMethod, Byte typeId, System::Type^ type)
      {
        if (typeId < 0 || typeId > WrapperEndGeneric)
        {
          throw gcnew GemFireException("The TypeID (" + typeId + ") being "
            "registered is beyond acceptable range of 0-" + WrapperEndGeneric);
        }
        NativeWrappersGeneric[typeId] = wrapperMethod;
				ManagedTypeMappingGeneric[type] = typeId;
      }

      void GemStone::GemFire::Cache::Generic::Serializable::UnregisterNativesGeneric()
      {
        BuiltInDelegatesGeneric->Clear();
        for (Byte typeId = 0; typeId <= WrapperEndGeneric; ++typeId) {
          NativeWrappersGeneric[typeId] = nullptr;
        }
         //TODO:: unregister from managed hashmap as well.
      //  ManagedDelegates->Clear();
      }

      generic<class TValue>
      TValue Serializable::GetManagedValueGeneric(apache::geode::client::SerializablePtr val)
      {
        if (val == NULLPTR)
        {
          return TValue();
        }

        Byte typeId = val->typeId();
				 //Log::Debug("Serializable::GetManagedValueGeneric typeid = " + typeId);
        switch(typeId)
        {
        case apache::geode::client::GemfireTypeIds::CacheableByte:
          {
						return (TValue)(int8_t)safe_cast<int8_t>(Serializable::getByte(val));
           /* if (TValue::typeid == System::SByte::typeid) {
              return (TValue)(int8_t)safe_cast<int8_t>(Serializable::getByte(val));              
            }
            else {
              return (TValue)(uint8_t)safe_cast<int8_t>(Serializable::getByte(val));              
            }
            return safe_cast<TValue>(Serializable::getByte(val));*/
          }
        case apache::geode::client::GemfireTypeIds::CacheableBoolean:
          {
            return safe_cast<TValue>(Serializable::getBoolean(val));
          }
        case apache::geode::client::GemfireTypeIds::CacheableWideChar:
          {
            return safe_cast<TValue>(Serializable::getChar(val));
          }
        case apache::geode::client::GemfireTypeIds::CacheableDouble:
          {
            return safe_cast<TValue>(Serializable::getDouble(val));
          }
        case apache::geode::client::GemfireTypeIds::CacheableASCIIString:
        case apache::geode::client::GemfireTypeIds::CacheableASCIIStringHuge:
        case apache::geode::client::GemfireTypeIds::CacheableString:
        case apache::geode::client::GemfireTypeIds::CacheableStringHuge:
          {
            //TODO: need to look all strings types
            return safe_cast<TValue>(Serializable::getASCIIString(val));
          }
        case apache::geode::client::GemfireTypeIds::CacheableFloat:
          {
            return safe_cast<TValue>(Serializable::getFloat(val));
          }
        case apache::geode::client::GemfireTypeIds::CacheableInt16:
          {
           /* if (TValue::typeid == System::Int16::typeid) {
              return (TValue)(int16_t)safe_cast<int16_t>(Serializable::getInt16(val));              
            }
            else {
              return (TValue)(uint16_t)safe_cast<int16_t>(Serializable::getInt16(val));              
            }*/
            return safe_cast<TValue>(Serializable::getInt16(val));
          }
        case apache::geode::client::GemfireTypeIds::CacheableInt32:
          {  
           /* if (TValue::typeid == System::Int32::typeid) {
              return (TValue)(int32_t)safe_cast<int32_t>(Serializable::getInt32(val));              
            }
            else {
              return (TValue)(uint32_t)safe_cast<int32_t>(Serializable::getInt32(val));              
            }  */          
            return safe_cast<TValue>(Serializable::getInt32(val));
          }
        case apache::geode::client::GemfireTypeIds::CacheableInt64:
          {
            /*if (TValue::typeid == System::Int64::typeid) {
              return (TValue)(int64_t)safe_cast<int64_t>(Serializable::getInt64(val));              
            }
            else {
              return (TValue)(uint64_t)safe_cast<int64_t>(Serializable::getInt64(val));              
            }*/
            return safe_cast<TValue>(Serializable::getInt64(val));
          }
					case apache::geode::client::GemfireTypeIds::CacheableDate:
          {
						//TODO::
            GemStone::GemFire::Cache::Generic::CacheableDate^ ret = static_cast<GemStone::GemFire::Cache::Generic::CacheableDate ^>
              ( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::CacheableDate^>(val.ptr()));

            System::DateTime dt(ret->Value.Ticks);
            return safe_cast<TValue>(dt);
          }
        case apache::geode::client::GemfireTypeIdsImpl::CacheableUserData:
        case apache::geode::client::GemfireTypeIdsImpl::CacheableUserData2:
        case apache::geode::client::GemfireTypeIdsImpl::CacheableUserData4:
          {
						//TODO::split 
            IGFSerializable^ ret = SafeUMSerializableConvertGeneric(val.ptr());
            return safe_cast<TValue>(ret);
						//return TValue();
          }
        case apache::geode::client::GemfireTypeIdsImpl::PDX:
          {
            IPdxSerializable^ ret = SafeUMSerializablePDXConvert(val.ptr());

            PdxWrapper^ pdxWrapper = dynamic_cast<PdxWrapper^>(ret);

            if(pdxWrapper != nullptr)
            {
              return safe_cast<TValue>(pdxWrapper->GetObject());
            }

            return safe_cast<TValue>(ret);
          }
        case apache::geode::client::GemfireTypeIds::CacheableBytes:
          {
            GemStone::GemFire::Cache::Generic::CacheableBytes^ ret = safe_cast<GemStone::GemFire::Cache::Generic::CacheableBytes ^>
              ( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::CacheableBytes^>(val.ptr()));

            return safe_cast<TValue>(ret->Value);            
          }
        case apache::geode::client::GemfireTypeIds::CacheableDoubleArray:
          {
            GemStone::GemFire::Cache::Generic::CacheableDoubleArray^ ret = safe_cast<GemStone::GemFire::Cache::Generic::CacheableDoubleArray ^>
              ( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::CacheableDoubleArray^>(val.ptr()));
            
            return safe_cast<TValue>(ret->Value);
          }
        case apache::geode::client::GemfireTypeIds::CacheableFloatArray:
          {
            GemStone::GemFire::Cache::Generic::CacheableFloatArray^ ret = safe_cast<GemStone::GemFire::Cache::Generic::CacheableFloatArray^>
              ( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::CacheableFloatArray^>(val.ptr()));
            
            return safe_cast<TValue>(ret->Value);
          }
        case apache::geode::client::GemfireTypeIds::CacheableInt16Array:
          {
            GemStone::GemFire::Cache::Generic::CacheableInt16Array^ ret = safe_cast<GemStone::GemFire::Cache::Generic::CacheableInt16Array^>
              ( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::CacheableInt16Array^>(val.ptr()));
            
            return safe_cast<TValue>(ret->Value);
          }
        case apache::geode::client::GemfireTypeIds::CacheableInt32Array:
          {
            GemStone::GemFire::Cache::Generic::CacheableInt32Array^ ret = safe_cast<GemStone::GemFire::Cache::Generic::CacheableInt32Array^>
              ( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::CacheableInt32Array^>(val.ptr()));

            return safe_cast<TValue>(ret->Value);
          }
        case apache::geode::client::GemfireTypeIds::CacheableInt64Array:
          {
						GemStone::GemFire::Cache::Generic::CacheableInt64Array^ ret = safe_cast<GemStone::GemFire::Cache::Generic::CacheableInt64Array^>
              ( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::CacheableInt64Array^>(val.ptr()));
            
            return safe_cast<TValue>(ret->Value);
          }
        case apache::geode::client::GemfireTypeIds::CacheableStringArray:
          {
            GemStone::GemFire::Cache::Generic::CacheableStringArray^ ret = safe_cast<GemStone::GemFire::Cache::Generic::CacheableStringArray^>
              ( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::CacheableStringArray^>(val.ptr()));
                        
           /* array<String^>^ str = gcnew array<String^>(ret->GetValues()->Length);
            for(int i=0; i<ret->GetValues()->Length; i++ ) {
              str[i] = ret->GetValues()[i];
            }*/

            return safe_cast<TValue>(ret->GetValues());
          }
        case apache::geode::client::GemfireTypeIds::CacheableArrayList://Ilist generic
          {
            GemStone::GemFire::Cache::Generic::CacheableArrayList^ ret = safe_cast<GemStone::GemFire::Cache::Generic::CacheableArrayList^>
             ( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::CacheableArrayList^>(val.ptr()));
            
            return safe_cast<TValue>(ret->Value);
          }
        case apache::geode::client::GemfireTypeIds::CacheableLinkedList://LinkedList generic
          {
            GemStone::GemFire::Cache::Generic::CacheableLinkedList^ ret = safe_cast<GemStone::GemFire::Cache::Generic::CacheableLinkedList^>
             ( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::CacheableLinkedList^>(val.ptr()));
            
            return safe_cast<TValue>(ret->Value);
          }		  
        case apache::geode::client::GemfireTypeIds::CacheableHashTable://collection::hashtable
          {
            GemStone::GemFire::Cache::Generic::CacheableHashTable^ ret = safe_cast<GemStone::GemFire::Cache::Generic::CacheableHashTable^>
              ( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::CacheableHashTable^>(val.ptr()));
            
            return safe_cast<TValue>(ret->Value);
          }
        case apache::geode::client::GemfireTypeIds::CacheableHashMap://generic dictionary
          {
           GemStone::GemFire::Cache::Generic::CacheableHashMap^ ret = safe_cast<GemStone::GemFire::Cache::Generic::CacheableHashMap^>
             ( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::CacheableHashMap^>(val.ptr()));
            
            return safe_cast<TValue>(ret->Value);
          }
				case apache::geode::client::GemfireTypeIds::CacheableIdentityHashMap:
          {
            GemStone::GemFire::Cache::Generic::CacheableIdentityHashMap^ ret = static_cast<GemStone::GemFire::Cache::Generic::CacheableIdentityHashMap^>
							( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::CacheableIdentityHashMap^>(val.ptr()));            
            return safe_cast<TValue>(ret->Value);
          }
				 case apache::geode::client::GemfireTypeIds::CacheableHashSet://no need of it, default case should work
        {
          GemStone::GemFire::Cache::Generic::CacheableHashSet^ ret = static_cast<GemStone::GemFire::Cache::Generic::CacheableHashSet^>
						( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::CacheableHashSet^>(val.ptr()));
          return safe_cast<TValue>(ret);
        }
        case apache::geode::client::GemfireTypeIds::CacheableLinkedHashSet://no need of it, default case should work
        {
          GemStone::GemFire::Cache::Generic::CacheableLinkedHashSet^ ret = static_cast<GemStone::GemFire::Cache::Generic::CacheableLinkedHashSet^>
						( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::CacheableLinkedHashSet^>(val.ptr()));
          return safe_cast<TValue>(ret);
        }
				case apache::geode::client::GemfireTypeIds::CacheableFileName:
        {
          GemStone::GemFire::Cache::Generic::CacheableFileName^ ret = static_cast<GemStone::GemFire::Cache::Generic::CacheableFileName^>
						( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::CacheableFileName^>(val.ptr()));
          return safe_cast<TValue>(ret);
        }
				 case apache::geode::client::GemfireTypeIds::CacheableObjectArray:
        {
          GemStone::GemFire::Cache::Generic::CacheableObjectArray^ ret = static_cast<GemStone::GemFire::Cache::Generic::CacheableObjectArray^>
						( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::CacheableObjectArray^>(val.ptr()));
          return safe_cast<TValue>(ret);
        }
				case apache::geode::client::GemfireTypeIds::CacheableVector://collection::arraylist
        {
          GemStone::GemFire::Cache::Generic::CacheableVector^ ret = static_cast<GemStone::GemFire::Cache::Generic::CacheableVector^>
						( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::CacheableVector^>(val.ptr()));
          return safe_cast<TValue>(ret->Value);
        }
				case apache::geode::client::GemfireTypeIds::CacheableUndefined:
        {
          GemStone::GemFire::Cache::Generic::CacheableUndefined^ ret = static_cast<GemStone::GemFire::Cache::Generic::CacheableUndefined^>
						( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::CacheableUndefined^>(val.ptr()));
          return safe_cast<TValue>(ret);
        }
        case apache::geode::client::GemfireTypeIds::Struct:
        {
          return safe_cast<TValue>(GemStone::GemFire::Cache::Generic::Struct::Create(val.ptr()));
        }
				case apache::geode::client::GemfireTypeIds::CacheableStack:
        {
          GemStone::GemFire::Cache::Generic::CacheableStack^ ret = static_cast<GemStone::GemFire::Cache::Generic::CacheableStack^>
						( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::CacheableStack^>(val.ptr()));
          return safe_cast<TValue>(ret->Value);
        }
        case 7: //GemFireClassIds::CacheableManagedObject
        {
          GemStone::GemFire::Cache::Generic::CacheableObject^ ret = static_cast<GemStone::GemFire::Cache::Generic::CacheableObject^>
						( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::CacheableObject^>(val.ptr()));
          return safe_cast<TValue>(ret);
        }
        case 8://GemFireClassIds::CacheableManagedObjectXml
        {
          GemStone::GemFire::Cache::Generic::CacheableObjectXml^ ret = static_cast<GemStone::GemFire::Cache::Generic::CacheableObjectXml^>
						( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::CacheableObjectXml^>(val.ptr()));
          return safe_cast<TValue>(ret);
        }
          /*  TODO: replace with IDictionary<K, V>
        case apache::geode::client::GemfireTypeIds::Properties:
          {
            GemStone::GemFire::Cache::Generic::Properties^ ret = safe_cast<GemStone::GemFire::Cache::Generic::Properties^>
              ( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::Properties^>(val.ptr()));
            
            return safe_cast<TValue>(ret);
          }*/
       
        case apache::geode::client::GemfireTypeIds::BooleanArray:
          {
						GemStone::GemFire::Cache::Generic::BooleanArray^ ret = safe_cast<GemStone::GemFire::Cache::Generic::BooleanArray^>
              ( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::BooleanArray^>(val.ptr()));
            
            return safe_cast<TValue>(ret->Value);
					}
        case apache::geode::client::GemfireTypeIds::CharArray:
          {
						GemStone::GemFire::Cache::Generic::CharArray^ ret = safe_cast<GemStone::GemFire::Cache::Generic::CharArray^>
              ( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::CharArray^>(val.ptr()));
            
            return safe_cast<TValue>(ret->Value);
					}
        case 0://UserFunctionExecutionException unregistered
          {            
						GemStone::GemFire::Cache::Generic::UserFunctionExecutionException^ ret = static_cast<GemStone::GemFire::Cache::Generic::UserFunctionExecutionException^>
              ( SafeGenericUMSerializableConvert<GemStone::GemFire::Cache::Generic::UserFunctionExecutionException^>(val.ptr()));            
            return safe_cast<TValue>(ret);
					}
        default:
          throw gcnew System::Exception("not found typeid");
        }
         throw gcnew System::Exception("not found typeid");
      }

      generic<class TKey>
      apache::geode::client::CacheableKeyPtr Serializable::GetUnmanagedValueGeneric(TKey key)
      {
        //System::Type^ managedType = TKey::typeid;  
        if (key != nullptr) {
          //System::Type^ managedType = key->GetType();
          return GetUnmanagedValueGeneric(key->GetType(), key);        
        }
        return NULLPTR;
      }

      generic<class TKey>
      apache::geode::client::CacheableKeyPtr Serializable::GetUnmanagedValueGeneric(TKey key, bool isAciiChar)
      {
        //System::Type^ managedType = TKey::typeid;  
        if (key != nullptr) {
          //System::Type^ managedType = key->GetType();
          return GetUnmanagedValueGeneric(key->GetType(), key, isAciiChar);        
        }
        return NULLPTR;
      }

      void Serializable::RegisterPdxSerializer(IPdxSerializer^ pdxSerializer)
      {
        /*if(PdxSerializer != nullptr )
        {
          throw gcnew IllegalStateException("IPdxSerializer is already registered: " + PdxSerializer->GetType());
        }*/
        PdxSerializer = pdxSerializer;
      }

      void Serializable::SetPdxTypeMapper(IPdxTypeMapper^ pdxTypeMapper)
      {
        if(pdxTypeMapper != nullptr)
          PdxTypeMapper = pdxTypeMapper;
      }      

      String^ Serializable::GetPdxTypeName(String^ localTypeName)
      {
        if(PdxTypeMapper == nullptr)
          return localTypeName;
        IDictionary<String^, String^>^ tmp = LocalTypeNameToPdx;
        String^ pdxTypeName = nullptr;
        tmp->TryGetValue(localTypeName, pdxTypeName);

        if(pdxTypeName != nullptr)
          return pdxTypeName;
        
        {
          msclr::lock lockInstance(LockObj);
          tmp->TryGetValue(localTypeName, pdxTypeName);

          if(pdxTypeName != nullptr)
            return pdxTypeName;
          if(PdxTypeMapper != nullptr)
          {
            pdxTypeName = PdxTypeMapper->ToPdxTypeName(localTypeName);
            if(pdxTypeName == nullptr)
            {
              throw gcnew IllegalStateException("PdxTypeName should not be null for local type " + localTypeName);
            }

            Dictionary<String^, String^>^ localToPdx = gcnew Dictionary<String^, String^>(LocalTypeNameToPdx);
            localToPdx[localTypeName] = pdxTypeName;
            LocalTypeNameToPdx = localToPdx;
            Dictionary<String^, String^>^ pdxToLocal = gcnew Dictionary<String^, String^>(PdxTypeNameToLocal);
            localToPdx[pdxTypeName] = localTypeName;
            PdxTypeNameToLocal = pdxToLocal;
          } 
        }
        return pdxTypeName;
      }
      
      String^ Serializable::GetLocalTypeName(String^ pdxTypeName)
      {
        if(PdxTypeMapper == nullptr)
          return pdxTypeName;

        IDictionary<String^, String^>^ tmp = PdxTypeNameToLocal;
        String^ localTypeName = nullptr;
        tmp->TryGetValue(pdxTypeName, localTypeName);

        if(localTypeName != nullptr)
          return localTypeName;
        
        {
          msclr::lock lockInstance(LockObj);
          tmp->TryGetValue(pdxTypeName, localTypeName);

          if(localTypeName != nullptr)
            return localTypeName;
          if(PdxTypeMapper != nullptr)
          {
            localTypeName = PdxTypeMapper->FromPdxTypeName(pdxTypeName);
            if(localTypeName == nullptr)
            {
              throw gcnew IllegalStateException("LocalTypeName should not be null for pdx type " + pdxTypeName);
            }

            Dictionary<String^, String^>^ localToPdx = gcnew Dictionary<String^, String^>(LocalTypeNameToPdx);
            localToPdx[localTypeName] = pdxTypeName;
            LocalTypeNameToPdx = localToPdx;
            Dictionary<String^, String^>^ pdxToLocal = gcnew Dictionary<String^, String^>(PdxTypeNameToLocal);
            localToPdx[pdxTypeName] = localTypeName;
            PdxTypeNameToLocal = pdxToLocal;
          } 
        }
        return localTypeName;
      }
      
      void Serializable::Clear()
      {
        PdxTypeMapper = nullptr;
        LocalTypeNameToPdx->Clear();
        PdxTypeNameToLocal->Clear();
        ClassNameVsCreateNewObjectDelegate->Clear();
        ClassNameVsType->Clear();
        ClassNameVsCreateNewObjectArrayDelegate->Clear();
      }

      IPdxSerializer^ Serializable::GetPdxSerializer()
      {
        return PdxSerializer;
      }

      bool Serializable::IsObjectAndPdxSerializerRegistered(String^ className)
      {
        return PdxSerializer != nullptr;
       // return CreateObject(className) != nullptr;
       /* if(PdxSerializer != nullptr)
        {
          FactoryMethod^ retVal = nullptr;
			    PdxSerializerObjectDelegateMap->TryGetValue(className, retVal);
          return retVal != nullptr;
        }*/
        //return false;
      }
      
      generic<class TKey>
      apache::geode::client::CacheableKeyPtr Serializable::GetUnmanagedValueGeneric(
        Type^ managedType, TKey key)
      {
        return GetUnmanagedValueGeneric(managedType, key, false);
      }
      
      generic<class TKey>
      apache::geode::client::CacheableKeyPtr Serializable::GetUnmanagedValueGeneric(
        Type^ managedType, TKey key, bool isAsciiChar)
      {
        Byte typeId = GemStone::GemFire::Cache::Generic::Serializable::GetManagedTypeMappingGeneric(managedType);        

        switch(typeId)
        {
        case apache::geode::client::GemfireTypeIds::CacheableByte: {
					return Serializable::getCacheableByte((SByte)key);
         /* if( managedType == System::SByte::typeid )
          {
            return Serializable::getCacheableByte((SByte)key);
          }
          else 
          {
            return Serializable::getCacheableByte((Byte)key);
          }*/
        }
        case apache::geode::client::GemfireTypeIds::CacheableBoolean:
          return Serializable::getCacheableBoolean((bool)key);
        case apache::geode::client::GemfireTypeIds::CacheableWideChar:
          return Serializable::getCacheableWideChar((Char)key);
        case apache::geode::client::GemfireTypeIds::CacheableDouble:
          return Serializable::getCacheableDouble((double)key);
        case apache::geode::client::GemfireTypeIds::CacheableASCIIString: {
          if (isAsciiChar)
            return Serializable::getCacheableASCIIString2((String^)key);
          else
            return Serializable::getCacheableASCIIString((String^)key);
                                                            }
        case apache::geode::client::GemfireTypeIds::CacheableFloat:
          return Serializable::getCacheableFloat((float)key);
        case apache::geode::client::GemfireTypeIds::CacheableInt16: {
          /*if( managedType == System::Int16::typeid )
            {      
              return Serializable::getCacheableInt16((int16_t)key);
            }
           else 
            {
              return Serializable::getCacheableInt16((uint16_t)key);
            }*/
          return Serializable::getCacheableInt16((int16_t)key);
         }
        case apache::geode::client::GemfireTypeIds::CacheableInt32: {
          /* if( managedType == System::Int32::typeid )
            {      
              return Serializable::getCacheableInt32((int32_t)key);
            }
           else 
            {
              return Serializable::getCacheableInt32((int32_t)key);
            }*/
					 return Serializable::getCacheableInt32((int32_t)key);
         }
        case apache::geode::client::GemfireTypeIds::CacheableInt64: {
          /*if( managedType == System::Int64::typeid )
            {      
              return Serializable::getCacheableInt64((int64_t)key);
            }
           else 
            {
              return Serializable::getCacheableInt64((uint64_t)key);
            }*/
          return Serializable::getCacheableInt64((int64_t)key);
         }
        case apache::geode::client::GemfireTypeIds::CacheableBytes:
          {
						apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)GemStone::GemFire::Cache::Generic::CacheableBytes::Create((array<Byte>^)key)));
              return kPtr;
            /*if( managedType == Type::GetType("System.Byte[]") ) {
              apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)GemStone::GemFire::Cache::Generic::CacheableBytes::Create((array<Byte>^)key)));
              return kPtr;
            }
            else {
              apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)GemStone::GemFire::Cache::Generic::CacheableBytes::Create(getSByteArray((array<SByte>^)key))));
              return kPtr;
            }*/
          }
        case apache::geode::client::GemfireTypeIds::CacheableDoubleArray:
          {
            apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)GemStone::GemFire::Cache::Generic::CacheableDoubleArray::Create((array<Double>^)key)));
            return kPtr;
          }
        case apache::geode::client::GemfireTypeIds::CacheableFloatArray:
        {
          apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)GemStone::GemFire::Cache::Generic::CacheableFloatArray::Create((array<float>^)key)));
           return kPtr;
        }
        case apache::geode::client::GemfireTypeIds::CacheableInt16Array:
        {
					 apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)GemStone::GemFire::Cache::Generic::CacheableInt16Array::Create((array<Int16>^)key)));
            return kPtr;
         /* if( managedType == Type::GetType("System.Int16[]") ) {
            apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)GemStone::GemFire::Cache::Generic::CacheableInt16Array::Create((array<Int16>^)key)));
            return kPtr;
          }
          else { 
            apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)GemStone::GemFire::Cache::Generic::CacheableInt16Array::Create(getInt16Array((array<uint16_t>^)key))));
            return kPtr;
          }  */          
        }
        case apache::geode::client::GemfireTypeIds::CacheableInt32Array:
        {
					 apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)GemStone::GemFire::Cache::Generic::CacheableInt32Array::Create((array<Int32>^)key)));
            return kPtr;
        /*  if( managedType == Type::GetType("System.Int32[]") ) {
            apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)GemStone::GemFire::Cache::Generic::CacheableInt32Array::Create((array<Int32>^)key)));
            return kPtr;
          }
          else { 
            apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)GemStone::GemFire::Cache::Generic::CacheableInt32Array::Create(getInt32Array((array<uint32_t>^)key))));
            return kPtr;
          }       */   
        }
        case apache::geode::client::GemfireTypeIds::CacheableInt64Array:
        {
					apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)GemStone::GemFire::Cache::Generic::CacheableInt64Array::Create((array<Int64>^)key)));
            return kPtr;
          /*if( managedType == Type::GetType("System.Int64[]") ) {
            apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)GemStone::GemFire::Cache::Generic::CacheableInt64Array::Create((array<Int64>^)key)));
            return kPtr;
          }
          else { 
            apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)GemStone::GemFire::Cache::Generic::CacheableInt64Array::Create(getInt64Array((array<uint64_t>^)key))));
            return kPtr;
          }     */                  
        }
        case apache::geode::client::GemfireTypeIds::CacheableStringArray:
        {
          apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)GemStone::GemFire::Cache::Generic::CacheableStringArray::Create((array<String^>^)key)));
           return kPtr;
        }
				case apache::geode::client::GemfireTypeIds::CacheableFileName:
        {
					apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)(GemStone::GemFire::Cache::Generic::CacheableFileName^)key));
          return kPtr;
        }
        case apache::geode::client::GemfireTypeIds::CacheableHashTable://collection::hashtable
        {
          apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)GemStone::GemFire::Cache::Generic::CacheableHashTable::Create((System::Collections::Hashtable^)key)));
          return kPtr;
        }
        case apache::geode::client::GemfireTypeIds::CacheableHashMap://generic dictionary
        {
          apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)GemStone::GemFire::Cache::Generic::CacheableHashMap::Create((System::Collections::IDictionary^)key)));
          return kPtr;
        }
        case apache::geode::client::GemfireTypeIds::CacheableVector://collection::arraylist
        {
          apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)CacheableVector::Create((System::Collections::IList^)key)));
          return kPtr;
        }
        case apache::geode::client::GemfireTypeIds::CacheableArrayList://generic ilist
        {
          apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)GemStone::GemFire::Cache::Generic::CacheableArrayList::Create((System::Collections::IList^)key)));
          return kPtr;
        } 
        case apache::geode::client::GemfireTypeIds::CacheableLinkedList://generic linked list
        {
          apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)GemStone::GemFire::Cache::Generic::CacheableLinkedList::Create((System::Collections::Generic::LinkedList<Object^>^)key)));
          return kPtr;
        }		
		case apache::geode::client::GemfireTypeIds::CacheableStack:
        {
          apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert(GemStone::GemFire::Cache::Generic::CacheableStack::Create((System::Collections::ICollection^)key)));
          return kPtr;
        }
        case 7: //GemFireClassIds::CacheableManagedObject
        {
          apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((GemStone::GemFire::Cache::Generic::CacheableObject^)key));
          return kPtr;
        }
        case 8://GemFireClassIds::CacheableManagedObjectXml
        {
          apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((GemStone::GemFire::Cache::Generic::CacheableObjectXml^)key));
          return kPtr;
        }
				 case apache::geode::client::GemfireTypeIds::CacheableObjectArray:
        {
          apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((GemStone::GemFire::Cache::Generic::CacheableObjectArray^)key));
          return kPtr;
        }
			  case apache::geode::client::GemfireTypeIds::CacheableIdentityHashMap:
        {
          apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert(GemStone::GemFire::Cache::Generic::CacheableIdentityHashMap::Create((System::Collections::IDictionary^)key)));
          return kPtr;
        }
        case apache::geode::client::GemfireTypeIds::CacheableHashSet://no need of it, default case should work
        {
          apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((GemStone::GemFire::Cache::Generic::CacheableHashSet^)key));
          return kPtr;
        }
        case apache::geode::client::GemfireTypeIds::CacheableLinkedHashSet://no need of it, default case should work
        {
          apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((GemStone::GemFire::Cache::Generic::CacheableLinkedHashSet^)key));
          return kPtr;
        }
        case apache::geode::client::GemfireTypeIds::CacheableDate:
          {
            apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)GemStone::GemFire::Cache::Generic::CacheableDate::Create((System::DateTime)key)));
            return kPtr;
          }
        case apache::geode::client::GemfireTypeIds::BooleanArray:
          {   
					  apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)GemStone::GemFire::Cache::Generic::BooleanArray::Create((array<bool>^)key)));
            return kPtr;
					}
        case apache::geode::client::GemfireTypeIds::CharArray:
          {
					  apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert((IGFSerializable^)GemStone::GemFire::Cache::Generic::CharArray::Create((array<Char>^)key)));
            return kPtr;
					}
        default:
          {
						apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert(key));
            /*IGFSerializable^ ct = safe_cast<IGFSerializable^>(key);
            if(ct != nullptr) {
              apache::geode::client::CacheablePtr kPtr(SafeGenericMSerializableConvert(ct));
              return kPtr;
            }*/
            //throw gcnew System::Exception("not found typeid");
						return kPtr;
          }
        }
      } //

				String^ Serializable::GetString(apache::geode::client::CacheableStringPtr cStr)//apache::geode::client::CacheableString*
        {
          if (cStr == NULLPTR) {
            return nullptr;
          }
          else if (cStr->isWideString()) {
            return ManagedString::Get(cStr->asWChar());
          }
          else {
            return ManagedString::Get(cStr->asChar());
          }
        }

        // These are the new static methods to get/put data from c++

        //byte
        Byte Serializable::getByte(apache::geode::client::SerializablePtr nativeptr)
        {
          apache::geode::client::CacheableByte* ci = static_cast<apache::geode::client::CacheableByte*>(nativeptr.ptr());
          return ci->value();
        }

        apache::geode::client::CacheableKeyPtr Serializable::getCacheableByte(SByte val)
        {
          return apache::geode::client::CacheableByte::create(val);
        }

        //boolean
        bool Serializable::getBoolean(apache::geode::client::SerializablePtr nativeptr)
        {
          apache::geode::client::CacheableBoolean* ci = static_cast<apache::geode::client::CacheableBoolean*>(nativeptr.ptr());
          return ci->value();
        }

        apache::geode::client::CacheableKeyPtr Serializable::getCacheableBoolean(bool val)
        {
          return apache::geode::client::CacheableBoolean::create(val);
        }

        //widechar
        Char Serializable::getChar(apache::geode::client::SerializablePtr nativeptr)
        {
          apache::geode::client::CacheableWideChar* ci = static_cast<apache::geode::client::CacheableWideChar*>(nativeptr.ptr());
          return ci->value();
        }

        apache::geode::client::CacheableKeyPtr Serializable::getCacheableWideChar(Char val)
        {
          return apache::geode::client::CacheableWideChar::create(val);
        }

        //double
        double Serializable::getDouble(apache::geode::client::SerializablePtr nativeptr)
        {
          apache::geode::client::CacheableDouble* ci = static_cast<apache::geode::client::CacheableDouble*>(nativeptr.ptr());
          return ci->value();
        }

        apache::geode::client::CacheableKeyPtr Serializable::getCacheableDouble(double val)
        {
          return apache::geode::client::CacheableDouble::create(val);
        }

        //float
        float Serializable::getFloat(apache::geode::client::SerializablePtr nativeptr)
        {
          apache::geode::client::CacheableFloat* ci = static_cast<apache::geode::client::CacheableFloat*>(nativeptr.ptr());
          return ci->value();
        }

        apache::geode::client::CacheableKeyPtr Serializable::getCacheableFloat(float val)
        {
          return apache::geode::client::CacheableFloat::create(val);
        }

        //int16
        int16 Serializable::getInt16(apache::geode::client::SerializablePtr nativeptr)
        {
          apache::geode::client::CacheableInt16* ci = static_cast<apache::geode::client::CacheableInt16*>(nativeptr.ptr());
          return ci->value();
        }

        apache::geode::client::CacheableKeyPtr Serializable::getCacheableInt16(int val)
        {
          return apache::geode::client::CacheableInt16::create(val);
        }

        //int32
        int32 Serializable::getInt32(apache::geode::client::SerializablePtr nativeptr)
        {
          apache::geode::client::CacheableInt32* ci = static_cast<apache::geode::client::CacheableInt32*>(nativeptr.ptr());
          return ci->value();
        }

        apache::geode::client::CacheableKeyPtr Serializable::getCacheableInt32(int32 val)
        {
          return apache::geode::client::CacheableInt32::create(val);
        }

        //int64
        int64 Serializable::getInt64(apache::geode::client::SerializablePtr nativeptr)
        {
          apache::geode::client::CacheableInt64* ci = static_cast<apache::geode::client::CacheableInt64*>(nativeptr.ptr());
          return ci->value();
        }

        apache::geode::client::CacheableKeyPtr Serializable::getCacheableInt64(int64 val)
        {
          return apache::geode::client::CacheableInt64::create(val);
        }

        //cacheable ascii string
        String^ Serializable::getASCIIString(apache::geode::client::SerializablePtr nativeptr)
        {
          //apache::geode::client::CacheableString* ci = static_cast<apache::geode::client::CacheableString*>(nativeptr.ptr());          
          //return GetString(ci);
          return GetString(nativeptr->toString());
        }

        apache::geode::client::CacheableKeyPtr Serializable::getCacheableASCIIString(String^ val)
        {
          return GetCacheableString(val);
        }

        apache::geode::client::CacheableKeyPtr Serializable::getCacheableASCIIString2(String^ val)
        {
          return GetCacheableString2(val);
        }

        //cacheable ascii string huge
        String^ Serializable::getASCIIStringHuge(apache::geode::client::SerializablePtr nativeptr)
        {
          //apache::geode::client::CacheableString* ci = static_cast<apache::geode::client::CacheableString*>(nativeptr.ptr());          
          //return GetString(ci);
          return GetString(nativeptr->toString());
        }

        apache::geode::client::CacheableKeyPtr Serializable::getCacheableASCIIStringHuge(String^ val)
        {
          return GetCacheableString(val);
        }

        //cacheable string
        String^ Serializable::getUTFString(apache::geode::client::SerializablePtr nativeptr)
        {
          //apache::geode::client::CacheableString* ci = static_cast<apache::geode::client::CacheableString*>(nativeptr.ptr());          
          //return GetString(ci);
          return GetString(nativeptr->toString());
        }

        apache::geode::client::CacheableKeyPtr Serializable::getCacheableUTFString(String^ val)
        {
          return GetCacheableString(val);
        }

        //cacheable string huge
        String^ Serializable::getUTFStringHuge(apache::geode::client::SerializablePtr nativeptr)
        {
          //apache::geode::client::CacheableString* ci = static_cast<apache::geode::client::CacheableString*>(nativeptr.ptr());
          //return GetString(ci);
          return GetString(nativeptr->toString());
        }

        apache::geode::client::CacheableKeyPtr Serializable::getCacheableUTFStringHuge(String^ val)
        {
          return GetCacheableString(val);
        }

       apache::geode::client::CacheableStringPtr Serializable::GetCacheableString(String^ value)
       {
          apache::geode::client::CacheableStringPtr cStr;
          size_t len = 0;
          if (value != nullptr) {
            len = value->Length;
            pin_ptr<const wchar_t> pin_value = PtrToStringChars(value);
            cStr = apache::geode::client::CacheableString::create(pin_value, static_cast<int32_t> (len));
          }
          else {
            cStr = (apache::geode::client::CacheableString*)
              apache::geode::client::CacheableString::createDeserializable();
          }

          return cStr;
        }

       apache::geode::client::CacheableStringPtr Serializable::GetCacheableString2(String^ value)
       {
          apache::geode::client::CacheableStringPtr cStr;
          size_t len = 0;
          if (value != nullptr) {
            len = value->Length;
            const char* chars = 
              (const char*)(Marshal::StringToHGlobalAnsi(value)).ToPointer();
            cStr = apache::geode::client::CacheableString::create(chars, static_cast<int32_t> (len));
            Marshal::FreeHGlobal(IntPtr((void*)chars));
          }
          else {
            cStr = (apache::geode::client::CacheableString*)
              apache::geode::client::CacheableString::createDeserializable();
          }

          return cStr;
        }

       /*
        static String^ GetString(apache::geode::client::CacheableStringPtr cStr)//apache::geode::client::CacheableString*
        {
          if (cStr == NULLPTR) {
            return nullptr;
          }
          else if (cStr->isWideString()) {
            return ManagedString::Get(cStr->asWChar());
          }
          else {
            return ManagedString::Get(cStr->asChar());
          }
        }
        */

        array<Byte>^ Serializable::getSByteArray(array<SByte>^ sArray)
        {
          array<Byte>^ dArray = gcnew array<Byte>(sArray->Length);
          for (int index = 0; index < dArray->Length; index++) 
          {
            dArray[index] = sArray[index];
          }          
          return dArray;
        }

        array<int16_t>^ Serializable::getInt16Array(array<uint16_t>^ sArray)
        {
          array<int16_t>^ dArray = gcnew array<int16_t>(sArray->Length);
          for (int index = 0; index < dArray->Length; index++) 
          {
            dArray[index] = sArray[index];
          }          
          return dArray;
        }

        array<int32_t>^ Serializable::getInt32Array(array<uint32_t>^ sArray)
        {
          array<int32_t>^ dArray = gcnew array<int32_t>(sArray->Length);
          for (int index = 0; index < dArray->Length; index++) 
          {
            dArray[index] = sArray[index];
          }          
          return dArray;
        }

        array<int64_t>^ Serializable::getInt64Array(array<uint64_t>^ sArray)
        {
          array<int64_t>^ dArray = gcnew array<int64_t>(sArray->Length);
          for (int index = 0; index < dArray->Length; index++) 
          {
            dArray[index] = sArray[index];
          }          
          return dArray;
        }

      } // end namespace Generic
    }
  }
}

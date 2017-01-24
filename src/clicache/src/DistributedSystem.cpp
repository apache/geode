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

//#include "gf_includes.hpp"
#include "version.h"
#include "Serializable.hpp"
#include "DistributedSystem.hpp"
#include "SystemProperties.hpp"
#include "CacheFactory.hpp"
#include "CacheableDate.hpp"
#include "CacheableFileName.hpp"
#include "CacheableHashMap.hpp"
#include "CacheableHashSet.hpp"
#include "CacheableHashTable.hpp"
#include "CacheableIdentityHashMap.hpp"
#include "CacheableObjectArray.hpp"
#include "CacheableString.hpp"
#include "CacheableStringArray.hpp"
#include "CacheableUndefined.hpp"
#include "CacheableVector.hpp"
#include "CacheableArrayList.hpp"
#include "CacheableStack.hpp"
#include "CacheableObject.hpp"
#include "CacheableObjectXml.hpp"
#include "CacheableBuiltins.hpp"
#include "Log.hpp"
#include "Struct.hpp"
#include "impl/MemoryPressureHandler.hpp"
#include <gfcpp/CacheLoader.hpp>
#include <gfcpp/CacheListener.hpp>
#include <gfcpp/FixedPartitionResolver.hpp>
#include <gfcpp/CacheWriter.hpp>
#include <gfcpp/GeodeTypeIds.hpp>
#include <CacheImpl.hpp>
#include <PooledBasePool.hpp>
#include <CacheXmlParser.hpp>
#include "impl/SafeConvert.hpp"
#include <DistributedSystemImpl.hpp>
#include "impl/PdxType.hpp"
#include "impl/EnumInfo.hpp"
#include "impl/ManagedPersistenceManager.hpp"
#include "impl/AppDomainContext.hpp"

// disable spurious warning
#pragma warning(disable:4091)
#include <msclr/lock.h>
#pragma warning(default:4091)
#include <ace/Process.h> // Added to get rid of unresolved token warning


using namespace System;

using namespace Apache::Geode::Client;

namespace apache
{
  namespace geode
  {
    namespace client
    {
      class ManagedCacheLoaderGeneric
        : public CacheLoader
      {
      public:

        static CacheLoader* create(const char* assemblyPath,
          const char* factoryFunctionName);
      };

      class ManagedCacheListenerGeneric
        : public CacheListener
      {
      public:

        static CacheListener* create(const char* assemblyPath,
          const char* factoryFunctionName);
      };

      class ManagedFixedPartitionResolverGeneric
        : public FixedPartitionResolver
      {
      public:

        static PartitionResolver* create(const char* assemblyPath,
          const char* factoryFunctionName);
      };

      class ManagedCacheWriterGeneric
        : public CacheWriter
      {
      public:

        static CacheWriter* create(const char* assemblyPath,
          const char* factoryFunctionName);
      };

      class ManagedAuthInitializeGeneric
        : public AuthInitialize
      {
      public:

        static AuthInitialize* create(const char* assemblyPath,
          const char* factoryFunctionName);
      };
    }  // namespace client
  }  // namespace geode
}  // namespace apache


namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      DistributedSystem^ DistributedSystem::Connect(String^ name)
      {
        return DistributedSystem::Connect(name, nullptr);
      }

      DistributedSystem^ DistributedSystem::Connect(String^ name, Properties<String^, String^>^ config)
      {
        apache::geode::client::DistributedSystemImpl::acquireDisconnectLock();

        _GF_MG_EXCEPTION_TRY2

          ManagedString mg_name(name);

          apache::geode::client::PropertiesPtr nativepropsptr(
            GetNativePtr<apache::geode::client::Properties>(config));
          
         // apache::geode::client::PropertiesPtr nativepropsptr;
          DistributedSystem::AppDomainInstanceInitialization(nativepropsptr);

          // this we are calling after all .NET initialization required in
          // each AppDomain
          apache::geode::client::DistributedSystemPtr& nativeptr(
            apache::geode::client::DistributedSystem::connect(mg_name.CharPtr,
            nativepropsptr));

          ManagedPostConnect();

//          DistributedSystem::AppDomainInstancePostInitialization();

          return Create(nativeptr.ptr());

        _GF_MG_EXCEPTION_CATCH_ALL2

        finally {
          apache::geode::client::DistributedSystemImpl::releaseDisconnectLock();
        }
      }

      void DistributedSystem::Disconnect()
      {
        apache::geode::client::DistributedSystemImpl::acquireDisconnectLock();

        _GF_MG_EXCEPTION_TRY2

          if (apache::geode::client::DistributedSystem::isConnected()) {
           // apache::geode::client::CacheImpl::expiryTaskManager->cancelTask(
             // s_memoryPressureTaskID);
            Serializable::UnregisterNativesGeneric();
            DistributedSystem::UnregisterBuiltinManagedTypes();
          }
          apache::geode::client::DistributedSystem::disconnect();
      
        _GF_MG_EXCEPTION_CATCH_ALL2

        finally {
          apache::geode::client::DistributedSystemImpl::releaseDisconnectLock();
        }
      }


    /*  DistributedSystem^ DistributedSystem::ConnectOrGetInstance(String^ name)
      {
        return DistributedSystem::ConnectOrGetInstance(name, nullptr);
      }

      DistributedSystem^ DistributedSystem::ConnectOrGetInstance(String^ name,
          Properties^ config)
      {
        apache::geode::client::DistributedSystemImpl::acquireDisconnectLock();

        _GF_MG_EXCEPTION_TRY

          ManagedString mg_name(name);
          apache::geode::client::PropertiesPtr nativepropsptr(
            GetNativePtr<apache::geode::client::Properties>(config));
          DistributedSystem::AppDomainInstanceInitialization(nativepropsptr);

          // this we are calling after all .NET initialization required in
          // each AppDomain
          apache::geode::client::DistributedSystemPtr& nativeptr(
            apache::geode::client::DistributedSystem::connectOrGetInstance(mg_name.CharPtr,
            nativepropsptr));

          if (apache::geode::client::DistributedSystem::currentInstances() == 1) {
            // stuff to be done only for the first connect
            ManagedPostConnect();
          }

          DistributedSystem::AppDomainInstancePostInitialization();

          return Create(nativeptr.ptr());
          
        _GF_MG_EXCEPTION_CATCH_ALL

        finally {
          apache::geode::client::DistributedSystemImpl::releaseDisconnectLock();
        }
      }
*/
   /*   int DistributedSystem::DisconnectInstance()
      {
        apache::geode::client::DistributedSystemImpl::acquireDisconnectLock();

        _GF_MG_EXCEPTION_TRY

          int remainingInstances =
            apache::geode::client::DistributedSystem::currentInstances();
          if (remainingInstances <= 0) {
            throw gcnew NotConnectedException("DistributedSystem."
              "DisconnectInstance: no remaining instance connections");
          }

          //apache::geode::client::CacheImpl::expiryTaskManager->cancelTask(
            //s_memoryPressureTaskID);
          Serializable::UnregisterNatives();

          if (remainingInstances == 1) { // last instance
            DistributedSystem::UnregisterBuiltinManagedTypes();
          }
          return apache::geode::client::DistributedSystem::disconnectInstance();

        _GF_MG_EXCEPTION_CATCH_ALL

        finally {
          apache::geode::client::DistributedSystemImpl::releaseDisconnectLock();
        }
      }
*/

      void DistributedSystem::AppDomainInstanceInitialization(
        const apache::geode::client::PropertiesPtr& nativepropsptr)
      {
        _GF_MG_EXCEPTION_TRY2
          
          // Register wrapper types for built-in types, this are still cpp wrapper
           
          /*
            Serializable::RegisterWrapperGeneric(
              gcnew WrapperDelegateGeneric(Apache::Geode::Client::CacheableHashSet::Create),
              apache::geode::client::GemfireTypeIds::CacheableHashSet);
            
             Serializable::RegisterWrapperGeneric(
              gcnew WrapperDelegateGeneric(Apache::Geode::Client::CacheableLinkedHashSet::Create),
              apache::geode::client::GemfireTypeIds::CacheableLinkedHashSet);

             Serializable::RegisterWrapperGeneric(
              gcnew WrapperDelegateGeneric(Apache::Geode::Client::Struct::Create),
              apache::geode::client::GemfireTypeIds::Struct);
            
             Serializable::RegisterWrapperGeneric(
              gcnew WrapperDelegateGeneric(Apache::Geode::Client::Properties::CreateDeserializable),
              apache::geode::client::GemfireTypeIds::Properties);

          // End register wrapper types for built-in types

  // Register with cpp using unmanaged Cacheablekey wrapper
            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableByte,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableByte::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableBoolean,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableBoolean::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableBytes,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableBytes::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::BooleanArray, 
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::BooleanArray::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableWideChar,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableCharacter::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CharArray,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CharArray::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableDouble,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableDouble::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableDoubleArray,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableDoubleArray::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableFloat,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableFloat::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableFloatArray,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableFloatArray::CreateDeserializable));

           
            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableHashSet,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableHashSet::CreateDeserializable));

           Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableLinkedHashSet,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableLinkedHashSet::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableInt16,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableInt16::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableInt16Array,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableInt16Array::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableInt32,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableInt32::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableInt32Array,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableInt32Array::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableInt64,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableInt64::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableInt64Array,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableInt64Array::CreateDeserializable));
              */

            /*Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableASCIIString,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableString::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableASCIIStringHuge,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableString::createDeserializableHuge));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableString,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableString::createUTFDeserializable));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableStringHuge,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableString::createUTFDeserializableHuge));*/

            /*
            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableNullString,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableString::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableStringArray,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableStringArray::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::Struct,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::Struct::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::Properties,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::Properties::CreateDeserializable));
            */
            
            // End register other built-in types

            //primitive types are still C++, as these are used as keys mostly
          // Register generic wrapper types for built-in types
          //byte

         /* Serializable::RegisterWrapperGeneric(
            gcnew WrapperDelegateGeneric(CacheableByte::Create),
            apache::geode::client::GemfireTypeIds::CacheableByte, Byte::typeid);*/

          Serializable::RegisterWrapperGeneric(
            gcnew WrapperDelegateGeneric(CacheableByte::Create),
            apache::geode::client::GemfireTypeIds::CacheableByte, SByte::typeid);
          
          //boolean
          Serializable::RegisterWrapperGeneric(
            gcnew WrapperDelegateGeneric(CacheableBoolean::Create),
            apache::geode::client::GemfireTypeIds::CacheableBoolean, Boolean::typeid);
          //wide char
          Serializable::RegisterWrapperGeneric(
            gcnew WrapperDelegateGeneric(CacheableCharacter::Create),
            apache::geode::client::GemfireTypeIds::CacheableWideChar, Char::typeid);
          //double
          Serializable::RegisterWrapperGeneric(
            gcnew WrapperDelegateGeneric(CacheableDouble::Create),
            apache::geode::client::GemfireTypeIds::CacheableDouble, Double::typeid);
          //ascii string
          Serializable::RegisterWrapperGeneric(
            gcnew WrapperDelegateGeneric(CacheableString::Create),
            apache::geode::client::GemfireTypeIds::CacheableASCIIString, String::typeid);
            
          //TODO:
          ////ascii string huge
          //Serializable::RegisterWrapperGeneric(
          //  gcnew WrapperDelegateGeneric(CacheableString::Create),
          //  apache::geode::client::GemfireTypeIds::CacheableASCIIStringHuge, String::typeid);
          ////string
          //Serializable::RegisterWrapperGeneric(
          //  gcnew WrapperDelegateGeneric(CacheableString::Create),
          //  apache::geode::client::GemfireTypeIds::CacheableString, String::typeid);
          ////string huge
          //Serializable::RegisterWrapperGeneric(
          //  gcnew WrapperDelegateGeneric(CacheableString::Create),
          //  apache::geode::client::GemfireTypeIds::CacheableStringHuge, String::typeid);
          //float

          Serializable::RegisterWrapperGeneric(
            gcnew WrapperDelegateGeneric(CacheableFloat::Create),
            apache::geode::client::GemfireTypeIds::CacheableFloat, float::typeid);
          //int 16
          Serializable::RegisterWrapperGeneric(
            gcnew WrapperDelegateGeneric(CacheableInt16::Create),
            apache::geode::client::GemfireTypeIds::CacheableInt16, Int16::typeid);
          //int32
          Serializable::RegisterWrapperGeneric(
            gcnew WrapperDelegateGeneric(CacheableInt32::Create),
            apache::geode::client::GemfireTypeIds::CacheableInt32, Int32::typeid);
          //int64
          Serializable::RegisterWrapperGeneric(
            gcnew WrapperDelegateGeneric(CacheableInt64::Create),
            apache::geode::client::GemfireTypeIds::CacheableInt64, Int64::typeid);

          ////uint16
          //Serializable::RegisterWrapperGeneric(
          //  gcnew WrapperDelegateGeneric(CacheableInt16::Create),
          //  apache::geode::client::GemfireTypeIds::CacheableInt16, UInt16::typeid);
          ////uint32
          //Serializable::RegisterWrapperGeneric(
          //  gcnew WrapperDelegateGeneric(CacheableInt32::Create),
          //  apache::geode::client::GemfireTypeIds::CacheableInt32, UInt32::typeid);
          ////uint64
          //Serializable::RegisterWrapperGeneric(
          //  gcnew WrapperDelegateGeneric(CacheableInt64::Create),
          //  apache::geode::client::GemfireTypeIds::CacheableInt64, UInt64::typeid);
          //=======================================================================

            //Now onwards all will be wrap in managed cacheable key..

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableBytes,
              gcnew TypeFactoryMethodGeneric(CacheableBytes::CreateDeserializable), 
              Type::GetType("System.Byte[]"));

           /* Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableBytes,
              gcnew TypeFactoryMethodGeneric(CacheableBytes::CreateDeserializable), 
              Type::GetType("System.SByte[]"));*/

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableDoubleArray,
              gcnew TypeFactoryMethodGeneric(CacheableDoubleArray::CreateDeserializable),
              Type::GetType("System.Double[]"));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableFloatArray,
              gcnew TypeFactoryMethodGeneric(CacheableFloatArray::CreateDeserializable),
              Type::GetType("System.Single[]"));

           //TODO:
            //as it is
            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableHashSet,
              gcnew TypeFactoryMethodGeneric(CacheableHashSet::CreateDeserializable),
              nullptr);

            //as it is
           Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableLinkedHashSet,
              gcnew TypeFactoryMethodGeneric(CacheableLinkedHashSet::CreateDeserializable),
              nullptr);


            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableInt16Array,
              gcnew TypeFactoryMethodGeneric(CacheableInt16Array::CreateDeserializable),
              Type::GetType("System.Int16[]"));

          /*  Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableInt16Array,
              gcnew TypeFactoryMethodGeneric(CacheableInt16Array::CreateDeserializable),
              Type::GetType("System.UInt16[]"));*/


            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableInt32Array,
              gcnew TypeFactoryMethodGeneric(CacheableInt32Array::CreateDeserializable),
              Type::GetType("System.Int32[]"));

           /* Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableInt32Array,
              gcnew TypeFactoryMethodGeneric(CacheableInt32Array::CreateDeserializable),
              Type::GetType("System.UInt32[]"));*/


            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableInt64Array,
              gcnew TypeFactoryMethodGeneric(CacheableInt64Array::CreateDeserializable),
              Type::GetType("System.Int64[]"));

           /* Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableInt64Array,
              gcnew TypeFactoryMethodGeneric(CacheableInt64Array::CreateDeserializable),
              Type::GetType("System.UInt64[]"));*/
						//TODO:;split

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::BooleanArray,
              gcnew TypeFactoryMethodGeneric(BooleanArray::CreateDeserializable),
              Type::GetType("System.Boolean[]"));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CharArray,
              gcnew TypeFactoryMethodGeneric(CharArray::CreateDeserializable),
              Type::GetType("System.Char[]"));

            //TODO::

            //Serializable::RegisterTypeGeneric(
            //  apache::geode::client::GemfireTypeIds::CacheableNullString,
            //  gcnew TypeFactoryMethodNew(Apache::Geode::Client::CacheableString::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableStringArray,
              gcnew TypeFactoryMethodGeneric(CacheableStringArray::CreateDeserializable),
              Type::GetType("System.String[]"));

            //as it is
            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::Struct,
              gcnew TypeFactoryMethodGeneric(Struct::CreateDeserializable),
              nullptr);

            //as it is
            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::Properties,
              gcnew TypeFactoryMethodGeneric(Properties<String^, String^>::CreateDeserializable),
              nullptr);
              
          /*  Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::PdxType,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::Internal::PdxType::CreateDeserializable),
              nullptr);*/

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::EnumInfo,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::Internal::EnumInfo::CreateDeserializable),
              nullptr);
          
          // End register generic wrapper types for built-in types

          if (!apache::geode::client::DistributedSystem::isConnected()) 
          {
            // Set the Generic ManagedAuthInitialize factory function
            apache::geode::client::SystemProperties::managedAuthInitializeFn =
              apache::geode::client::ManagedAuthInitializeGeneric::create;

            // Set the Generic ManagedCacheLoader/Listener/Writer factory functions.
            apache::geode::client::CacheXmlParser::managedCacheLoaderFn =
              apache::geode::client::ManagedCacheLoaderGeneric::create;
            apache::geode::client::CacheXmlParser::managedCacheListenerFn =
              apache::geode::client::ManagedCacheListenerGeneric::create;
            apache::geode::client::CacheXmlParser::managedCacheWriterFn =
              apache::geode::client::ManagedCacheWriterGeneric::create;

            // Set the Generic ManagedPartitionResolver factory function
            apache::geode::client::CacheXmlParser::managedPartitionResolverFn =
              apache::geode::client::ManagedFixedPartitionResolverGeneric::create;

            // Set the Generic ManagedPersistanceManager factory function
            apache::geode::client::CacheXmlParser::managedPersistenceManagerFn =
              apache::geode::client::ManagedPersistenceManagerGeneric::create;
          }

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      void DistributedSystem::ManagedPostConnect()
      {
        //  The registration into the native map should be after
        // native connect since it can be invoked only once

        // Register other built-in types
        /*
        Serializable::RegisterTypeGeneric(
          apache::geode::client::GemfireTypeIds::CacheableDate,
          gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableDate::CreateDeserializable));
        Serializable::RegisterTypeGeneric(
          apache::geode::client::GemfireTypeIds::CacheableFileName,
          gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableFileName::CreateDeserializable));
        Serializable::RegisterTypeGeneric(
          apache::geode::client::GemfireTypeIds::CacheableHashMap,
          gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableHashMap::CreateDeserializable));
        Serializable::RegisterTypeGeneric(
          apache::geode::client::GemfireTypeIds::CacheableHashTable,
          gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableHashTable::CreateDeserializable));
        Serializable::RegisterTypeGeneric(
          apache::geode::client::GemfireTypeIds::CacheableIdentityHashMap,
          gcnew TypeFactoryMethodGeneric(
          Apache::Geode::Client::CacheableIdentityHashMap::CreateDeserializable));
        Serializable::RegisterTypeGeneric(
          apache::geode::client::GemfireTypeIds::CacheableUndefined,
          gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableUndefined::CreateDeserializable));
        Serializable::RegisterTypeGeneric(
          apache::geode::client::GemfireTypeIds::CacheableVector,
          gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableVector::CreateDeserializable));
        Serializable::RegisterTypeGeneric(
          apache::geode::client::GemfireTypeIds::CacheableObjectArray,
          gcnew TypeFactoryMethodGeneric(
          Apache::Geode::Client::CacheableObjectArray::CreateDeserializable));
        Serializable::RegisterTypeGeneric(
          apache::geode::client::GemfireTypeIds::CacheableArrayList,
          gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableArrayList::CreateDeserializable));
        Serializable::RegisterTypeGeneric(
          apache::geode::client::GemfireTypeIds::CacheableStack,
          gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableStack::CreateDeserializable));
        Serializable::RegisterTypeGeneric(
          GemFireClassIds::CacheableManagedObject - 0x80000000,
          gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableObject::CreateDeserializable));
        Serializable::RegisterTypeGeneric(
          GemFireClassIds::CacheableManagedObjectXml - 0x80000000,
          gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::CacheableObjectXml::CreateDeserializable));
          */
        // End register other built-in types

        // Register other built-in types for generics
        //c# datatime
        
        Serializable::RegisterTypeGeneric(
          apache::geode::client::GemfireTypeIds::CacheableDate,
          gcnew TypeFactoryMethodGeneric(CacheableDate::CreateDeserializable),
          Type::GetType("System.DateTime"));
        
        //as it is
        Serializable::RegisterTypeGeneric(
          apache::geode::client::GemfireTypeIds::CacheableFileName,
          gcnew TypeFactoryMethodGeneric(CacheableFileName::CreateDeserializable),
          nullptr);
        
        //for generic dictionary define its type in static constructor of Serializable.hpp
        Serializable::RegisterTypeGeneric(
          apache::geode::client::GemfireTypeIds::CacheableHashMap,
          gcnew TypeFactoryMethodGeneric(CacheableHashMap::CreateDeserializable),
          nullptr);

        //c# hashtable
        Serializable::RegisterTypeGeneric(
          apache::geode::client::GemfireTypeIds::CacheableHashTable,
          gcnew TypeFactoryMethodGeneric(CacheableHashTable::CreateDeserializable),
          Type::GetType("System.Collections.Hashtable"));

        //Need to keep public as no counterpart in c#
        Serializable::RegisterTypeGeneric(
          apache::geode::client::GemfireTypeIds::CacheableIdentityHashMap,
          gcnew TypeFactoryMethodGeneric(
          CacheableIdentityHashMap::CreateDeserializable),
          nullptr);
        
        //keep as it is
        Serializable::RegisterTypeGeneric(
          apache::geode::client::GemfireTypeIds::CacheableUndefined,
          gcnew TypeFactoryMethodGeneric(CacheableUndefined::CreateDeserializable),
          nullptr);

        //c# arraylist
        Serializable::RegisterTypeGeneric(
          apache::geode::client::GemfireTypeIds::CacheableVector,
          gcnew TypeFactoryMethodGeneric(CacheableVector::CreateDeserializable),
          nullptr);

        //as it is
        Serializable::RegisterTypeGeneric(
          apache::geode::client::GemfireTypeIds::CacheableObjectArray,
          gcnew TypeFactoryMethodGeneric(
          CacheableObjectArray::CreateDeserializable),
          nullptr);

        //Generic::List
        Serializable::RegisterTypeGeneric(
          apache::geode::client::GemfireTypeIds::CacheableArrayList,
          gcnew TypeFactoryMethodGeneric(CacheableArrayList::CreateDeserializable),
          nullptr);
        
        //c# generic stack 
        Serializable::RegisterTypeGeneric(
          apache::geode::client::GemfireTypeIds::CacheableStack,
          gcnew TypeFactoryMethodGeneric(CacheableStack::CreateDeserializable),
          nullptr);

        //as it is
        Serializable::RegisterTypeGeneric(
          GemFireClassIds::CacheableManagedObject - 0x80000000,
          gcnew TypeFactoryMethodGeneric(CacheableObject::CreateDeserializable),
          nullptr);

        //as it is
        Serializable::RegisterTypeGeneric(
          GemFireClassIds::CacheableManagedObjectXml - 0x80000000,
          gcnew TypeFactoryMethodGeneric(CacheableObjectXml::CreateDeserializable),
          nullptr);
        
        // End register other built-in types

        // TODO: what will happen for following if first appDomain unload ??
        // Testing shows that there are problems; need to discuss -- maybe
        // maintain per AppDomainID functions in C++ layer.

        // Log the version of the C# layer being used
       Log::Config(".NET layer assembly version: {0}({1})", System::Reflection::
          Assembly::GetExecutingAssembly()->GetName()->Version->ToString(), 
          System::Reflection::Assembly::GetExecutingAssembly()->ImageRuntimeVersion);
        
        Log::Config(".NET runtime version: {0} ",System::Environment::Version);
        Log::Config(".NET layer source repository (revision): {0} ({1})",
          PRODUCT_SOURCE_REPOSITORY, PRODUCT_SOURCE_REVISION);
      }

      void DistributedSystem::AppDomainInstancePostInitialization()
      {
        //to create .net memory pressure handler 
        Create(apache::geode::client::DistributedSystem::getInstance().ptr());

        // Register managed AppDomain context with unmanaged.
        apache::geode::client::createAppDomainContext = &Apache::Geode::Client::createAppDomainContext;
      }

      void DistributedSystem::UnregisterBuiltinManagedTypes()
      {
         _GF_MG_EXCEPTION_TRY2

         apache::geode::client::DistributedSystemImpl::acquireDisconnectLock();

         Serializable::UnregisterNativesGeneric();
         
         int remainingInstances =
           apache::geode::client::DistributedSystemImpl::currentInstances();

          if (remainingInstances == 0) { // last instance
           
            
            Serializable::UnregisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableDate);
            Serializable::UnregisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableFileName);
            Serializable::UnregisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableHashMap);
            Serializable::UnregisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableHashTable);
            Serializable::UnregisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableIdentityHashMap);
            Serializable::UnregisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableVector);
            Serializable::UnregisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableObjectArray);
            Serializable::UnregisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableArrayList);
            Serializable::UnregisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::CacheableStack);
            Serializable::UnregisterTypeGeneric(
              GemFireClassIds::CacheableManagedObject - 0x80000000);
            Serializable::UnregisterTypeGeneric(
              GemFireClassIds::CacheableManagedObjectXml - 0x80000000);            
              
          }

           _GF_MG_EXCEPTION_CATCH_ALL2

        finally {
          apache::geode::client::DistributedSystemImpl::releaseDisconnectLock();
        }
      }

      Apache::Geode::Client::SystemProperties^ DistributedSystem::SystemProperties::get()
      {
        _GF_MG_EXCEPTION_TRY2

          //  TODO
					return Apache::Geode::Client::SystemProperties::Create(
            apache::geode::client::DistributedSystem::getSystemProperties());
            
         //return nullptr;

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      String^ DistributedSystem::Name::get()
      {
        return ManagedString::Get(NativePtr->getName());
      }

      bool DistributedSystem::IsConnected::get()
      {
        return apache::geode::client::DistributedSystem::isConnected();
      }

      DistributedSystem^ DistributedSystem::GetInstance()
      {
        apache::geode::client::DistributedSystemPtr& nativeptr(
          apache::geode::client::DistributedSystem::getInstance());
        return Create(nativeptr.ptr());
      }

      void DistributedSystem::HandleMemoryPressure(System::Object^ state)
      {
        ACE_Time_Value dummy(1);
        MemoryPressureHandler handler;
        handler.handle_timeout(dummy, nullptr);
      }

      DistributedSystem^ DistributedSystem::Create(
        apache::geode::client::DistributedSystem* nativeptr)
      {
        if (m_instance == nullptr) {
          msclr::lock lockInstance(m_singletonSync);
          if (m_instance == nullptr) {
            m_instance = (nativeptr != nullptr
                ? gcnew DistributedSystem(nativeptr) : nullptr);
          }
        }
        DistributedSystem^ instance = (DistributedSystem^)m_instance;
        return instance;
      }

      DistributedSystem::DistributedSystem(apache::geode::client::DistributedSystem* nativeptr)
        : SBWrap(nativeptr) 
      {
        System::Threading::TimerCallback^ timerCallback = gcnew System::
          Threading::TimerCallback(&DistributedSystem::HandleMemoryPressure);
        m_memoryPressureHandler = gcnew System::Threading::Timer(
          timerCallback, "MemoryPressureHandler", 3*60000, 3*60000);
      }

      DistributedSystem::~DistributedSystem()
      {
        m_memoryPressureHandler->Dispose(nullptr);
      }

      void DistributedSystem::acquireDisconnectLock()
      {
        apache::geode::client::DistributedSystemImpl::acquireDisconnectLock();
      }

      void DistributedSystem::disconnectInstance()
      {
        apache::geode::client::DistributedSystemImpl::disconnectInstance();
      }

      void DistributedSystem::releaseDisconnectLock()
      {
        apache::geode::client::DistributedSystemImpl::releaseDisconnectLock();
      }

      void DistributedSystem::connectInstance()
      {
        apache::geode::client::DistributedSystemImpl::connectInstance();
      }

      void DistributedSystem::registerCliCallback()
      {
        m_cliCallBackObj = gcnew CliCallbackDelegate();
        cliCallback^ nativeCallback =
          gcnew cliCallback(m_cliCallBackObj,
          &CliCallbackDelegate::Callback);

        apache::geode::client::DistributedSystemImpl::registerCliCallback( System::Threading::Thread::GetDomainID(),
              (apache::geode::client::CliCallbackMethod)System::Runtime::InteropServices::
							Marshal::GetFunctionPointerForDelegate(
							nativeCallback).ToPointer());
      }

      void DistributedSystem::unregisterCliCallback()
      {
        apache::geode::client::DistributedSystemImpl::unregisterCliCallback( System::Threading::Thread::GetDomainID());
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

}
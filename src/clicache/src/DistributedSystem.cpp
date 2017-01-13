/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
#include <gfcpp/GemfireTypeIds.hpp>
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

using namespace GemStone::GemFire::Cache;

namespace gemfire
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
}


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {
      DistributedSystem^ DistributedSystem::Connect(String^ name)
      {
        return DistributedSystem::Connect(name, nullptr);
      }

      DistributedSystem^ DistributedSystem::Connect(String^ name, Properties<String^, String^>^ config)
      {
        gemfire::DistributedSystemImpl::acquireDisconnectLock();

        _GF_MG_EXCEPTION_TRY2

          ManagedString mg_name(name);

          gemfire::PropertiesPtr nativepropsptr(
            GetNativePtr<gemfire::Properties>(config));
          
         // gemfire::PropertiesPtr nativepropsptr;
          DistributedSystem::AppDomainInstanceInitialization(nativepropsptr);

          // this we are calling after all .NET initialization required in
          // each AppDomain
          gemfire::DistributedSystemPtr& nativeptr(
            gemfire::DistributedSystem::connect(mg_name.CharPtr,
            nativepropsptr));

          ManagedPostConnect();

//          DistributedSystem::AppDomainInstancePostInitialization();

          return Create(nativeptr.ptr());

        _GF_MG_EXCEPTION_CATCH_ALL2

        finally {
          gemfire::DistributedSystemImpl::releaseDisconnectLock();
        }
      }

      void DistributedSystem::Disconnect()
      {
        gemfire::DistributedSystemImpl::acquireDisconnectLock();

        _GF_MG_EXCEPTION_TRY2

          if (gemfire::DistributedSystem::isConnected()) {
           // gemfire::CacheImpl::expiryTaskManager->cancelTask(
             // s_memoryPressureTaskID);
            Serializable::UnregisterNativesGeneric();
            DistributedSystem::UnregisterBuiltinManagedTypes();
          }
          gemfire::DistributedSystem::disconnect();
      
        _GF_MG_EXCEPTION_CATCH_ALL2

        finally {
          gemfire::DistributedSystemImpl::releaseDisconnectLock();
        }
      }


    /*  DistributedSystem^ DistributedSystem::ConnectOrGetInstance(String^ name)
      {
        return DistributedSystem::ConnectOrGetInstance(name, nullptr);
      }

      DistributedSystem^ DistributedSystem::ConnectOrGetInstance(String^ name,
          Properties^ config)
      {
        gemfire::DistributedSystemImpl::acquireDisconnectLock();

        _GF_MG_EXCEPTION_TRY

          ManagedString mg_name(name);
          gemfire::PropertiesPtr nativepropsptr(
            GetNativePtr<gemfire::Properties>(config));
          DistributedSystem::AppDomainInstanceInitialization(nativepropsptr);

          // this we are calling after all .NET initialization required in
          // each AppDomain
          gemfire::DistributedSystemPtr& nativeptr(
            gemfire::DistributedSystem::connectOrGetInstance(mg_name.CharPtr,
            nativepropsptr));

          if (gemfire::DistributedSystem::currentInstances() == 1) {
            // stuff to be done only for the first connect
            ManagedPostConnect();
          }

          DistributedSystem::AppDomainInstancePostInitialization();

          return Create(nativeptr.ptr());
          
        _GF_MG_EXCEPTION_CATCH_ALL

        finally {
          gemfire::DistributedSystemImpl::releaseDisconnectLock();
        }
      }
*/
   /*   int DistributedSystem::DisconnectInstance()
      {
        gemfire::DistributedSystemImpl::acquireDisconnectLock();

        _GF_MG_EXCEPTION_TRY

          int remainingInstances =
            gemfire::DistributedSystem::currentInstances();
          if (remainingInstances <= 0) {
            throw gcnew NotConnectedException("DistributedSystem."
              "DisconnectInstance: no remaining instance connections");
          }

          //gemfire::CacheImpl::expiryTaskManager->cancelTask(
            //s_memoryPressureTaskID);
          Serializable::UnregisterNatives();

          if (remainingInstances == 1) { // last instance
            DistributedSystem::UnregisterBuiltinManagedTypes();
          }
          return gemfire::DistributedSystem::disconnectInstance();

        _GF_MG_EXCEPTION_CATCH_ALL

        finally {
          gemfire::DistributedSystemImpl::releaseDisconnectLock();
        }
      }
*/

      void DistributedSystem::AppDomainInstanceInitialization(
        const gemfire::PropertiesPtr& nativepropsptr)
      {
        _GF_MG_EXCEPTION_TRY2
          
          // Register wrapper types for built-in types, this are still cpp wrapper
           
          /*
            Serializable::RegisterWrapperGeneric(
              gcnew WrapperDelegateGeneric(GemStone::GemFire::Cache::CacheableHashSet::Create),
              gemfire::GemfireTypeIds::CacheableHashSet);
            
             Serializable::RegisterWrapperGeneric(
              gcnew WrapperDelegateGeneric(GemStone::GemFire::Cache::CacheableLinkedHashSet::Create),
              gemfire::GemfireTypeIds::CacheableLinkedHashSet);

             Serializable::RegisterWrapperGeneric(
              gcnew WrapperDelegateGeneric(GemStone::GemFire::Cache::Struct::Create),
              gemfire::GemfireTypeIds::Struct);
            
             Serializable::RegisterWrapperGeneric(
              gcnew WrapperDelegateGeneric(GemStone::GemFire::Cache::Properties::CreateDeserializable),
              gemfire::GemfireTypeIds::Properties);

          // End register wrapper types for built-in types

  // Register with cpp using unmanaged Cacheablekey wrapper
            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableByte,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableByte::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableBoolean,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableBoolean::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableBytes,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableBytes::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::BooleanArray, 
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::BooleanArray::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableWideChar,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableCharacter::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CharArray,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CharArray::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableDouble,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableDouble::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableDoubleArray,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableDoubleArray::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableFloat,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableFloat::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableFloatArray,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableFloatArray::CreateDeserializable));

           
            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableHashSet,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableHashSet::CreateDeserializable));

           Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableLinkedHashSet,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableLinkedHashSet::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableInt16,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableInt16::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableInt16Array,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableInt16Array::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableInt32,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableInt32::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableInt32Array,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableInt32Array::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableInt64,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableInt64::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableInt64Array,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableInt64Array::CreateDeserializable));
              */

            /*Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableASCIIString,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableString::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableASCIIStringHuge,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableString::createDeserializableHuge));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableString,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableString::createUTFDeserializable));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableStringHuge,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableString::createUTFDeserializableHuge));*/

            /*
            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableNullString,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableString::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableStringArray,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableStringArray::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::Struct,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::Struct::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::Properties,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::Properties::CreateDeserializable));
            */
            
            // End register other built-in types

            //primitive types are still C++, as these are used as keys mostly
          // Register generic wrapper types for built-in types
          //byte

         /* Serializable::RegisterWrapperGeneric(
            gcnew WrapperDelegateGeneric(CacheableByte::Create),
            gemfire::GemfireTypeIds::CacheableByte, Byte::typeid);*/

          Serializable::RegisterWrapperGeneric(
            gcnew WrapperDelegateGeneric(CacheableByte::Create),
            gemfire::GemfireTypeIds::CacheableByte, SByte::typeid);
          
          //boolean
          Serializable::RegisterWrapperGeneric(
            gcnew WrapperDelegateGeneric(CacheableBoolean::Create),
            gemfire::GemfireTypeIds::CacheableBoolean, Boolean::typeid);
          //wide char
          Serializable::RegisterWrapperGeneric(
            gcnew WrapperDelegateGeneric(CacheableCharacter::Create),
            gemfire::GemfireTypeIds::CacheableWideChar, Char::typeid);
          //double
          Serializable::RegisterWrapperGeneric(
            gcnew WrapperDelegateGeneric(CacheableDouble::Create),
            gemfire::GemfireTypeIds::CacheableDouble, Double::typeid);
          //ascii string
          Serializable::RegisterWrapperGeneric(
            gcnew WrapperDelegateGeneric(CacheableString::Create),
            gemfire::GemfireTypeIds::CacheableASCIIString, String::typeid);
            
          //TODO:
          ////ascii string huge
          //Serializable::RegisterWrapperGeneric(
          //  gcnew WrapperDelegateGeneric(CacheableString::Create),
          //  gemfire::GemfireTypeIds::CacheableASCIIStringHuge, String::typeid);
          ////string
          //Serializable::RegisterWrapperGeneric(
          //  gcnew WrapperDelegateGeneric(CacheableString::Create),
          //  gemfire::GemfireTypeIds::CacheableString, String::typeid);
          ////string huge
          //Serializable::RegisterWrapperGeneric(
          //  gcnew WrapperDelegateGeneric(CacheableString::Create),
          //  gemfire::GemfireTypeIds::CacheableStringHuge, String::typeid);
          //float

          Serializable::RegisterWrapperGeneric(
            gcnew WrapperDelegateGeneric(CacheableFloat::Create),
            gemfire::GemfireTypeIds::CacheableFloat, float::typeid);
          //int 16
          Serializable::RegisterWrapperGeneric(
            gcnew WrapperDelegateGeneric(CacheableInt16::Create),
            gemfire::GemfireTypeIds::CacheableInt16, Int16::typeid);
          //int32
          Serializable::RegisterWrapperGeneric(
            gcnew WrapperDelegateGeneric(CacheableInt32::Create),
            gemfire::GemfireTypeIds::CacheableInt32, Int32::typeid);
          //int64
          Serializable::RegisterWrapperGeneric(
            gcnew WrapperDelegateGeneric(CacheableInt64::Create),
            gemfire::GemfireTypeIds::CacheableInt64, Int64::typeid);

          ////uint16
          //Serializable::RegisterWrapperGeneric(
          //  gcnew WrapperDelegateGeneric(CacheableInt16::Create),
          //  gemfire::GemfireTypeIds::CacheableInt16, UInt16::typeid);
          ////uint32
          //Serializable::RegisterWrapperGeneric(
          //  gcnew WrapperDelegateGeneric(CacheableInt32::Create),
          //  gemfire::GemfireTypeIds::CacheableInt32, UInt32::typeid);
          ////uint64
          //Serializable::RegisterWrapperGeneric(
          //  gcnew WrapperDelegateGeneric(CacheableInt64::Create),
          //  gemfire::GemfireTypeIds::CacheableInt64, UInt64::typeid);
          //=======================================================================

            //Now onwards all will be wrap in managed cacheable key..

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableBytes,
              gcnew TypeFactoryMethodGeneric(CacheableBytes::CreateDeserializable), 
              Type::GetType("System.Byte[]"));

           /* Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableBytes,
              gcnew TypeFactoryMethodGeneric(CacheableBytes::CreateDeserializable), 
              Type::GetType("System.SByte[]"));*/

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableDoubleArray,
              gcnew TypeFactoryMethodGeneric(CacheableDoubleArray::CreateDeserializable),
              Type::GetType("System.Double[]"));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableFloatArray,
              gcnew TypeFactoryMethodGeneric(CacheableFloatArray::CreateDeserializable),
              Type::GetType("System.Single[]"));

           //TODO:
            //as it is
            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableHashSet,
              gcnew TypeFactoryMethodGeneric(CacheableHashSet::CreateDeserializable),
              nullptr);

            //as it is
           Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableLinkedHashSet,
              gcnew TypeFactoryMethodGeneric(CacheableLinkedHashSet::CreateDeserializable),
              nullptr);


            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableInt16Array,
              gcnew TypeFactoryMethodGeneric(CacheableInt16Array::CreateDeserializable),
              Type::GetType("System.Int16[]"));

          /*  Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableInt16Array,
              gcnew TypeFactoryMethodGeneric(CacheableInt16Array::CreateDeserializable),
              Type::GetType("System.UInt16[]"));*/


            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableInt32Array,
              gcnew TypeFactoryMethodGeneric(CacheableInt32Array::CreateDeserializable),
              Type::GetType("System.Int32[]"));

           /* Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableInt32Array,
              gcnew TypeFactoryMethodGeneric(CacheableInt32Array::CreateDeserializable),
              Type::GetType("System.UInt32[]"));*/


            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableInt64Array,
              gcnew TypeFactoryMethodGeneric(CacheableInt64Array::CreateDeserializable),
              Type::GetType("System.Int64[]"));

           /* Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableInt64Array,
              gcnew TypeFactoryMethodGeneric(CacheableInt64Array::CreateDeserializable),
              Type::GetType("System.UInt64[]"));*/
						//TODO:;split

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::BooleanArray,
              gcnew TypeFactoryMethodGeneric(BooleanArray::CreateDeserializable),
              Type::GetType("System.Boolean[]"));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CharArray,
              gcnew TypeFactoryMethodGeneric(CharArray::CreateDeserializable),
              Type::GetType("System.Char[]"));

            //TODO::

            //Serializable::RegisterTypeGeneric(
            //  gemfire::GemfireTypeIds::CacheableNullString,
            //  gcnew TypeFactoryMethodNew(GemStone::GemFire::Cache::CacheableString::CreateDeserializable));

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableStringArray,
              gcnew TypeFactoryMethodGeneric(CacheableStringArray::CreateDeserializable),
              Type::GetType("System.String[]"));

            //as it is
            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::Struct,
              gcnew TypeFactoryMethodGeneric(Struct::CreateDeserializable),
              nullptr);

            //as it is
            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::Properties,
              gcnew TypeFactoryMethodGeneric(Properties<String^, String^>::CreateDeserializable),
              nullptr);
              
          /*  Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::PdxType,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::Generic::Internal::PdxType::CreateDeserializable),
              nullptr);*/

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::EnumInfo,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::Generic::Internal::EnumInfo::CreateDeserializable),
              nullptr);
          
          // End register generic wrapper types for built-in types

          if (!gemfire::DistributedSystem::isConnected()) 
          {
            // Set the Generic ManagedAuthInitialize factory function
            gemfire::SystemProperties::managedAuthInitializeFn =
              gemfire::ManagedAuthInitializeGeneric::create;

            // Set the Generic ManagedCacheLoader/Listener/Writer factory functions.
            gemfire::CacheXmlParser::managedCacheLoaderFn =
              gemfire::ManagedCacheLoaderGeneric::create;
            gemfire::CacheXmlParser::managedCacheListenerFn =
              gemfire::ManagedCacheListenerGeneric::create;
            gemfire::CacheXmlParser::managedCacheWriterFn =
              gemfire::ManagedCacheWriterGeneric::create;

            // Set the Generic ManagedPartitionResolver factory function
            gemfire::CacheXmlParser::managedPartitionResolverFn =
              gemfire::ManagedFixedPartitionResolverGeneric::create;

            // Set the Generic ManagedPersistanceManager factory function
            gemfire::CacheXmlParser::managedPersistenceManagerFn =
              gemfire::ManagedPersistenceManagerGeneric::create;
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
          gemfire::GemfireTypeIds::CacheableDate,
          gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableDate::CreateDeserializable));
        Serializable::RegisterTypeGeneric(
          gemfire::GemfireTypeIds::CacheableFileName,
          gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableFileName::CreateDeserializable));
        Serializable::RegisterTypeGeneric(
          gemfire::GemfireTypeIds::CacheableHashMap,
          gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableHashMap::CreateDeserializable));
        Serializable::RegisterTypeGeneric(
          gemfire::GemfireTypeIds::CacheableHashTable,
          gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableHashTable::CreateDeserializable));
        Serializable::RegisterTypeGeneric(
          gemfire::GemfireTypeIds::CacheableIdentityHashMap,
          gcnew TypeFactoryMethodGeneric(
          GemStone::GemFire::Cache::CacheableIdentityHashMap::CreateDeserializable));
        Serializable::RegisterTypeGeneric(
          gemfire::GemfireTypeIds::CacheableUndefined,
          gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableUndefined::CreateDeserializable));
        Serializable::RegisterTypeGeneric(
          gemfire::GemfireTypeIds::CacheableVector,
          gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableVector::CreateDeserializable));
        Serializable::RegisterTypeGeneric(
          gemfire::GemfireTypeIds::CacheableObjectArray,
          gcnew TypeFactoryMethodGeneric(
          GemStone::GemFire::Cache::CacheableObjectArray::CreateDeserializable));
        Serializable::RegisterTypeGeneric(
          gemfire::GemfireTypeIds::CacheableArrayList,
          gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableArrayList::CreateDeserializable));
        Serializable::RegisterTypeGeneric(
          gemfire::GemfireTypeIds::CacheableStack,
          gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableStack::CreateDeserializable));
        Serializable::RegisterTypeGeneric(
          GemFireClassIds::CacheableManagedObject - 0x80000000,
          gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableObject::CreateDeserializable));
        Serializable::RegisterTypeGeneric(
          GemFireClassIds::CacheableManagedObjectXml - 0x80000000,
          gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::CacheableObjectXml::CreateDeserializable));
          */
        // End register other built-in types

        // Register other built-in types for generics
        //c# datatime
        
        Serializable::RegisterTypeGeneric(
          gemfire::GemfireTypeIds::CacheableDate,
          gcnew TypeFactoryMethodGeneric(CacheableDate::CreateDeserializable),
          Type::GetType("System.DateTime"));
        
        //as it is
        Serializable::RegisterTypeGeneric(
          gemfire::GemfireTypeIds::CacheableFileName,
          gcnew TypeFactoryMethodGeneric(CacheableFileName::CreateDeserializable),
          nullptr);
        
        //for generic dictionary define its type in static constructor of Serializable.hpp
        Serializable::RegisterTypeGeneric(
          gemfire::GemfireTypeIds::CacheableHashMap,
          gcnew TypeFactoryMethodGeneric(CacheableHashMap::CreateDeserializable),
          nullptr);

        //c# hashtable
        Serializable::RegisterTypeGeneric(
          gemfire::GemfireTypeIds::CacheableHashTable,
          gcnew TypeFactoryMethodGeneric(CacheableHashTable::CreateDeserializable),
          Type::GetType("System.Collections.Hashtable"));

        //Need to keep public as no counterpart in c#
        Serializable::RegisterTypeGeneric(
          gemfire::GemfireTypeIds::CacheableIdentityHashMap,
          gcnew TypeFactoryMethodGeneric(
          CacheableIdentityHashMap::CreateDeserializable),
          nullptr);
        
        //keep as it is
        Serializable::RegisterTypeGeneric(
          gemfire::GemfireTypeIds::CacheableUndefined,
          gcnew TypeFactoryMethodGeneric(CacheableUndefined::CreateDeserializable),
          nullptr);

        //c# arraylist
        Serializable::RegisterTypeGeneric(
          gemfire::GemfireTypeIds::CacheableVector,
          gcnew TypeFactoryMethodGeneric(CacheableVector::CreateDeserializable),
          nullptr);

        //as it is
        Serializable::RegisterTypeGeneric(
          gemfire::GemfireTypeIds::CacheableObjectArray,
          gcnew TypeFactoryMethodGeneric(
          CacheableObjectArray::CreateDeserializable),
          nullptr);

        //Generic::List
        Serializable::RegisterTypeGeneric(
          gemfire::GemfireTypeIds::CacheableArrayList,
          gcnew TypeFactoryMethodGeneric(CacheableArrayList::CreateDeserializable),
          nullptr);
        
        //c# generic stack 
        Serializable::RegisterTypeGeneric(
          gemfire::GemfireTypeIds::CacheableStack,
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
          GEMFIRE_SOURCE_REPOSITORY, GEMFIRE_SOURCE_REVISION);
      }

      void DistributedSystem::AppDomainInstancePostInitialization()
      {
        //to create .net memory pressure handler 
        Create(gemfire::DistributedSystem::getInstance().ptr());

        // Register managed AppDomain context with unmanaged.
        gemfire::createAppDomainContext = &GemStone::GemFire::Cache::Generic::createAppDomainContext;
      }

      void DistributedSystem::UnregisterBuiltinManagedTypes()
      {
         _GF_MG_EXCEPTION_TRY2

         gemfire::DistributedSystemImpl::acquireDisconnectLock();

         Serializable::UnregisterNativesGeneric();
         
         int remainingInstances =
           gemfire::DistributedSystemImpl::currentInstances();

          if (remainingInstances == 0) { // last instance
           
            
            Serializable::UnregisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableDate);
            Serializable::UnregisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableFileName);
            Serializable::UnregisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableHashMap);
            Serializable::UnregisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableHashTable);
            Serializable::UnregisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableIdentityHashMap);
            Serializable::UnregisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableVector);
            Serializable::UnregisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableObjectArray);
            Serializable::UnregisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableArrayList);
            Serializable::UnregisterTypeGeneric(
              gemfire::GemfireTypeIds::CacheableStack);
            Serializable::UnregisterTypeGeneric(
              GemFireClassIds::CacheableManagedObject - 0x80000000);
            Serializable::UnregisterTypeGeneric(
              GemFireClassIds::CacheableManagedObjectXml - 0x80000000);            
              
          }

           _GF_MG_EXCEPTION_CATCH_ALL2

        finally {
          gemfire::DistributedSystemImpl::releaseDisconnectLock();
        }
      }

      GemStone::GemFire::Cache::Generic::SystemProperties^ DistributedSystem::SystemProperties::get()
      {
        _GF_MG_EXCEPTION_TRY2

          //  TODO
					return GemStone::GemFire::Cache::Generic::SystemProperties::Create(
            gemfire::DistributedSystem::getSystemProperties());
            
         //return nullptr;

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      String^ DistributedSystem::Name::get()
      {
        return ManagedString::Get(NativePtr->getName());
      }

      bool DistributedSystem::IsConnected::get()
      {
        return gemfire::DistributedSystem::isConnected();
      }

      DistributedSystem^ DistributedSystem::GetInstance()
      {
        gemfire::DistributedSystemPtr& nativeptr(
          gemfire::DistributedSystem::getInstance());
        return Create(nativeptr.ptr());
      }

      void DistributedSystem::HandleMemoryPressure(System::Object^ state)
      {
        ACE_Time_Value dummy(1);
        MemoryPressureHandler handler;
        handler.handle_timeout(dummy, nullptr);
      }

      DistributedSystem^ DistributedSystem::Create(
        gemfire::DistributedSystem* nativeptr)
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

      DistributedSystem::DistributedSystem(gemfire::DistributedSystem* nativeptr)
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
        gemfire::DistributedSystemImpl::acquireDisconnectLock();
      }

      void DistributedSystem::disconnectInstance()
      {
        gemfire::DistributedSystemImpl::disconnectInstance();
      }

      void DistributedSystem::releaseDisconnectLock()
      {
        gemfire::DistributedSystemImpl::releaseDisconnectLock();
      }

      void DistributedSystem::connectInstance()
      {
        gemfire::DistributedSystemImpl::connectInstance();
      }

      void DistributedSystem::registerCliCallback()
      {
        m_cliCallBackObj = gcnew CliCallbackDelegate();
        cliCallback^ nativeCallback =
          gcnew cliCallback(m_cliCallBackObj,
          &CliCallbackDelegate::Callback);

        gemfire::DistributedSystemImpl::registerCliCallback( System::Threading::Thread::GetDomainID(),
              (gemfire::CliCallbackMethod)System::Runtime::InteropServices::
							Marshal::GetFunctionPointerForDelegate(
							nativeCallback).ToPointer());
      }

      void DistributedSystem::unregisterCliCallback()
      {
        gemfire::DistributedSystemImpl::unregisterCliCallback( System::Threading::Thread::GetDomainID());
      }
      } // end namespace generic
    }
  }
}

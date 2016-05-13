/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "version.h"
#include "DistributedSystemM.hpp"
#include "SystemPropertiesM.hpp"
#include "PropertiesM.hpp"
#include "CacheFactoryM.hpp"
#include "CacheableDateM.hpp"
#include "CacheableFileNameM.hpp"
#include "CacheableHashMapM.hpp"
#include "CacheableHashSetM.hpp"
#include "CacheableHashTableM.hpp"
#include "CacheableIdentityHashMapM.hpp"
#include "CacheableObjectArrayM.hpp"
#include "CacheableStringM.hpp"
#include "CacheableStringArrayM.hpp"
#include "CacheableUndefinedM.hpp"
#include "CacheableVectorM.hpp"
#include "CacheableArrayListM.hpp"
#include "CacheableStackM.hpp"
#include "CacheableObject.hpp"
#include "CacheableObjectXml.hpp"
#include "LogM.hpp"
#include "StructM.hpp"
#include "impl/MemoryPressureHandler.hpp"
#include "cppcache/CacheLoader.hpp"
#include "cppcache/CacheListener.hpp"
#include "cppcache/PartitionResolver.hpp"
#include "cppcache/CacheWriter.hpp"
#include "cppcache/GemfireTypeIds.hpp"
#include "cppcache/impl/CacheImpl.hpp"
#include "cppcache/impl/PooledBasePool.hpp"
#include "cppcache/impl/CacheXmlParser.hpp"
#include "impl/SafeConvert.hpp"
#include "cppcache/impl/DistributedSystemImpl.hpp"
#include "CacheableBuiltinsM.hpp"
// disable spurious warning
#pragma warning(disable:4091)
#include <msclr/lock.h>
#pragma warning(default:4091)
#include <ace/Process.h> // Added to get rid of unresolved token warning


using namespace System;

namespace gemfire
{

  class ManagedCacheLoader
    : public CacheLoader
  {
  public:

    static CacheLoader* create(const char* assemblyPath,
      const char* factoryFunctionName);
  };

  class ManagedCacheListener
    : public CacheListener
  {
  public:

    static CacheListener* create(const char* assemblyPath,
      const char* factoryFunctionName);
  };

  class ManagedPartitionResolver
    : public PartitionResolver
  {
  public:

    static PartitionResolver* create(const char* assemblyPath,
      const char* factoryFunctionName);
  };

  class ManagedCacheWriter
    : public CacheWriter
  {
  public:

    static CacheWriter* create(const char* assemblyPath,
      const char* factoryFunctionName);
  };

  class ManagedAuthInitialize
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
      DistributedSystem^ DistributedSystem::Connect(String^ name)
      {
        return DistributedSystem::Connect(name, nullptr);
      }

      DistributedSystem^ DistributedSystem::Connect(String^ name,
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
            gemfire::DistributedSystem::connect(mg_name.CharPtr,
            nativepropsptr));

          ManagedPostConnect();

//          DistributedSystem::AppDomainInstancePostInitialization();

          return Create(nativeptr.ptr());

        _GF_MG_EXCEPTION_CATCH_ALL

        finally {
          gemfire::DistributedSystemImpl::releaseDisconnectLock();
        }
      }

      void DistributedSystem::Disconnect()
      {
        gemfire::DistributedSystemImpl::acquireDisconnectLock();

        _GF_MG_EXCEPTION_TRY

          if (gemfire::DistributedSystem::isConnected()) {
           // gemfire::CacheImpl::expiryTaskManager->cancelTask(
             // s_memoryPressureTaskID);
            Serializable::UnregisterNatives();
            DistributedSystem::UnregisterBuiltinManagedTypes();
          }
          gemfire::DistributedSystem::disconnect();
      
        _GF_MG_EXCEPTION_CATCH_ALL

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
        _GF_MG_EXCEPTION_TRY

          // Register wrapper types for built-in types, this are still cpp wrapper
           
            Serializable::RegisterWrapper(
              gcnew WrapperDelegate(GemStone::GemFire::Cache::CacheableHashSet::Create),
              gemfire::GemfireTypeIds::CacheableHashSet);
            
             Serializable::RegisterWrapper(
              gcnew WrapperDelegate(GemStone::GemFire::Cache::CacheableLinkedHashSet::Create),
              gemfire::GemfireTypeIds::CacheableLinkedHashSet);

             Serializable::RegisterWrapper(
              gcnew WrapperDelegate(GemStone::GemFire::Cache::Struct::Create),
              gemfire::GemfireTypeIds::Struct);
            
             Serializable::RegisterWrapper(
              gcnew WrapperDelegate(GemStone::GemFire::Cache::Properties::CreateDeserializable),
              gemfire::GemfireTypeIds::Properties);

          // End register wrapper types for built-in types

  // Register with cpp using unmanaged Cacheablekey wrapper
            Serializable::RegisterType(
              gemfire::GemfireTypeIds::CacheableByte,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableByte::CreateDeserializable));

            Serializable::RegisterType(
              gemfire::GemfireTypeIds::CacheableBoolean,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableBoolean::CreateDeserializable));

            Serializable::RegisterType(
              gemfire::GemfireTypeIds::CacheableBytes,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableBytes::CreateDeserializable));

            Serializable::RegisterType(
              gemfire::GemfireTypeIds::BooleanArray, 
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::BooleanArray::CreateDeserializable));

            Serializable::RegisterType(
              gemfire::GemfireTypeIds::CacheableWideChar,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableCharacter::CreateDeserializable));

            Serializable::RegisterType(
              gemfire::GemfireTypeIds::CharArray,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CharArray::CreateDeserializable));

            Serializable::RegisterType(
              gemfire::GemfireTypeIds::CacheableDouble,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableDouble::CreateDeserializable));

            Serializable::RegisterType(
              gemfire::GemfireTypeIds::CacheableDoubleArray,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableDoubleArray::CreateDeserializable));

            Serializable::RegisterType(
              gemfire::GemfireTypeIds::CacheableFloat,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableFloat::CreateDeserializable));

            Serializable::RegisterType(
              gemfire::GemfireTypeIds::CacheableFloatArray,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableFloatArray::CreateDeserializable));

           
            Serializable::RegisterType(
              gemfire::GemfireTypeIds::CacheableHashSet,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableHashSet::CreateDeserializable));

           Serializable::RegisterType(
              gemfire::GemfireTypeIds::CacheableLinkedHashSet,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableLinkedHashSet::CreateDeserializable));

            Serializable::RegisterType(
              gemfire::GemfireTypeIds::CacheableInt16,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableInt16::CreateDeserializable));

            Serializable::RegisterType(
              gemfire::GemfireTypeIds::CacheableInt16Array,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableInt16Array::CreateDeserializable));

            Serializable::RegisterType(
              gemfire::GemfireTypeIds::CacheableInt32,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableInt32::CreateDeserializable));

            Serializable::RegisterType(
              gemfire::GemfireTypeIds::CacheableInt32Array,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableInt32Array::CreateDeserializable));

            Serializable::RegisterType(
              gemfire::GemfireTypeIds::CacheableInt64,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableInt64::CreateDeserializable));

            Serializable::RegisterType(
              gemfire::GemfireTypeIds::CacheableInt64Array,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableInt64Array::CreateDeserializable));

            Serializable::RegisterType(
              gemfire::GemfireTypeIds::CacheableASCIIString,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableString::CreateDeserializable));

            Serializable::RegisterType(
              gemfire::GemfireTypeIds::CacheableASCIIStringHuge,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableString::createDeserializableHuge));

            Serializable::RegisterType(
              gemfire::GemfireTypeIds::CacheableString,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableString::createUTFDeserializable));

            Serializable::RegisterType(
              gemfire::GemfireTypeIds::CacheableStringHuge,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableString::createUTFDeserializableHuge));

            Serializable::RegisterType(
              gemfire::GemfireTypeIds::CacheableNullString,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableString::CreateDeserializable));

            Serializable::RegisterType(
              gemfire::GemfireTypeIds::CacheableStringArray,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableStringArray::CreateDeserializable));

            Serializable::RegisterType(
              gemfire::GemfireTypeIds::Struct,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::Struct::CreateDeserializable));

            Serializable::RegisterType(
              gemfire::GemfireTypeIds::Properties,
              gcnew TypeFactoryMethod(GemStone::GemFire::Cache::Properties::CreateDeserializable));
            
            
            // End register other built-in types

          if (!gemfire::DistributedSystem::isConnected()) 
          {
            // Set the ManagedAuthInitialize factory function
            gemfire::SystemProperties::managedAuthInitializeFn =
              gemfire::ManagedAuthInitialize::create;

            // Set the ManagedCacheLoader/Listener/Writer factory functions.
            gemfire::CacheXmlParser::managedCacheLoaderFn =
              gemfire::ManagedCacheLoader::create;
            gemfire::CacheXmlParser::managedCacheListenerFn =
              gemfire::ManagedCacheListener::create;
            gemfire::CacheXmlParser::managedCacheWriterFn =
              gemfire::ManagedCacheWriter::create;

            // Set the ManagedPartitionResolver factory function
            gemfire::CacheXmlParser::managedPartitionResolverFn =
              gemfire::ManagedPartitionResolver::create;
          }

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void DistributedSystem::ManagedPostConnect()
      {
        // [sumedh] The registration into the native map should be after
        // native connect since it can be invoked only once

        // Register other built-in types
        Serializable::RegisterType(
          gemfire::GemfireTypeIds::CacheableDate,
          gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableDate::CreateDeserializable));
        Serializable::RegisterType(
          gemfire::GemfireTypeIds::CacheableFileName,
          gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableFileName::CreateDeserializable));
        Serializable::RegisterType(
          gemfire::GemfireTypeIds::CacheableHashMap,
          gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableHashMap::CreateDeserializable));
        Serializable::RegisterType(
          gemfire::GemfireTypeIds::CacheableHashTable,
          gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableHashTable::CreateDeserializable));
        Serializable::RegisterType(
          gemfire::GemfireTypeIds::CacheableIdentityHashMap,
          gcnew TypeFactoryMethod(
          GemStone::GemFire::Cache::CacheableIdentityHashMap::CreateDeserializable));
        Serializable::RegisterType(
          gemfire::GemfireTypeIds::CacheableUndefined,
          gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableUndefined::CreateDeserializable));
        Serializable::RegisterType(
          gemfire::GemfireTypeIds::CacheableVector,
          gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableVector::CreateDeserializable));
        Serializable::RegisterType(
          gemfire::GemfireTypeIds::CacheableObjectArray,
          gcnew TypeFactoryMethod(
          GemStone::GemFire::Cache::CacheableObjectArray::CreateDeserializable));
        Serializable::RegisterType(
          gemfire::GemfireTypeIds::CacheableArrayList,
          gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableArrayList::CreateDeserializable));
        Serializable::RegisterType(
          gemfire::GemfireTypeIds::CacheableStack,
          gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableStack::CreateDeserializable));
        Serializable::RegisterType(
          GemFireClassIds::CacheableManagedObject - 0x80000000,
          gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableObject::CreateDeserializable));
        Serializable::RegisterType(
          GemFireClassIds::CacheableManagedObjectXml - 0x80000000,
          gcnew TypeFactoryMethod(GemStone::GemFire::Cache::CacheableObjectXml::CreateDeserializable));
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
      }

      void DistributedSystem::UnregisterBuiltinManagedTypes()
      {
         _GF_MG_EXCEPTION_TRY

         gemfire::DistributedSystemImpl::acquireDisconnectLock();

         Serializable::UnregisterNatives();
         
         int remainingInstances =
           gemfire::DistributedSystemImpl::currentInstances();

          if (remainingInstances == 0) { // last instance
           
            Serializable::UnregisterType(
              gemfire::GemfireTypeIds::CacheableDate);
            Serializable::UnregisterType(
              gemfire::GemfireTypeIds::CacheableFileName);
            Serializable::UnregisterType(
              gemfire::GemfireTypeIds::CacheableHashMap);
            Serializable::UnregisterType(
              gemfire::GemfireTypeIds::CacheableHashTable);
            Serializable::UnregisterType(
              gemfire::GemfireTypeIds::CacheableIdentityHashMap);
            Serializable::UnregisterType(
              gemfire::GemfireTypeIds::CacheableVector);
            Serializable::UnregisterType(
              gemfire::GemfireTypeIds::CacheableObjectArray);
            Serializable::UnregisterType(
              gemfire::GemfireTypeIds::CacheableArrayList);
            Serializable::UnregisterType(
              gemfire::GemfireTypeIds::CacheableStack);
            Serializable::UnregisterType(
              GemFireClassIds::CacheableManagedObject - 0x80000000);
            Serializable::UnregisterType(
              GemFireClassIds::CacheableManagedObjectXml - 0x80000000);            
          }

           _GF_MG_EXCEPTION_CATCH_ALL

        finally {
          gemfire::DistributedSystemImpl::releaseDisconnectLock();
        }
      }

      SystemProperties^ DistributedSystem::SystemProperties::get()
      {
        _GF_MG_EXCEPTION_TRY

          return GemStone::GemFire::Cache::SystemProperties::Create(
            gemfire::DistributedSystem::getSystemProperties());

        _GF_MG_EXCEPTION_CATCH_ALL
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
    }
  }
}

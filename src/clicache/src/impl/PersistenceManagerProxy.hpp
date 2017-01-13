/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#pragma once

#include "../IPersistenceManager.hpp"
#include "SafeConvert.hpp"
#include "../Region.hpp"
#include "../Properties.hpp"
using namespace System;
namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    { 
      namespace Generic
      {
        public interface class IPersistenceManagerProxy
        {
          public:
            void write(const CacheableKeyPtr&  key, const CacheablePtr&  value/*, void *& PersistenceInfo*/);
            bool writeAll();
            void init(const RegionPtr& region, PropertiesPtr& diskProperties);
            CacheablePtr read(const CacheableKeyPtr& key/*, void *& PersistenceInfo*/);
            bool readAll();
            void destroy(const CacheableKeyPtr& key/*, void *& PersistenceInfo*/);
            void close();
        };

        generic<class TKey, class TValue>
        public ref class PersistenceManagerGeneric : IPersistenceManagerProxy 
        {
          private:
            IPersistenceManager<TKey, TValue>^ m_persistenceManager;
          public:
            virtual void SetPersistenceManager(IPersistenceManager<TKey, TValue>^ persistenceManager)
            {
              m_persistenceManager = persistenceManager;
            }
            virtual void write(const CacheableKeyPtr&  key, const CacheablePtr&  value/*, void *& PersistenceInfo*/)
            {
               TKey gKey = Serializable::GetManagedValueGeneric<TKey>(key);
               TValue gValue = Serializable::GetManagedValueGeneric<TValue>(value);
               m_persistenceManager->Write(gKey, gValue);
            }
            
            virtual bool writeAll()
            {
              throw gcnew System::NotSupportedException;
            }

            virtual void init(const RegionPtr& region, PropertiesPtr& diskProperties)
            {
              IRegion<TKey, TValue>^ gRegion = Region<TKey, TValue>::Create(region.ptr());
              Properties<String^, String^>^ gProps = Properties<String^, String^>::Create<String^, String^>(diskProperties.ptr());
              m_persistenceManager->Init(gRegion, gProps);
            }
            
            virtual CacheablePtr read(const CacheableKeyPtr& key/*, void *& PersistenceInfo*/)
            {
              TKey gKey = Serializable::GetManagedValueGeneric<TKey>(key);
              return Serializable::GetUnmanagedValueGeneric<TValue>(m_persistenceManager->Read(gKey));
            }
            
            virtual bool readAll()
            {
              throw gcnew System::NotSupportedException;
            }
            
            virtual void destroy(const CacheableKeyPtr& key/*, void *& PersistenceInfo*/)
            {
              TKey gKey = Serializable::GetManagedValueGeneric<TKey>(key);
              m_persistenceManager->Destroy(gKey);
            }
            
            virtual void close()
            {
              m_persistenceManager->Close();
            }
        };
      }
    }
  }
}


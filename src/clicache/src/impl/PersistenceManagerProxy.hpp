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


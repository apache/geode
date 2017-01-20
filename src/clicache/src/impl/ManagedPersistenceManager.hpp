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
#include "../gf_defs.hpp"
#include <vcclr.h>
#include <gfcpp/PersistenceManager.hpp>
#include "PersistenceManagerProxy.hpp"

namespace apache {
  namespace geode {
    namespace client {

      /// <summary>
      /// Wraps the managed <see cref="Apache.Geode.Client.IPersistenceManager" />
      /// object and implements the native <c>apache::geode::client::PersistenceManager</c> interface.
      /// </summary>
      class ManagedPersistenceManagerGeneric : public apache::geode::client::PersistenceManager
      {
      public:

        inline ManagedPersistenceManagerGeneric(Object^ userptr) : m_userptr(userptr) { }

        static apache::geode::client::PersistenceManager* create(const char* assemblyPath,
          const char* factoryFunctionName);

        virtual ~ManagedPersistenceManagerGeneric() { }


        virtual void write(const CacheableKeyPtr&  key, const CacheablePtr&  value, void *& PersistenceInfo);
        virtual bool writeAll();
        virtual void init(const RegionPtr& region, PropertiesPtr& diskProperties);
        virtual CacheablePtr read(const CacheableKeyPtr& key, void *& PersistenceInfo);
        virtual bool readAll();
        virtual void destroy(const CacheableKeyPtr& key, void *& PersistenceInfo);
        virtual void close();

        inline void setptr(Apache::Geode::Client::Generic::IPersistenceManagerProxy^ managedptr)
        {
          m_managedptr = managedptr;
        }

        inline Object^ userptr() const
        {
          return m_userptr;
        }

      private:


        /// <summary>
        /// Using gcroot to hold the managed delegate pointer (since it cannot be stored directly).
        /// Note: not using auto_gcroot since it will result in 'Dispose' of the IPersistenceManager
        /// to be called which is not what is desired when this object is destroyed. Normally this
        /// managed object may be created by the user and will be handled automatically by the GC.
        /// </summary>
        gcroot<Apache::Geode::Client::Generic::IPersistenceManagerProxy^> m_managedptr;

        gcroot<Object^> m_userptr;

        // Disable the copy and assignment constructors
        ManagedPersistenceManagerGeneric(const ManagedPersistenceManagerGeneric&);
        ManagedPersistenceManagerGeneric& operator = (const ManagedPersistenceManagerGeneric&);
      };

    }  // namespace client
  }  // namespace geode
}  // namespace apache

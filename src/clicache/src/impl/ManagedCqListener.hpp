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
#include <gfcpp/CqListener.hpp>
//#include "../ICqListener.hpp"
#include "../ICqListener.hpp"
#include "CqListenerProxy.hpp"

//using namespace apache::geode::client;
namespace apache {
  namespace geode {
    namespace client {

      /// <summary>
      /// Wraps the managed <see cref="Apache.Geode.Client.ICacheListener" />
      /// object and implements the native <c>apache::geode::client::CacheListener</c> interface.
      /// </summary>
      class ManagedCqListenerGeneric
        : public apache::geode::client::CqListener
      {
      public:

        /// <summary>
        /// Constructor to initialize with the provided managed object.
        /// </summary>
        /// <param name="userptr">
        /// The user object.
        /// </param>
        inline ManagedCqListenerGeneric( /*Generic::ICqListener<Object^, Object^>^ managedptr*/Object^ userptr)
          : /*m_managedptr( managedptr )*/m_userptr(userptr) { }

        /// <summary>
        /// Static function to create a <c>ManagedCacheListener</c> using given
        /// managed assembly path and given factory function.
        /// </summary>
        /// <param name="assemblyPath">
        /// The path of the managed assembly that contains the <c>ICacheListener</c>
        /// factory function.
        /// </param>
        /// <param name="factoryFunctionName">
        /// The name of the factory function of the managed class for creating
        /// an object that implements <c>ICacheListener</c>.
        /// This should be a static function of the format
        /// {Namespace}.{Class Name}.{Method Name}.
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// If the managed library cannot be loaded or the factory function fails.
        /// </exception>
        static CqListener* create(const char* assemblyPath,
          const char* factoryFunctionName);

        /// <summary>
        /// Destructor -- does nothing.
        /// </summary>
        virtual ~ManagedCqListenerGeneric() { }

        /// <summary>
        /// Handles the event of a new key being added to a region.
        /// </summary>
        /// <remarks>
        /// The entry did not previously exist in this region in the local cache
        /// (even with a null value).
        /// <para>
        /// This function does not throw any exception.
        /// </para>
        /// </remarks>
        /// <param name="ev">
        /// Denotes the event object associated with the entry creation.
        /// </param>
        /// <seealso cref="Apache.Geode.Client.Region.Create" />
        /// <seealso cref="Apache.Geode.Client.Region.Put" />
        /// <seealso cref="Apache.Geode.Client.Region.Get" />
        virtual void onEvent(const apache::geode::client::CqEvent& ev);

        /// <summary>
        /// Handles the event of an entry's value being modified in a region.
        /// </summary>
        /// <remarks>
        /// This entry previously existed in this region in the local cache,
        /// but its previous value may have been null.
        /// </remarks>
        /// <param name="ev">
        /// EntryEvent denotes the event object associated with updating the entry.
        /// </param>
        /// <seealso cref="Apache.Geode.Client.Region.Put" />
        virtual void onError(const apache::geode::client::CqEvent& ev);

        /// <summary>
        /// Handles the event of an entry's value being invalidated.
        /// </summary>
        /// EntryEvent denotes the event object associated with the entry invalidation.
        /// </param>
        virtual void close();
        /// <summary>
        /// Returns the wrapped managed object reference.
        /// </summary>
        inline Apache::Geode::Client::ICqListener<Object^, Object^>^ ptr() const
        {
          return m_managedptr;
        }

        inline void setptr(Apache::Geode::Client::ICqListener<Object^, Object^>^ managedptr)
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
        /// Note: not using auto_gcroot since it will result in 'Dispose' of the ICacheListener
        /// to be called which is not what is desired when this object is destroyed. Normally this
        /// managed object may be created by the user and will be handled automatically by the GC.
        /// </summary>
        gcroot<Apache::Geode::Client::ICqListener<Object^, Object^>^> m_managedptr;

        gcroot<Object^> m_userptr;
      };

    }  // namespace client
  }  // namespace geode
}  // namespace apache

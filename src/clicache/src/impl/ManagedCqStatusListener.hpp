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
#include <gfcpp/CqStatusListener.hpp>
#include "../ICqStatusListener.hpp"
#include "CqStatusListenerProxy.hpp"

namespace apache {
  namespace geode {
    namespace client {

      /// <summary>
      /// Wraps the managed <see cref="GemStone.GemFire.Cache.ICqStatusListener" />
      /// object and implements the native <c>apache::geode::client::CqStatusListener</c> interface.
      /// </summary>
      class ManagedCqStatusListenerGeneric
        : public apache::geode::client::CqStatusListener
      {
      public:

        /// <summary>
        /// Constructor to initialize with the provided managed object.
        /// </summary>
        /// <param name="userptr">
        /// The user object.
        /// </param>
        inline ManagedCqStatusListenerGeneric(Object^ userptr)
          : m_userptr(userptr) { }

        /// <summary>
        /// Static function to create a <c>ManagedCqStatusListenerGeneric</c> using given
        /// managed assembly path and given factory function.
        /// </summary>
        /// <param name="assemblyPath">
        /// The path of the managed assembly that contains the <c>ICqStatusListener</c>
        /// factory function.
        /// </param>
        /// <param name="factoryFunctionName">
        /// The name of the factory function of the managed class for creating
        /// an object that implements <c>ICqStatusListener</c>.
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
        virtual ~ManagedCqStatusListenerGeneric() { }

        /// <summary>
        /// This method is invoked when an event is occurred on the region
        /// that satisfied the query condition of this CQ.
        /// This event does not contain an error.
        /// </summary>
        virtual void onEvent(const apache::geode::client::CqEvent& ev);

        /// <summary>
        /// This method is invoked when there is an error during CQ processing.  
        /// The error can appear while applying query condition on the event.
        /// e.g if the event doesn't has attributes as specified in the CQ query.
        /// This event does contain an error. The newValue may or may not be 
        /// available, and will be NULLPTR if not available.
        /// </summary>
        virtual void onError(const apache::geode::client::CqEvent& ev);

        /// <summary>
        /// Handles the event of an entry's value being invalidated.
        /// </summary>
        /// EntryEvent denotes the event object associated with the entry invalidation.
        /// </param>
        virtual void close();

        /// <summary>
        /// Called when the cq loses connection with all servers
        /// </summary>
        virtual void onCqDisconnected();

        /// <summary>
        /// Called when the cq establishes a connection with a server
        /// </summary>
        virtual void onCqConnected();

        /// <summary>
        /// Returns the wrapped managed object reference.
        /// </summary>
        inline GemStone::GemFire::Cache::Generic::ICqStatusListener<Object^, Object^>^ ptr() const
        {
          return m_managedptr;
        }

        inline void setptr(GemStone::GemFire::Cache::Generic::ICqStatusListener<Object^, Object^>^ managedptr)
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
        /// Note: not using auto_gcroot since it will result in 'Dispose' of the ICqStatusListener
        /// to be called which is not what is desired when this object is destroyed. Normally this
        /// managed object may be created by the user and will be handled automatically by the GC.
        /// </summary>
        gcroot<GemStone::GemFire::Cache::Generic::ICqStatusListener<Object^, Object^>^> m_managedptr;

        gcroot<Object^> m_userptr;
      };

    }  // namespace client
  }  // namespace geode
}  // namespace apache

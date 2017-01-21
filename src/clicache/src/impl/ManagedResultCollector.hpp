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
#include <gfcpp/ResultCollector.hpp>
//#include "../ResultCollector.hpp"
//#include "../IResultCollector.hpp"
//#include "../../../IResultCollector.hpp"
#include "ResultCollectorProxy.hpp"
#include "SafeConvert.hpp"
//#include "SafeConvert.hpp"
//using namespace apache::geode::client;

//using namespace apache::geode::client;
namespace apache {
  namespace geode {
    namespace client {

      /// <summary>
      /// Wraps the managed <see cref="Apache.Geode.Client.IResultCollector" />
      /// object and implements the native <c>apache::geode::client::ResultCollector</c> interface.
      /// </summary>
      class ManagedResultCollectorGeneric
        : public apache::geode::client::ResultCollector
      {
      public:

        /// <summary>
        /// Constructor to initialize with the provided managed object.
        /// </summary>
        /// <param name="userptr">
        /// The managed object.
        /// </param>
        inline ManagedResultCollectorGeneric(Apache::Geode::Client::ResultCollectorG^ userptr)
          : m_managedptr(userptr) { }

        /// <summary>
        /// Static function to create a <c>ManagedResultCollector</c> using given
        /// managed assembly path and given factory function.
        /// </summary>
        /// <param name="assemblyPath">
        /// The path of the managed assembly that contains the <c>ICacheListener</c>
        /// factory function.
        /// </param>
        /// <param name="factoryFunctionName">
        /// The name of the factory function of the managed class for creating
        /// an object that implements <c>IResultCollector</c>.
        /// This should be a static function of the format
        /// {Namespace}.{Class Name}.{Method Name}.
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// If the managed library cannot be loaded or the factory function fails.
        /// </exception>
        static apache::geode::client::ResultCollector* create(const char* assemblyPath,
          const char* factoryFunctionName);

        /// <summary>
        /// Destructor -- does nothing.
        /// </summary>
        virtual ~ManagedResultCollectorGeneric() { }

        CacheableVectorPtr getResult(uint32_t timeout = DEFAULT_QUERY_RESPONSE_TIMEOUT);
        void addResult(CacheablePtr& result);
        void endResults();
        void clearResults();
        /// <summary>
        /// Returns the wrapped managed object reference.
        /// </summary>
        inline Apache::Geode::Client::ResultCollectorG^ ptr() const
        {
          return m_managedptr;
        }


      private:


        /// <summary>
        /// Using gcroot to hold the managed delegate pointer (since it cannot be stored directly).
        /// Note: not using auto_gcroot since it will result in 'Dispose' of the ICacheListener
        /// to be called which is not what is desired when this object is destroyed. Normally this
        /// managed object may be created by the user and will be handled automatically by the GC.
        /// </summary>
        gcroot<Apache::Geode::Client::ResultCollectorG^> m_managedptr;
        //Apache::Geode::Client::IResultCollector^ m_managedptr;
        //gcroot<Object^> m_userptr;
      };

    }  // namespace client
  }  // namespace geode
}  // namespace apache

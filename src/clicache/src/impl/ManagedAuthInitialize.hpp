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
#include <gfcpp/AuthInitialize.hpp>
#include <vcclr.h>
#include "../IAuthInitialize.hpp"

//using namespace apache::geode::client;

namespace apache
{
  namespace geode
  {
    namespace client
    {

      /// <summary>
      /// Wraps the managed <see cref="Apache.Geode.Client.Generic.IAuthInitialize" />
      /// object and implements the native <c>apache::geode::client::AuthInitialize</c> interface.
      /// </summary>
      class ManagedAuthInitializeGeneric
        : public apache::geode::client::AuthInitialize
      {
      public:

        /// <summary>
        /// Constructor to initialize with the provided managed object.
        /// </summary>
        /// <param name="managedptr">
        /// The managed object.
        /// </param>
        inline ManagedAuthInitializeGeneric(Apache::Geode::Client::Generic::IAuthInitialize^ managedptr)
          : m_managedptr(managedptr) { }

        /// <summary>
        /// Static function to create a <c>ManagedAuthInitialize</c> using given
        /// managed assembly path and given factory function.
        /// </summary>
        /// <param name="assemblyPath">
        /// The path of the managed assembly that contains the <c>IAuthInitialize</c>
        /// factory function.
        /// </param>
        /// <param name="factoryFunctionName">
        /// The name of the factory function of the managed class for creating
        /// an object that implements <c>IAuthInitialize</c>.
        /// This should be a static function of the format
        /// {Namespace}.{Class Name}.{Method Name}.
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// If the managed library cannot be loaded or the factory function fails.
        /// </exception>
        static AuthInitialize* create(const char* assemblyPath,
          const char* factoryFunctionName);

        /// <summary>
        /// Called when the cache is going down
        /// </summary>
        /// <remarks>
        /// Implementations should clean up any external
        /// resources, such as database connections. Any runtime exceptions this method
        /// throws will be logged.
        /// <para>
        /// It is possible for this method to be called multiple times on a single
        /// callback instance, so implementations must be tolerant of this.
        /// </para>
        /// </remarks>
        /// <seealso cref="Apache.Geode.Client.Cache.Close" />
        virtual void close();

        /// <summary>
        /// Initialize with the given set of security properties and return the
        /// credentials for the given client as properties.
        /// </summary>
        /// <param name="securityprops">Given set of properties with which
        /// to initialize
        /// </param>
        /// <param name="server">It is the ID of the endpoint
        /// </param>
        virtual PropertiesPtr getCredentials(PropertiesPtr& securityprops, const char* server);

        virtual ~ManagedAuthInitializeGeneric() { }

        /// <summary>
        /// Returns the wrapped managed object reference.
        /// </summary>
        inline Apache::Geode::Client::Generic::IAuthInitialize^ ptr() const
        {
          return m_managedptr;
        }

      private:

        /// <summary>
        /// Using gcroot to hold the managed delegate pointer (since it cannot be stored directly).
        /// Note: not using auto_gcroot since it will result in 'Dispose' of the ICacheLoader
        /// to be called which is not what is desired when this object is destroyed. Normally this
        /// managed object may be created by the user and will be handled automatically by the GC.
        /// </summary>
        gcroot<Apache::Geode::Client::Generic::IAuthInitialize^> m_managedptr;

        // Disable the copy and assignment constructors
        ManagedAuthInitializeGeneric(const ManagedAuthInitializeGeneric&);
        ManagedAuthInitializeGeneric& operator = (const ManagedAuthInitializeGeneric&);
      };
    }  // namespace client
  }  // namespace geode
}  // namespace apache

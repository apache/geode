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

#include "gf_defs.hpp"
#include <gfcpp/DistributedSystem.hpp>
//#include "impl/NativeWrapper.hpp"
#include "SystemProperties.hpp"
#include "Properties.hpp"
#include "impl/CliCallbackDelgate.hpp"

using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
namespace Generic
      {
      /// <summary>
      /// DistributedSystem encapsulates this applications "connection" into the
      /// GemFire Java servers.
      /// </summary>
      /// <remarks>
      /// In order to participate as a client in the GemFire Java servers
      /// distributed system, each application needs to connect to the
      /// DistributedSystem.
      /// </remarks>
      public ref class DistributedSystem sealed
				: public Generic::Internal::SBWrap<apache::geode::client::DistributedSystem>
      {
      public:

        /// <summary>
        /// Initializes the Native Client system to be able to connect to the GemFire Java servers.
        /// </summary>
        /// <param name="name">the name of the system to connect to</param>
        /// <exception cref="IllegalArgumentException">if name is null</exception>
        /// <exception cref="NoSystemException">
        /// if the connecting target is not running
        /// </exception>
        /// <exception cref="AlreadyConnectedException">
        /// if trying a second connect.
        /// An application can have one only one connection to a DistributedSystem.
        /// </exception>
        /// <exception cref="UnknownException">otherwise</exception>
        static DistributedSystem^ Connect( String^ name );

        /// <summary>
        /// Initializes the Native Client system to be able to connect to the
        /// GemFire Java servers.
        /// </summary>
        /// <param name="name">the name of the system to connect to</param>
        /// <param name="config">the set of properties</param>
        /// <exception cref="IllegalArgumentException">if name is null</exception>
        /// <exception cref="NoSystemException">
        /// if the connecting target is not running
        /// </exception>
        /// <exception cref="AlreadyConnectedException">
        /// if trying a second connect.
        /// An application can have one only one connection to a DistributedSystem.
        /// </exception>
        /// <exception cref="UnknownException">otherwise</exception>
        static DistributedSystem^ Connect( String^ name, Properties<String^, String^>^ config );
        
        /// <summary>
        /// Disconnect from the distributed system.
        /// </summary>
        /// <exception cref="IllegalStateException">if not connected</exception>
        static void Disconnect();

        /// <summary>
        /// Returns the SystemProperties used to create this instance of a
        /// <c>DistributedSystem</c>.
        /// </summary>
        /// <returns>the SystemProperties</returns>
        static property Apache::Geode::Client::Generic::SystemProperties^ SystemProperties
        {
          static Apache::Geode::Client::Generic::SystemProperties^ get( );
        }

        /// <summary>
        /// Get the name that identifies this DistributedSystem instance.
        /// </summary>
        /// <returns>the name of the DistributedSystem instance.</returns>
        property String^ Name
        {
          String^ get( );
        }

        /// <summary>
        /// The current connection status of this client to the <c>DistributedSystem</c>.
        /// </summary>
        /// <returns>true if connected, false otherwise</returns>
        static property bool IsConnected
        {
          static bool get( );
        }

        /// <summary>
        /// Returns a reference to this DistributedSystem instance.
        /// </summary>
        /// <returns>the DistributedSystem instance</returns>
        static DistributedSystem^ GetInstance( );


      internal:

        /// <summary>
        /// Internal factory function to wrap a native object pointer inside
        /// this managed class with null pointer check.
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        /// <returns>
        /// The managed wrapper object; null if the native pointer is null.
        /// </returns>
        static DistributedSystem^ Create(apache::geode::client::DistributedSystem* nativeptr);

        static void acquireDisconnectLock();

        static void disconnectInstance();

        static void releaseDisconnectLock();

        static void connectInstance();

        delegate void cliCallback();

        static void registerCliCallback();

        static void unregisterCliCallback();
          /// <summary>
        /// Stuff that needs to be done for Connect in each AppDomain.
        /// </summary>
        static void AppDomainInstanceInitialization(
          const apache::geode::client::PropertiesPtr& nativepropsptr);

        /// <summary>
        /// Managed registrations and other stuff to be done for the manage
        /// layer after the first connect.
        /// </summary>
        static void ManagedPostConnect();

        /// <summary>
        /// Stuff that needs to be done for Connect in each AppDomain but
        /// only after the first Connect has completed.
        /// </summary>
        static void AppDomainInstancePostInitialization();

        /// <summary>
        /// Unregister the builtin managed types like CacheableObject.
        /// </summary>
        static void UnregisterBuiltinManagedTypes();

      private:

        ///// <summary>
        ///// Stores the task ID of the task that adjusts unmanaged memory
        ///// pressure in managed GC.
        ///// </summary>
        //static long s_memoryPressureTaskID = -1;

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        DistributedSystem(apache::geode::client::DistributedSystem* nativeptr);

        /// <summary>
        /// Finalizer for the singleton instance of this class.
        /// </summary>
        ~DistributedSystem();

       

        /// <summary>
        /// Periodically adjust memory pressure of unmanaged heap for GC.
        /// </summary>
        static void HandleMemoryPressure(System::Object^ state);

        /// <summary>
        /// Timer task to periodically invoke <c>HandleMemoryPressure</c>.
        /// </summary>
        System::Threading::Timer^ m_memoryPressureHandler;

        /// <summary>
        /// Singleton instance of this class.
        /// </summary>
        static volatile DistributedSystem^ m_instance;

        /// <summary>
        /// Static lock object to protect the singleton instance of this class.
        /// </summary>
        static System::Object^ m_singletonSync = gcnew System::Object();

        static CliCallbackDelegate^ m_cliCallBackObj;
      };
      } // end namespace generic
    }
  }
}

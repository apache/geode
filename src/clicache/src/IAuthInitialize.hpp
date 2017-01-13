/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"

#include "Properties.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache {
    
    namespace Generic
    {
      /// <summary>
      /// Specifies the mechanism to obtain credentials for a client.
      /// It is mandantory for clients when the server is running in secure
      /// mode having a <c>security-client-authenticator</c> module specified.
      /// Implementations should register the library path as
      /// <c>security-client-auth-library</c> system property and factory
      /// function (a zero argument function returning pointer to an
      /// AuthInitialize object) as the <c>security-client-auth-factory</c>
      /// system property.
      ///
      /// For a managed class implementing <c>IAuthInitialize</c> the fully
      /// qualified name of the factory function should be provided in the
      /// form {Namespace}.{Class Name}.{Method Name} as the
      /// <c>security-client-auth-factory</c> property.
      /// </summary>
      public interface class IAuthInitialize
      {
      public:

        /// <summary>
        /// Initialize with the given set of security properties
        /// return the credentials for the client as properties.
        /// </summary>
        /// <param name="props">
        /// the set of <c>security-*</c> properties provided to the
        /// <see cref="DistributedSystem.connect"/> method
        /// </param>
        /// <param name="server">
        /// the ID of the current endpoint in the format "host:port"
        /// </param>
        /// <returns>
        /// the credentials to be used for the given server
        /// </returns>
        /// <remarks>
        /// This method can modify the given set of properties. For
        /// example it may invoke external agents or even interact with
        /// the user.
        /// </remarks>
        //generic <class TPropKey, class TPropValue>
        Properties<String^, Object^>^ GetCredentials(Properties<String^, String^>^ props, String^ server);

        /// <summary>
        /// Invoked before the cache goes down.
        /// </summary>
        void Close();

      };
    }
  }
}
 } //namespace 

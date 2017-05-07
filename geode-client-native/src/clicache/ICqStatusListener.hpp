/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/

#pragma once

#include "gf_defs.hpp"
#include "ICqListener.hpp"

using namespace System;
namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      /// <summary>
      /// Extension of CqListener. Adds two new methods to CqListener, one that
      /// is called when the cq is connected and one that is called when
      /// the cq is disconnected.
      /// </summary>
      [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public interface class ICqStatusListener : public ICqListener
      {
      public:

        /// <summary>
        /// Called when the cq loses connection with all servers.
        /// </summary>
        virtual void OnCqDisconnected();

        /// <summary>
        /// Called when the cq establishes a connection with a server
        /// </summary>
        virtual void OnCqConnected();

      };
    }
  }
}

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "../gf_defs.hpp"
#include "../Serializable.hpp"
#include "ManagedCacheableKey.hpp"
#include "SafeConvert.hpp"
#include "../Log.hpp"
#include "PdxTypeRegistry.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      /// <summary>
      /// to get the callback from c++ layer
      /// </summary>
      ref class CliCallbackDelegate
      {
      public:

        inline CliCallbackDelegate( )
        { 
        }

        void Callback( )
        {
          GemStone::GemFire::Cache::Generic::Log::Fine("CliCallbackDelgate::Callback( ) ");
          GemStone::GemFire::Cache::Generic::Internal::PdxTypeRegistry::clear();
        }


      private:

      };

    }
  }
}
}
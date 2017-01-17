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
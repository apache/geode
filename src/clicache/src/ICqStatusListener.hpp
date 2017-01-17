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
#include "ICqListener.hpp"

using namespace System;
namespace GemStone
{
  namespace GemFire
  {
    namespace Cache 
    { 
      namespace Generic
      {

        /// <summary>
        /// Extension of CqListener. Adds two new methods to CqListener, one that
        /// is called when the cq is connected and one that is called when
        /// the cq is disconnected.
        /// </summary>

        generic<class TKey, class TResult>
        public interface class ICqStatusListener : public ICqListener<TKey, TResult>
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
}

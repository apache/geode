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

#include <gfcpp/gfcpp_globals.hpp>
#include <ExpiryTaskManager.hpp>

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
namespace Generic {

      class MemoryPressureHandler
        : public ACE_Event_Handler
      {
        public:
          int handle_timeout( const ACE_Time_Value& current_time,
              const void* arg );

          int handle_close( ACE_HANDLE handle, ACE_Reactor_Mask close_mask );
      };
    }
  }
}
}

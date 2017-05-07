/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include <cppcache/gfcpp_globals.hpp>
#include <cppcache/impl/ExpiryTaskManager.hpp>

namespace GemStone {

  namespace GemFire {

    namespace Cache {

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

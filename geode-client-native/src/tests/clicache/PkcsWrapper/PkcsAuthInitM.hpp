/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "PkcsAuthInit.hpp"
#include "impl/NativeWrapper.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
        namespace Tests
        {
          public ref class PkcsAuthInit sealed
          : public Internal::SBWrap<gemfire::PKCSAuthInitInternal>, public IAuthInitialize
        {
        public:
          
          PkcsAuthInit();          

          ~PkcsAuthInit();          

          virtual Properties^ GetCredentials(Properties^ props, String^ server);

          virtual void Close();

        internal:            
          PkcsAuthInit( gemfire::PKCSAuthInitInternal* nativeptr )
            : SBWrap( nativeptr ) { }
        };
      }
    }
  }
}


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

using namespace GemStone::GemFire::Cache::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
        namespace Tests
        {
          namespace NewAPI
          {
          public ref class PkcsAuthInit sealed
          : public Internal::SBWrap<gemfire::PKCSAuthInitInternal>,
            public GemStone::GemFire::Cache::Generic::IAuthInitialize/*<String^, Object^>*/
        {
        public:
          
          PkcsAuthInit();          

          ~PkcsAuthInit();          

          //generic <class TPropKey, class TPropValue>
          virtual GemStone::GemFire::Cache::Generic::Properties<String^, Object^> ^
            GetCredentials(
            GemStone::GemFire::Cache::Generic::Properties<String^, String^>^ props, String^ server);

          virtual void Close();

        internal:            
          PkcsAuthInit( gemfire::PKCSAuthInitInternal* nativeptr )
            : SBWrap( nativeptr ) { }
        };
        } // end namespace NewAPI
      }
    }
  }
}


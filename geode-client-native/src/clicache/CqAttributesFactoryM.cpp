/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/

#include "gf_includes.hpp"
#include "CqAttributesFactoryM.hpp"
#include "CqAttributesM.hpp"
#include "impl/ManagedCqListener.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      void CqAttributesFactory::AddCqListener(ICqListener^ cqListener )
      {
        gemfire::CqListenerPtr listenerptr;
        if ( cqListener != nullptr ) {
          listenerptr = new gemfire::ManagedCqListener( cqListener );
        }

        NativePtr->addCqListener( listenerptr );
      }
      void CqAttributesFactory::InitCqListeners(array<ICqListener^>^ cqListeners)
      {
        gemfire::VectorOfCqListener vrr(cqListeners->GetLength(1));

        for( int i = 0; i < cqListeners->GetLength(1); i++ )
        {
          ICqListener^ lister = cqListeners[i];
          vrr[i] = new gemfire::ManagedCqListener( lister );
        }

        NativePtr->initCqListeners( vrr );
      }

      CqAttributes^ CqAttributesFactory::Create( )
      {
        return CqAttributes::Create(NativePtr->create().ptr());
      }

    }
  }
}

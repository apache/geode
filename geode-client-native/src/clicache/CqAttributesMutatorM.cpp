/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "CqAttributesMutatorM.hpp"
#include "impl/ManagedCqListener.hpp"

using namespace System;


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      void CqAttributesMutator::AddCqListener( ICqListener^ cqListener )
      {
        gemfire::CqListenerPtr listenerptr;
        if (cqListener != nullptr)
        {
          listenerptr = new gemfire::ManagedCqListener( cqListener );
        }
        NativePtr->addCqListener( listenerptr );
      }

      void CqAttributesMutator::RemoveCqListener( ICqListener^ cqListener )
      {
	gemfire::CqListenerPtr lptr(new gemfire::ManagedCqListener( cqListener ));
        NativePtr->removeCqListener(lptr);
      }

      void CqAttributesMutator::SetCqListeners(array<ICqListener^>^ newListeners)
      {
	gemfire::VectorOfCqListener vrr(newListeners->GetLength(1));

        for( int i = 0; i < newListeners->GetLength(1); i++ )
        {
             ICqListener^ lister = newListeners[i];
	     vrr[i] = new gemfire::ManagedCqListener( lister );
        }

        NativePtr->setCqListeners( vrr );
      }

    }
  }
}

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "CqAttributesM.hpp"
#include "impl/ManagedCqListener.hpp"
#include "ICqListener.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      array<ICqListener^>^ CqAttributes::getCqListeners( )
      {
	      gemfire::VectorOfCqListener vrr;
	      NativePtr->getCqListeners( vrr );
              array<ICqListener^>^ listners = gcnew array<ICqListener^>( vrr.size( ) );

              for( int32_t index = 0; index < vrr.size( ); index++ )
	      {
	            gemfire::CqListenerPtr& nativeptr( vrr[ index ] );
                    gemfire::ManagedCqListener* mg_listener =
                         dynamic_cast<gemfire::ManagedCqListener*>( nativeptr.ptr( ) );
                    if (mg_listener != nullptr)
                    {
	              listners[ index ] =  mg_listener->ptr( );
                    }else 
		    {
	              listners[ index ] =  nullptr;
		    }
	       }
	       return listners;
      }

    }
  }
}

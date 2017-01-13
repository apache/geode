/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/

//#include "gf_includes.hpp"
#include "CqAttributes.hpp"
#include "impl/ManagedCqListener.hpp"
#include "ICqListener.hpp"
#include "impl/ManagedCqStatusListener.hpp"
#include "ICqStatusListener.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      generic<class TKey, class TResult>
      array<ICqListener<TKey, TResult>^>^ CqAttributes<TKey, TResult>::getCqListeners( )
      {
        gemfire::VectorOfCqListener vrr;
        NativePtr->getCqListeners( vrr );
        array<ICqListener<TKey, TResult>^>^ listners = gcnew array<ICqListener<TKey, TResult>^>( vrr.size( ) );

        for( int32_t index = 0; index < vrr.size( ); index++ )
        {
          gemfire::CqListenerPtr& nativeptr( vrr[ index ] );
          gemfire::ManagedCqListenerGeneric* mg_listener =
            dynamic_cast<gemfire::ManagedCqListenerGeneric*>( nativeptr.ptr( ) );
          if (mg_listener != nullptr)
          {
            listners[ index ] =  (ICqListener<TKey, TResult>^) mg_listener->userptr( );
          }else 
          {
            gemfire::ManagedCqStatusListenerGeneric* mg_statuslistener =
              dynamic_cast<gemfire::ManagedCqStatusListenerGeneric*>( nativeptr.ptr( ) );
            if (mg_statuslistener != nullptr) {
              listners[ index ] =  (ICqStatusListener<TKey, TResult>^) mg_statuslistener->userptr( );
            }
            else {
              listners[ index ] =  nullptr;
            }
          }
        }
        return listners;
      }

    }
    }
  }
} //namespace 

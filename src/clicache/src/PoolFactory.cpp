/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

//#include "gf_includes.hpp"
#include "Pool.hpp"
#include "PoolFactory.hpp"

#include "impl/ManagedString.hpp"
#include "ExceptionTypes.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      //generic<class TKey, class TValue>
      PoolFactory^ PoolFactory/*<TKey, TValue>*/::SetFreeConnectionTimeout( Int32 connectionTimeout )
		  {
			  _GF_MG_EXCEPTION_TRY2/* due to auto replace */

			  NativePtr->setFreeConnectionTimeout( connectionTimeout );

			  _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
          return this;
		  }

      //generic<class TKey, class TValue>
		  PoolFactory^ PoolFactory/*<TKey, TValue>*/::SetLoadConditioningInterval( Int32 loadConditioningInterval )
		  {
			  _GF_MG_EXCEPTION_TRY2/* due to auto replace */

			  NativePtr->setLoadConditioningInterval( loadConditioningInterval );

			  _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
          return this;
		  }

      //generic<class TKey, class TValue>
		  PoolFactory^ PoolFactory/*<TKey, TValue>*/::SetSocketBufferSize( Int32 bufferSize )
      {
			  _GF_MG_EXCEPTION_TRY2/* due to auto replace */

			  NativePtr->setSocketBufferSize( bufferSize );

			  _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
          return this;
		  }

      //generic<class TKey, class TValue>
		  PoolFactory^ PoolFactory/*<TKey, TValue>*/::SetReadTimeout( Int32 timeout )
      {
			  _GF_MG_EXCEPTION_TRY2/* due to auto replace */

			  NativePtr->setReadTimeout( timeout );

			  _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
          return this;
		  }

      //generic<class TKey, class TValue>
		  PoolFactory^ PoolFactory/*<TKey, TValue>*/::SetMinConnections( Int32 minConnections )
      {
			  _GF_MG_EXCEPTION_TRY2/* due to auto replace */

			  NativePtr->setMinConnections( minConnections );

			  _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
          return this;
		  }

      //generic<class TKey, class TValue>
		  PoolFactory^ PoolFactory/*<TKey, TValue>*/::SetMaxConnections( Int32 maxConnections )
      {
			  _GF_MG_EXCEPTION_TRY2/* due to auto replace */

			  NativePtr->setMaxConnections( maxConnections );

			  _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
          return this;
		  }

      //generic<class TKey, class TValue>
		  PoolFactory^ PoolFactory/*<TKey, TValue>*/::SetIdleTimeout( Int32 idleTimeout )
      {
			  _GF_MG_EXCEPTION_TRY2/* due to auto replace */

			  NativePtr->setIdleTimeout( idleTimeout );

			  _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
          return this;
		  }

      //generic<class TKey, class TValue>
		  PoolFactory^ PoolFactory/*<TKey, TValue>*/::SetRetryAttempts( Int32 retryAttempts )
      {
			  _GF_MG_EXCEPTION_TRY2/* due to auto replace */

			  NativePtr->setRetryAttempts( retryAttempts );

			  _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
          return this;
		  }

      //generic<class TKey, class TValue>
		  PoolFactory^ PoolFactory/*<TKey, TValue>*/::SetPingInterval( Int32 pingInterval )
      {
			  _GF_MG_EXCEPTION_TRY2/* due to auto replace */

			  NativePtr->setPingInterval( pingInterval );

			  _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
          return this;
		  }

      //generic<class TKey, class TValue>
		  PoolFactory^ PoolFactory/*<TKey, TValue>*/::SetUpdateLocatorListInterval( Int32 updateLocatorListInterval )
      {
			  _GF_MG_EXCEPTION_TRY2/* due to auto replace */

			  NativePtr->setUpdateLocatorListInterval( updateLocatorListInterval );

			  _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
          return this;
		  }

      //generic<class TKey, class TValue>
      PoolFactory^ PoolFactory/*<TKey, TValue>*/::SetStatisticInterval( Int32 statisticInterval )
      {
			  _GF_MG_EXCEPTION_TRY2/* due to auto replace */

			  NativePtr->setStatisticInterval( statisticInterval );

			  _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
          return this;
		  }

      //generic<class TKey, class TValue>
      PoolFactory^ PoolFactory/*<TKey, TValue>*/::SetServerGroup( String^ group )
      {
			  _GF_MG_EXCEPTION_TRY2/* due to auto replace */

        ManagedString mg_servergroup( group );
			  NativePtr->setServerGroup( mg_servergroup.CharPtr );

			  _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
          return this;
		  }

      //generic<class TKey, class TValue>
		  PoolFactory^ PoolFactory/*<TKey, TValue>*/::AddLocator( String^ host, Int32 port )
      {
			  _GF_MG_EXCEPTION_TRY2/* due to auto replace */

        ManagedString mg_host( host );
			  NativePtr->addLocator( mg_host.CharPtr, port );

			  _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
          return this;
		  }

      //generic<class TKey, class TValue>
      PoolFactory^ PoolFactory/*<TKey, TValue>*/::AddServer( String^ host, Int32 port )
      {
			  _GF_MG_EXCEPTION_TRY2/* due to auto replace */

			  ManagedString mg_host( host );
			  NativePtr->addServer( mg_host.CharPtr, port );

			  _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
          return this;
		  }

      //generic<class TKey, class TValue>
		  PoolFactory^ PoolFactory/*<TKey, TValue>*/::SetSubscriptionEnabled( Boolean enabled )
      {
			  _GF_MG_EXCEPTION_TRY2/* due to auto replace */

			  NativePtr->setSubscriptionEnabled( enabled );

			  _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
          return this;
		  }

      //generic<class TKey, class TValue>
          PoolFactory^ PoolFactory/*<TKey, TValue>*/::SetPRSingleHopEnabled( Boolean enabled )
          {
            _GF_MG_EXCEPTION_TRY2/* due to auto replace */

              NativePtr->setPRSingleHopEnabled(enabled);

             _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
               return this;
          }

          //generic<class TKey, class TValue>
		  PoolFactory^ PoolFactory/*<TKey, TValue>*/::SetSubscriptionRedundancy( Int32 redundancy )
      {
			  _GF_MG_EXCEPTION_TRY2/* due to auto replace */

			  NativePtr->setSubscriptionRedundancy( redundancy );

			  _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
          return this;
		  }

      //generic<class TKey, class TValue>
		  PoolFactory^ PoolFactory/*<TKey, TValue>*/::SetSubscriptionMessageTrackingTimeout( Int32 messageTrackingTimeout )
      {
			  _GF_MG_EXCEPTION_TRY2/* due to auto replace */

			  NativePtr->setSubscriptionMessageTrackingTimeout( messageTrackingTimeout );

			  _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
          return this;
		  }

      //generic<class TKey, class TValue>
		  PoolFactory^ PoolFactory/*<TKey, TValue>*/::SetSubscriptionAckInterval( Int32 ackInterval )
      {
			  _GF_MG_EXCEPTION_TRY2/* due to auto replace */

			  NativePtr->setSubscriptionAckInterval( ackInterval );

			  _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
          return this;
		  }
      PoolFactory^ PoolFactory/*<TKey, TValue>*/::SetThreadLocalConnections( Boolean enabled )
      {
			  _GF_MG_EXCEPTION_TRY2

			  NativePtr->setThreadLocalConnections( enabled );

			  _GF_MG_EXCEPTION_CATCH_ALL2
          return this;
	  }
      //generic<class TKey, class TValue>
      PoolFactory^ PoolFactory/*<TKey, TValue>*/::SetMultiuserAuthentication( bool multiuserAuthentication )
      {
			  _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          NativePtr->setMultiuserAuthentication( multiuserAuthentication );

			  _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
          return this;
	   }

      //generic<class TKey, class TValue>
		  PoolFactory^ PoolFactory/*<TKey, TValue>*/::Reset()
      {
			  _GF_MG_EXCEPTION_TRY2/* due to auto replace */

			  NativePtr->reset( );

			  _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
          return this;
		  }

      //generic<class TKey, class TValue>
		  Pool/*<TKey, TValue>*/^ PoolFactory/*<TKey, TValue>*/::Create( String^ name )
      {
			  _GF_MG_EXCEPTION_TRY2/* due to auto replace */

        ManagedString mg_name( name );
        gemfire::PoolPtr & pool = NativePtr->create(mg_name.CharPtr);
        return Pool/*<TKey, TValue>*/::Create(pool.ptr());

			  _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
		  }
    }
  }
}
 } //namespace 

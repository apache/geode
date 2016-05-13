/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_includes.hpp"
#include "PoolM.hpp"
#include "PoolFactoryM.hpp"
#include "impl/ManagedString.hpp"
//#include "impl/SafeConvert.hpp"
#include "ExceptionTypesM.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      PoolFactory^ PoolFactory::SetFreeConnectionTimeout( Int32 connectionTimeout )
		  {
			  _GF_MG_EXCEPTION_TRY

			  NativePtr->setFreeConnectionTimeout( connectionTimeout );

			  _GF_MG_EXCEPTION_CATCH_ALL
          return this;
		  }

		  PoolFactory^ PoolFactory::SetLoadConditioningInterval( Int32 loadConditioningInterval )
		  {
			  _GF_MG_EXCEPTION_TRY

			  NativePtr->setLoadConditioningInterval( loadConditioningInterval );

			  _GF_MG_EXCEPTION_CATCH_ALL
          return this;
		  }

		  PoolFactory^ PoolFactory::SetSocketBufferSize( Int32 bufferSize )
      {
			  _GF_MG_EXCEPTION_TRY

			  NativePtr->setSocketBufferSize( bufferSize );

			  _GF_MG_EXCEPTION_CATCH_ALL
          return this;
		  }

		  PoolFactory^ PoolFactory::SetReadTimeout( Int32 timeout )
      {
			  _GF_MG_EXCEPTION_TRY

			  NativePtr->setReadTimeout( timeout );

			  _GF_MG_EXCEPTION_CATCH_ALL
          return this;
		  }

		  PoolFactory^ PoolFactory::SetMinConnections( Int32 minConnections )
      {
			  _GF_MG_EXCEPTION_TRY

			  NativePtr->setMinConnections( minConnections );

			  _GF_MG_EXCEPTION_CATCH_ALL
          return this;
		  }

		  PoolFactory^ PoolFactory::SetMaxConnections( Int32 maxConnections )
      {
			  _GF_MG_EXCEPTION_TRY

			  NativePtr->setMaxConnections( maxConnections );

			  _GF_MG_EXCEPTION_CATCH_ALL
          return this;
		  }

		  PoolFactory^ PoolFactory::SetIdleTimeout( Int32 idleTimeout )
      {
			  _GF_MG_EXCEPTION_TRY

			  NativePtr->setIdleTimeout( idleTimeout );

			  _GF_MG_EXCEPTION_CATCH_ALL
          return this;
		  }

		  PoolFactory^ PoolFactory::SetRetryAttempts( Int32 retryAttempts )
      {
			  _GF_MG_EXCEPTION_TRY

			  NativePtr->setRetryAttempts( retryAttempts );

			  _GF_MG_EXCEPTION_CATCH_ALL
          return this;
		  }

		  PoolFactory^ PoolFactory::SetPingInterval( Int32 pingInterval )
      {
			  _GF_MG_EXCEPTION_TRY

			  NativePtr->setPingInterval( pingInterval );

			  _GF_MG_EXCEPTION_CATCH_ALL
          return this;
		  }

      PoolFactory^ PoolFactory::SetStatisticInterval( Int32 statisticInterval )
      {
			  _GF_MG_EXCEPTION_TRY

			  NativePtr->setStatisticInterval( statisticInterval );

			  _GF_MG_EXCEPTION_CATCH_ALL
          return this;
		  }

      PoolFactory^ PoolFactory::SetServerGroup( String^ group )
      {
			  _GF_MG_EXCEPTION_TRY

        ManagedString mg_servergroup( group );
			  NativePtr->setServerGroup( mg_servergroup.CharPtr );

			  _GF_MG_EXCEPTION_CATCH_ALL
          return this;
		  }

		  PoolFactory^ PoolFactory::AddLocator( String^ host, Int32 port )
      {
			  _GF_MG_EXCEPTION_TRY

        ManagedString mg_host( host );
			  NativePtr->addLocator( mg_host.CharPtr, port );

			  _GF_MG_EXCEPTION_CATCH_ALL
          return this;
		  }

      PoolFactory^ PoolFactory::AddServer( String^ host, Int32 port )
      {
			  _GF_MG_EXCEPTION_TRY

			  ManagedString mg_host( host );
			  NativePtr->addServer( mg_host.CharPtr, port );

			  _GF_MG_EXCEPTION_CATCH_ALL
          return this;
		  }

		  PoolFactory^ PoolFactory::SetSubscriptionEnabled( Boolean enabled )
      {
			  _GF_MG_EXCEPTION_TRY

			  NativePtr->setSubscriptionEnabled( enabled );

			  _GF_MG_EXCEPTION_CATCH_ALL
          return this;
		  }

      PoolFactory^ PoolFactory::SetPRSingleHopEnabled( Boolean enabled )
      {
        _GF_MG_EXCEPTION_TRY

          NativePtr->setPRSingleHopEnabled(enabled);

         _GF_MG_EXCEPTION_CATCH_ALL
           return this;
      }

		  PoolFactory^ PoolFactory::SetSubscriptionRedundancy( Int32 redundancy )
      {
			  _GF_MG_EXCEPTION_TRY

			  NativePtr->setSubscriptionRedundancy( redundancy );

			  _GF_MG_EXCEPTION_CATCH_ALL
          return this;
		  }

		  PoolFactory^ PoolFactory::SetSubscriptionMessageTrackingTimeout( Int32 messageTrackingTimeout )
      {
			  _GF_MG_EXCEPTION_TRY

			  NativePtr->setSubscriptionMessageTrackingTimeout( messageTrackingTimeout );

			  _GF_MG_EXCEPTION_CATCH_ALL
          return this;
		  }

		  PoolFactory^ PoolFactory::SetSubscriptionAckInterval( Int32 ackInterval )
      {
			  _GF_MG_EXCEPTION_TRY

			  NativePtr->setSubscriptionAckInterval( ackInterval );

			  _GF_MG_EXCEPTION_CATCH_ALL
          return this;
		  }
       PoolFactory^ PoolFactory::SetThreadLocalConnections( Boolean enabled )
      {
			  _GF_MG_EXCEPTION_TRY

			  NativePtr->setThreadLocalConnections( enabled );

			  _GF_MG_EXCEPTION_CATCH_ALL
          return this;
	  }
      PoolFactory^ PoolFactory::SetMultiuserAuthentication( bool multiuserAuthentication )
      {
			  _GF_MG_EXCEPTION_TRY

          NativePtr->setMultiuserAuthentication( multiuserAuthentication );

			  _GF_MG_EXCEPTION_CATCH_ALL
          return this;
	   }

		  PoolFactory^ PoolFactory::Reset()
      {
			  _GF_MG_EXCEPTION_TRY

			  NativePtr->reset( );

			  _GF_MG_EXCEPTION_CATCH_ALL
          return this;
		  }

		  Pool^ PoolFactory::Create( String^ name )
      {
			  _GF_MG_EXCEPTION_TRY

        ManagedString mg_name( name );
        gemfire::PoolPtr & pool = NativePtr->create(mg_name.CharPtr);
        return Pool::Create(pool.ptr());

			  _GF_MG_EXCEPTION_CATCH_ALL
		  }
    }
  }
}

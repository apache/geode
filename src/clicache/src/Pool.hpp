/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/Pool.hpp>
#include "impl/NativeWrapper.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache {
        
    namespace Generic
    {
      //generic<class TKey, class TResult>
      //ref class Properties;

      generic<class TKey, class TResult>
      ref class QueryService;

      ref class Cache;

      /// <summary>
      /// A pool of connections.
      /// </summary>
      /// <remarks>
      /// A pool of connections from a GemFire client to a set of GemFire servers.
      /// </remarks>
     // generic<class TKey, class TValue>
      public ref class Pool sealed
				: public Generic::Internal::SBWrap<gemfire::Pool>
      {
      public:

		/// <summary>
	    /// Get the name of the pool
		/// </summary>
	    property String^ Name
		{
		  String^ get( );
		}

		/// <summary>
		/// Returns the connection timeout of this pool.
		/// </summary>
		property Int32 FreeConnectionTimeout
		{
		  Int32 get( );
		}

		/// <summary>
		/// Returns the load conditioning interval of this pool.
		/// </summary>
		property Int32 LoadConditioningInterval
		{
      Int32 get( );
		}

		/// <summary>
		/// Returns the socket buffer size of this pool.
		/// </summary>
    property Int32 SocketBufferSize
		{
		  Int32 get( );
		}
 
		/// <summary>
		/// Returns the read timeout of this pool.
		/// </summary>
    property Int32 ReadTimeout
		{
		  Int32 get( );
		}

		/// <summary>
		/// Get the minimum connections for this pool.
		/// </summary>
    property Int32 MinConnections
		{
		  Int32 get( );
		}

		/// <summary>
		/// Get the maximum connections for this pool.
		/// </summary>
		property Int32 MaxConnections
		{
		  Int32 get( );
		}

		/// <summary>
		/// Get the Idle connection timeout for this pool.
		/// </summary>
    property Int32 IdleTimeout
		{
		  Int32 get( );
		}

		/// <summary>
		/// Get the ping interval for this pool.
		/// </summary>
		property Int32 PingInterval
		{
		  Int32 get( );
		}

    /// <summary>
		/// Get the update locator list interval for this pool.
		/// </summary>
		property Int32 UpdateLocatorListInterval
		{
		  Int32 get( );
		}

		/// <summary>
		/// Get the statistic interval for this pool.
		/// </summary>
    property Int32 StatisticInterval
		{
		  Int32 get( );
		}

		/// <summary>
		/// Get the retry attempts for this pool.
		/// </summary>
		property Int32 RetryAttempts
		{
		  Int32 get( );
		}

		/// <summary>
		/// Returns the true if a server-to-client subscriptions are enabled on this pool.
		/// </summary>
		property Boolean SubscriptionEnabled
		{
		  Boolean get( );
		}

        /// <summary>
		/// Returns the true if a pr-single-hop is set to true on this pool.
		/// </summary>
		property Boolean PRSingleHopEnabled
		{
		  Boolean get( );
		}

		/// <summary>
		/// Returns the subscription redundancy level of this pool.
		/// </summary>
		property Int32 SubscriptionRedundancy
		{
		  Int32 get( );
		}

		/// <summary>
		/// Returns the subscription message tracking timeout of this pool.
		/// </summary>
		property Int32 SubscriptionMessageTrackingTimeout
		{
		  Int32 get( );
		}

		/// <summary>
		/// Returns the subscription ack interval of this pool.
		/// </summary>
    property Int32 SubscriptionAckInterval
		{
		  Int32 get( );
		}

		/// <summary>
		/// Returns the server group of this pool.
		/// </summary>
		property String^ ServerGroup
		{
		  String^ get( );
		}

		/// <summary>
		/// Returns an unmodifiable list of locators
		/// this pool is using. Each locator is either one
		/// added explicitly when the pool was created or
		/// were discovered using the explicit locators.
		/// </summary>
		/// <remarks>
		/// If a pool has no locators then it can not discover servers or locators at runtime.
		/// </remarks>
		property array< String^ >^ Locators
		{
		  array< String^ >^ get( );
		}

		/// <summary>
		/// Returns an unmodifiable list of
		/// servers this pool is using. These servers were added
		/// explicitly when the pool was created.
		property array< String^ >^ Servers
		{
		  array< String^ >^ get( );
		}

		/// <summary>
		/// Returns the true if ThreadLocalConnections are enabled on this pool.
		/// </summary>
		property Boolean ThreadLocalConnections
		{
		  Boolean get( );
		}

		/// <summary>
		/// Returns <code>true</code> if multiuser authentication is enabled on this pool.
		/// <summary>
    property bool MultiuserAuthentication
    {
      bool get( );
    }
		/// <summary>
		/// Destroys this pool closing any connections it produced.
		/// </summary>
		/// <param name="keepAlive">
		/// whether the server should keep the durable client's
		/// subscriptions alive for the timeout period
		/// </param>
		/// <exception cref="IllegalStateException">
		/// if the pool is still in use
		/// </exception>
		void Destroy( Boolean keepAlive );

		/// <summary>
		/// Destroys this pool closing any connections it produced.
		/// </summary>
		/// <exception cref="IllegalStateException">
		/// if the pool is still in use
		/// </exception>
		void Destroy( );
		
		/// <summary>
		/// Indicates whether this Pool has been
		/// destroyed.
		/// </summary>
		property Boolean Destroyed
		{
		  Boolean get( );
		}

		/// <summary>
		/// Returns the QueryService for this Pool.
		/// </summary>
		/// <remarks>
		/// The query operations performed using this QueryService will be executed
		/// on the servers that are associated with this pool.
		/// To perform Query operation on the local cache obtain the QueryService
		/// instance from the Cache.
		/// </remarks>
    generic<class TKey, class TResult>
    QueryService<TKey, TResult>^ GetQueryService();

    void ReleaseThreadLocalConnection();

     /// <summary>  
     /// Returns the approximate number of pending subscription events maintained at
     /// server for this durable client pool at the time it (re)connected to the
     /// server. Server would start dispatching these events to this durable client
     /// pool when it receives {@link Cache#readyForEvents()} from it.
     /// <p>
     /// Durable clients can call this method on reconnect to assess the amount of
     /// 'stale' data i.e. events accumulated at server while this client was away
     /// and, importantly, before calling {@link Cache#readyForEvents()}.
     /// <p>
     /// Any number of invocations of this method during a single session will
     /// return the same value.
     /// <p>
     /// It may return a zero value if there are no events pending at server for
     /// this client pool. A negative value returned tells us that no queue was
     /// available at server for this client pool.
     /// <p>
     /// A value -1 indicates that this client pool reconnected to server after its
     /// 'durable-client-timeout' period elapsed and hence its subscription queue at
     /// server was removed, possibly causing data loss.
     /// <p>
     /// A value -2 indicates that this client pool connected to server for the
     /// first time.
     /// 
     /// @return int The number of subscription events maintained at server for this
     ///         durable client pool at the time this pool (re)connected. A negative
     ///         value indicates no queue was found for this client pool.
     /// @throws IllegalStateException
     ///           If called by a non-durable client or if invoked any time after
     ///           invocation of {@link Cache#readyForEvents()}.
     /// @since 8.1
     ///
     /// </summary>
      property Int32 PendingEventCount
      {
        Int32 get( );
      }

		  internal:
      /// <summary>
      /// Internal factory function to wrap a native object pointer inside
      /// this managed class with null pointer check.
      /// </summary>
      /// <param name="nativeptr">The native object pointer</param>
      /// <returns>
      /// The managed wrapper object; null if the native pointer is null.
      /// </returns>
      inline static Pool^ Create( gemfire::Pool* nativeptr )
      {
        return ( nativeptr != nullptr ?
          gcnew Pool( nativeptr ) : nullptr );
      }

      private:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline Pool( gemfire::Pool* nativeptr )
          : SBWrap( nativeptr ) { }
      };

    }
  }
}
 } //namespace 

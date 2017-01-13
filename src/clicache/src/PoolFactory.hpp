/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/PoolFactory.hpp>
//#include "impl/NativeWrapper.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
     // generic<class TKey, class TValue>
      ref class Pool;

      /// <summary>
      /// This interface provides for the configuration and creation of instances of Pool.
      /// </summary>
     // generic<class TKey, class TValue>
      public ref class PoolFactory sealed
        : public Internal::SBWrap<gemfire::PoolFactory>
      {
      public:

		  /// <summary>
		  /// Sets the free connection timeout for this pool.
		  /// </summary>
		  /// <remarks>
		  /// If the pool has a max connections setting, operations will block
		  /// if all of the connections are in use. The free connection timeout
		  /// specifies how long those operations will block waiting for
		  /// a free connection before receiving an AllConnectionsInUseException.
		  /// If max connections is not set this setting has no effect.
          /// </remarks>
		  /// <param>
		  /// connectionTimeout the connection timeout in milliseconds
		  /// </param>
		  /// <exception>
		  /// IllegalArgumentException if connectionTimeout 
		  /// is less than or equal to 0.
		  /// </exception>
		  PoolFactory^ SetFreeConnectionTimeout( Int32 connectionTimeout );

		  /// <summary>
		  /// Sets the load conditioning interval for this pool.
		  /// </summary>
		  /// <remarks>
		  /// This interval controls how frequently the pool will check to see if
		  /// a connection to a given server should be moved to a different
		  /// server to improve the load balance.
		  /// </remarks>
		  /// <param>
		  /// loadConditioningInterval the connection lifetime in milliseconds
		  /// A value of -1 disables load conditioning.
		  /// </param>
		  /// <exception>
		  /// throws IllegalArgumentException if connectionLifetime
		  /// is less than -1.
		  /// </exception>
      PoolFactory^ SetLoadConditioningInterval( Int32 loadConditioningInterval );

		  /// <summary>
		  /// Sets the socket buffer size for each connection made in this pool.
		  /// </summary>
		  /// <remarks>
		  /// Large messages can be received and sent faster when this buffer is larger.
		  /// Larger buffers also optimize the rate at which servers can send events
		  /// for client subscriptions.
		  /// </remarks>
		  /// <param>
		  /// bufferSize the size of the socket buffers used for reading and
		  /// writing on each connection in this pool.
		  /// </param>
		  /// <exception>
		  /// throws IllegalArgumentException if bufferSize
		  /// is less than or equal to 0.
		  /// </exception>
		  PoolFactory^ SetSocketBufferSize( Int32 bufferSize );

		  /// <summary>
		  /// Sets the number of milliseconds to wait for a response from a server before
		  /// timing out the operation and trying another server (if any are available).
		  /// </summary>
		  /// <param>
		  /// timeout number of milliseconds to wait for a response from a server
		  /// </param>
		  /// <exception>
		  /// throws IllegalArgumentException if timeout
		  /// is less than or equal to 0.
		  /// </exception>
		  PoolFactory^ SetReadTimeout( Int32 timeout );

		  /// <summary>
		  /// Set the minimum number of connections to keep available at all times.
		  /// </summary>
		  /// <remarks>
		  /// When the pool is created, it will create this many connections.
		  /// If 0 then connections will not be made until an actual operation
		  /// is done that requires client-to-server communication.
		  /// </remarks>
		  /// <param>
		  /// minConnections the initial number of connections this pool will create.
		  /// </param>
		  /// <exception>
		  /// throws IllegalArgumentException if minConnections is less than 0.
		  /// </exception>
		  PoolFactory^ SetMinConnections( Int32 minConnections );

		  /// <summary>
		  /// Set the max number of client to server connections that the pool will create.
		  /// </summary>
		  /// <remarks>
		  /// If all of the connections are in use, an operation requiring a client to
		  /// server connection will block until a connection is available.
		  /// see setFreeConnectionTimeout(int)
		  /// </remarks>
		  /// <param>
		  /// maxConnections the maximum number of connections in the pool.
		  /// -1 indicates that there is no maximum number of connections.
		  /// </param>
		  /// <exception>
		  /// throws IllegalArgumentException if maxConnections is less than minConnections.
		  /// </exception>
		  PoolFactory^ SetMaxConnections( Int32 maxConnections );

		  /// <summary>
		  /// Set the amount of time a connection can be idle before expiring the connection.
		  /// </summary>
		  /// <remarks>
		  /// If the pool size is greater than the minimum specified, connections which have
		  /// been idle for longer than the idleTimeout will be closed.
		  /// </remarks>
		  /// <param>
		  /// idleTimeout The amount of time in milliseconds that an idle connection
		  /// should live before expiring. -1 indicates that connections should never expire.
		  /// </param>
		  /// <exception>
		  /// throws IllegalArgumentException if idleTimout is less than 0.
		  /// </exception>
		  PoolFactory^ SetIdleTimeout( Int32 idleTimeout );

		  /// <summary>
		  /// Set the number of times to retry a request after timeout/exception.
		  /// </summary>
		  /// <param>
		  /// retryAttempts The number of times to retry a request
		  /// after timeout/exception. -1 indicates that a request should be
		  /// tried against every available server before failing.
		  /// </param>
		  /// <exception>
		  /// throws IllegalArgumentException if idleTimout is less than 0.
		  /// </exception>
		  PoolFactory^ SetRetryAttempts( Int32 retryAttempts );

		  /// <summary>
		  /// Set how often to ping servers to verify that they are still alive.
		  /// </summary>
		  /// <remarks>
		  /// Each server will be sent a ping every pingInterval if there has not
		  /// been any other communication with the server.
		  /// These pings are used by the server to monitor the health of
		  /// the client. Make sure that the pingInterval is less than the
		  /// maximum time between pings allowed by the bridge server.
		  /// see in CacheServer: setMaximumTimeBetweenPings(int)
		  /// </remarks>
		  /// <param>
		  /// pingInterval The amount of time in milliseconds between pings.
		  /// </param>
		  /// <exception>
		  /// throws IllegalArgumentException if pingInterval is less than 0.
		  /// </exception>
		  PoolFactory^ SetPingInterval( Int32 pingInterval );

      /// <summary>
		  /// Set how often to update locator list from locator
		  /// </summary>
		  /// <param>
		  /// updateLocatorListInterval The amount of time in milliseconds between
      /// updating locator list. If its set to 0 then client will not update
      /// the locator list.
		  /// </param>
		  /// <returns>
      /// a instance of <c>CacheFactory</c> 
      /// </returns>
		  PoolFactory^ SetUpdateLocatorListInterval( Int32 updateLocatorListInterval );

		  /// <summary>
		  /// Set how often to send client statistics to the server.
		  /// </summary>
		  /// <remarks>
		  /// Doing this allows gfmon to monitor clients.
		  /// A value of -1 disables the sending of client statistics
		  /// to the server.
          /// </remarks>
		  /// <param>
		  /// statisticInterval The amount of time in milliseconds between
		  /// sends of client statistics to the server.
		  /// </param>
		  /// <exception>
		  /// throws IllegalArgumentException if statisticInterval
		  /// is less than -1.
		  /// </exception>
      PoolFactory^ SetStatisticInterval( Int32 statisticInterval);

		  /// <summary>
		  /// Configures the group that all servers this pool connects to must belong to.
		  /// </summary>
		  /// <param>
		  /// group the server group that this pool will connect to.
		  /// If null or "" then all servers will be connected to.
		  /// </param>
      PoolFactory^ SetServerGroup( String^ group );

		  /// <summary>
		  /// Add a locator, given its host and port, to this factory.
		  /// </summary>
		  /// <remarks>
		  /// The locator must be a server locator and will be used to discover other running
		  /// bridge servers and locators.
		  /// </remarks>
		  /// <param>
		  /// host the host name or ip address that the locator is listening on.
		  /// </param>
		  /// <param>
		  /// port the port that the locator is listening on
		  /// </param>
		  /// <exception>
		  /// throws IllegalArgumentException if host is an unknown host
		  /// or if port is outside the valid range of [1..65535] inclusive.
		  /// </exception>
		  /// <exception>
		  /// throws IllegalStateException if a locator has already been added to this factory.
		  /// </exception>
		  PoolFactory^ AddLocator( String^ host, Int32 port );

		  /// <summary>
		  /// Add a server, given its host and port, to this factory.
		  /// </summary>
		  /// <remarks>
		  /// The server must be a bridge server and this client will
		  /// directly connect to without consulting a server locator.
		  /// </remarks>
		  /// <param>
		  /// host the host name or ip address that the server is listening on.
		  /// </param>
		  /// <param>
		  /// port the port that the server is listening on
		  /// </param>
          /// <exception>
		  /// throws IllegalArgumentException if host is an unknown host
		  /// or if port is outside the valid range of [1..65535] inclusive.
		  /// </exception>
		  /// <exception>
		  /// throws IllegalStateException if a server has already been added to this factory.
		  /// </exception>
      PoolFactory^ AddServer( String^ host, Int32 port );

		  /// <summary>
		  /// Enable subscriptions.
		  /// </summary>
		  /// <remarks>
		  /// If set to true then the created pool will have server-to-client
		  /// subscriptions enabled. If set to false then all Subscription*
		  /// attributes are ignored at create time.
		  /// </remarks>
		  PoolFactory^ SetSubscriptionEnabled( Boolean enabled );

           /// <summary>
		  /// By default SetPRSingleHopEnabled is true.
		  /// </summary>
		  /// <remarks>
		  /// The client is aware of location of partitions on servers hosting
          /// Using this information, the client routes the client cache operations
		  /// directly to the server which is hosting the required partition for the
		  /// cache operation. 
          /// If SetPRSingleHopEnabled is false the client can do an extra hop on servers
          /// to go to the required partition for that cache operation.
          /// The SetPRSingleHopEnabled avoids extra hops only for following cache operations :
          /// put, get & destroy operations.
		  /// </remarks>
		  PoolFactory^ SetPRSingleHopEnabled( Boolean enabled );

		  /// <summary>
		  /// Sets the redundancy level for this pools server-to-client subscriptions.
		  /// </summary>
		  /// <remarks>
		  /// If 0 then no redundant copies will be kept on the servers.
		  /// Otherwise an effort will be made to maintain the requested number of
		  /// copies of the server-to-client subscriptions. At most one copy per server will
		  /// be made up to the requested level.
		  /// </remarks>
		  /// <param>
		  /// redundancy the number of redundant servers for this client's subscriptions.
		  /// </param>
		  /// <exception>
		  /// throws IllegalArgumentException if redundancyLevel is less than -1.
		  /// </exception>
		  PoolFactory^ SetSubscriptionRedundancy( Int32 redundancy );

		  /// <summary>
		  /// Sets the messageTrackingTimeout attribute which is the time-to-live period,
		  /// in milliseconds, for subscription events the client has received from the server.
		  /// </summary>
		  /// <remarks>
		  /// It's used to minimize duplicate events. Entries that have not been modified
		  /// for this amount of time are expired from the list.
		  /// </remarks>
		  /// <param>
		  /// messageTrackingTimeout number of milliseconds to set the timeout to.
		  /// </param>
		  /// <exception>
		  /// throws IllegalArgumentException if messageTrackingTimeout is less than or equal to 0.
		  /// </exception>
		  PoolFactory^ SetSubscriptionMessageTrackingTimeout( Int32 messageTrackingTimeout );

		  /// <summary>
		  /// Sets the is the interval in milliseconds to wait before sending
		  /// acknowledgements to the bridge server for events received from the server subscriptions.
		  /// </summary>
		  /// <param>
		  /// ackInterval number of milliseconds to wait before sending event acknowledgements.
		  /// </param>
		  /// <exception>
		  /// throws IllegalArgumentException if ackInterval is less than or equal to 0.
		  /// </exception>
		  PoolFactory^ SetSubscriptionAckInterval( Int32 ackInterval );

		  /// <summary>
		  /// Enable ThreadLocalConnection.
		  /// </summary>
		  /// <remarks>
		  /// Sets the thread local connections policy for this pool.
		  /// If true then any time a thread goes to use a connection
		  /// from this pool it will check a thread local cache and see if it already
		  /// has a connection in it. If so it will use it. If not it will get one from
		  /// this pool and cache it in the thread local. This gets rid of thread contention
		  /// for the connections but increases the number of connections the servers see.
		  /// If false then connections are returned to the pool as soon
		  /// as the operation being done with the connection completes. This allows
		  /// connections to be shared amonst multiple threads keeping the number of
		  /// connections down.
		  /// </remarks>
		  PoolFactory^ SetThreadLocalConnections( Boolean enabled );

      /// <summary>
		  /// Sets whether pool is in multiuser mode
      /// If its in multiuser mode then app needs to get instance of cache from pool.getCache("creds"), to do the operations on cache.
		  /// </summary>
		  /// <param>
		  /// multiuserAuthentication should be true/false. Default value is false;
		  /// </param>
      PoolFactory^ SetMultiuserAuthentication( bool multiuserAuthentication );

		  /// <summary>
		  /// Resets the configuration of this factory to its defaults.
		  /// </summary>
		  PoolFactory^ Reset();

		  /// <summary>
		  /// Create a new Pool for connecting a client to a set of GemFire Cache Servers.
		  /// using this factory's settings for attributes.
		  /// </summary>
		  /// <param>
		  /// name the name of the pool, used when connecting regions to it
		  /// </param>
		  /// <exception>
		  /// throws IllegalStateException if a pool with name already exists
		  /// throws IllegalStateException if a locator or server has not been added.
                  /// </exception>
		  Pool/*<TKey, TValue>*/^ Create( String^ name );

    internal:

      /// <summary>
      /// Internal factory function to wrap a native object pointer inside
      /// this managed class with null pointer check.
      /// </summary>
      /// <param name="nativeptr">The native object pointer</param>
      /// <returns>
      /// The managed wrapper object; null if the native pointer is null.
      /// </returns>
      inline static PoolFactory/*<TKey, TValue>*/^ Create( gemfire::PoolFactory* nativeptr )
      {
        return ( nativeptr != nullptr ?
          gcnew PoolFactory( nativeptr ) : nullptr );
      }

	  private:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline PoolFactory( gemfire::PoolFactory* nativeptr )
          : SBWrap( nativeptr ) { }
      };

    }
  }
}

 } //namespace 

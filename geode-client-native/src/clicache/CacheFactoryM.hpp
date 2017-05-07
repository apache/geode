/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "cppcache/CacheFactory.hpp"
#include "PropertiesM.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      ref class Cache;
      ref class CacheAttributes;
      ref class DistributedSystem;

      /// <summary>
      /// A factory class that must be used to obtain instance of <see cref="Cache" />.
      /// </summary>
      /// <remarks>
      /// To create a new cache instance, use <see cref="CacheFactory.CreateCacheFactory" />.
      /// <para>
      /// To get an existing unclosed cache instance, use <see cref="CacheFactory.GetInstance" />.
      /// </para>
      /// </remarks>
      [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public ref class CacheFactory :public Internal::SBWrap<gemfire::CacheFactory>
      {
      public:

        /// <summary>
        /// A factory class that must be used to obtain instance of <see cref="Cache" />.
        /// This should be called once. Using this one can set default values of <see cref="Pool" />.
        /// </summary>
        /// <param name="dsProps">Properties which are applicable at client level.</param>
        static CacheFactory^ CreateCacheFactory(Properties^ dsProps);

        /// <summary>
        /// A factory class that must be used to obtain instance of <see cref="Cache" />.
        /// This should be called once. Using this one can set default values of <see cref="Pool" />.
        /// </summary>       
        static CacheFactory^ CreateCacheFactory();

        /// <summary>
        /// To create the instance of <see cref="Cache" />.
        /// </summary>
        Cache^ Create();

        /// <summary>
        /// Creates a new cache using the specified system.
        /// </summary>
        /// <param name="name">the name to associate with the new cache</param>
        /// <param name="system">
        /// a DistributedSystem obtained by calling
        /// <see cref="DistributedSystem.Connect" />
        /// </param>
        /// <returns>
        /// a <c>Cache</c> that uses the specified <c>DistributedSystem</c>
        /// for distribution.
        /// </returns>
        /// <exception cref="IllegalArgumentException">
        /// If <c>system</c> is not connected
        /// ( <see cref="DistributedSystem.IsConnected" /> ) or name is null.
        /// </exception>
        /// <exception cref="CacheExistsException">
        /// If an open cache already exists.
        /// </exception>
        /// <deprecated>
        /// as of NativeClient 3.5, use <see cref="CacheFactory.CreateCacheFactory" /> instead.
        /// </deprecated>
        static Cache^ Create( String^ name, DistributedSystem^ system );

        /// <summary>
        /// Creates a new cache using the specified system using parameters
        /// from the given <a href="cacheXml.html">XML</a> file.
        /// </summary>
        /// <param name="name">the name to associate with the new cache</param>
        /// <param name="system">
        /// a DistributedSystem obtained by calling
        /// <see cref="DistributedSystem.Connect" />
        /// </param>
        /// <param name="cacheXml">
        /// name of the cache configuration XML file
        /// </param>
        /// <returns>
        /// a <c>Cache</c> that uses the specified <c>DistributedSystem</c>
        /// for distribution.
        /// </returns>
        /// <exception cref="IllegalArgumentException">
        /// If <c>system</c> is not <see cref="DistributedSystem.IsConnected"/>
        /// or name is null
        /// </exception>
        /// <exception cref="CacheExistsException">
        /// ff an open cache already exists
        /// </exception>
        /// <exception cref="CacheXmlException">
        /// if something went wrong while parsing the XML
        /// </exception>
        /// <exception cref="IllegalStateException">
        /// if the XML file is well-formed but not valid (consistent)
        /// </exception>
        /// <deprecated>
        /// as of NativeClient 3.5, use <see cref="CacheFactory.CreateCacheFactory" /> instead.
        /// </deprecated>
        static Cache^ Create( String^ name, DistributedSystem^ system,
          String^ cacheXml );

        /// <summary>
        /// Creates a new cache using the specified system using the given
        /// <c>CacheAttributes</c>.
        /// </summary>
        /// <param name="name">the name to associate with the new cache</param>
        /// <param name="system">
        /// a DistributedSystem obtained by calling
        /// <see cref="DistributedSystem.Connect" />
        /// </param>
        /// <param name="attributes">
        /// optional <c>CacheAttributes</c> for this cache
        /// </param>
        /// <returns>
        /// a <c>Cache</c> that uses the specified <c>DistributedSystem</c>
        /// for distribution.
        /// </returns>
        /// <exception cref="IllegalArgumentException">
        /// If <c>system</c> is not <see cref="DistributedSystem.IsConnected"/>
        /// or name is null
        /// </exception>
        /// <exception cref="CacheExistsException">
        /// ff an open cache already exists
        /// </exception>
        /// <deprecated>
        /// as of NativeClient 3.5, use <see cref="CacheFactory.CreateCacheFactory" /> instead.
        /// </deprecated>
        static Cache^ Create( String^ name, DistributedSystem^ system,
          CacheAttributes^ attributes );

        /// <summary>
        /// Creates a new cache using the specified system using parameters
        /// from the given <a href="cacheXml.html">XML</a> file and with
        /// the given <c>CacheAttributes</c>.
        /// </summary>
        /// <param name="name">the name to associate with the new cache</param>
        /// <param name="system">
        /// a DistributedSystem obtained by calling
        /// <see cref="DistributedSystem.Connect" />
        /// </param>
        /// <param name="cacheXml">
        /// name of the cache configuration XML file
        /// </param>
        /// <param name="attributes">
        /// optional <c>CacheAttributes</c> for this cache; these
        /// override the ones provided in <c>cacheXml</c>.
        /// </param>
        /// <returns>
        /// a <c>Cache</c> that uses the specified <c>DistributedSystem</c>
        /// for distribution.
        /// </returns>
        /// <exception cref="IllegalArgumentException">
        /// If <c>system</c> is not <see cref="DistributedSystem.IsConnected"/>
        /// or name is null
        /// </exception>
        /// <exception cref="CacheExistsException">
        /// ff an open cache already exists
        /// </exception>
        /// <exception cref="CacheXmlException">
        /// if something went wrong while parsing the XML
        /// </exception>
        /// <exception cref="IllegalStateException">
        /// if the XML file is well-formed but not valid (consistent)
        /// </exception>
        /// <deprecated>
        /// as of NativeClient 3.5, use <see cref="CacheFactory.CreateCacheFactory" /> instead.
        /// </deprecated>
        static Cache^ Create( String^ name, DistributedSystem^ system,
          String^ cacheXml, CacheAttributes^ attributes );

        /// <summary>
        /// Gets the instance of <see cref="Cache" /> produced by an
        /// earlier call to <see cref="CacheFactory.Create" />.
        /// </summary>
        /// <param name="system">
        /// the <see cref="DistributedSystem" /> the cache was created with.
        /// </param>
        /// <returns>the <see cref="Cache" /> associated with the specified system.</returns>
        /// <exception cref="IllegalArgumentException">
        /// if the distributed system argument is null
        /// </exception>
        /// <exception cref="CacheClosedException">
        /// if a cache has not been created or the created one is closed
        /// ( <see cref="Cache.IsClosed" /> )
        /// </exception>
        /// <exception cref="EntryNotFoundException">
        /// if a cache with specified system not found
        /// </exception>
        static Cache^ GetInstance( DistributedSystem^ system );

        /// <summary>
        /// Gets the instance of <see cref="Cache" /> produced by an
        /// earlier call to <see cref="CacheFactory.Create" />, even if it has been closed.
        /// </summary>
        /// <param name="system">
        /// the <see cref="DistributedSystem" /> the cache was created with.
        /// </param>
        /// <returns>
        /// the <c>Cache</c> associated with the specified system.
        /// </returns>
        /// <exception cref="IllegalArgumentException">
        /// if the distributed system argument is null
        /// </exception>
        /// <exception cref="CacheClosedException">
        /// if a cache has not been created.
        /// </exception>
        /// <exception cref="EntryNotFoundException">
        /// if a cache with specified system not found
        /// </exception>
        static Cache^ GetInstanceCloseOk( DistributedSystem^ system );

        /// <summary>
        /// Gets an arbitrary open instance of <see cref="Cache" /> produced by an
        /// earlier call to <see cref="CacheFactory.Create" />.
        /// </summary>
        /// <exception cref="CacheClosedException">
        /// if a cache has not been created or the only created one is
        /// closed ( <see cref="Cache.IsClosed" /> )
        /// </exception>
        /// <exception cref="EntryNotFoundException">
        /// if a cache with specified system not found
        /// </exception>
        static Cache^ GetAnyInstance( );

        /// <summary>
        /// Set allocators for non default Microsoft CRT versions.
        /// </summary>
        static void SetNewAndDelete()
        {
          gemfire::setNewAndDelete( & operator new, & operator delete );
        }

        /// <summary>
        /// Returns the version of the cache implementation.
        /// For the 1.0 release of GemFire, the string returned is <c>1.0</c>.
        /// </summary>
        /// <returns>the version of the cache implementation as a <c>String</c></returns>
        static property String^ Version
        {
          static String^ get( );
        }

        /// <summary>
        /// Returns the product description string including product name and version.
        /// </summary>
        static property String^ ProductDescription
        {
          static String^ get( );
        }

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
		  CacheFactory^ SetFreeConnectionTimeout( Int32 connectionTimeout );

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
      /// <returns>
      /// a instance of <c>CacheFactory</c> 
      /// </returns>
      CacheFactory^ SetLoadConditioningInterval( Int32 loadConditioningInterval );

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
      /// <returns>
      /// a instance of <c>CacheFactory</c> 
      /// </returns>
		  CacheFactory^ SetSocketBufferSize( Int32 bufferSize );

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
      /// <returns>
      /// a instance of <c>CacheFactory</c> 
      /// </returns>
		  CacheFactory^ SetReadTimeout( Int32 timeout );

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
      /// <returns>
      /// a instance of <c>CacheFactory</c> 
      /// </returns>
		  CacheFactory^ SetMinConnections( Int32 minConnections );

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
      /// <returns>
      /// a instance of <c>CacheFactory</c> 
      /// </returns>
		  CacheFactory^ SetMaxConnections( Int32 maxConnections );

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
      /// <returns>
      /// a instance of <c>CacheFactory</c> 
      /// </returns>
		  CacheFactory^ SetIdleTimeout( Int32 idleTimeout );

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
      /// <returns>
      /// a instance of <c>CacheFactory</c> 
      /// </returns>
		  CacheFactory^ SetRetryAttempts( Int32 retryAttempts );

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
      /// <returns>
      /// a instance of <c>CacheFactory</c> 
      /// </returns>
		  CacheFactory^ SetPingInterval( Int32 pingInterval );

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
      /// <returns>
      /// a instance of <c>CacheFactory</c> 
      /// </returns>
      CacheFactory^ SetStatisticInterval( Int32 statisticInterval);

		  /// <summary>
		  /// Configures the group that all servers this pool connects to must belong to.
		  /// </summary>
		  /// <param>
		  /// group the server group that this pool will connect to.
		  /// If null or "" then all servers will be connected to.
		  /// </param>
      /// <returns>
      /// a instance of <c>CacheFactory</c> 
      /// </returns>
      CacheFactory^ SetServerGroup( String^ group );

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
      /// <returns>
      /// a instance of <c>CacheFactory</c> 
      /// </returns>
		  CacheFactory^ AddLocator( String^ host, Int32 port );

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
      /// <returns>
      /// a instance of <c>CacheFactory</c> 
      /// </returns>
      CacheFactory^ AddServer( String^ host, Int32 port );

		  /// <summary>
		  /// Enable subscriptions.
		  /// </summary>
		  /// <remarks>
		  /// If set to true then the created pool will have server-to-client
		  /// subscriptions enabled. If set to false then all Subscription*
		  /// attributes are ignored at create time.
		  /// </remarks>
      /// <returns>
      /// a instance of <c>CacheFactory</c> 
      /// </returns>
		  CacheFactory^ SetSubscriptionEnabled( Boolean enabled );

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
      /// <returns>
      /// a instance of <c>CacheFactory</c> 
      /// </returns>
		  CacheFactory^ SetPRSingleHopEnabled( Boolean enabled );

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
      /// <returns>
      /// a instance of <c>CacheFactory</c> 
      /// </returns>
		  CacheFactory^ SetSubscriptionRedundancy( Int32 redundancy );

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
      /// <returns>
      /// a instance of <c>CacheFactory</c> 
      /// </returns>
		  CacheFactory^ SetSubscriptionMessageTrackingTimeout( Int32 messageTrackingTimeout );

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
      /// <returns>
      /// a instance of <c>CacheFactory</c> 
      /// </returns>
		  CacheFactory^ SetSubscriptionAckInterval( Int32 ackInterval );


      /// <summary>
		  /// Sets whether pool is in multiuser mode
		  /// </summary>
		  /// <param>
		  /// multiuserAuthentication should be true/false. Default value is false;
		  /// </param>
      /// <returns>
      /// a instance of <c>CacheFactory</c> 
      /// </returns>
      CacheFactory^ SetMultiuserAuthentication( bool multiuserAuthentication );


      /// <summary>
		  /// Sets a gemfire property that will be used when creating the ClientCache.
      /// </summary>
		  /// <param>
		  /// name the name of the gemfire property
		  /// </param>
      /// <param>
		  /// value the value of the gemfire property
		  /// </param>
      /// <returns>
      /// a instance of <c>CacheFactory</c> 
      /// </returns>
      CacheFactory^ Set(String^ name, String^ value);

       private:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
         inline CacheFactory( gemfire::CacheFactory* nativeptr, Properties^ dsProps )
          : SBWrap( nativeptr ) 
         { 
            m_dsProps = dsProps;
         }

         Properties^ m_dsProps; 
         static System::Object^ m_singletonSync = gcnew System::Object();

        internal:
          static bool m_connected = false;
      };

    }
  }
}

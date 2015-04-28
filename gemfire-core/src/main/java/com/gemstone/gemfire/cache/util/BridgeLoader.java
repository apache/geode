/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.util;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.internal.BridgePoolImpl;
import com.gemstone.gemfire.cache.client.internal.ServerProxy;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.internal.cache.tier.ConnectionProxy;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * A <code>CacheLoader</code> that loads data from one or more remote
 * <code>cacheserver</code> processes. This allows for a hierarchical caching
 * scheme in which one cache ('client' cache) delegates a request to another
 * cache ('server' cache) when it cannot find the data locally.
 * 
 * 
 * When using the <code>BridgeLoader</code>, at least two GemFire Caches must
 * be running in a client/server mode (they should not be part of the same
 * distributed system).
 * 
 * The 'server' cache must be running a gemfire <code>cacheserver</code>
 * process, while the 'client' cache must have a <code>BridgeLoader</code>
 * installed in one or more of its <code>Regions</code>. If a
 * <code>BridgeLoader</code> is defined in a client <code>Region</code>,
 * there must also be a <code>Region</code> defined in the 'server' cache with
 * the same exact name.
 * 
 * <p>
 * 
 * The <code>BridgeLoader</code> performs <code>get()</code> operations on
 * the remote server cache, and does not provide the distribution behavior that
 * can be enabled by using a <code>DISTRIBUTED</code> or
 * <code>DISTRIBUTED_NO_ACK</code> <code>Region</code>. This mechanism is
 * designed as a more targeted alternative to netSearch, in which the 'client'
 * cache completely delegates the loading of the data to the 'server' cache if
 * it is not yet cached in the client. This directed behavior enables a remote
 * network <code>get()</code> operation to be performed much more efficiently
 * in a scenario where there is a hierarchical cache topology. Updates and
 * invalidation remain local, in fact the <code>Regions</code> that are used
 * for this loosely coupled cache may even be <code>LOCAL</code> in scope.
 * 
 * The <code>BridgeLoader</code> may be used to configure caches with
 * multi-layer hierarchies.
 * 
 * 
 * <p>
 * <b>Load Balancing: </b>
 * <p>
 * The <code>BridgeLoader</code> supports these load balancing mechanisms
 * (specified by the <code>LBPolicy</code> config attribute):
 * <p>
 * <ul>
 * <li><b>Sticky </b> <br>
 * In this mode, the client loader picks the first server from the list of
 * servers and establishes a connection to it. Once this connection has been
 * established, every request from that particular 'client' cache is sent on
 * that connection. If requests time out or produce exceptions, the
 * <code>BridgeLoader</code> picks another server and then sends further
 * requests to that server. This achieves a level of load balancing by
 * redirecting requests away from servers that produce timeouts.</li>
 *
 * <li><b>RandomSticky </b> <br>
 * The behavior is the same as Sticky, however the initial assignment of the
 * connection is randomly selected from the list of servers.</li>
 * 
 * <li><b>RoundRobin </b> <br>
 * In this mode, the client establishes connections to all the servers in the
 * server list and then randomly picks a server for each given request. For the
 * next request, it picks the next server in the list.</li>
 * 
 * <li><b>Random </b>: <br>
 * In this mode, the edge establishes connections to all the servers in the
 * server list and then randomly picks a server for every request.</li>
 * 
 * 
 * </ul>
 * 
 * <p>
 * <b>Failover: </b>
 * <p>
 * 
 * If a remote server cache throws an exception or times out, the client will
 * retry based on the configured <code>retryCount</code> parameter. If the
 * <code>retryCount</code> is exceeded, the server in question will be added
 * to a failed server list, and the client will select another server to connect
 * to. The servers in the failed server list will be periodically pinged with an
 * intelligent ping that ensures cache health. If a server is determined to be
 * healthy again, it will be promoted back to the healthy server list. The time
 * period between failed server pings is configurable via the
 * <code>retryInterval</code> parameter.
 * 
 * <p>
 * <b>Configuration: </b>
 * <p>
 * The <code>BridgeLoader</code> is configurable declaratively or
 * programmatically. Declarative configuration is achieved through defining the
 * configuration parameters in a <code>cache.xml</code> file. Programmatic
 * configuration may be achieved by first instantiating a
 * <code>BridgeLoader</code> object and subsequently calling
 * {@link #init(Properties)}with a <code>Properties</code> object containing
 * each desired parameter and value.
 * <p>
 * <b>The supported parameters are: </b>
 * <p>
 * <ul>
 * <li><b>endpoints </b> (required) <br>
 * A comma delimited list of logical names, hostnames, and ports of 'server'
 * caches to connect to <br>
 * The endpoints parameter follows this syntax:
 * logicalName=host:port,logicalName2=host2:port2,.... <br>
 * Example:
 * 
 * <pre>
 *<code>
 * &lt;parameter name=&quot;endpoints&quot;&gt;
 *   &lt;string&gt;MyPrimaryServer=hostsrv:40404,MySecondary=hostsrv2:40404&lt;/string&gt;
 * &lt;/parameter&gt;
 * </code>
 *</pre>
 * 
 * </li>
 * <li><b>readTimeout </b> (optional: default 10000) <br>
 * A millisecond value representing the amount of time to wait for a response
 * from a cache server. <br>
 * Example:
 * 
 * <pre>
 *<code>
 * &lt;parameter name=&quot;readTimeout&quot;&gt;
 *   &lt;string&gt;5000&lt;/string&gt;
 * &lt;/parameter&gt;
 * </code>
 *</pre>
 *  </li>
 *  
 * <li><b>retryAttempts </b> (optional: default 5)<br>
 * The number of times to retry a request after timeout/exception. <br>
 * Example:
 * 
 * <pre>
 *<code>
 * &lt;parameter name=&quot;retryAttempts&quot;&gt;
 *   &lt;string&gt;5&lt;/string&gt;
 * &lt;/parameter&gt;
 * </code>
 *</pre>
 * 
 * </li>
 * <li><b>retryInterval </b> (optional: default 10000) <br>
 * A millisecond value representing the amount of time to wait between attempts
 * by the <code>ServerMonitor</code> to ping living servers to verify that
 * they are still alive and dead servers to verify that they are still dead.
 * <br>
 * Example:</li>
 * 
 * <pre>
 *<code>
 * &lt;parameter name=&quot;retryInterval&quot;&gt;
 *   &lt;string&gt;10000&lt;/string&gt;
 * &lt;/parameter&gt;
 * </code>
 *</pre>
 * 
 * <li><b>LBPolicy </b> (optional: default "Sticky") <br>
 * A String value representing the load balancing policy to use. See above for
 * more details. <br>
 * Options are:
 * <ul>
 * <li>"Sticky"</li>
 * <li>"RandomSticky"</li>
 * <li>"RoundRobin"</li>
 * <li>"Random"</li>
 * </ul>
 * Example:
 * 
 * <pre>
 *<code>
 * &lt;parameter name=&quot;LBPolicy&quot;&gt;
 *   &lt;string&gt;Sticky&lt;/string&gt;
 * &lt;/parameter&gt;
 * </code>
 *</pre>
 * 
 * </li>
 * <li><b>connectionsPerServer </b> (optional: default 1)<br>
 * The number of initial connections created to each time it is
 * determined to be alive.
 * The minimum of <code>0</code> causes no initial connections to be created (they are only created on demand).
 * <br>
 * Example:
 * 
 * <pre>
 *<code>
 * &lt;parameter name=&quot;connectionsPerServer&quot;&gt;
 *   &lt;string&gt;10&lt;/string&gt;
 * &lt;/parameter&gt;
 * </code>
 *</pre>
 * 
 * </li>
 * <li><b>socketBufferSize </b> (optional: default 32768) <br>
 * The size of the socket buffers in bytes. <br>
 * Example:
 * 
 * <pre>
 *<code>
 * &lt;parameter name=&quot;socketBufferSize&quot;&gt;
 *   &lt;string&gt;32768&lt;/string&gt;
 * &lt;/parameter&gt;
 * </code>
 *</pre>
 * 
 * </li>
 * </ul>
 * 
 * <p>
 * 
 * If you are using a <code>cache.xml</code> file to create a
 * <code>Region</code> declaratively, you can include the following
 * &lt;cache-loader&gt; definition to associate a <code>BridgeLoader</code>
 * with a <code>Region</code> (default values shown for optional parameters):
 * 
 * <pre>
 * 
 * &lt;cache-loader&gt;
 *   &lt;class-name&gt;com.gemstone.gemfire.cache.util.BridgeLoader&lt;/class-name&gt;
 *   &lt;parameter name=&quot;endpoints&quot;&gt;
 *     &lt;string&gt;MyHost=ninja.gemstone.com:40404&lt;/string&gt;
 *   &lt;/parameter&gt;
 *   &lt;parameter name=&quot;readTimeout&quot;&gt;
 *     &lt;string&gt;10000&lt;/string&gt;
 *   &lt;/parameter&gt;
 *   &lt;parameter name=&quot;retryAttempts&quot;&gt;
 *     &lt;string&gt;5&lt;/string&gt;
 *   &lt;/parameter&gt;
 *   &lt;parameter name=&quot;retryInterval&quot;&gt;
 *     &lt;string&gt;10000&lt;/string&gt;
 *   &lt;/parameter&gt;
 *   &lt;parameter name=&quot;LBPolicy&quot;&gt;
 *     &lt;string&gt;Sticky&lt;/string&gt;
 *   &lt;/parameter&gt;
 *   &lt;parameter name=&quot;socketBufferSize&quot;&gt;
 *     &lt;string&gt;32768&lt;/string&gt;
 *   &lt;/parameter&gt;
 *   &lt;/parameter&gt;
 * &lt;/cache-loader&gt;
 * </pre>
 * 
 * @since 2.0.2
 * @author Greg Passmore
 * @deprecated as of 5.7 use {@link com.gemstone.gemfire.cache.client pools} instead.
 */

@Deprecated
public class BridgeLoader implements CacheLoader, Declarable
{
  ConnectionProxy proxy = null; // package access for
                                    // tests/com/gemstone/gemfire/cache/util/BridgeHelper

  private Properties properties;
  
  public BridgeLoader() { }

  /**
   * Creates a loader from an existing <code>BridgeWriter</code>. This
   * method reuses the existing <code>BridgeWriter</code>'s proxy.
   * 
   * @param bw
   *          The existing <code>BridgeWriter</code>
   * 
   * @since 5.7
   */
  public BridgeLoader(BridgeWriter bw) {
    init(bw);
  }

  private volatile boolean isClosed = false;

  private final AtomicInteger refCount = new AtomicInteger();

  /**
   * Initializes the loader with supplied config parameters. If instantiating
   * the loader programmatically, this method must be called with a
   * <code>Properties</code> object that at a minimum contains the 'endpoints'
   * parameter before the loader can be used. If a LicenseException is thrown
   * during initialization the BridgeLoader will trhow IllegalStateExceptions
   * until properly initialized.
   * 
   * @param p
   *          configuration data such as 'endpoint' definitions
   * @throws IllegalStateException if the loader is already initialized
   */
  public void init(Properties p)
  {
    if (this.proxy != null) throw new IllegalStateException(LocalizedStrings.BridgeLoader_ALREADY_INITIALIZED.toLocalizedString());
    this.properties = p;
    if (Boolean.getBoolean("skipConnection")) {
      // used by hydra when generating XML via RegionAttributesCreation
      return;
    }
    this.proxy = BridgePoolImpl.create(properties, false/*useByBridgeWriter*/);
  }

  /**
   * Initializes this loader from an existing <code>BridgeLoader</code>.
   * This method reuses the existing <code>BridgeLoader</code>'s connections
   * to the server.
   *
   * @param bridgeLoader The existing <code>BridgeLoader</code>
   * @throws IllegalStateException if the loader is already initialized
   *
   * @since 4.2
   */
  public void init(BridgeLoader bridgeLoader)
  {
    if (this.proxy != null) throw new IllegalStateException(LocalizedStrings.BridgeLoader_ALREADY_INITIALIZED.toLocalizedString());
    ConnectionProxy p = bridgeLoader.proxy;
    p.reuse();
    this.proxy = p;
  }

  /**
   * Initializes this loader from an existing <code>BridgeWriter</code>. This
   * method reuses the existing <code>BridgeWriter</code>'s proxy.
   * 
   * @param bridgeWriter
   *          The existing <code>BridgeWriter</code>
   * @throws IllegalStateException if the loader is already initialized
   * 
   * @since 5.7
   */
  public void init(BridgeWriter bridgeWriter)
  {
    if (this.proxy != null) throw new IllegalStateException("Already initialized");
    ConnectionProxy p = bridgeWriter.proxy;
    p.reuse();
    this.proxy = p;
  }

  /**
   * Ensure that the ConnectionProxyImpl class gets loaded.
   * 
   * @see SystemFailure#loadEmergencyClasses()
   */
  public static void loadEmergencyClasses() {
    BridgePoolImpl.loadEmergencyClasses();
  }
  
  /**
   * Called when the region containing this <code>BridgeLoader</code> is
   * destroyed, when the {@link Cache}is closed, or when a callback is removed
   * from a region using an {@link AttributesMutator}
   *
   * Closes connections to {@link BridgeServer}s when all {@link Region}s are
   * finished using this BridgeLoader,
   *
   * @see #detach(Region)
   * @see #attach(Region)
   */
  public void close() {
    if (this.refCount.get() <= 0) {
      this.isClosed = true;
      proxy.close();
    }
  }

  /**
   * Returns true if this <code>BridgeLoader</code> has been closed.
   */
  public boolean isClosed() {
    return this.isClosed;
  }

  /**
   * For speed optimizations, a connection to a server may be assigned to the
   * calling thread when load is called. When the application thread is done
   * doing its work it should invoke the BridgeLoader close method. This frees
   * up the connection assigned to the application thread.
   */
  public void release()
  {
    proxy.release();
  }

  /**
   * This method should be invoked when the BridgeLoader mechanism is to be shut
   * down explicitly , outside of closing the cache.
   */
  public void terminate()
  {
    this.isClosed = true;
    proxy.terminate();
  }

  // removed checkForTransaction

  /**
   * This method is invoked implicitly when an object requested on the client
   * cache cannot be found. The server cache will attempt to be contacted, and
   * if no server cache is available (or healthy) a CacheLoaderException will
   * be thrown.
   * 
   */
  public Object load(LoaderHelper helper) throws CacheLoaderException
  {
    throw new IllegalStateException("this method should not be called"); 
  }

  private void checkClosed() {
    String reason = this.proxy.getCancelCriterion().cancelInProgress();
    if(reason != null) {
      throw new BridgeWriterException("The BridgeWriter has been closed: " + reason);
    }
    
    if (this.isClosed) {
      throw new CacheLoaderException(LocalizedStrings.BridgeLoader_THE_BRIDGELOADER_HAS_BEEN_CLOSED.toLocalizedString());
    }
    if (this.proxy != null && !this.proxy.isOpen()) {
      throw new CacheLoaderException(LocalizedStrings.BridgeLoader_THE_BRIDGELOADER_HAS_BEEN_CLOSED.toLocalizedString());
    }
  }

  /**
   * Invoke a query on the cache server.
   * @deprecated use {@link Region#query} instead
   */
  @Deprecated
  public SelectResults query(String queryStr) throws CacheLoaderException {
    ServerProxy sp = new ServerProxy((BridgePoolImpl)this.proxy);
    return sp.query(queryStr, null);
  }

  /**
   * Returns the retry interval in use. Retry interval refers to the interval at
   * which dead servers are attempted to be reconnected. Internal use only.
   */
  public int getRetryInterval()
  {
    return proxy.getRetryInterval();
  }

  /**
   * Returns the read timeout being used to time out requests to the server
   * Internal use only.
   */
  public int getReadTimeout()
  {
    return proxy.getReadTimeout();
  }

  /**
   * Returns the number of times the bridge loader tries to get data on
   * encountering certain types of exceptions. Internal use only
   */
  public int getRetryAttempts()
  {
    return this.proxy.getRetryAttempts();
  }

  /**
   * Returns the load balancing policy being used by the bridge loader Internal
   * use only
   */
  public String getLBPolicy()
  {
    return proxy.getLBPolicy();
  }

  /**
   * Returns the properties that defined this <code>BridgeWriter</code>.
   * 
   * @return the properties that defined this <code>BridgeWriter</code>
   * 
   * @since 4.2
   */
  public Properties getProperties()
  {
    return this.properties;
  }

  /**
   * Returns the <code>ConnectionProxy</code> associated with this
   * <code>BridgeLoader</code>.
   * 
   * For internal use only.
   * 
   * @return the <code>ConnectionProxy</code> associated with this
   *         <code>BridgeLoader</code>
   */
  public Object/*ConnectionProxy*/ getConnectionProxy()
  {
    return proxy;
  }

  /**
   * Add an <code>Endpoint</code> to the known <code>Endpoint</code>s.
   * 
   * @param name The name of the endpoint to add
   * @param host The host name or ip address of the endpoint to add
   * @param port The port of the endpoint to add
   * 
   * @throws EndpointExistsException if the <code>Endpoint</code> to be
   * added already exists.
   * 
   * @since 5.0.2
   */
  public void addEndpoint(String name, String host, int port)
  throws EndpointExistsException {
    this.proxy.addEndpoint(name, host, port);
  }

  /**
   * Remove an <code>Endpoint</code> from the dead <code>Endpoint</code>s.
   * The specified <code>Endpoint</code> must be dead.
   * 
   * @param name The name of the endpoint to remove
   * @param host The host name or ip address of the endpoint to remove
   * @param port The port of the endpoint to remove
   * 
   * @throws EndpointDoesNotExistException if the <code>Endpoint</code> to be
   * removed doesn't exist.
   * 
   * @throws EndpointInUseException if the <code>Endpoint</code> to be removed
   * contains <code>Connection</code>s
   * 
   * @since 5.0.2
   */
  public void removeEndpoint(String name, String host, int port)
  throws EndpointDoesNotExistException, EndpointInUseException {
    this.proxy.removeEndpoint(name, host, port);
  }

  // removed handleException

  // removed getExceptionMessage

  /**
   * Returns a brief description of this <code>BridgeLoader</code>
   * 
   * @since 4.0
   */
  @Override
  public String toString()
  {
    return LocalizedStrings.BridgeLoader_BRIDGELOADER_CONNECTED_TO_0.toLocalizedString(this.proxy);
  }

  /**
   * Notify the BridgeLoader that the given region is no longer relevant. This
   * method is used internally during Region
   * {@link Region#destroyRegion() destruction}and
   * {@link Region#close() closure}. This method effects the behavor of
   * {@link #close()}.
   *
   * @param r
   *          the Region which will no longer use this BridgeLoader
   * @see #close()
   * @see #attach(Region)
   * @since 4.3
   */
  public void detach(Region r)
  {
    this.refCount.decrementAndGet();
  }

  /**
   * Notify the BridgeLoader that the given Region will begin calling
   * {@link #load(LoaderHelper)}.
   *
   * This method affects the behavior of {@link #close()}.
   *
   * This is called internally when the BridgeLoader is added to a Region via
   * {@link AttributesFactory#setCacheLoader(CacheLoader)}
   * 
   * @param r
   *          the Region which will begin use this BridgeWriter.
   * @since 4.3
   *
   * @see #detach(Region)
   * @see #close()
   */
  public void attach(Region r)
  {
    checkClosed();
    this.refCount.incrementAndGet();
  }


}

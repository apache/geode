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

import com.gemstone.gemfire.LicenseException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.client.internal.BridgePoolImpl;
import com.gemstone.gemfire.internal.cache.tier.ConnectionProxy;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * A <code>CacheWriter</code> that writes data to one or more remote
 * <code>CacheServer</code> processes. This allows for a hierarchical caching
 * scheme in which one cache ('client' cache) delegates a request to another
 * cache ('server' cache).
 * 
 * 
 * When using the <code>BridgeWriter</code>, at least two GemFire Caches must
 * exist in a client/server mode (they should not be part of the same
 * distributed system).
 * 
 * The 'server' cache must be running a gemfire <code>CacheServer</code>
 * process, while the 'client' cache must have a <code>BridgeWriter</code>
 * installed in one or more of its <code>Regions</code>. If a
 * <code>BridgeWriter</code> is defined in a client <code>Region</code>,
 * there must also be a <code>Region</code> defined in the 'server' cache with
 * the same exact name.
 * 
 * <p>
 * 
 * The <code>BridgeWriter</code> performs <code>put()</code> operations on
 * the remote server cache, and does not provide the distribution behavior that
 * can be enabled by using a <code>DISTRIBUTED</code> or
 * <code>DISTRIBUTED_NO_ACK</code> <code>Region</code>. This mechanism is
 * designed as a more targeted alternative to netSearch, in which the 'client'
 * cache completely delegates the loading of the data to the 'server' cache if
 * it is not yet cached in the client. This directed behavior enables a remote
 * network <code>put()</code> operation to be performed much more efficiently
 * in a scenario where there is a hierarchical cache topology. Updates and
 * invalidation remain local, in fact the <code>Regions</code> that are used
 * for this loosely coupled cache may even be <code>LOCAL</code> in scope.
 * 
 * The <code>BridgeWriter</code> may be used to configure caches with
 * multi-layer hierarchies.
 * 
 * 
 * <p>
 * <b>Load Balancing: </b>
 * <p>
 * The <code>BridgeWriter</code> supports these load balancing mechanisms
 * (specified by the <code>LBPolicy</code> config attribute):
 * <p>
 * <ul>
 * <li><b>Sticky </b> <br>
 * In this mode, the client writer picks the first server from the list of
 * servers and establishes a connection to it. Once this connection has been
 * established, every request from that particular 'client' cache is sent on
 * that connection. If requests time out or produce exceptions, the
 * <code>BridgeWriter</code> picks another server and then sends further
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
 * The <code>BridgeWriter</code> is configurable declaratively or
 * programmatically. Declarative configuration is achieved through defining the
 * configuration parameters in a <code>cache.xml</code> file. Programmatic
 * configuration may be achieved by first instantiating a
 * <code>BridgeWriter</code> object and subsequently calling
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
 * 
 * </li>
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
 * </li>
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
 * The number of initial connections created to each time it is determined to be
 * alive. The minimum of <code>0</code> causes no initial connections to be
 * created (they are only created on demand). <br>
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
 * <li><b>establishCallbackConnection </b> (optional: default false) <br>
 * Instruct the server to make a connection back to this edge client through
 * which the client receives cache updates. <br>
 * Example:
 * 
 * <pre>
 *<code>
 * &lt;parameter name=&quot;establishCallbackConnection&quot;&gt;
 *   &lt;string&gt;true&lt;/string&gt;
 * &lt;/parameter&gt;
 * </code>
 *</pre>
 * 
 * </li>
 * 
 * <li><b>redundancyLevel </b> (optional: default 0) <br>
 * The number of secondary servers set for backup to the primary server for the
 * high availability of client queue. <br>
 * Example:
 * 
 * <pre>
 *<code>
 * &lt;parameter name=&quot;redundancyLevel&quot;&gt;
 *   &lt;string&gt;1&lt;/string&gt;
 * &lt;/parameter&gt;
 * </code>
 *</pre>
 * 
 * </li>
 * 
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
 * 
 * <li><b>messageTrackingTimeout </b> (optional: default 300000 milliseconds)
 * <br>
 * messageTrackingTimeout property specifies the time-to-live period, in
 * milliseconds, for entries in the client's message tracking list, to minimize
 * duplicate events. Entries that have not been modified for this amount of time
 * are expired from the list <br>
 * Example:
 * 
 * <pre>
 *<code>
 * &lt;parameter name=&quot;messageTrackingTimeout&quot;&gt;
 *   &lt;string&gt;300000&lt;/string&gt;
 * &lt;/parameter&gt;
 * </code>
 *</pre>
 * 
 * </li>
 * <li><b>clientAckInterval</b> (optional: default 500 milliseconds) <br>
 * Bridge client sends an acknowledgement to its primary server for the events
 * it has got after every ClientAckInterval time.Client will send an ack to the
 * primary server only when redundancy level is greater than 0 or -1.<br>
 * Example:
 * 
 * <pre>
 *<code>
 * &lt;parameter name=&quot;clientAckInterval&quot;&gt;
 *   &lt;string&gt;5000&lt;/string&gt;
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
 * <code>Region</code> declaratively, you can include the following to
 * associate a <code>BridgeWriter</code> with a <code>Region</code> (default
 * values shown for optional parameters):
 * 
 * <pre>
 * 
 * &lt;cache-writer&gt;
 *   &lt;classname&gt;com.gemstone.gemfire.cache.util.BridgeWriter&lt;/classname&gt;
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
 *   &lt;parameter name=&quot;establishCallbackConnection&quot;&gt;
 *     &lt;string&gt;false&lt;/string&gt;
 *   &lt;/parameter&gt;
 *   &lt;parameter name=&quot;socketBufferSize&quot;&gt;
 *     &lt;string&gt;32768&lt;/string&gt;
 *   &lt;/parameter&gt;
 *   &lt;/parameter&gt;
 *   &lt;parameter name=&quot;messageTrackingTimeout&quot;&gt;
 *     &lt;string&gt;300000&lt;/string&gt;
 *   &lt;/parameter&gt;
 *   &lt;/parameter&gt;
 *   &lt;/parameter&gt;
 *   &lt;parameter name=&quot;clientAckInterval&quot;&gt;
 *      &lt;string&gt;5000&lt;/string&gt;
 *    &lt;/parameter&gt;
 * &lt;/cache-writer&gt;
 * </pre>
 * 
 * @since 3.5
 * @author Barry Oglesby
 * @deprecated as of 5.7 use {@link com.gemstone.gemfire.cache.client pools} instead.
 */

@Deprecated
public class BridgeWriter implements CacheWriter, Declarable
{
  protected ConnectionProxy proxy = null; // package access for
                                    // tests/com/gemstone/gemfire/cache/util/BridgeHelper

  private Properties properties;

  private volatile boolean isClosed = false;

  private final AtomicInteger refCount = new AtomicInteger();

  // all writers logic was moved to ConnectionProxyImpl
  
  /**
   * Initializes the writer with supplied config parameters. If instantiating
   * the writer programmatically, this method must be called with a
   * <code>Properties</code> object that at a minimum contains the 'endpoints'
   * parameter before the writer can be used. If init fails with a
   * LicenseException, the resulting BridgeWriter will throw
   * IllegalStateException until it is properly initialized.
   * 
   * @param p configuration data such as 'endpoint' definitions
   * @throws IllegalStateException if the writer is already initialized
   */
  public void init(Properties p)
  {
    if (this.proxy != null) throw new IllegalStateException(LocalizedStrings.BridgeWriter_ALREADY_INITIALIZED.toLocalizedString());
    this.properties = p;
    if (Boolean.getBoolean("skipConnection")) {
      // used by hydra when generating XML via RegionAttributesCreation
      return;
    }
    this.proxy = BridgePoolImpl.create(properties, true/*useByBridgeWriter*/);
  }

  /**
   * Initializes this writer from an existing <code>BridgeWriter</code>. This
   * method reuses the existing <code>BridgeWriter</code>'s proxy.
   * 
   * @param bridgeWriter
   *          The existing <code>BridgeWriter</code>
   * @throws IllegalStateException if the writer is already initialized
   * 
   * @since 4.2
   */
  public void init(BridgeWriter bridgeWriter)
  {
    if (this.proxy != null) throw new IllegalStateException(LocalizedStrings.BridgeWriter_ALREADY_INITIALIZED.toLocalizedString());
    ConnectionProxy p = bridgeWriter.proxy;
    p.reuse();
    this.proxy = p;
  }

  /**
   * Ensure that the BridgeClient and BridgePoolImpl classes
   * get loaded.
   * 
   * @see SystemFailure#loadEmergencyClasses()
   */
  public static void loadEmergencyClasses() {
    BridgeClient.loadEmergencyClasses(); // make sure subclass is taken care of
    BridgePoolImpl.loadEmergencyClasses();
  }

  // emergency logic was moved to ConnectionProxyImpl
  
  /**
   * Called when a region using this <code>BridgeWriter</code> is destroyed,
   * when the cache is closed, or when a callback is removed from a region using
   * an {@link AttributesMutator}.
   * 
   * Closes connections to {@link BridgeServer BridgeServers}when all
   * {@link Region Regions}are finished using this BridgeWriter,
   * 
   * 
   * @see #attach(Region)
   * @see #detach(Region)
   */
  public void close()
  {
    if (this.refCount.get() <= 0) {
      this.isClosed = true;
      this.proxy.close();
    }
  }

  // handleMarker moved to ConnectionProxyImpl

  /**
   * Returns true if this <code>BridgeWriter</code> has been closed.
   */
  public boolean isClosed() {
    return this.isClosed;
  }

  private void checkClosed() {
    String reason = this.proxy.getCancelCriterion().cancelInProgress();
    if(reason != null) {
      throw new BridgeWriterException("The BridgeWriter has been closed: " + reason);
    }
    
    if (this.isClosed) {
      throw new BridgeWriterException(LocalizedStrings.BridgeWriter_THE_BRIDGEWRITER_HAS_BEEN_CLOSED.toLocalizedString());
    }
    if (this.proxy != null && !this.proxy.isOpen()) {
      throw new BridgeWriterException(LocalizedStrings.BridgeWriter_THE_BRIDGEWRITER_HAS_BEEN_CLOSED.toLocalizedString());
    }
  }
  
  /**
   * Notify the BridgeWriter that the given region is no longer relevant. This
   * method is used internally during Region
   * {@link Region#destroyRegion() destruction}and
   * {@link Region#close() closure}. This method effects the behavor of
   * {@link #close()}.
   * 
   * @see #attach(Region)
   * @see #close()
   * @param r
   *          the Region which will no longer use this BridgeWriter
   * @since 4.3
   */
  public void detach(Region r)
  {
    this.refCount.decrementAndGet();
    if (r != null) {
      this.proxy.detachRegion(r);
    }
//    close(); // only closes if refCount is zero
  }

  /**
   * Returns the number of attaches that have not yet called detach.
   * @since 5.7
   */
  public int getAttachCount() {
    return this.refCount.get();
  }
  
  /**
   * For speed optimizations, a connection to a server may be assigned to the
   * calling thread when the BridgeWriter is used to do an operation.
   * When the application thread is done doing its work it can invoke
   * the BridgeWriter release method to make the connection available
   * to other application threads.
   */
  public void release()
  {
    proxy.release();
  }

  /**
   * This method should be invoked when the BridgeWriter mechanism is to be shut
   * down explicitly , outside of closing the cache.
   */
  public void terminate()
  {
    this.isClosed = true;
    proxy.terminate();
  }

  // removed checkForTransaction

  /**
   * Called before an entry is updated. The entry update is initiated by a
   * <code>put</code> or a <code>get</code> that causes the writer to update
   * an existing entry. The entry previously existed in the cache where the
   * operation was initiated, although the old value may have been null. The
   * entry being updated may or may not exist in the local cache where the
   * CacheWriter is installed.
   * 
   * @param event
   *          an EntryEvent that provides information about the operation in
   *          progress
   * @throws CacheWriterException
   *           if thrown will abort the operation in progress, and the exception
   *           will be propagated back to caller that initiated the operation
   * @see Region#put(Object, Object)
   * @see Region#get(Object)
   */
  public void beforeUpdate(EntryEvent event) throws CacheWriterException
  {
    throw new IllegalStateException("this method should not be called"); 
  }

  /**
   * Called before an entry is created. Entry creation is initiated by a
   * <code>create</code>, a <code>put</code>, or a <code>get</code>.
   * The <code>CacheWriter</code> can determine whether this value comes from
   * a <code>get</code> or not from {@link EntryEvent#isLoad}. The entry
   * being created may already exist in the local cache where this
   * <code>CacheWriter</code> is installed, but it does not yet exist in the
   * cache where the operation was initiated.
   * 
   * @param event
   *          an EntryEvent that provides information about the operation in
   *          progress
   * @throws CacheWriterException
   *           if thrown will abort the operation in progress, and the exception
   *           will be propagated back to caller that initiated the operation
   * @see Region#create(Object, Object)
   * @see Region#put(Object, Object)
   * @see Region#get(Object)
   */
  public void beforeCreate(EntryEvent event) throws CacheWriterException
  {
    throw new IllegalStateException("this method should not be called"); 
  }

  /**
   * Called before an entry is destroyed. The entry being destroyed may or may
   * not exist in the local cache where the CacheWriter is installed. This
   * method is <em>not</em> called as a result of expiration or
   * {@link Region#localDestroy(Object)}.
   * 
   * @param event
   *          an EntryEvent that provides information about the operation in
   *          progress
   * @throws CacheWriterException
   *           if thrown will abort the operation in progress, and the exception
   *           will be propagated back to caller that initiated the operation
   * 
   * @see Region#destroy(Object)
   */
  public void beforeDestroy(EntryEvent event) throws CacheWriterException
  {
    throw new IllegalStateException("this method should not be called"); 
  }

  /**
   * Called before a region is destroyed. The <code>CacheWriter</code> will
   * not additionally be called for each entry that is destroyed in the region
   * as a result of a region destroy. If the region's subregions have
   * <code>CacheWriter</code> s installed, then they will be called for the
   * cascading subregion destroys. This method is <em>not</em> called as a
   * result of expiration or {@link Region#localDestroyRegion()}. However, the
   * {@link #close}method is invoked regardless of whether a region is
   * destroyed locally. A non-local region destroy results in an invocation of
   * {@link #beforeRegionDestroy}followed by an invocation of {@link #close}.
   * <p>
   * WARNING: This method should not destroy or create any regions itself or a
   * deadlock will occur.
   * 
   * @param event
   *          a RegionEvent that provides information about the
   * 
   * @throws CacheWriterException
   *           if thrown, will abort the operation in progress, and the
   *           exception will be propagated back to the caller that initiated
   *           the operation
   * 
   * @see Region#destroyRegion()
   */
  public void beforeRegionDestroy(RegionEvent event)
      throws CacheWriterException
  {
    throw new IllegalStateException("this method should not be called"); 
  }

  
  /**
   * Called before a region is cleared. The <code>CacheWriter</code> will
   * not additionally be called for each entry that is cleared in the region
   * as a result of a region clear. If the region's subregions have
   * <code>CacheWriter</code> s installed, then they will be called for the
   * cascading subregion clears. This method is <em>not</em> called as a
   * result of expiration or {@link Region#localDestroyRegion()}. However, the
   * {@link #close}method is invoked regardless of whether a region is
   * cleared locally. A non-local region clear results in an invocation of
   * {@link #beforeRegionClear}followed by an invocation of {@link #close}.
   * <p>
   * WARNING: This method should not destroy or create or clear any regions itself or a
   * deadlock will occur.
   * 
   * @param event
   *          a RegionEvent that provides information about the
   * 
   * @throws CacheWriterException
   *           if thrown, will abort the operation in progress, and the
   *           exception will be propagated back to the caller that initiated
   *           the operation
   * 
   */
  
  public void beforeRegionClear(RegionEvent event) throws CacheWriterException
  {
    throw new IllegalStateException("this method should not be called"); 
  }

  /**
   * Return true if this writer has not been closed and it was configured to
   * establish a callback connection.
   * 
   * @since 4.3
   */
  public boolean hasEstablishCallbackConnection()
  {
    if (this.isClosed) {
      return false;
    }
    else {
      return this.proxy.getEstablishCallbackConnection();
    }
  }

  // removed unregisterInterest

  // removed getInterestList

  // removed getObjectFromPrimaryServer

  // removed keySet

  // removed containsKey

  /** Returns the retry interval in use. Retry interval refers to the interval
   *  at which dead servers are attempted to be reconnected.
   *  Internal use only.
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
   * Returns the number of times the bridge writer tries to write data on
   * encountering certain types of exceptions. Internal use only
   */
  public int getRetryAttempts()
  {
    return this.proxy.getRetryAttempts();
  }

  /**
   * Returns the load balancing policy being used by the bridge writer Internal
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
   * Returns a brief description of this <code>BridgeWriter</code>
   * 
   * @since 4.0
   */
  @Override
  public String toString()
  {
    return LocalizedStrings.BridgeWriter_BRIDGEWRITER_CONNECTED_TO_0.toLocalizedString(this.proxy);
  }

  /**
   * Notify the BridgeWriter that the given Region will begin delivering events
   * to this BridgeWriter. This method effects the behavior of {@link #close()}
   * 
   * This is called internally when the BridgeWriter is added to a Region via
   * {@link AttributesFactory#setCacheWriter(CacheWriter)}}
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

  /**
   * Returns the <code>ConnectionProxy</code> associated with this
   * <code>BridgeWriter</code>.
   * 
   * For internal use only.
   * 
   * @return the <code>ConnectionProxy</code> associated with this
   *         <code>BridgeWriter</code>
   */
  public Object/*ConnectionProxy*/ getConnectionProxy() {
    return proxy;
  }

}

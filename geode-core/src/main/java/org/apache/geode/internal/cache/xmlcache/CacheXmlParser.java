/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.xmlcache;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EmptyStackException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.Stack;
import java.util.StringTokenizer;

import javax.naming.NamingException;
import javax.xml.XMLConstants;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.ext.DefaultHandler2;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.DiskWriteAttributes;
import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.GatewayException;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.LossAction;
import org.apache.geode.cache.MembershipAttributes;
import org.apache.geode.cache.MirrorType;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.ResumptionAction;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.TransactionListener;
import org.apache.geode.cache.TransactionWriter;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.SocketFactory;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.partition.PartitionListener;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.internal.index.IndexCreationData;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.server.ClientSubscriptionConfig;
import org.apache.geode.cache.server.ServerLoadProbe;
import org.apache.geode.cache.util.GatewayConflictResolver;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayEventSubstitutionFilter;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.compression.Compressor;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.DiskStoreAttributes;
import org.apache.geode.internal.cache.DiskWriteAttributesImpl;
import org.apache.geode.internal.cache.EvictionAttributesImpl;
import org.apache.geode.internal.cache.FixedPartitionAttributesImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionAttributesImpl;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.datasource.ConfigProperty;
import org.apache.geode.internal.datasource.DataSourceCreateException;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.pdx.PdxSerializer;

/**
 * Parses an XML file and creates a {@link Cache}/{@link ClientCache} and {@link Region}s from it.
 * It works in two phases. The first phase parses the XML and instantiates {@link Declarable}s. If
 * any problems occur, a {@link CacheXmlException} is thrown. The second phase actually
 * {@linkplain CacheCreation#create creates} the {@link Cache}/{@link ClientCache},{@link Region}s,
 * etc.
 *
 *
 * @since GemFire 3.0
 */
@SuppressWarnings("deprecation")
public class CacheXmlParser extends CacheXml implements ContentHandler {

  private static final Logger logger = LogService.getLogger();

  /**
   * @since GemFire 8.1
   */
  private static final String BUFFER_SIZE = "http://apache.org/xml/properties/input-buffer-size";

  /**
   * @since GemFire 8.1
   */
  private static final String DISALLOW_DOCTYPE_DECL_FEATURE =
      "http://apache.org/xml/features/disallow-doctype-decl";

  /**
   * @since GemFire 8.1
   */
  private static final String JAXP_SCHEMA_LANGUAGE =
      "http://java.sun.com/xml/jaxp/properties/schemaLanguage";

  /** The cache to be created */
  private CacheCreation cache;
  /** The stack of intermediate values used while parsing */
  protected Stack<Object> stack = new Stack<>();

  /**
   * Delegate {@link XmlParser}s mapped by namespace URI.
   *
   * @since GemFire 8.1
   */
  private HashMap<String, XmlParser> delegates = new HashMap<>();

  /**
   * Document {@link Locator} used for {@link SAXParseException}.
   *
   * @since GemFire 8.2
   */
  protected Locator documentLocator;

  ////////////////////// Static Methods //////////////////////
  /**
   * Parses XML data and from it creates an instance of <code>CacheXmlParser</code> that can be used
   * to {@link #create}the {@link Cache}, etc.
   *
   * @param is the <code>InputStream</code> of XML to be parsed
   *
   * @return a <code>CacheXmlParser</code>, typically used to create a cache from the parsed XML
   *
   * @throws CacheXmlException Something went wrong while parsing the XML
   *
   * @since GemFire 4.0
   *
   */
  public static CacheXmlParser parse(InputStream is) {

    /*
     * The API doc http://java.sun.com/javase/6/docs/api/org/xml/sax/InputSource.html for the SAX
     * InputSource says: "... standard processing of both byte and character streams is to close
     * them on as part of end-of-parse cleanup, so applications should not attempt to re-use such
     * streams after they have been handed to a parser."
     *
     * In order to block the parser from closing the stream, we wrap the InputStream in a filter,
     * i.e., UnclosableInputStream, whose close() function does nothing.
     *
     */
    class UnclosableInputStream extends BufferedInputStream {
      public UnclosableInputStream(InputStream stream) {
        super(stream);
      }

      @Override
      public void close() {}
    }

    CacheXmlParser handler = new CacheXmlParser();
    try {
      SAXParserFactory factory = SAXParserFactory.newInstance();
      factory.setFeature(DISALLOW_DOCTYPE_DECL_FEATURE, true);
      factory.setValidating(true);
      factory.setNamespaceAware(true);
      UnclosableInputStream bis = new UnclosableInputStream(is);
      try {
        SAXParser parser = factory.newSAXParser();
        // Parser always reads one buffer plus a little extra worth before
        // determining that the DTD is there. Setting mark twice the parser
        // buffer size.
        bis.mark((Integer) parser.getProperty(BUFFER_SIZE) * 2);
        parser.setProperty(JAXP_SCHEMA_LANGUAGE, XMLConstants.W3C_XML_SCHEMA_NS_URI);
        parser.parse(bis, new DefaultHandlerDelegate(handler));
      } catch (CacheXmlException e) {
        if (null != e.getCause()
            && e.getCause().getMessage().contains(DISALLOW_DOCTYPE_DECL_FEATURE)) {
          // Not schema based document, try dtd.
          bis.reset();
          factory.setFeature(DISALLOW_DOCTYPE_DECL_FEATURE, false);
          SAXParser parser = factory.newSAXParser();
          parser.parse(bis, new DefaultHandlerDelegate(handler));
        } else {
          throw e;
        }
      }
      return handler;
    } catch (Exception ex) {
      if (ex instanceof CacheXmlException) {
        while (true /* ex instanceof CacheXmlException */) {
          Throwable cause = ex.getCause();
          if (!(cause instanceof CacheXmlException)) {
            break;
          } else {
            ex = (CacheXmlException) cause;
          }
        }
        throw (CacheXmlException) ex;
      } else if (ex instanceof SAXException) {
        // Silly JDK 1.4.2 XML parser wraps RunTime exceptions in a
        // SAXException. Pshaw!
        SAXException sax = (SAXException) ex;
        Exception cause = sax.getException();
        if (cause instanceof CacheXmlException) {
          while (true /* cause instanceof CacheXmlException */) {
            Throwable cause2 = cause.getCause();
            if (!(cause2 instanceof CacheXmlException)) {
              break;
            } else {
              cause = (CacheXmlException) cause2;
            }
          }
          throw (CacheXmlException) cause;
        }
      }
      throw new CacheXmlException(
          "While parsing XML", ex);
    }
  }

  /**
   * Helper method for parsing an integer
   *
   * @throws CacheXmlException If <code>s</code> is a malformed integer
   */
  private static int parseInt(String s) {
    try {
      return Integer.parseInt(s);
    } catch (NumberFormatException ex) {
      throw new CacheXmlException(
          String.format("Malformed integer %s", s), ex);
    }
  }

  /**
   * Helper method for parsing a long
   *
   * @throws CacheXmlException If <code>s</code> is a malformed long
   */
  private static long parseLong(String s) {
    try {
      return Long.parseLong(s);
    } catch (NumberFormatException ex) {
      throw new CacheXmlException(
          String.format("Malformed integer %s", s), ex);
    }
  }

  /**
   * Helper method for parsing a boolean
   *
   */
  private static boolean parseBoolean(String s) {
    return Boolean.valueOf(s).booleanValue();
  }

  /**
   * Helper method for parsing an float
   *
   * @throws CacheXmlException If <code>s</code> is a malformed float
   */
  private static float parseFloat(String s) {
    try {
      return Float.parseFloat(s);
    } catch (NumberFormatException ex) {
      throw new CacheXmlException(
          String.format("Malformed float %s", s), ex);
    }
  }

  /**
   * Returns the {@link CacheCreation} generated by this parser.
   */
  public CacheCreation getCacheCreation() {
    return this.cache;
  }

  /**
   * Creates cache artifacts ({@link Cache}s, etc.) based upon the XML parsed by this parser.
   */
  public void create(InternalCache cache)
      throws TimeoutException, GatewayException, CacheWriterException, RegionExistsException {
    if (this.cache == null) {
      String s = "A cache or client-cache element is required";
      throw new CacheXmlException(
          "No cache element specified.");
    }
    this.cache.create(cache);
  }

  /**
   * When a <code>cache</code> element is first encountered, we create a {@link CacheCreation}and
   * fill it in appropriately
   */
  private void startCache(Attributes atts) {
    if (this.cache != null) {
      throw new CacheXmlException("Only a single cache or client-cache element is allowed");
    }
    this.cache = new CacheCreation(true);
    String lockLease = atts.getValue(LOCK_LEASE);
    if (lockLease != null) {
      this.cache.setLockLease(parseInt(lockLease));
    }
    String lockTimeout = atts.getValue(LOCK_TIMEOUT);
    if (lockTimeout != null) {
      this.cache.setLockTimeout(parseInt(lockTimeout));
    }
    String searchTimeout = atts.getValue(SEARCH_TIMEOUT);
    if (searchTimeout != null) {
      this.cache.setSearchTimeout(parseInt(searchTimeout));
    }
    String messageSyncInterval = atts.getValue(MESSAGE_SYNC_INTERVAL);
    if (messageSyncInterval != null) {
      this.cache.setMessageSyncInterval(parseInt(messageSyncInterval));
    }
    String isServer = atts.getValue(IS_SERVER);
    if (isServer != null) {
      boolean b = Boolean.valueOf(isServer).booleanValue();
      this.cache.setIsServer(b);
    }
    String copyOnRead = atts.getValue(COPY_ON_READ);
    if (copyOnRead != null) {
      this.cache.setCopyOnRead(Boolean.valueOf(copyOnRead).booleanValue());
    }
    stack.push(this.cache);
  }

  /**
   * When a <code>client-cache</code> element is first encountered, we create a
   * {@link ClientCacheCreation}and fill it in appropriately
   */
  private void startClientCache(Attributes atts) {
    if (this.cache != null) {
      throw new CacheXmlException("Only a single cache or client-cache element is allowed");
    }
    this.cache = new ClientCacheCreation(true);
    String copyOnRead = atts.getValue(COPY_ON_READ);
    if (copyOnRead != null) {
      this.cache.setCopyOnRead(Boolean.valueOf(copyOnRead).booleanValue());
    }
    stack.push(this.cache);
  }

  /**
   * @since GemFire 5.7
   */
  private void startPool(Attributes atts) {
    PoolFactory f = this.cache.createPoolFactory();
    String name = atts.getValue(NAME).trim();
    stack.push(name);
    stack.push(f);
    String v;
    v = atts.getValue(SUBSCRIPTION_TIMEOUT_MULTIPLIER);
    if (v != null) {
      f.setSubscriptionTimeoutMultiplier(parseInt(v));
    }
    v = atts.getValue(SOCKET_CONNECT_TIMEOUT);
    if (v != null) {
      f.setSocketConnectTimeout(parseInt(v));
    }
    v = atts.getValue(FREE_CONNECTION_TIMEOUT);
    if (v != null) {
      f.setFreeConnectionTimeout(parseInt(v));
    }
    v = atts.getValue(SERVER_CONNECTION_TIMEOUT);
    if (v != null) {
      f.setServerConnectionTimeout(parseInt(v));
    }
    v = atts.getValue(LOAD_CONDITIONING_INTERVAL);
    if (v != null) {
      f.setLoadConditioningInterval(parseInt(v));
    }
    v = atts.getValue(MIN_CONNECTIONS);
    if (v != null) {
      f.setMinConnections(parseInt(v));
    }
    v = atts.getValue(MAX_CONNECTIONS);
    if (v != null) {
      f.setMaxConnections(parseInt(v));
    }
    v = atts.getValue(RETRY_ATTEMPTS);
    if (v != null) {
      f.setRetryAttempts(parseInt(v));
    }
    v = atts.getValue(IDLE_TIMEOUT);
    if (v != null) {
      f.setIdleTimeout(parseLong(v));
    }
    v = atts.getValue(PING_INTERVAL);
    if (v != null) {
      f.setPingInterval(parseLong(v));
    }
    v = atts.getValue(SUBSCRIPTION_ENABLED);
    if (v != null) {
      f.setSubscriptionEnabled(parseBoolean(v));
    }
    v = atts.getValue(PR_SINGLE_HOP_ENABLED);
    if (v != null) {
      f.setPRSingleHopEnabled(parseBoolean(v));
    }
    v = atts.getValue(SUBSCRIPTION_MESSAGE_TRACKING_TIMEOUT);
    if (v != null) {
      f.setSubscriptionMessageTrackingTimeout(parseInt(v));
    }
    v = atts.getValue(SUBSCRIPTION_ACK_INTERVAL);
    if (v != null) {
      f.setSubscriptionAckInterval(parseInt(v));
    }
    v = atts.getValue(SUBSCRIPTION_REDUNDANCY);
    if (v != null) {
      f.setSubscriptionRedundancy(parseInt(v));
    }
    v = atts.getValue(READ_TIMEOUT);
    if (v != null) {
      f.setReadTimeout(parseInt(v));
    }
    v = atts.getValue(SERVER_GROUP);
    if (v != null) {
      f.setServerGroup(v.trim());
    }
    v = atts.getValue(SOCKET_BUFFER_SIZE);
    if (v != null) {
      f.setSocketBufferSize(parseInt(v));
    }
    v = atts.getValue(STATISTIC_INTERVAL);
    if (v != null) {
      f.setStatisticInterval(parseInt(v));
    }
    v = atts.getValue(THREAD_LOCAL_CONNECTIONS);
    if (v != null) {
      f.setThreadLocalConnections(parseBoolean(v));
    }
    v = atts.getValue(MULTIUSER_SECURE_MODE_ENABLED);
    if (v != null) {
      f.setMultiuserAuthentication(parseBoolean(v));
    }
  }

  /**
   * @since GemFire 5.7
   */
  private void endPool() {
    PoolFactory f = (PoolFactory) stack.pop();
    String name = (String) stack.pop();
    f.create(name);
  }

  /**
   * @since GemFire 5.7
   */
  private void doLocator(Attributes atts) {
    PoolFactory f = (PoolFactory) stack.peek();
    String host = atts.getValue(HOST).trim();
    int port = parseInt(atts.getValue(PORT));
    f.addLocator(host, port);
  }

  /**
   * @since GemFire 5.7
   */
  private void doServer(Attributes atts) {
    PoolFactory f = (PoolFactory) stack.peek();
    String host = atts.getValue(HOST).trim();
    int port = parseInt(atts.getValue(PORT));
    f.addServer(host, port);
  }

  /**
   * When a <code>cache-server</code> element is first encountered, we create a new
   * {@link CacheCreation#addCacheServer() CacheServer} in the cache.
   *
   * @since GemFire 4.0
   */
  private void startCacheServer(Attributes atts) {
    CacheServer bridge = this.cache.addCacheServer();
    String port = atts.getValue(PORT);
    if (port != null) {
      bridge.setPort(parseInt(port));
    }
    String bindAddress = atts.getValue(BIND_ADDRESS);
    if (bindAddress != null) {
      bridge.setBindAddress(bindAddress.trim());
    }
    String hostnameForClients = atts.getValue(HOSTNAME_FOR_CLIENTS);
    if (hostnameForClients != null) {
      bridge.setHostnameForClients(hostnameForClients.trim());
    }
    String maxConnections = atts.getValue(MAX_CONNECTIONS);
    if (maxConnections != null) {
      bridge.setMaxConnections(parseInt(maxConnections));
    }
    String maxThreads = atts.getValue(MAX_THREADS);
    if (maxThreads != null) {
      bridge.setMaxThreads(parseInt(maxThreads));
    }
    String notifyBySubscription = atts.getValue(NOTIFY_BY_SUBSCRIPTION);
    if (notifyBySubscription != null) {
      boolean b = Boolean.valueOf(notifyBySubscription).booleanValue();
      bridge.setNotifyBySubscription(b);
    }
    String socketBufferSize = atts.getValue(SOCKET_BUFFER_SIZE);
    if (socketBufferSize != null) {
      bridge.setSocketBufferSize(Integer.parseInt(socketBufferSize));
    }
    String tcpDelay = atts.getValue(TCP_NO_DELAY);
    if (tcpDelay != null) {
      bridge.setTcpNoDelay(Boolean.valueOf(tcpDelay).booleanValue());
    }
    String maximumTimeBetweenPings = atts.getValue(MAXIMUM_TIME_BETWEEN_PINGS);
    if (maximumTimeBetweenPings != null) {
      bridge.setMaximumTimeBetweenPings(Integer.parseInt(maximumTimeBetweenPings));
    }
    String maximumMessageCount = atts.getValue(MAXIMUM_MESSAGE_COUNT);
    if (maximumMessageCount != null) {
      bridge.setMaximumMessageCount(Integer.parseInt(maximumMessageCount));
    }
    String messageTimeToLive = atts.getValue(MESSAGE_TIME_TO_LIVE);
    if (messageTimeToLive != null) {
      bridge.setMessageTimeToLive(Integer.parseInt(messageTimeToLive));
    }
    String loadPollInterval = atts.getValue(LOAD_POLL_INTERVAL);
    if (loadPollInterval != null) {
      bridge.setLoadPollInterval(Long.parseLong(loadPollInterval));
    }
    this.stack.push(bridge);
  }

  private void startGatewaySender(Attributes atts) {
    GatewaySenderFactory gatewaySenderFactory = this.cache.createGatewaySenderFactory();

    String parallel = atts.getValue(PARALLEL);
    if (parallel == null) {
      gatewaySenderFactory.setParallel(GatewaySender.DEFAULT_IS_PARALLEL);
    } else {
      gatewaySenderFactory.setParallel(Boolean.parseBoolean(parallel));
    }

    // manual-start
    String manualStart = atts.getValue(MANUAL_START);
    if (manualStart == null) {
      gatewaySenderFactory.setManualStart(GatewaySender.DEFAULT_MANUAL_START);
    } else {
      gatewaySenderFactory.setManualStart(Boolean.parseBoolean(manualStart));
    }

    // socket-buffer-size
    String socketBufferSize = atts.getValue(SOCKET_BUFFER_SIZE);
    if (socketBufferSize == null) {
      gatewaySenderFactory.setSocketBufferSize(GatewaySender.DEFAULT_SOCKET_BUFFER_SIZE);
    } else {
      gatewaySenderFactory.setSocketBufferSize(Integer.parseInt(socketBufferSize));
    }

    // socket-read-timeout
    String socketReadTimeout = atts.getValue(SOCKET_READ_TIMEOUT);
    if (socketReadTimeout == null) {
      gatewaySenderFactory.setSocketReadTimeout(GatewaySender.DEFAULT_SOCKET_READ_TIMEOUT);
    } else {
      gatewaySenderFactory.setSocketReadTimeout(Integer.parseInt(socketReadTimeout));
    }

    // batch-conflation
    String batchConflation = atts.getValue(ENABLE_BATCH_CONFLATION);
    if (batchConflation == null) {
      gatewaySenderFactory.setBatchConflationEnabled(GatewaySender.DEFAULT_BATCH_CONFLATION);
    } else {
      gatewaySenderFactory.setBatchConflationEnabled(Boolean.parseBoolean(batchConflation));
    }

    // batch-size
    String batchSize = atts.getValue(BATCH_SIZE);
    if (batchSize == null) {
      gatewaySenderFactory.setBatchSize(GatewaySender.DEFAULT_BATCH_SIZE);
    } else {
      gatewaySenderFactory.setBatchSize(Integer.parseInt(batchSize));
    }

    // batch-time-interval
    String batchTimeInterval = atts.getValue(BATCH_TIME_INTERVAL);
    if (batchTimeInterval == null) {
      gatewaySenderFactory.setBatchTimeInterval(GatewaySender.DEFAULT_BATCH_TIME_INTERVAL);
    } else {
      gatewaySenderFactory.setBatchTimeInterval(Integer.parseInt(batchTimeInterval));
    }

    // enable-persistence
    String enablePersistence = atts.getValue(ENABLE_PERSISTENCE);
    if (enablePersistence == null) {
      gatewaySenderFactory.setPersistenceEnabled(GatewaySender.DEFAULT_PERSISTENCE_ENABLED);
    } else {
      gatewaySenderFactory.setPersistenceEnabled(Boolean.parseBoolean(enablePersistence));
    }

    String diskStoreName = atts.getValue(DISK_STORE_NAME);
    if (diskStoreName == null) {
      gatewaySenderFactory.setDiskStoreName(null);
    } else {
      gatewaySenderFactory.setDiskStoreName(diskStoreName);
    }

    String diskSynchronous = atts.getValue(DISK_SYNCHRONOUS);
    if (diskSynchronous == null) {
      gatewaySenderFactory.setDiskSynchronous(GatewaySender.DEFAULT_DISK_SYNCHRONOUS);
    } else {
      gatewaySenderFactory.setDiskSynchronous(Boolean.parseBoolean(diskSynchronous));
    }

    // maximum-queue-memory
    String maxQueueMemory = atts.getValue(MAXIMUM_QUEUE_MEMORY);
    if (maxQueueMemory == null) {
      gatewaySenderFactory.setMaximumQueueMemory(GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY);
    } else {
      gatewaySenderFactory.setMaximumQueueMemory(Integer.parseInt(maxQueueMemory));
    }


    String alertThreshold = atts.getValue(ALERT_THRESHOLD);
    if (alertThreshold == null) {
      gatewaySenderFactory.setAlertThreshold(GatewaySender.DEFAULT_ALERT_THRESHOLD);
    } else {
      gatewaySenderFactory.setAlertThreshold(Integer.parseInt(alertThreshold));
    }

    String dispatcherThreads = atts.getValue(DISPATCHER_THREADS);
    if (dispatcherThreads == null) {
      gatewaySenderFactory.setDispatcherThreads(GatewaySender.DEFAULT_DISPATCHER_THREADS);
    } else {
      gatewaySenderFactory.setDispatcherThreads(Integer.parseInt(dispatcherThreads));
    }

    String id = atts.getValue(ID);
    String orderPolicy = atts.getValue(ORDER_POLICY);
    if (orderPolicy != null) {
      try {
        gatewaySenderFactory
            .setOrderPolicy(GatewaySender.OrderPolicy.valueOf(orderPolicy.toUpperCase()));
      } catch (IllegalArgumentException e) {
        throw new InternalGemFireException(
            String.format("An invalid order-policy value (%s) was configured for gateway sender %s",
                new Object[] {id, orderPolicy}));
      }
    }

    String remoteDS = atts.getValue(REMOTE_DISTRIBUTED_SYSTEM_ID);
    stack.push(id);
    stack.push(remoteDS);
    stack.push(gatewaySenderFactory);
    // GatewaySender sender = gatewaySenderFactory.create(id, Integer.parseInt(remoteDS));
    // stack.push(sender);
  }

  private void startGatewayReceiver(Attributes atts) {
    GatewayReceiverFactory receiverFactory = this.cache.createGatewayReceiverFactory();

    // port
    String startPort = atts.getValue(START_PORT);
    if (startPort == null) {
      receiverFactory.setStartPort(GatewayReceiver.DEFAULT_START_PORT);
    } else {
      receiverFactory.setStartPort(Integer.parseInt(startPort));
    }

    String endPort = atts.getValue(END_PORT);
    if (endPort == null) {
      receiverFactory.setEndPort(GatewayReceiver.DEFAULT_END_PORT);
    } else {
      receiverFactory.setEndPort(Integer.parseInt(endPort));
    }

    String bindAddress = atts.getValue(BIND_ADDRESS);
    if (bindAddress == null) {
      receiverFactory.setBindAddress(GatewayReceiver.DEFAULT_BIND_ADDRESS);
    } else {
      receiverFactory.setBindAddress(bindAddress);
    }

    // maximum-time-between-pings
    String maxTimeBetweenPings = atts.getValue(MAXIMUM_TIME_BETWEEN_PINGS);
    if (maxTimeBetweenPings == null) {
      receiverFactory
          .setMaximumTimeBetweenPings(GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS);
    } else {
      receiverFactory.setMaximumTimeBetweenPings(Integer.parseInt(maxTimeBetweenPings));
    }

    // socket-buffer-size
    String socketBufferSize = atts.getValue(SOCKET_BUFFER_SIZE);
    if (socketBufferSize == null) {
      receiverFactory.setSocketBufferSize(GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE);
    } else {
      receiverFactory.setSocketBufferSize(Integer.parseInt(socketBufferSize));
    }

    // manual-start
    String manualStart = atts.getValue(MANUAL_START);
    if (manualStart == null) {
      receiverFactory.setManualStart(GatewayReceiver.DEFAULT_MANUAL_START);
    } else {
      receiverFactory.setManualStart(Boolean.parseBoolean(manualStart));
    }

    // hostname-for-senders
    String hostnameForSenders = atts.getValue(HOSTNAME_FOR_SENDERS);
    if (hostnameForSenders == null) {
      receiverFactory.setHostnameForSenders(GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS);
    } else {
      receiverFactory.setHostnameForSenders(hostnameForSenders);
    }

    stack.push(receiverFactory);
  }


  /**
   * set attributes from clientHaQueue when we finish a cache server
   */
  private void endCacheServer() {
    List groups = new ArrayList();
    ServerLoadProbe probe = null;
    ClientHaQueueCreation haCreation = null;

    if (stack.peek() instanceof ServerLoadProbe) {
      probe = (ServerLoadProbe) stack.pop();
    }



    if (stack.peek() instanceof ClientHaQueueCreation) {
      haCreation = (ClientHaQueueCreation) stack.pop();
    }

    while (stack.peek() instanceof String) {
      groups.add(stack.pop());
    }
    CacheServer bs = (CacheServer) stack.pop();
    if (groups.size() > 0) {
      Collections.reverse(groups);
      String[] groupArray = new String[groups.size()];
      groups.toArray(groupArray);
      bs.setGroups(groupArray);
    }
    if (probe != null) {
      bs.setLoadProbe(probe);
    }
    if (haCreation != null) {
      ClientSubscriptionConfig csc = bs.getClientSubscriptionConfig();
      String diskStoreName = haCreation.getDiskStoreName();
      if (diskStoreName != null) {
        csc.setDiskStoreName(diskStoreName);
      } else {
        csc.setOverflowDirectory(haCreation.getOverflowDirectory() == null
            ? ClientSubscriptionConfig.DEFAULT_OVERFLOW_DIRECTORY
            : haCreation.getOverflowDirectory());
      }
      csc.setCapacity(haCreation.getCapacity());
      csc.setEvictionPolicy(haCreation.getEvictionPolicy());
    }
  }

  /**
   * When a <code>load-probe</code> element is encountered, create a new probe for the current
   * <code>CacheServer</code>.
   *
   * @since GemFire 5.7
   */
  private void endLoadProbe() {
    Declarable d = createDeclarable();
    if (!(d instanceof ServerLoadProbe)) {
      throw new CacheXmlException(String.format("A %s is not an instance of a %s",
          new Object[] {d.getClass().getName(), "BridgeLoadProbe"}));
    }
    stack.push(d);
  }

  private void endSerialGatewaySender() {
    GatewaySenderFactory senderFactory = (GatewaySenderFactory) stack.pop();
    String remoteDSString = (String) stack.pop();
    String id = (String) stack.pop();
    senderFactory.create(id, Integer.parseInt(remoteDSString));
  }

  private void endGatewayReceiver() {
    GatewayReceiverFactory receiverFactory = (GatewayReceiverFactory) stack.pop();
    receiverFactory.create();
  }

  private void startDynamicRegionFactory(Attributes atts) {
    // push attributes onto the stack for processing in endDynamicRegionFactory
    String disablePersist = atts.getValue(DISABLE_PERSIST_BACKUP);
    if (disablePersist == null) {
      stack.push("false");
    } else {
      stack.push(disablePersist);
    }
    String disableRegisterInterest = atts.getValue(DISABLE_REGISTER_INTEREST);
    if (disableRegisterInterest == null) {
      stack.push("false");
    } else {
      stack.push(disableRegisterInterest);
    }
    String poolName = atts.getValue(POOL_NAME);
    if (poolName == null) {
      stack.push(null);
    } else {
      stack.push(poolName);
    }

    // hi-jack RegionAttributesCreation for the disk-dirs, loader, writer and compressor
    RegionAttributesCreation attrs = new RegionAttributesCreation(this.cache);
    stack.push(attrs);
  }


  private void endDynamicRegionFactory() {
    File dir = null;
    RegionAttributesCreation attrs;
    {
      Object o = stack.pop();
      if (o instanceof File) {
        dir = (File) o;
        stack.pop(); // dir size to be popped out. being used by persistent directories
        attrs = (RegionAttributesCreation) stack.pop();
      } else {
        attrs = (RegionAttributesCreation) o;
      }
    }
    String poolName = (String) stack.pop();
    String disableRegisterInterest = (String) stack.pop();
    String disablePersistBackup = (String) stack.pop();
    DynamicRegionFactory.Config cfg;
    cfg = new DynamicRegionFactory.Config(dir, poolName,
        !Boolean.valueOf(disablePersistBackup).booleanValue(),
        !Boolean.valueOf(disableRegisterInterest).booleanValue());
    CacheCreation cache = (CacheCreation) stack.peek();
    cache.setDynamicRegionFactoryConfig(cfg);
  }

  /**
   * When a <code>gateway-conflict-resolver</code> element is encountered, create a new listener for
   * the <code>Cache</code>.
   */
  private void endGatewayConflictResolver() {
    Declarable d = createDeclarable();
    if (!(d instanceof GatewayConflictResolver)) {
      throw new CacheXmlException(
          String.format("A %s is not an instance of a GatewayConflictResolver.",
              d.getClass().getName()));
    }
    CacheCreation c = (CacheCreation) stack.peek();
    c.setGatewayConflictResolver((GatewayConflictResolver) d);
  }

  /**
   * When a <code>region</code> element is first encountered, we create a {@link RegionCreation}and
   * push it on the stack.
   */
  private void startRegion(Attributes atts) {
    String name = atts.getValue(NAME);
    String refid = atts.getValue(REFID);
    Assert.assertTrue(name != null);
    RegionCreation region = new RegionCreation(this.cache, name, refid);
    stack.push(region);
  }

  /**
   * When a <code>cache-transaction-manager</code> element is found, create a container for the
   * potential <code>transaction-listener</code> and push it on the stack
   */
  private void startCacheTransactionManager() {
    stack.push(new CacheTransactionManagerCreation());
  }

  /**
   * After popping the current <code>RegionCreation</code> off the stack, if the element on top of
   * the stack is a <code>RegionCreation</code>, then it is the parent region.
   */
  private void endRegion() throws RegionExistsException {
    RegionCreation region = (RegionCreation) stack.pop();
    boolean isRoot = false;
    if (stack.isEmpty()) {
      isRoot = true;
    } else if (!(stack.peek() instanceof RegionCreation)) {
      isRoot = true;
    }
    if (isRoot) {
      this.cache.addRootRegion(region);
    } else {
      RegionCreation parent = (RegionCreation) stack.peek();
      parent.addSubregion(region.getName(), region);
    }
  }

  /**
   * Add the <code>transaction-manager</code> creation code to the cache creation code
   */
  private void endCacheTransactionManager() {
    CacheTransactionManagerCreation txMgrCreation = (CacheTransactionManagerCreation) stack.pop();
    this.cache.addCacheTransactionManagerCreation(txMgrCreation);
  }

  /**
   * Create a <code>transaction-listener</code> using the declarable interface and set the
   * transaction manager with the newly instantiated listener.
   */
  private void endTransactionListener() {
    Declarable d = createDeclarable();
    if (!(d instanceof TransactionListener)) {
      throw new CacheXmlException(
          String.format("A %s is not an instance of a CacheListener.",
              d.getClass().getName()));
    }
    CacheTransactionManagerCreation txMgrCreation = (CacheTransactionManagerCreation) stack.peek();
    txMgrCreation.addListener((TransactionListener) d);
  }

  /**
   * When a <code>disk-store</code> element is first encountered, we create a
   * {@link DiskStoreAttributes}, populate it accordingly, and push it on the stack.
   */
  private void startDiskStore(Attributes atts) {
    // this is the only place to create DSAC objects
    DiskStoreAttributesCreation attrs = new DiskStoreAttributesCreation();
    String name = atts.getValue(NAME);
    if (name == null) {
      throw new InternalGemFireException(
          "Disk Store name is configured to use null name.");
    } else {
      attrs.setName(name);
    }

    String autoCompact = atts.getValue(AUTO_COMPACT);
    if (autoCompact != null) {
      attrs.setAutoCompact(Boolean.valueOf(autoCompact).booleanValue());
    }

    String compactionThreshold = atts.getValue(COMPACTION_THRESHOLD);
    if (compactionThreshold != null) {
      attrs.setCompactionThreshold(parseInt(compactionThreshold));
    }

    String allowForceCompaction = atts.getValue(ALLOW_FORCE_COMPACTION);
    if (allowForceCompaction != null) {
      attrs.setAllowForceCompaction(Boolean.valueOf(allowForceCompaction).booleanValue());
    }

    String maxOplogSize = atts.getValue(MAX_OPLOG_SIZE);
    if (maxOplogSize != null) {
      attrs.setMaxOplogSize(parseInt(maxOplogSize));
    }

    String timeInterval = atts.getValue(TIME_INTERVAL);
    if (timeInterval != null) {
      attrs.setTimeInterval(parseInt(timeInterval));
    }

    String writeBufferSize = atts.getValue(WRITE_BUFFER_SIZE);
    if (writeBufferSize != null) {
      attrs.setWriteBufferSize(parseInt(writeBufferSize));
    }

    String queueSize = atts.getValue(QUEUE_SIZE);
    if (queueSize != null) {
      attrs.setQueueSize(parseInt(queueSize));
    }

    String warnPct = atts.getValue(DISK_USAGE_WARNING_PERCENTAGE);
    if (warnPct != null) {
      attrs.setDiskUsageWarningPercentage(parseFloat(warnPct));
    }

    String criticalPct = atts.getValue(DISK_USAGE_CRITICAL_PERCENTAGE);
    if (criticalPct != null) {
      attrs.setDiskUsageCriticalPercentage(parseFloat(criticalPct));
    }

    stack.push(attrs);
  }

  private Integer getIntValue(Attributes atts, String param) {
    String maxInputFileSizeMB = atts.getValue(param);
    if (maxInputFileSizeMB != null) {
      try {
        return Integer.valueOf(maxInputFileSizeMB);
      } catch (NumberFormatException e) {
        throw new CacheXmlException(
            String.format("%s is not a valid integer for %s",
                new Object[] {maxInputFileSizeMB, param}),
            e);
      }
    }
    return null;
  }

  /**
   * Create a <code>transaction-writer</code> using the declarable interface and set the transaction
   * manager with the newly instantiated writer.
   */
  private void endTransactionWriter() {
    Declarable d = createDeclarable();
    if (!(d instanceof TransactionWriter)) {
      throw new CacheXmlException(
          String.format("A %s is not an instance of a TransactionWriter.",
              d.getClass().getName()));
    }
    CacheTransactionManagerCreation txMgrCreation = (CacheTransactionManagerCreation) stack.peek();
    txMgrCreation.setWriter((TransactionWriter) d);
  }

  /**
   * When a <code>region-attributes</code> element is first encountered, we create a
   * {@link RegionAttributesCreation}, populate it accordingly, and push it on the stack.
   */
  private void startRegionAttributes(Attributes atts) {
    RegionAttributesCreation attrs = new RegionAttributesCreation(this.cache);
    String scope = atts.getValue(SCOPE);
    if (scope == null) {
    } else if (scope.equals(LOCAL)) {
      attrs.setScope(Scope.LOCAL);
    } else if (scope.equals(DISTRIBUTED_NO_ACK)) {
      attrs.setScope(Scope.DISTRIBUTED_NO_ACK);
    } else if (scope.equals(DISTRIBUTED_ACK)) {
      attrs.setScope(Scope.DISTRIBUTED_ACK);
    } else if (scope.equals(GLOBAL)) {
      attrs.setScope(Scope.GLOBAL);
    } else {
      throw new InternalGemFireException(
          String.format("Unknown scope: %s", scope));
    }
    String mirror = atts.getValue(MIRROR_TYPE);
    if (mirror == null) {
    } else if (mirror.equals(NONE)) {
      attrs.setMirrorType(MirrorType.NONE);
    } else if (mirror.equals(KEYS)) {
      attrs.setMirrorType(MirrorType.KEYS);
    } else if (mirror.equals(KEYS_VALUES)) {
      attrs.setMirrorType(MirrorType.KEYS_VALUES);
    } else {
      throw new InternalGemFireException(
          String.format("Unknown mirror type: %s", mirror));
    }
    {
      String dp = atts.getValue(DATA_POLICY);
      if (dp == null) {
      } else if (dp.equals(NORMAL_DP)) {
        attrs.setDataPolicy(DataPolicy.NORMAL);
      } else if (dp.equals(PRELOADED_DP)) {
        attrs.setDataPolicy(DataPolicy.PRELOADED);
      } else if (dp.equals(EMPTY_DP)) {
        attrs.setDataPolicy(DataPolicy.EMPTY);
      } else if (dp.equals(REPLICATE_DP)) {
        attrs.setDataPolicy(DataPolicy.REPLICATE);
      } else if (dp.equals(PERSISTENT_REPLICATE_DP)) {
        attrs.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      } else if (dp.equals(PARTITION_DP)) {
        attrs.setDataPolicy(DataPolicy.PARTITION);
      } else if (dp.equals(PERSISTENT_PARTITION_DP)) {
        attrs.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
      } else {
        throw new InternalGemFireException(
            String.format("Unknown data policy: %s", dp));
      }
    }

    String initialCapacity = atts.getValue(INITIAL_CAPACITY);
    if (initialCapacity != null) {
      attrs.setInitialCapacity(parseInt(initialCapacity));
    }
    String concurrencyLevel = atts.getValue(CONCURRENCY_LEVEL);
    if (concurrencyLevel != null) {
      attrs.setConcurrencyLevel(parseInt(concurrencyLevel));
    }
    String concurrencyChecksEnabled = atts.getValue(CONCURRENCY_CHECKS_ENABLED);
    if (concurrencyChecksEnabled != null) {
      attrs.setConcurrencyChecksEnabled(Boolean.valueOf(concurrencyChecksEnabled).booleanValue());
    }
    String loadFactor = atts.getValue(LOAD_FACTOR);
    if (loadFactor != null) {
      attrs.setLoadFactor(parseFloat(loadFactor));
    }
    String statisticsEnabled = atts.getValue(STATISTICS_ENABLED);
    if (statisticsEnabled != null) {
      attrs.setStatisticsEnabled(Boolean.valueOf(statisticsEnabled).booleanValue());
    }
    String ignoreJTA = atts.getValue(IGNORE_JTA);
    if (ignoreJTA != null) {
      attrs.setIgnoreJTA(Boolean.valueOf(ignoreJTA).booleanValue());
    }
    String isLockGrantor = atts.getValue(IS_LOCK_GRANTOR);
    if (isLockGrantor != null) {
      attrs.setLockGrantor(Boolean.valueOf(isLockGrantor).booleanValue());
    }
    String persistBackup = atts.getValue(PERSIST_BACKUP);
    if (persistBackup != null) {
      attrs.setPersistBackup(Boolean.valueOf(persistBackup).booleanValue());
    }
    String earlyAck = atts.getValue(EARLY_ACK);
    if (earlyAck != null) {
      attrs.setEarlyAck(Boolean.valueOf(earlyAck).booleanValue());
    }
    String mcastEnabled = atts.getValue(MULTICAST_ENABLED);
    if (mcastEnabled != null) {
      attrs.setMulticastEnabled(Boolean.valueOf(mcastEnabled).booleanValue());
    }
    String indexUpdateType = atts.getValue(INDEX_UPDATE_TYPE);
    attrs.setIndexMaintenanceSynchronous(
        indexUpdateType == null || indexUpdateType.equals(INDEX_UPDATE_TYPE_SYNCH));

    String poolName = atts.getValue(POOL_NAME);
    if (poolName != null) {
      attrs.setPoolName(poolName);
    }
    String diskStoreName = atts.getValue(DISK_STORE_NAME);
    if (diskStoreName != null) {
      attrs.setDiskStoreName(diskStoreName);
    }
    String isDiskSynchronous = atts.getValue(DISK_SYNCHRONOUS);
    if (isDiskSynchronous != null) {
      attrs.setDiskSynchronous(Boolean.valueOf(isDiskSynchronous).booleanValue());
    }

    String id = atts.getValue(ID);
    if (id != null) {
      attrs.setId(id);
    }
    String refid = atts.getValue(REFID);
    if (refid != null) {
      attrs.setRefid(refid);
    }
    String enableSubscriptionConflation = atts.getValue(ENABLE_SUBSCRIPTION_CONFLATION);
    if (enableSubscriptionConflation != null) {
      attrs.setEnableSubscriptionConflation(
          Boolean.valueOf(enableSubscriptionConflation).booleanValue());
    }
    String enableBridgeConflation = atts.getValue(ENABLE_BRIDGE_CONFLATION);
    // as of 5.7 enable-bridge-conflation is deprecated.
    // so ignore it if enable-subscription-conflation is set
    if (enableBridgeConflation != null && enableSubscriptionConflation == null) {
      attrs.setEnableSubscriptionConflation(Boolean.valueOf(enableBridgeConflation).booleanValue());
    }
    if (enableBridgeConflation == null && enableSubscriptionConflation == null) {
      // 4.1 compatibility
      enableBridgeConflation = atts.getValue("enable-conflation");
      if (enableBridgeConflation != null) {
        attrs.setEnableSubscriptionConflation(
            Boolean.valueOf(enableBridgeConflation).booleanValue());
      }
    }
    /*
     * deprecated in prPersistSprint1 String publisherStr = atts.getValue(PUBLISHER); if
     * (publisherStr != null) { attrs.setPublisher(Boolean.valueOf(publisherStr).booleanValue()); }
     */
    String enableAsyncConflation = atts.getValue(ENABLE_ASYNC_CONFLATION);
    if (enableAsyncConflation != null) {
      attrs.setEnableAsyncConflation(Boolean.valueOf(enableAsyncConflation).booleanValue());
    }
    String cloningEnabledStr = atts.getValue(CLONING_ENABLED);
    if (cloningEnabledStr != null) {
      attrs.setCloningEnable(Boolean.valueOf(cloningEnabledStr).booleanValue());
    }
    String gatewaySenderIds = atts.getValue(GATEWAY_SENDER_IDS);
    if (gatewaySenderIds != null && (gatewaySenderIds.length() != 0)) {
      StringTokenizer st = new StringTokenizer(gatewaySenderIds, ",");
      while (st.hasMoreElements()) {
        attrs.addGatewaySenderId(st.nextToken());
      }
    }
    String asyncEventQueueIds = atts.getValue(ASYNC_EVENT_QUEUE_IDS);
    if (asyncEventQueueIds != null && (asyncEventQueueIds.length() != 0)) {
      StringTokenizer st = new StringTokenizer(asyncEventQueueIds, ",");
      while (st.hasMoreElements()) {
        attrs.addAsyncEventQueueId(st.nextToken());
      }
    }
    String offHeapStr = atts.getValue(OFF_HEAP);
    if (offHeapStr != null) {
      attrs.setOffHeap(Boolean.valueOf(offHeapStr).booleanValue());
    }

    stack.push(attrs);
  }

  /**
   * After popping the current <code>DiskStoreAttributesCreation</code> off the stack, we add it to
   * the <code>DiskStoreAttionCreation</code> that should be on the top of the stack.
   */
  private void endDiskStore() {
    DiskStoreAttributesCreation dsac = (DiskStoreAttributesCreation) stack.pop();
    CacheCreation cache;
    Object top = stack.peek();
    if (top instanceof CacheCreation) {
      cache = (CacheCreation) top;
    } else {
      String s = "Did not expected a " + top.getClass().getName() + " on top of the stack.";
      Assert.assertTrue(false, s);
      cache = null; // Dead code
    }
    String name = dsac.getName();
    if (name != null) {
      cache.setDiskStore(name, dsac);
    }
  }

  /**
   * After popping the current <code>RegionAttributesCreation</code> off the stack, we add it to the
   * <code>RegionCreation</code> that should be on the top of the stack.
   */
  private void endRegionAttributes() {
    RegionAttributesCreation attrs = (RegionAttributesCreation) stack.pop();
    CacheCreation cache;
    Object top = stack.peek();
    if (top instanceof RegionCreation) {
      RegionCreation region = (RegionCreation) top;
      region.setAttributes(attrs);
      cache = (CacheCreation) region.getCache();
    } else if (top instanceof CacheCreation) {
      cache = (CacheCreation) top;
    } else {
      String s = "Did not expected a " + top.getClass().getName() + " on top of the stack.";
      Assert.assertTrue(false, s);
      cache = null; // Dead code
    }
    String id = attrs.getId();
    if (id != null) {
      cache.setRegionAttributes(id, attrs);
    }
  }

  /**
   * When a <code>cache</code> element is finished
   */
  private void endCache() {}

  /**
   * When a <code>client-cache</code> element is finished
   */
  private void endClientCache() {}

  /**
   * <p>
   * When the end of a <code>string</code> element is encountered, convert the data to a
   * <code>String</code>
   */
  // This converts the <code>StringBuffer</code> to a
  // <code>String</code> because a </code>StringBuffer</code> is
  // solely used (as a marker) by the <code>characters</code> method
  // and by doing this conversion we allow for multiple consecutive string
  // elements, otherwise <code>characters</code> would continue to
  // append and our stack order would be out of whack. See bug 32122.
  private void endString() {
    StringBuffer str = (StringBuffer) stack.pop();
    stack.push(str.toString()/* .trim() */);
  }

  /**
   * finish parsing a "group" element which is just a string
   *
   * @since GemFire 5.7
   */
  private void endGroup() {
    StringBuffer str = (StringBuffer) stack.pop();
    stack.push(str.toString().trim());
  }

  private void endClassName() {
    StringBuffer str = (StringBuffer) stack.pop();
    stack.push(str.toString().trim()); // trim fixes bug 32928
  }

  /**
   * When an <code>entry</code> element is finished, the <code>value</code> should be on the stop of
   * the stack followed by the <code>key</code>. The <code>RegionCreation</code> for the region
   * being created should be below that.
   */
  private void endEntry() {
    Object value = stack.pop();
    Object key = stack.pop();
    RegionCreation region = (RegionCreation) stack.peek();
    // changed by mitul after modifying code for Region implements Map
    region.put(key, value);
  }

  /**
   * When a <code>key-constraint</code> element is finished, the name of the class should be on top
   * of the stack.
   *
   * @throws CacheXmlException If the key constraint class cannot be loaded
   */
  private void endKeyConstraint() {
    String className = ((StringBuffer) stack.pop()).toString().trim();
    Class c;
    try {
      c = InternalDataSerializer.getCachedClass(className);
    } catch (Exception ex) {
      throw new CacheXmlException(
          String.format("Could not load key-constraint class: %s",
              className),
          ex);
    }
    // The region attributes should be on top of the stack
    RegionAttributesCreation attrs = peekRegionAttributesContext("key-constraint");
    attrs.setKeyConstraint(c);
  }

  /**
   * When a <code>value-constraint</code> element is finished, the name of the class should be on
   * top of the stack.
   *
   * @throws CacheXmlException If the value constraint class cannot be loaded
   */
  private void endValueConstraint() {
    String className = ((StringBuffer) stack.pop()).toString().trim();
    Class c;
    try {
      c = InternalDataSerializer.getCachedClass(className);
    } catch (Exception ex) {
      throw new CacheXmlException(
          String.format("Could not load value-constraint class: %s",
              className),
          ex);
    }
    // The region attributes should be on top of the stack
    RegionAttributesCreation attrs = peekRegionAttributesContext("value-constraint");
    attrs.setValueConstraint(c);
  }

  /**
   * When a <code>region-time-to-live</code> element is finished, the {@link ExpirationAttributes}
   * are on top of the stack followed by the {@link RegionAttributesCreation} to which the
   * expiration attributes are assigned.
   */
  private void endRegionTimeToLive() {
    ExpirationAttributes expire = (ExpirationAttributes) stack.pop();
    RegionAttributesCreation attrs = peekRegionAttributesContext("region-time-to-live");
    attrs.setRegionTimeToLive(expire);
  }

  /**
   * When a <code>region-idle-time</code> element is finished, the {@link ExpirationAttributes} are
   * on top of the stack followed by the {@link RegionAttributesCreation} to which the expiration
   * attributes are assigned.
   */
  private void endRegionIdleTime() {
    ExpirationAttributes expire = (ExpirationAttributes) stack.pop();
    RegionAttributesCreation attrs = peekRegionAttributesContext("region-idle-time");
    attrs.setRegionIdleTimeout(expire);
  }

  private RegionAttributesCreation peekRegionAttributesContext(String dependentElement) {
    Object a = stack.peek();
    if (!(a instanceof RegionAttributesCreation)) {
      throw new CacheXmlException(
          String.format("A %s must be defined in the context of region-attributes.",
              dependentElement));
    }
    return (RegionAttributesCreation) a;
  }

  private PartitionAttributesImpl peekPartitionAttributesImpl(String dependentElement) {
    Object a = stack.peek();
    if (!(a instanceof PartitionAttributesImpl)) {
      throw new CacheXmlException(
          String.format("A %s must be defined in the context of partition-attributes",
              dependentElement));
    }
    return (PartitionAttributesImpl) a;
  }


  /**
   * When a <code>entry-time-to-live</code> element is finished, an optional Declarable (the
   * custom-expiry) is followed by the {@link ExpirationAttributes} are on top of the stack followed
   * by either the {@link RegionAttributesCreation} to which the expiration attributes are assigned,
   * or the attributes for a {@link PartitionAttributes} to which the attributes are assigned.
   */
  private void endEntryTimeToLive() {
    Declarable custom = null;
    if (stack.peek() instanceof Declarable) {
      custom = (Declarable) stack.pop();
    }
    ExpirationAttributes expire = (ExpirationAttributes) stack.pop();
    Object a = stack.peek();
    // if (a instanceof PartitionAttributesFactory) {
    // ((PartitionAttributesFactory) a).setEntryTimeToLive(expire);
    // } else
    if (a instanceof RegionAttributesCreation) {
      ((RegionAttributesCreation) a).setEntryTimeToLive(expire);
      if (custom != null) {
        ((RegionAttributesCreation) a).setCustomEntryTimeToLive((CustomExpiry) custom);
      }
    } else {
      throw new CacheXmlException(
          String.format(
              "A %s must be defined in the context of region-attributes or partition-attributes.",
              ENTRY_TIME_TO_LIVE));
    }
  }

  /**
   * When a <code>entry-idle-time</code> element is finished, an optional Declarable (the
   * custom-expiry) is followed by the {@link ExpirationAttributes} are on top of the stack followed
   * by the {@link RegionAttributesCreation} to which the expiration attributes are assigned.
   */
  private void endEntryIdleTime() {
    Declarable custom = null;
    if (stack.peek() instanceof Declarable) {
      custom = (Declarable) stack.pop();
    }
    ExpirationAttributes expire = (ExpirationAttributes) stack.pop();
    Object a = stack.peek();
    // if (a instanceof PartitionAttributesFactory) {
    // ((PartitionAttributesFactory) a).setEntryIdleTimeout(expire);
    // } else
    if (a instanceof RegionAttributesCreation) {
      ((RegionAttributesCreation) a).setEntryIdleTimeout(expire);
      if (custom != null) {
        ((RegionAttributesCreation) a).setCustomEntryIdleTimeout((CustomExpiry) custom);
      }
    } else {
      throw new CacheXmlException(
          String.format(
              "A %s must be defined in the context of region-attributes or partition-attributes.",
              ENTRY_IDLE_TIME));
    }
  }

  /**
   * When a <code>partition-attributes</code> element is finished, the {@link PartitionAttributes}
   * are on top of the stack followed by the {@link RegionAttributesCreation} to which the partition
   * attributes are assigned.
   */
  private void endPartitionAttributes() {
    PartitionAttributesImpl paf = (PartitionAttributesImpl) stack.pop();
    paf.validateAttributes();

    RegionAttributesCreation rattrs = peekRegionAttributesContext(PARTITION_ATTRIBUTES);
    // change the 5.0 default data policy (EMPTY) to the current default
    if (rattrs.hasDataPolicy() && rattrs.getDataPolicy().isEmpty()
        && (this.version.compareTo(CacheXmlVersion.GEMFIRE_5_0) == 0)) {
      rattrs.setDataPolicy(PartitionedRegionHelper.DEFAULT_DATA_POLICY);
    }
    rattrs.setPartitionAttributes(paf);
  }

  /**
   * When a <code>fixed-partition-attributes</code> element is finished
   */
  private void endFixedPartitionAttributes() {}

  /**
   * When a <code>membership-attributes</code> element is finished, the arguments for constructing
   * the MembershipAttributes are on the stack.
   */
  private void endMembershipAttributes() {
    Set roles = new HashSet();
    Object obj = null;
    while (!(obj instanceof Object[])) {
      obj = stack.pop();
      if (obj instanceof String) {
        // found a required-role name
        roles.add(obj);
      }
    }

    Object[] attrs = (Object[]) obj;
    String laName = ((String) attrs[0]).toUpperCase().replace('-', '_');
    String raName = ((String) attrs[1]).toUpperCase().replace('-', '_');

    LossAction laction = LossAction.fromName(laName);
    ResumptionAction raction = ResumptionAction.fromName(raName);

    MembershipAttributes ra = new MembershipAttributes(
        (String[]) roles.toArray(new String[0]), laction, raction);
    RegionAttributesCreation rattrs = (RegionAttributesCreation) stack.peek();
    rattrs.setMembershipAttributes(ra);
  }

  /**
   * When a <code>required-role</code> element is finished,
   */
  private void endRequiredRole() {
    // do nothing... wait for endMembershipAttributes()
  }

  /**
   * When a <code>disk-write-attributes</code> element is finished, the {@link DiskWriteAttributes}
   * is on top of the stack followed by the {@link RegionAttributesCreation} to which the expiration
   * attributes are assigned.
   */
  private void endDiskWriteAttributes() {
    DiskWriteAttributes dwa = (DiskWriteAttributes) stack.pop();
    RegionAttributesCreation attrs = peekRegionAttributesContext(DISK_WRITE_ATTRIBUTES);
    attrs.setDiskWriteAttributes(dwa);
  }

  /**
   * When a <code>disk-dir</code> element is finished, the name of the directory is on top of the
   * stack. Create a new {@link File}and push it on the stack.
   */
  private void endDiskDir() {
    StringBuffer dirName = (StringBuffer) stack.pop();
    File dir = new File(dirName.toString().trim());
    if (!dir.exists()) {

    }
    stack.push(dir);
  }

  /**
   * When a <code>disk-dirs</code> element is finished, the directory {@link File}s are on the stack
   * followed by the {@link RegionAttributesCreation} to which the expiration attributes are
   * assigned.
   */
  private void endDiskDirs() {
    List dirs = new ArrayList();
    List sizes = new ArrayList();
    while (stack.peek() instanceof File) {
      dirs.add(stack.pop());
      sizes.add(stack.pop());
    }
    Assert.assertTrue(!dirs.isEmpty());
    Assert.assertTrue(!sizes.isEmpty());

    // should set the disk-dirs and sizes in reverse order since parsers would have reversed
    // the order because of pushing into stack
    File[] disks = new File[dirs.size()];
    int dirsLength = dirs.size();
    for (int i = 0; i < dirsLength; i++) {
      disks[i] = (File) dirs.get((dirsLength - 1) - i);
    }

    int[] diskSizes = new int[sizes.size()];
    for (int i = 0; i < dirsLength; i++) {
      diskSizes[i] = ((Integer) sizes.get((dirsLength - 1) - i)).intValue();
    }

    Object a = stack.peek();
    if (a instanceof RegionAttributesCreation) {
      RegionAttributesCreation attrs = (RegionAttributesCreation) a;
      attrs.setDiskDirsAndSize(disks, diskSizes);
    } else if (a instanceof DiskStoreAttributesCreation) {
      DiskStoreAttributesCreation attrs = (DiskStoreAttributesCreation) a;
      attrs.setDiskDirsAndSize(disks, diskSizes);
    } else {
      throw new CacheXmlException(
          String.format("A %s must be defined in the context of region-attributes.",
              DISK_DIRS));
    }
  }

  /**
   * When a <code>synchronous-writes</code> element is encounter, we push a
   * {@link DiskWriteAttributes} for doing synchronous writes on the stack.
   */
  private void startSynchronousWrites() {
    int maxOplogSize = ((Integer) stack.pop()).intValue();
    String rollOplog = (String) stack.pop();
    // convery megabytes to bytes for DiskWriteAttributes creation
    long maxOplogSizeInBytes = maxOplogSize;
    maxOplogSizeInBytes = maxOplogSizeInBytes * 1024 * 1024;
    Properties props = new Properties();
    props.setProperty(MAX_OPLOG_SIZE, String.valueOf(maxOplogSizeInBytes));
    props.setProperty(ROLL_OPLOG, rollOplog);
    props.setProperty(DiskWriteAttributesImpl.SYNCHRONOUS_PROPERTY, "true");
    stack.push(new DiskWriteAttributesImpl(props));
  }

  /**
   * When a <code>asynchronous-writes</code> element is encounter, we push a
   * {@link DiskWriteAttributes} for doing asynchronous writes on the stack.
   */
  private void startAsynchronousWrites(Attributes atts) {
    int maxOplogSize = ((Integer) stack.pop()).intValue();
    String rollOplog = (String) stack.pop();
    // convery megabytes to bytes for DiskWriteAttributes creation
    long maxOplogSizeInBytes = maxOplogSize;
    maxOplogSizeInBytes = maxOplogSizeInBytes * 1024 * 1024;
    long timeInterval = parseLong(atts.getValue(TIME_INTERVAL));
    long bytesThreshold = parseLong(atts.getValue(BYTES_THRESHOLD));
    Properties props = new Properties();
    props.setProperty(MAX_OPLOG_SIZE, String.valueOf(maxOplogSizeInBytes));
    props.setProperty(ROLL_OPLOG, rollOplog);
    props.setProperty(TIME_INTERVAL, String.valueOf(timeInterval));
    props.setProperty(DiskWriteAttributesImpl.SYNCHRONOUS_PROPERTY, "false");
    props.setProperty(BYTES_THRESHOLD, String.valueOf(bytesThreshold));
    stack.push(new DiskWriteAttributesImpl(props));
  }

  /**
   * When a <code>parition-attributes</code> element is encountered, we push a ParitionAttributes??
   * for configuring paritioned storage on the stack.
   */
  private void startPartitionAttributes(Attributes atts) {
    PartitionAttributesImpl paf = new PartitionAttributesImpl();
    String redundancy = atts.getValue(PARTITION_REDUNDANT_COPIES);
    if (redundancy != null) {
      paf.setRedundantCopies(parseInt(redundancy));
    }
    String localMaxMem = atts.getValue(LOCAL_MAX_MEMORY);
    if (localMaxMem != null) {
      paf.setLocalMaxMemory(parseInt(localMaxMem));
    }
    String totalMaxMem = atts.getValue(TOTAL_MAX_MEMORY);
    if (totalMaxMem != null) {
      paf.setTotalMaxMemory(parseLong(totalMaxMem));
    }
    String totalNumBuckets = atts.getValue(TOTAL_NUM_BUCKETS);
    if (totalNumBuckets != null) {
      paf.setTotalNumBuckets(parseInt(totalNumBuckets));
    }
    String colocatedWith = atts.getValue(PARTITION_COLOCATED_WITH);
    if (colocatedWith != null) {
      paf.setColocatedWith(colocatedWith);
    }
    String recoveryDelay = atts.getValue(RECOVERY_DELAY);
    if (recoveryDelay != null) {
      paf.setRecoveryDelay(parseInt(recoveryDelay));
    }
    String startupRecoveryDelay = atts.getValue(STARTUP_RECOVERY_DELAY);
    if (startupRecoveryDelay != null) {
      paf.setStartupRecoveryDelay(parseInt(startupRecoveryDelay));
    }
    stack.push(paf);
  }

  /**
   * When a <code>fixed-partition-attributes</code> element is encountered, we create an instance of
   * FixedPartitionAttributesImpl and add it to the PartitionAttributesImpl stack.
   */
  private void startFixedPartitionAttributes(Attributes atts) {
    FixedPartitionAttributesImpl fpai = new FixedPartitionAttributesImpl();
    String partitionName = atts.getValue(PARTITION_NAME);
    if (partitionName != null) {
      fpai.setPartitionName(partitionName);
    }
    String isPrimary = atts.getValue(IS_PRIMARY);
    if (isPrimary != null) {
      fpai.isPrimary(parseBoolean(isPrimary));
    }
    String numBuckets = atts.getValue(NUM_BUCKETS);
    if (numBuckets != null) {
      fpai.setNumBuckets(parseInt(numBuckets));
    }
    Object a = stack.peek();
    if (a instanceof PartitionAttributesImpl) {
      ((PartitionAttributesImpl) a).addFixedPartitionAttributes(fpai);
    }
  }

  /**
   * When a <code>membership-attributes</code> element is encountered, we push an array of
   * attributes for creation of a MembershipAttributes.
   */
  private void startMembershipAttributes(Attributes atts) {
    Object[] attrs = new Object[2]; // loss-action, resumption-action
    attrs[0] = atts.getValue(LOSS_ACTION) == null ? LossAction.NO_ACCESS.toString()
        : atts.getValue(LOSS_ACTION);
    attrs[1] = atts.getValue(RESUMPTION_ACTION) == null ? ResumptionAction.REINITIALIZE.toString()
        : atts.getValue(RESUMPTION_ACTION);

    stack.push(attrs);
  }

  /**
   * When a <code>subscription-attributes</code> element is first encountered, we create an
   * SubscriptionAttibutes?? object from the element's attributes and stick it in the current region
   * attributes.
   */
  private void startSubscriptionAttributes(Attributes atts) {
    String ip = atts.getValue(INTEREST_POLICY);
    SubscriptionAttributes sa;
    if (ip == null) {
      sa = new SubscriptionAttributes();
    } else if (ip.equals(ALL)) {
      sa = new SubscriptionAttributes(InterestPolicy.ALL);
    } else if (ip.equals(CACHE_CONTENT)) {
      sa = new SubscriptionAttributes(InterestPolicy.CACHE_CONTENT);
    } else {
      throw new InternalGemFireException(
          String.format("Unknown interest-policy: %s", ip));
    }
    RegionAttributesCreation rattrs = (RegionAttributesCreation) stack.peek();
    rattrs.setSubscriptionAttributes(sa);
  }

  /**
   * When a <code>required-role</code> element is encountered, we push a string for creation of
   * MembershipAttributes.
   */
  private void startRequiredRole(Attributes atts) {
    stack.push(atts.getValue(NAME));
  }

  /**
   * When a <code>index</code> element is encounter, we create the IndexCreationData object from the
   * Stack. Set the required parameters in the IndexCreationData object & push it on stack.
   *
   */
  private void startIndex(Attributes atts) {
    boolean isPrimary = false;
    String type = "";
    IndexCreationData icd = new IndexCreationData(atts.getValue(NAME));

    int len = atts.getLength();
    if (len > 1) {
      if (Boolean.valueOf(atts.getValue(KEY_INDEX))) {
        icd.setIndexType(IndexType.PRIMARY_KEY);
        isPrimary = true;
      }
      type = atts.getValue(INDEX_TYPE);
    }

    if (len > 2) {
      String fromClause = atts.getValue(FROM_CLAUSE);
      String expression = atts.getValue(EXPRESSION);
      String importStr = atts.getValue(IMPORTS);
      if (isPrimary) {
        icd.setIndexData(IndexType.PRIMARY_KEY, null, expression, null);
      } else {
        if (type == null) {
          type = RANGE_INDEX_TYPE;
        }
        if (type.equals(HASH_INDEX_TYPE)) {
          icd.setIndexData(IndexType.HASH, fromClause, expression, importStr);
        } else if (type.equals(RANGE_INDEX_TYPE)) {
          icd.setIndexData(IndexType.FUNCTIONAL, fromClause, expression, importStr);
        } else {
          if (logger.isTraceEnabled(LogMarker.CACHE_XML_PARSER_VERBOSE)) {
            logger.trace(LogMarker.CACHE_XML_PARSER_VERBOSE,
                "Unknown index type defined as {}, will be set to range index", type);
          }
          icd.setIndexData(IndexType.FUNCTIONAL, fromClause, expression, importStr);
        }
      }
    }
    this.stack.push(icd);
  }

  /**
   * When index element is ending we need to verify all attributes because of new index tag
   * definition since 6.6.1 and support previous definition also.
   *
   * if <code>functional</code> element was not there then we need to validate expression and
   * fromClause as not null.
   */
  private void endIndex() {
    boolean throwExcep = false;
    IndexCreationData icd = (IndexCreationData) this.stack.pop();

    if (icd.getIndexType() == null) {
      throwExcep = true;
    } else {
      if (icd.getIndexType().equals(IndexType.PRIMARY_KEY)) {
        if (icd.getIndexExpression() == null) {
          throwExcep = true;
        }
      } else {
        if (icd.getIndexExpression() == null && icd.getIndexFromClause() == null) {
          throwExcep = true;
        }
      }
    }

    if (!throwExcep) {
      RegionCreation rc = (RegionCreation) this.stack.peek();
      rc.addIndexData(icd);
    } else {
      throw new InternalGemFireException(
          "CacheXmlParser::endIndex:Index creation attribute not correctly specified.");
    }
  }

  /**
   * When a <code>functional</code> element is encounter, we pop the IndexCreationData object from
   * the Stack. Set the required parameters in the IndexCreationData object & set it in
   * RegionCreation object.
   *
   */
  private void startFunctionalIndex(Attributes atts) {
    boolean throwExcep = false;
    IndexCreationData icd = (IndexCreationData) this.stack.peek();
    // icd.setIndexType(FUNCTIONAL);
    int len = -1;
    if ((len = atts.getLength()) > 1) {
      String fromClause = atts.getValue(FROM_CLAUSE);
      String expression = atts.getValue(EXPRESSION);
      String importStr = null;
      if (len == 3)
        importStr = atts.getValue(IMPORTS);
      if (fromClause == null || expression == null) {
        throwExcep = true;
      } else {
        icd.setIndexData(IndexType.FUNCTIONAL, fromClause, expression, importStr);
      }
    } else {
      throwExcep = true;
    }
    if (throwExcep) {
      throw new InternalGemFireException(
          "CacheXmlParser::startFunctionalIndex:Index creation attribute not correctly specified.");
    }
  }


  /**
   * When a <code>primary-key</code> element is encounter, we pop the IndexCreationData object from
   * the Stack. Set the required parameters in the IndexCreationData object & set it in
   * RegionCreation object.
   *
   */
  private void startPrimaryKeyIndex(Attributes atts) {
    IndexCreationData icd = (IndexCreationData) this.stack.peek();
    // icd.setIndexType(PRIMARY_KEY);
    boolean throwExcep = false;
    if (atts.getLength() == 1) {
      String field = atts.getValue(FIELD);
      if (field == null) {
        throwExcep = true;
      } else {
        icd.setIndexData(IndexType.PRIMARY_KEY, null, field, null);
      }
    } else {
      throwExcep = true;
    }
    if (throwExcep) {
      throw new InternalGemFireException(
          "CacheXmlParser::startPrimaryKeyIndex:Primary-Key Index creation field is null.");
    }
  }

  /**
   * When a <code>expiration-attributes</code> element is first encountered, we create an
   * ExpirationAttibutes?? object from the element's attributes and push it on the stack.
   */
  private void startExpirationAttributes(Attributes atts) {
    int timeout = parseInt(atts.getValue(TIMEOUT));
    String action = atts.getValue(ACTION);
    ExpirationAttributes expire;
    if (action == null) {
      expire = new ExpirationAttributes(timeout);
    } else if (action.equals(INVALIDATE)) {
      expire = new ExpirationAttributes(timeout, ExpirationAction.INVALIDATE);
    } else if (action.equals(DESTROY)) {
      expire = new ExpirationAttributes(timeout, ExpirationAction.DESTROY);
    } else if (action.equals(LOCAL_INVALIDATE)) {
      expire = new ExpirationAttributes(timeout, ExpirationAction.LOCAL_INVALIDATE);
    } else if (action.equals(LOCAL_DESTROY)) {
      expire = new ExpirationAttributes(timeout, ExpirationAction.LOCAL_DESTROY);
    } else {
      throw new InternalGemFireException(
          String.format("Unknown expiration action: %s", action));
    }
    stack.push(expire);
  }

  /**
   * When a <code>serializer-registration element is first encountered, we need to create the
   * wrapper object to hold the data, and put it on the stack.
   */
  private void startSerializerRegistration() {
    SerializerCreation sc = new SerializerCreation();
    this.stack.push(sc);
  }

  /**
   * When an <code>instantiator</code> element is first encountered, we need to hang on to the id
   * attribute for use in registration in the end tag function.
   */
  private void startInstantiator(Attributes atts) {
    int id = parseInt(atts.getValue(ID));
    this.stack.push(id);
  }

  /**
   * Creates and initializes an instance of {@link Declarable} from the contents of the stack.
   *
   * @throws CacheXmlException Something goes wrong while instantiating or initializing the
   *         declarable
   */
  private Declarable createDeclarable() {
    return createDeclarable(cache, stack);
  }

  /**
   * Creates and initializes an instance of {@link Declarable} from the contents of the stack.
   *
   * @throws CacheXmlException Something goes wrong while instantiating or initializing the
   *         declarable
   */
  public static Declarable createDeclarable(CacheCreation cache, Stack<Object> stack) {
    Properties props = new Properties();
    Object top = stack.pop();
    while (top instanceof Parameter) {
      Parameter param = (Parameter) top;
      props.put(param.getName(), param.getValue());
      top = stack.pop();
    }
    if (logger.isTraceEnabled(LogMarker.CACHE_XML_PARSER_VERBOSE)) {
      logger.trace(LogMarker.CACHE_XML_PARSER_VERBOSE, "XML Parser createDeclarable properties: {}",
          props);
    }
    Assert.assertTrue(top instanceof String);
    String className = (String) top;
    if (logger.isTraceEnabled(LogMarker.CACHE_XML_PARSER_VERBOSE)) {
      logger.trace(LogMarker.CACHE_XML_PARSER_VERBOSE, "XML Parser createDeclarable class name: {}",
          className);
    }
    Class c;
    try {
      c = InternalDataSerializer.getCachedClass(className);
    } catch (Exception ex) {
      throw new CacheXmlException("Could not find the class: " + className, ex);
    }
    Object o;
    try {
      o = c.newInstance();
    } catch (Exception ex) {
      throw new CacheXmlException("Could not create an instance of " + className, ex);
    }
    if (!(o instanceof Declarable)) {
      throw new CacheXmlException(
          String.format("Class %s is not an instance of Declarable.",
              className));
    }
    Declarable d = (Declarable) o;
    // init call done later in GemFireCacheImpl.addDeclarableProperties
    cache.addDeclarableProperties(d, props);

    return d;
  }

  /**
   * Ending the <code>compressor</code> registration should leave us with a class name on the stack.
   * Pull it off and setup the {@link Compressor} on the region attributes.
   */
  private void endCompressor() {
    Class<?> klass = getClassFromStack();
    if (!Compressor.class.isAssignableFrom(klass)) {
      throw new CacheXmlException(
          String.format("A %s is not an instance of a Compressor.",
              klass.getName()));
    }

    Compressor compressor;
    try {
      compressor = (Compressor) klass.newInstance();
    } catch (Exception ex) {
      throw new CacheXmlException(String.format("While instantiating a %s",
          klass.getName()), ex);
    }

    Object a = stack.peek();

    if (a instanceof RegionAttributesCreation) {
      RegionAttributesCreation attrs = (RegionAttributesCreation) a;
      attrs.setCompressor(compressor);
    } else {
      throw new CacheXmlException(
          String.format("A %s must be defined in the context of region-attributes or %s",
              new Object[] {COMPRESSOR, DYNAMIC_REGION_FACTORY}));
    }
  }

  /**
   * When a <code>cache-loader</code> element is finished, the {@link Parameter}s and class names
   * are popped off the stack. The cache loader is instantiated and initialized with the parameters,
   * if appropriate. When the loader is being created in a dynamic-region-factory, there may be a
   * disk-dir element on the stack, represented by a File object. Otherwise, dynamic-region-factory
   * uses a RegionAttributesCreation, just like a region, and is treated the same.<p) The loader may
   * also be created in the context of partition-attributes.
   */
  private void endCacheLoader() {
    Declarable d = createDeclarable();
    if (!(d instanceof CacheLoader)) {
      throw new CacheXmlException(
          String.format("A %s is not an instance of a CacheLoader.",
              d.getClass().getName()));
    }
    // Two peeks required to handle dynamic region context
    Object a = stack.peek();
    // check for disk-dir
    if ((a instanceof File)) {
      Object sav = stack.pop();
      a = stack.peek();
      if (!(a instanceof RegionAttributesCreation)) {
        throw new CacheXmlException(
            "A cache-loader must be defined in the context of region-attributes.");
      }
      stack.push(sav);
      RegionAttributesCreation attrs = (RegionAttributesCreation) a;
      attrs.setCacheLoader((CacheLoader) d);
    }
    // check for normal region-attributes
    else if (a instanceof RegionAttributesCreation) {
      RegionAttributesCreation attrs = (RegionAttributesCreation) a;
      attrs.setCacheLoader((CacheLoader) d);
    } else {
      throw new CacheXmlException(
          String.format("A %s must be defined in the context of region-attributes or %s",
              new Object[] {CACHE_LOADER, DYNAMIC_REGION_FACTORY}));
    }
  }

  /**
   * When a <code>cache-writer</code> element is finished, the {@link Parameter}s and class names
   * are popped off the stack. The cache writer is instantiated and initialized with the parameters,
   * if appropriate.
   * <p>
   * A cache-writer may be created in the context of region-attributes or dynamic-region-factory. In
   * the latter case, there may be a disk-dir on top of the stack, represented by a File object.
   */
  private void endCacheWriter() {
    Declarable d = createDeclarable();
    if (!(d instanceof CacheWriter)) {
      throw new CacheXmlException(
          String.format("A %s is not an instance of a CacheWriter.",
              d.getClass().getName()));
    }

    Object a = stack.peek();
    // check for partition-attributes
    // if (a instanceof PartitionAttributesFactory) {
    // PartitionAttributesFactory fac = (PartitionAttributesFactory) a;
    // fac.setCacheWriter((CacheWriter) d);
    // }
    // else
    // check for disk-dir
    if ((a instanceof File)) {
      Object sav = stack.pop();
      Object size = stack.pop(); // pop out disk size
      a = stack.peek();
      //
      if (!(a instanceof RegionAttributesCreation)) {
        throw new CacheXmlException(
            String.format("%s must be defined in the context of %s",
                new Object[] {CACHE_WRITER, DYNAMIC_REGION_FACTORY}));
      }
      stack.push(size);
      stack.push(sav);
    }
    // check for normal region-attributes
    else if (!(a instanceof RegionAttributesCreation)) {
      throw new CacheXmlException(
          String.format("%s must be defined in the context of region-attributes.",
              CACHE_WRITER));
    }

    RegionAttributesCreation attrs = (RegionAttributesCreation) a;
    attrs.setCacheWriter((CacheWriter) d);
  }

  private void endCustomExpiry() {

    Declarable d = createDeclarable();
    if (!(d instanceof CustomExpiry)) {
      throw new CacheXmlException(
          String.format("A %s is not an instance of CustomExpiry",
              d.getClass().getName()));
    }
    stack.push(d);
  }


  /**
   * Create an <code>lru-entry-count</code> eviction controller, assigning it to the enclosed
   * <code>region-attributes</code>. Allow any combination of attributes to be provided. Use the
   * default values for any attribute that is not provided.
   *
   */
  private void startLRUEntryCount(Attributes atts) {
    final String maximum = atts.getValue(MAXIMUM);
    int max = EvictionAttributes.DEFAULT_ENTRIES_MAXIMUM;
    if (maximum != null) {
      max = parseInt(maximum);
    }
    final String lruAction = atts.getValue(ACTION);
    EvictionAction action = EvictionAction.DEFAULT_EVICTION_ACTION;
    if (lruAction != null) {
      action = EvictionAction.parseAction(lruAction);
    }
    RegionAttributesCreation regAttrs = peekRegionAttributesContext(LRU_ENTRY_COUNT);
    regAttrs.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(max, action));
  }

  /**
   * Start the configuration of a <code>lru-memory-size</code> eviction controller. Allow for any of
   * the attributes to be missing. Store the attributes on the stack anticipating the declaration of
   * an {@link ObjectSizer}.
   *
   */
  private void startLRUMemorySize(Attributes atts) {
    String lruAction = atts.getValue(ACTION);
    EvictionAction action = EvictionAction.DEFAULT_EVICTION_ACTION;
    if (lruAction != null) {
      action = EvictionAction.parseAction(lruAction);
    }
    String maximum = atts.getValue(MAXIMUM);
    int max = EvictionAttributes.DEFAULT_MEMORY_MAXIMUM;
    if (maximum != null) {
      max = parseInt(maximum);
    }
    // Store for later addition of ObjectSizer, if any (the cast is for clarity sake)
    stack.push(EvictionAttributes.createLRUMemoryAttributes(max, null, action));
  }

  /**
   * Complete the configuration of a <code>lru-memory-size</code> eviction controller. Check for the
   * declaration of an {@link ObjectSizer}. Assign the attributes to the enclose
   * <code>region-attributes</code>
   */
  private void endLRUMemorySize() {
    Object declCheck = stack.peek();
    Declarable d = null;
    if (declCheck instanceof String || declCheck instanceof Parameter) {
      d = createDeclarable();
      if (!(d instanceof ObjectSizer)) {
        throw new CacheXmlException(
            String.format("A %s is not an instance of a ObjectSizer.",
                d.getClass().getName()));
      }
    }
    EvictionAttributesImpl eai = (EvictionAttributesImpl) stack.pop();
    if (d != null) {
      eai.setObjectSizer((ObjectSizer) d);
    }
    RegionAttributesCreation regAttrs = peekRegionAttributesContext(LRU_MEMORY_SIZE);
    regAttrs.setEvictionAttributes(eai);
  }

  /**
   * Create an <code>lru-heap-percentage</code> eviction controller, assigning it to the enclosed
   * <code>region-attributes</code>
   *
   */
  private void startLRUHeapPercentage(Attributes atts) {
    final String lruAction = atts.getValue(ACTION);
    EvictionAction action = EvictionAction.DEFAULT_EVICTION_ACTION;
    if (lruAction != null) {
      action = EvictionAction.parseAction(lruAction);
    }
    // Store for later addition of ObjectSizer, if any
    stack.push(EvictionAttributes.createLRUHeapAttributes(null, action));
  }

  /**
   * Complete the configuration of a <code>lru-heap-percentage</code> eviction controller. Check for
   * the declaration of an {@link ObjectSizer}. Assign the attributes to the enclosed
   * <code>region-attributes</code>
   */
  private void endLRUHeapPercentage() {
    Object declCheck = stack.peek();
    Declarable d = null;
    if (declCheck instanceof String || declCheck instanceof Parameter) {
      d = createDeclarable();
      if (!(d instanceof ObjectSizer)) {
        String s = "A " + d.getClass().getName() + " is not an instance of a ObjectSizer";
        throw new CacheXmlException(s);
      }
    }
    EvictionAttributesImpl eai = (EvictionAttributesImpl) stack.pop();
    if (d != null) {
      eai.setObjectSizer((ObjectSizer) d);
    }
    RegionAttributesCreation regAttrs = peekRegionAttributesContext(LRU_HEAP_PERCENTAGE);
    regAttrs.setEvictionAttributes(eai);
  }

  /**
   * When a <code>cache-listener</code> element is finished, the {@link Parameter}s and class names
   * are popped off the stack. The cache listener is instantiated and initialized with the
   * parameters, if appropriate.
   */
  private void endCacheListener() {
    Declarable d = createDeclarable();
    if (!(d instanceof CacheListener)) {
      throw new CacheXmlException(
          String.format("A %s is not an instance of a CacheListener.",
              d.getClass().getName()));
    }
    RegionAttributesCreation attrs = peekRegionAttributesContext(CACHE_LISTENER);
    attrs.addCacheListener((CacheListener) d);
  }

  private void startAsyncEventQueue(Attributes atts) {
    AsyncEventQueueCreation asyncEventQueueCreation = new AsyncEventQueueCreation();

    // id
    String id = atts.getValue(ID);
    asyncEventQueueCreation.setId(id);

    String parallel = atts.getValue(PARALLEL);
    if (parallel == null) {
      asyncEventQueueCreation.setParallel(GatewaySender.DEFAULT_IS_PARALLEL);
    } else {
      asyncEventQueueCreation.setParallel(Boolean.parseBoolean(parallel));
    }
    // batch-size
    String batchSize = atts.getValue(BATCH_SIZE);
    if (batchSize == null) {
      asyncEventQueueCreation.setBatchSize(GatewaySender.DEFAULT_BATCH_SIZE);
    } else {
      asyncEventQueueCreation.setBatchSize(Integer.parseInt(batchSize));
    }

    // start in Paused state
    String paused = atts.getValue(PAUSE_EVENT_PROCESSING);
    if (paused != null) {
      asyncEventQueueCreation.setPauseEventDispatching(Boolean.parseBoolean(paused));
    } // no else block needed as default is set to false.

    // batch-time-interval
    String batchTimeInterval = atts.getValue(BATCH_TIME_INTERVAL);
    if (batchTimeInterval == null) {
      asyncEventQueueCreation.setBatchTimeInterval(GatewaySender.DEFAULT_BATCH_TIME_INTERVAL);
    } else {
      asyncEventQueueCreation.setBatchTimeInterval(Integer.parseInt(batchTimeInterval));
    }

    // batch-conflation
    String batchConflation = atts.getValue(ENABLE_BATCH_CONFLATION);
    if (batchConflation == null) {
      asyncEventQueueCreation.setBatchConflationEnabled(GatewaySender.DEFAULT_BATCH_CONFLATION);
    } else {
      asyncEventQueueCreation.setBatchConflationEnabled(Boolean.parseBoolean(batchConflation));
    }

    // maximum-queue-memory
    String maxQueueMemory = atts.getValue(MAXIMUM_QUEUE_MEMORY);
    if (maxQueueMemory == null) {
      asyncEventQueueCreation.setMaximumQueueMemory(GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY);
    } else {
      asyncEventQueueCreation.setMaximumQueueMemory(Integer.parseInt(maxQueueMemory));
    }

    // persistent
    String persistent = atts.getValue(PERSISTENT);
    if (persistent == null) {
      asyncEventQueueCreation.setPersistent(GatewaySender.DEFAULT_PERSISTENCE_ENABLED);
    } else {
      asyncEventQueueCreation.setPersistent(Boolean.parseBoolean(persistent));
    }

    // diskStoreName
    String diskStoreName = atts.getValue(DISK_STORE_NAME);
    if (diskStoreName == null) {
      asyncEventQueueCreation.setDiskStoreName(null);
    } else {
      asyncEventQueueCreation.setDiskStoreName(diskStoreName);
    }

    // diskSynchronous
    String diskSynchronous = atts.getValue(DISK_SYNCHRONOUS);
    if (diskSynchronous == null) {
      asyncEventQueueCreation.setDiskSynchronous(GatewaySender.DEFAULT_DISK_SYNCHRONOUS);
    } else {
      asyncEventQueueCreation.setDiskSynchronous(Boolean.parseBoolean(diskSynchronous));
    }

    String dispatcherThreads = atts.getValue(DISPATCHER_THREADS);
    if (dispatcherThreads == null) {
      asyncEventQueueCreation.setDispatcherThreads(GatewaySender.DEFAULT_DISPATCHER_THREADS);
    } else {
      asyncEventQueueCreation.setDispatcherThreads(Integer.parseInt(dispatcherThreads));
    }

    String orderPolicy = atts.getValue(ORDER_POLICY);
    if (orderPolicy != null) {
      try {
        asyncEventQueueCreation
            .setOrderPolicy(GatewaySender.OrderPolicy.valueOf(orderPolicy.toUpperCase()));
      } catch (IllegalArgumentException e) {
        throw new InternalGemFireException(String.format(
            "An invalid order-policy value (%s) was configured for AsyncEventQueue %s",
            new Object[] {id, orderPolicy}));
      }
    }

    // forward expiration destroy events.
    String forward = atts.getValue(FORWARD_EXPIRATION_DESTROY);
    if (forward != null) {
      asyncEventQueueCreation.setForwardExpirationDestroy(Boolean.parseBoolean(forward));
    }

    stack.push(asyncEventQueueCreation);
  }

  private void endAsyncEventListener() {
    Declarable d = createDeclarable();
    if (!(d instanceof AsyncEventListener)) {
      throw new CacheXmlException(
          String.format("A %s is not an instance of a AsyncEventListener",
              d.getClass().getName()));
    }
    AsyncEventQueueCreation eventChannel = peekAsyncEventQueueContext(ASYNC_EVENT_LISTENER);
    eventChannel.setAsyncEventListener((AsyncEventListener) d);
  }

  private AsyncEventQueueCreation peekAsyncEventQueueContext(String dependentElement) {
    Object a = stack.peek();
    if (!(a instanceof AsyncEventQueueCreation)) {
      throw new CacheXmlException(
          String.format("A %s must be defined in the context of async-event-queue.",
              dependentElement));
    }
    return (AsyncEventQueueCreation) a;
  }

  private void endAsyncEventQueue() {
    AsyncEventQueueCreation asyncEventChannelCreation = (AsyncEventQueueCreation) stack.peek();
    AsyncEventQueueFactory factory = cache.createAsyncEventQueueFactory();
    factory.setParallel(asyncEventChannelCreation.isParallel());
    factory.setBatchSize(asyncEventChannelCreation.getBatchSize());
    factory.setBatchTimeInterval(asyncEventChannelCreation.getBatchTimeInterval());
    factory.setBatchConflationEnabled(asyncEventChannelCreation.isBatchConflationEnabled());
    factory.setPersistent(asyncEventChannelCreation.isPersistent());
    factory.setDiskStoreName(asyncEventChannelCreation.getDiskStoreName());
    factory.setDiskSynchronous(asyncEventChannelCreation.isDiskSynchronous());
    factory.setMaximumQueueMemory(asyncEventChannelCreation.getMaximumQueueMemory());
    factory.setDispatcherThreads(asyncEventChannelCreation.getDispatcherThreads());
    factory.setOrderPolicy(asyncEventChannelCreation.getOrderPolicy());
    factory.setForwardExpirationDestroy(asyncEventChannelCreation.isForwardExpirationDestroy());
    List<GatewayEventFilter> gatewayEventFilters =
        asyncEventChannelCreation.getGatewayEventFilters();
    for (GatewayEventFilter gatewayEventFilter : gatewayEventFilters) {
      factory.addGatewayEventFilter(gatewayEventFilter);
    }
    if (asyncEventChannelCreation.isDispatchingPaused()) {
      factory.pauseEventDispatching();
    }
    factory.setGatewayEventSubstitutionListener(
        asyncEventChannelCreation.getGatewayEventSubstitutionFilter());
    AsyncEventQueue asyncEventChannel = factory.create(asyncEventChannelCreation.getId(),
        asyncEventChannelCreation.getAsyncEventListener());

    stack.pop();
  }

  /**
   * When a <code>partition-resolver</code> element is finished, the {@link Parameter}s and class
   * names are popped off the stack. The <code>PartitionResolver</code> is instantiated and
   * initialized with the parameters, if appropriate.
   */
  private void endPartitionResolver() {
    Declarable d = createDeclarable();
    if (!(d instanceof PartitionResolver)) {
      throw new CacheXmlException(String.format("A %s is not an instance of a %s",
          new Object[] {d.getClass().getName(), "PartitionResolver"}));

    }

    PartitionAttributesImpl pai = peekPartitionAttributesImpl(PARTITION_ATTRIBUTES);
    pai.setPartitionResolver((PartitionResolver) d);
  }

  /**
   * When a <code>partition-listener</code> element is finished, the {@link Parameter}s and class
   * names are popped off the stack. The <code>PartitionListener</code> is instantiated and
   * initialized with the parameters, if appropriate.
   */
  private void endPartitionListener() {
    Declarable d = createDeclarable();
    if (!(d instanceof PartitionListener)) {
      throw new CacheXmlException(String.format("A %s is not an instance of a %s",
          new Object[] {d.getClass().getName(), "PartitionListener"}));

    }
    PartitionAttributesImpl pai = peekPartitionAttributesImpl(PARTITION_ATTRIBUTES);
    pai.addPartitionListener((PartitionListener) d);
  }

  /**
   * When we have encountered a FunctionService element, we create the object and push it onto stack
   */
  private void startFunctionService() {
    this.stack.push(new FunctionServiceCreation());
  }

  /**
   * When we have finished a FunctionService element, we create the object and push it onto stack
   */
  private void endFunctionService() {
    Object top = stack.pop();
    if (!(top instanceof FunctionServiceCreation)) {
      throw new CacheXmlException(
          "Expected a FunctionServiceCreation instance");
    }
    FunctionServiceCreation fsc = (FunctionServiceCreation) top;
    this.cache.setFunctionServiceCreation(fsc);
  }

  /**
   * Start the Resource Manager element configuration
   *
   * @param atts XML attributes for the resource-manager
   */
  private void startResourceManager(final Attributes atts) {
    ResourceManagerCreation rmc = new ResourceManagerCreation();
    {
      String chp = atts.getValue(CRITICAL_HEAP_PERCENTAGE);
      if (chp != null) {
        rmc.setCriticalHeapPercentage(parseFloat(chp));
      } else {
        rmc.setCriticalHeapPercentageToDefault();
      }
    }

    {
      String ehp = atts.getValue(EVICTION_HEAP_PERCENTAGE);
      if (ehp != null) {
        rmc.setEvictionHeapPercentage(parseFloat(ehp));
      } else {
        rmc.setEvictionHeapPercentageToDefault();
      }
    }

    {
      String chp = atts.getValue(CRITICAL_OFF_HEAP_PERCENTAGE);
      if (chp != null) {
        rmc.setCriticalOffHeapPercentage(parseFloat(chp));
      } else {
        rmc.setCriticalOffHeapPercentageToDefault();
      }
    }

    {
      String ehp = atts.getValue(EVICTION_OFF_HEAP_PERCENTAGE);
      if (ehp != null) {
        rmc.setEvictionOffHeapPercentage(parseFloat(ehp));
      } else {
        rmc.setEvictionOffHeapPercentageToDefault();
      }
    }
    this.stack.push(rmc);
  }

  private void endResourceManager() {
    Object top = stack.pop();
    if (!(top instanceof ResourceManagerCreation)) {
      throw new CacheXmlException("Expected a ResourceManagerCreation instance");
    }
    ResourceManagerCreation rmc = (ResourceManagerCreation) top;
    // TODO set any listeners here
    // rmc.addResourceListener(null);
    this.cache.setResourceManagerCreation(rmc);
  }

  private void endBackup() {
    StringBuffer str = (StringBuffer) stack.pop();
    File backup = new File(str.toString().trim());
    this.cache.addBackup(backup);
  }

  /**
   * When we have finished a function element, we create the Declarable and push it onto stack
   */
  private void endFunctionName() {
    Declarable d = createDeclarable();
    if (!(d instanceof Function)) {
      String s = String.format("A %s is not an instance of a Function",
          d.getClass().getName());
      throw new CacheXmlException(s);
    }

    Object fs = stack.peek();
    if (!(fs instanceof FunctionServiceCreation)) {
      throw new CacheXmlException(
          String.format("A %s is only allowed in the context of %s MJTDEBUG e=%s",
              new Object[] {FUNCTION, FUNCTION_SERVICE, fs}));
    }
    FunctionServiceCreation funcService = (FunctionServiceCreation) fs;
    funcService.registerFunction((Function) d);
  }

  private Class getClassFromStack() {
    Object o = this.stack.peek();
    if (!(o instanceof String)) {
      throw new CacheXmlException(
          "A string class-name was expected, but not found while parsing.");
    }
    String className = (String) this.stack.pop();

    try {
      Class c = InternalDataSerializer.getCachedClass(className);
      return c;
    } catch (Exception e) {
      throw new CacheXmlException(
          String.format("Unable to load class %s", className), e);
    }
  }

  /**
   * Ending the top level <code>serialization-registration</code> element and actually doing the
   * work of registering all the components.
   */
  private void endSerializerRegistration() {
    SerializerCreation sc = (SerializerCreation) this.stack.pop();
    sc.create();
    this.cache.setSerializerCreation(sc);
  }

  /**
   * Ending the serialization registration should leave us with a class name on the stack. We will
   * call the DataSerializer.register() with the class once we find it.
   */
  private void endSerializer() {
    Class c = getClassFromStack();
    if (!(DataSerializer.class.isAssignableFrom(c))) {
      throw new CacheXmlException(
          String.format(
              "The class %s presented for serializer registration does not extend DataSerializer and cannot be registered.",
              c.getName()));
    }

    SerializerCreation sr = (SerializerCreation) this.stack.peek();
    sr.registerSerializer(c);
  }

  /**
   * Ending the instantiator registration should leave us with a class name and an Integer ID on the
   * stack. Pull them off, and setup the instantiator with an anonymous inner class to do the work.
   */
  private void endInstantiator() {
    final Class c = getClassFromStack();
    Class[] ifaces = c.getInterfaces();
    boolean found = false;
    for (Class clazz : ifaces) {
      if (clazz == DataSerializable.class) {
        found = true;
        break;
      }
    }
    if (!found) {
      throw new CacheXmlException(String.format(
          "The class %s, presented for instantiator registration is not an instance of DataSerializable and cannot be registered.",
          c.getName()));
    }

    // the next thing on the stack should be the Integer registration ID
    Object o = this.stack.peek();
    if (!(o instanceof Integer)) {
      String s = "The instantiator registration did not include an ID attribute.";
      throw new CacheXmlException(s);
    }

    Integer id = (Integer) this.stack.pop();
    SerializerCreation sc = (SerializerCreation) this.stack.peek();
    sc.registerInstantiator(c, id);
  }

  /**
   * When we first encounter a <code>parameter</code> element, we push its name element on to the
   * stack.
   */
  private void startParameter(Attributes atts) {
    String name = atts.getValue(NAME);
    Assert.assertTrue(name != null);
    stack.push(name);
  }

  /**
   * When we have finished a <code>parameter</code> element, create a {@link Parameter}from the top
   * two elements of the stack.
   */
  private void endParameter() {
    Object value = stack.pop();
    String name = (String) stack.pop();
    stack.push(new Parameter(name, value));
  }

  /**
   * When we have finished a <code>declarable</code>, instantiate an instance of the
   * {@link Declarable}and push it on the stack.
   */
  private void endDeclarable() {
    Declarable d = createDeclarable();
    stack.push(d);
  }

  @Override
  public void startElement(String namespaceURI, String localName, String qName, Attributes atts)
      throws SAXException {
    // This while loop pops all StringBuffers at the top of the stack
    // that contain only whitespace; see GEODE-3306
    while (!stack.empty()) {
      Object o = stack.peek();
      if (o instanceof StringBuffer && StringUtils.isBlank(((StringBuffer) o).toString())) {
        stack.pop();
      } else {
        break;
      }
    }

    if (qName.equals(CACHE)) {
      startCache(atts);
    } else if (qName.equals(CLIENT_CACHE)) {
      startClientCache(atts);
    } else if (qName.equals(BRIDGE_SERVER)) {
      startCacheServer(atts);
    } else if (qName.equals(CACHE_SERVER)) {
      startCacheServer(atts);
    } else if (qName.equals(LOAD_PROBE)) {
    } else if (qName.equals(CONNECTION_POOL)) {
      startPool(atts);
    } else if (qName.equals(CLIENT_SUBSCRIPTION)) {
      startClientHaQueue(atts);
    } else if (qName.equals(DYNAMIC_REGION_FACTORY)) {
      startDynamicRegionFactory(atts);
    } else if (qName.equals(GATEWAY_SENDER)) {
      startGatewaySender(atts);
    } else if (qName.equals(GATEWAY_RECEIVER)) {
      startGatewayReceiver(atts);
    } else if (qName.equals(GATEWAY_EVENT_FILTER)) {
    } else if (qName.equals(GATEWAY_TRANSPORT_FILTER)) {
    } else if (qName.equals(GATEWAY_EVENT_LISTENER)) {
    } else if (qName.equals(GATEWAY_EVENT_SUBSTITUTION_FILTER)) {
    } else if (qName.equals(ASYNC_EVENT_QUEUE)) {
      startAsyncEventQueue(atts);
    } else if (qName.equals(GATEWAY_CONFLICT_RESOLVER)) {
    } else if (qName.equals(LOCATOR)) {
      doLocator(atts);
    } else if (qName.equals(REGION)) {
      startRegion(atts);
    } else if (qName.equals(VM_ROOT_REGION)) {
      startRegion(atts);
    } else if (qName.equals(REGION_ATTRIBUTES)) {
      startRegionAttributes(atts);
    } else if (qName.equals(DISK_STORE)) {
      startDiskStore(atts);
    } else if (qName.equals(KEY_CONSTRAINT)) {
    } else if (qName.equals(VALUE_CONSTRAINT)) {
    } else if (qName.equals(INDEX_UPDATE_TYPE)) {
    } else if (qName.equals(REGION_TIME_TO_LIVE)) {
    } else if (qName.equals(REGION_IDLE_TIME)) {
    } else if (qName.equals(ENTRY_TIME_TO_LIVE)) {
    } else if (qName.equals(ENTRY_IDLE_TIME)) {
    } else if (qName.equals(EXPIRATION_ATTRIBUTES)) {
      startExpirationAttributes(atts);
    } else if (qName.equals(SERVER)) {
      doServer(atts);
    } else if (qName.equals(CUSTOM_EXPIRY)) {
    } else if (qName.equals(SUBSCRIPTION_ATTRIBUTES)) {
      startSubscriptionAttributes(atts);
    } else if (qName.equals(ENTRY)) {
    } else if (qName.equals(CLASS_NAME)) {
    } else if (qName.equals(PARAMETER)) {
      startParameter(atts);
    } else if (qName.equals(DISK_WRITE_ATTRIBUTES)) {
      startDiskWriteAttributes(atts);
    } else if (qName.equals(SYNCHRONOUS_WRITES)) {
      startSynchronousWrites();
    } else if (qName.equals(ASYNCHRONOUS_WRITES)) {
      startAsynchronousWrites(atts);
    } else if (qName.equals(DISK_DIRS)) {
    } else if (qName.equals(DISK_DIR)) {
      startDiskDir(atts);
    } else if (qName.equals(GROUP)) {
    } else if (qName.equals(PARTITION_ATTRIBUTES)) {
      startPartitionAttributes(atts);
    } else if (qName.equals(FIXED_PARTITION_ATTRIBUTES)) {
      startFixedPartitionAttributes(atts);
    } else if (qName.equals(REQUIRED_ROLE)) {
      startRequiredRole(atts);
    } else if (qName.equals(MEMBERSHIP_ATTRIBUTES)) {
      startMembershipAttributes(atts);
    } else if (qName.equals(LOCAL_PROPERTIES)) {
      startPartitionProperties(atts, LOCAL_PROPERTIES);
    } else if (qName.equals(GLOBAL_PROPERTIES)) {
      startPartitionProperties(atts, GLOBAL_PROPERTIES);
    } else if (qName.equals(CACHE_LOADER)) {
    } else if (qName.equals(CACHE_WRITER)) {
    } else if (qName.equals(EVICTION_ATTRIBUTES)) {
    } else if (qName.equals(LRU_ENTRY_COUNT)) {
      startLRUEntryCount(atts); // internal to eviction-attributes
    } else if (qName.equals(LRU_MEMORY_SIZE)) {
      // internal to eviction-attributes
      // Visit endLRUMemorySize() to know the completion
      // of lru-memory-size eviction configuration
      startLRUMemorySize(atts);
    } else if (qName.equals(LRU_HEAP_PERCENTAGE)) {
      startLRUHeapPercentage(atts); // internal to eviction-attributes
    } else if (qName.equals(CACHE_LISTENER)) {
    } else if (qName.equals(ASYNC_EVENT_LISTENER)) {
    } else if (qName.equals(KEY)) {
    } else if (qName.equals(VALUE)) {
    } else if (qName.equals(STRING)) {
    } else if (qName.equals(DECLARABLE)) {
    } else if (qName.equals(INDEX)) {
      // Asif: Create an object of type IndexCreationData &
      // push it in stack
      startIndex(atts);
      // this.stack.push(new IndexCreationData(atts.getValue(NAME)));
    } else if (qName.equals(FUNCTIONAL)) {
      startFunctionalIndex(atts);
    } else if (qName.equals(PRIMARY_KEY)) {
      startPrimaryKeyIndex(atts);
    } else if (qName.equals(TRANSACTION_MANAGER)) {
      startCacheTransactionManager();
    } else if (qName.equals(TRANSACTION_LISTENER)) {
    } else if (qName.equals(TRANSACTION_WRITER)) {
    } else if (qName.equals(JNDI_BINDINGS)) { // added by Nand Kishor
    } else if (qName.equals(JNDI_BINDING)) { // added by Nand Kishor
      // Asif: Push the BindingCreation object in the stack
      Map gfSpecific = new HashMap();
      mapJNDI(atts, gfSpecific);
      List vendorSpecific = new ArrayList();
      this.stack.push(new BindingCreation(gfSpecific, vendorSpecific));
    } else if (qName.equals(CONFIG_PROPERTY_BINDING)) {
      // Asif : Peek at the BindingCreation object from stack
      // & get the vendor specific data map
      BindingCreation bc = (BindingCreation) this.stack.peek();
      List vendorSpecific = bc.getVendorSpecificList();
      // Rohit: Add a ConfigProperty Data Object to the list.
      vendorSpecific.add(new ConfigProperty());
    } else if (qName.equals(CONFIG_PROPERTY_NAME)) {
    } else if (qName.equals(CONFIG_PROPERTY_VALUE)) {
    } else if (qName.equals(CONFIG_PROPERTY_TYPE)) {
    } else if (qName.equals(PARTITION_RESOLVER)) {
    } else if (qName.equals(PARTITION_LISTENER)) {
    } else if (qName.equals(FUNCTION_SERVICE)) {
      startFunctionService();
    } else if (qName.equals(FUNCTION)) {
    } else if (qName.equals(TOP_SERIALIZER_REGISTRATION)) {
      startSerializerRegistration();
    } else if (qName.equals(INITIALIZER)) {
      startInitializer();
    } else if (qName.equals(INSTANTIATOR_REGISTRATION)) {
      startInstantiator(atts);
    } else if (qName.equals(SERIALIZER_REGISTRATION)) {
      // do nothing
    } else if (qName.equals(RESOURCE_MANAGER)) {
      startResourceManager(atts);
    } else if (qName.equals(BACKUP)) {
      // do nothing
    } else if (qName.equals(PDX)) {
      startPdx(atts);
    } else if (qName.equals(PDX_SERIALIZER)) {
      // do nothing
    } else if (qName.equals(COMPRESSOR)) {
    } else if (qName.equals(SOCKET_FACTORY)) {
    } else {
      final XmlParser delegate = getDelegate(namespaceURI);
      if (null == delegate) {
        throw new CacheXmlException(
            String.format("Unknown XML element %s", qName));
      }

      delegate.startElement(namespaceURI, localName, qName, atts);
    }
  }

  /**
   * Get delegate {@link XmlParser} for the given <code>namespaceUri</code>
   *
   * @param namespaceUri to find {@link XmlParser} for.
   * @return {@link XmlParser} if found, otherwise null.
   * @since GemFire 8.1
   */
  // UnitTest CacheXmlParser.testGetDelegate()
  private XmlParser getDelegate(final String namespaceUri) {
    XmlParser delegate = delegates.get(namespaceUri);
    if (null == delegate) {
      try {
        final ServiceLoader<XmlParser> serviceLoader =
            ServiceLoader.load(XmlParser.class, ClassPathLoader.getLatestAsClassLoader());
        for (final XmlParser xmlParser : serviceLoader) {
          if (xmlParser.getNamespaceUri().equals(namespaceUri)) {
            delegate = xmlParser;
            delegate.setStack(stack);
            delegate.setDocumentLocator(documentLocator);
            delegates.put(xmlParser.getNamespaceUri(), xmlParser);
            break;
          }
        }
      } catch (final Exception e) {
        logger.error(e.getMessage(), e);
      }
    }
    return delegate;
  }

  private void startPdx(Attributes atts) {
    String readSerialized = atts.getValue(READ_SERIALIZED);
    if (readSerialized != null) {
      cache.setPdxReadSerialized(Boolean.parseBoolean(readSerialized));
    }
    String ignoreUnreadFields = atts.getValue(IGNORE_UNREAD_FIELDS);
    if (ignoreUnreadFields != null) {
      cache.setPdxIgnoreUnreadFields(Boolean.parseBoolean(ignoreUnreadFields));
    }
    String persistent = atts.getValue(PERSISTENT);
    if (persistent != null) {
      cache.setPdxPersistent(Boolean.parseBoolean(persistent));
    }
    String diskStoreName = atts.getValue(DISK_STORE_NAME);
    if (diskStoreName != null) {
      cache.setPdxDiskStore(diskStoreName);
    }
  }

  /**
   * When a <code>client-subscription</code> element is first encountered, create a new
   * {@link ClientSubscriptionConfig } to store the <code>eviction-policy</code>,
   * <p>
   * <code>capacity</code> and <code>overflow-directory</code>, then pass these values to Bridge
   * Server
   *
   * @since GemFire 5.7
   */
  private void startClientHaQueue(Attributes atts) {
    ClientHaQueueCreation clientHaQueue = new ClientHaQueueCreation();
    String haEvictionPolicy = atts.getValue(CLIENT_SUBSCRIPTION_EVICTION_POLICY);
    if (haEvictionPolicy != null) {
      clientHaQueue.setEvictionPolicy(haEvictionPolicy);
    }
    String haCapacity = atts.getValue(CLIENT_SUBSCRIPTION_CAPACITY);
    if (haCapacity != null) {
      clientHaQueue.setCapacity(Integer.parseInt(haCapacity));
    }
    String diskStoreName = atts.getValue(DISK_STORE_NAME);
    if (diskStoreName != null) {
      clientHaQueue.setDiskStoreName(diskStoreName);
    } else {
      String haOverflowDirectory = atts.getValue(OVERFLOW_DIRECTORY);
      if (haOverflowDirectory != null) {
        clientHaQueue.setOverflowDirectory(haOverflowDirectory);
      }
    }
    this.stack.push(clientHaQueue);
  }

  /**
   * Add a marker string to look for when in endPartitionProperties
   *
   * @param localOrGlobal either the string LOCAL_PROPERTIES or GLOBAL_PROPERTIES
   */
  private void startPartitionProperties(Attributes atts, String localOrGlobal) {
    stack.push(localOrGlobal);
  }

  private void startDiskDir(Attributes atts) {
    String size = atts.getValue(DIR_SIZE);
    Integer diskSize = null;
    if (size == null) {
      diskSize = Integer.valueOf(DiskStoreFactory.DEFAULT_DISK_DIR_SIZE);
    } else {
      diskSize = Integer.valueOf(size);
    }
    stack.push(diskSize);
  }

  private void startDiskWriteAttributes(Attributes atts) {
    String roll = atts.getValue(ROLL_OPLOG);
    if (roll == null) {
      roll = "true"; // because it defaults to true
    }

    String maxOp = atts.getValue(MAX_OPLOG_SIZE);
    int maxOplogSize = 0;
    if (maxOp != null) {
      maxOplogSize = parseInt(maxOp);
    } else {
      maxOplogSize = DiskWriteAttributesImpl.getDefaultMaxOplogSize();
    }
    stack.push(roll);
    stack.push(Integer.valueOf(maxOplogSize));
  }

  @Override
  public void endElement(String namespaceURI, String localName, String qName) throws SAXException {
    // This while loop pops all StringBuffers at the top of the stack
    // that contain only whitespace; see GEODE-3306
    while (!stack.empty()) {
      Object o = stack.peek();
      if (o instanceof StringBuffer && StringUtils.isBlank(((StringBuffer) o).toString())) {
        stack.pop();
      } else {
        break;
      }
    }

    try {
      // logger.debug("endElement namespaceURI=" + namespaceURI
      // + "; localName = " + localName + "; qName = " + qName);
      if (qName.equals(CACHE)) {
        endCache();
      } else if (qName.equals(CLIENT_CACHE)) {
        endClientCache();
      } else if (qName.equals(BRIDGE_SERVER)) {
        endCacheServer();
      } else if (qName.equals(CACHE_SERVER)) {
        endCacheServer();
      } else if (qName.equals(LOAD_PROBE)) {
        endLoadProbe();
      } else if (qName.equals(CLIENT_SUBSCRIPTION)) {
        endClientHaQueue();
      } else if (qName.equals(CONNECTION_POOL)) {
        endPool();
      } else if (qName.equals(DYNAMIC_REGION_FACTORY)) {
        endDynamicRegionFactory();
      } else if (qName.equals(GATEWAY_SENDER)) {
        endSerialGatewaySender();
      } else if (qName.equals(GATEWAY_RECEIVER)) {
        endGatewayReceiver();
      } else if (qName.equals(GATEWAY_EVENT_FILTER)) {
        endGatewayEventFilter();
      } else if (qName.equals(GATEWAY_EVENT_SUBSTITUTION_FILTER)) {
        endGatewayEventSubstitutionFilter();
      } else if (qName.equals(GATEWAY_TRANSPORT_FILTER)) {
        endGatewayTransportFilter();
      } else if (qName.equals(ASYNC_EVENT_QUEUE)) {
        endAsyncEventQueue();
      } else if (qName.equals(REGION)) {
        endRegion();
      } else if (qName.equals(GATEWAY_CONFLICT_RESOLVER)) {
        endGatewayConflictResolver();
      } else if (qName.equals(VM_ROOT_REGION)) {
        endRegion();
      } else if (qName.equals(REGION_ATTRIBUTES)) {
        endRegionAttributes();
      } else if (qName.equals(DISK_STORE)) {
        endDiskStore();
      } else if (qName.equals(KEY_CONSTRAINT)) {
        endKeyConstraint();
      } else if (qName.equals(VALUE_CONSTRAINT)) {
        endValueConstraint();
      } else if (qName.equals(REGION_TIME_TO_LIVE)) {
        endRegionTimeToLive();
      } else if (qName.equals(REGION_IDLE_TIME)) {
        endRegionIdleTime();
      } else if (qName.equals(ENTRY_TIME_TO_LIVE)) {
        endEntryTimeToLive();
      } else if (qName.equals(ENTRY_IDLE_TIME)) {
        endEntryIdleTime();
      } else if (qName.equals(CUSTOM_EXPIRY)) {
        endCustomExpiry();
      } else if (qName.equals(DISK_WRITE_ATTRIBUTES)) {
        endDiskWriteAttributes();
      } else if (qName.equals(SYNCHRONOUS_WRITES)) {
      } else if (qName.equals(ASYNCHRONOUS_WRITES)) {
      } else if (qName.equals(DISK_DIRS)) {
        endDiskDirs();
      } else if (qName.equals(DISK_DIR)) {
        endDiskDir();
      } else if (qName.equals(GROUP)) {
        endGroup();
      } else if (qName.equals(PARTITION_ATTRIBUTES)) {
        endPartitionAttributes();
      } else if (qName.equals(FIXED_PARTITION_ATTRIBUTES)) {
        endFixedPartitionAttributes();
      } else if (qName.equals(LOCAL_PROPERTIES)) {
        endPartitionProperites(LOCAL_PROPERTIES);
      } else if (qName.equals(GLOBAL_PROPERTIES)) {
        endPartitionProperites(GLOBAL_PROPERTIES);
      } else if (qName.equals(MEMBERSHIP_ATTRIBUTES)) {
        endMembershipAttributes();
      } else if (qName.equals(REQUIRED_ROLE)) {
        endRequiredRole();
      } else if (qName.equals(EXPIRATION_ATTRIBUTES)) {
      } else if (qName.equals(CUSTOM_EXPIRY)) {
        endCustomExpiry();
      } else if (qName.equals(SUBSCRIPTION_ATTRIBUTES)) {
      } else if (qName.equals(ENTRY)) {
        endEntry();
      } else if (qName.equals(CLASS_NAME)) {
        endClassName();
      } else if (qName.equals(PARAMETER)) {
        endParameter();
      } else if (qName.equals(CACHE_LOADER)) {
        endCacheLoader();
      } else if (qName.equals(CACHE_WRITER)) {
        endCacheWriter();
      } else if (qName.equals(EVICTION_ATTRIBUTES)) {
      } else if (qName.equals(LRU_ENTRY_COUNT)) {
        // internal to eviction-attributes
      } else if (qName.equals(LRU_MEMORY_SIZE)) {
        endLRUMemorySize(); // internal to eviction-attributes
      } else if (qName.equals(LRU_HEAP_PERCENTAGE)) {
        endLRUHeapPercentage(); // internal to eviction-attributes
      } else if (qName.equals(CACHE_LISTENER)) {
        endCacheListener();
      } else if (qName.equals(ASYNC_EVENT_LISTENER)) {
        endAsyncEventListener();
      } else if (qName.equals(KEY)) {
      } else if (qName.equals(VALUE)) {
      } else if (qName.equals(STRING)) {
        endString();
      } else if (qName.equals(DECLARABLE)) {
        endDeclarable();
      } else if (qName.equals(FUNCTIONAL)) {
      } else if (qName.equals(INDEX)) {
        endIndex();
      } else if (qName.equals(PRIMARY_KEY)) {
      } else if (qName.equals(TRANSACTION_MANAGER)) {
        endCacheTransactionManager();
      } else if (qName.equals(TRANSACTION_LISTENER)) {
        endTransactionListener();
      } else if (qName.equals(TRANSACTION_WRITER)) {
        endTransactionWriter();
      } else if (qName.equals(JNDI_BINDINGS)) {
      } else if (qName.equals(JNDI_BINDING)) {
        BindingCreation bc = (BindingCreation) this.stack.pop();
        Map map = bc.getGFSpecificMap();
        try {
          JNDIInvoker.mapDatasource(map, bc.getVendorSpecificList());
        } catch (NamingException | DataSourceCreateException | ClassNotFoundException | SQLException
            | InstantiationException | IllegalAccessException ex) {
          if (logger.isWarnEnabled()) {
            logger.warn("jndi-binding creation of {} failed with: {}", map.get("jndi-name"), ex);
          }
        }
      } else if (qName.equals(CONFIG_PROPERTY_BINDING)) {
      } else if (qName.equals(CONFIG_PROPERTY_NAME)) {
        String name = null;
        if (this.stack.peek() instanceof StringBuffer)
          // Pop the config-property-name element value from the stack.
          name = ((StringBuffer) this.stack.pop()).toString();
        BindingCreation bc = (BindingCreation) this.stack.peek();
        List vsList = bc.getVendorSpecificList();
        ConfigProperty cp = (ConfigProperty) vsList.get(vsList.size() - 1);
        if (name == null) {
          String excep =
              String.format("Exception in parsing element %s. This is a required field.",
                  qName);
          throw new CacheXmlException(excep);
        } else {
          // set the name.
          cp.setName(name);
        }
      } else if (qName.equals(CONFIG_PROPERTY_VALUE)) {
        String value = null;
        // Pop the config-property-value element value from the stack.
        if (this.stack.peek() instanceof StringBuffer)
          value = ((StringBuffer) this.stack.pop()).toString();
        BindingCreation bc = (BindingCreation) this.stack.peek();
        List vsList = bc.getVendorSpecificList();
        ConfigProperty cp = (ConfigProperty) vsList.get(vsList.size() - 1);
        // Set the value to the ConfigProperty Data Object.
        cp.setValue(value);
      } else if (qName.equals(CONFIG_PROPERTY_TYPE)) {
        String type = null;
        if (this.stack.peek() instanceof StringBuffer)
          type = ((StringBuffer) this.stack.pop()).toString();
        BindingCreation bc = (BindingCreation) this.stack.peek();
        List vsList = bc.getVendorSpecificList();
        ConfigProperty cp = (ConfigProperty) vsList.get(vsList.size() - 1);
        if (type == null) {
          String excep =
              String.format("Exception in parsing element %s. This is a required field.",
                  qName);
          throw new CacheXmlException(excep);
        } else {
          cp.setType(type);
        }
      } else if (qName.equals(LRU_MEMORY_SIZE)) { // internal to eviction-attributes
        // Visit startLRUMemorySize() to know the begining
        // of lru-memory-size eviction configuration
        endLRUMemorySize();
      } else if (qName.equals(LOCATOR)) {
        // nothing needed
      } else if (qName.equals(SERVER)) {
        // nothing needed
      } else if (qName.equals(PARTITION_RESOLVER)) {
        endPartitionResolver();
      } else if (qName.equals(PARTITION_LISTENER)) {
        endPartitionListener();
      } else if (qName.equals(FUNCTION)) {
        endFunctionName();
      } else if (qName.equals(FUNCTION_SERVICE)) {
        endFunctionService();
      } else if (qName.equals(TOP_SERIALIZER_REGISTRATION)) {
        endSerializerRegistration();
      } else if (qName.equals(INITIALIZER)) {
        endInitializer();
      } else if (qName.equals(SERIALIZER_REGISTRATION)) {
        endSerializer();
      } else if (qName.equals(INSTANTIATOR_REGISTRATION)) {
        endInstantiator();
      } else if (qName.equals(RESOURCE_MANAGER)) {
        endResourceManager();
      } else if (qName.equals(BACKUP)) {
        endBackup();
      } else if (qName.equals(PDX)) {
        // nothing needed
      } else if (qName.equals(PDX_SERIALIZER)) {
        endPdxSerializer();
      } else if (qName.equals(COMPRESSOR)) {
        endCompressor();
      } else if (qName.equals(SOCKET_FACTORY)) {
        endSocketFactory();
      } else {
        final XmlParser delegate = getDelegate(namespaceURI);
        if (null == delegate) {
          throw new CacheXmlException(
              String.format("Unknown XML element %s", qName));
        }

        delegate.endElement(namespaceURI, localName, qName);
      }
    } catch (CacheException ex) {
      throw new SAXException(
          "A CacheException was thrown while parsing XML.",
          ex);
    }
  }

  private void endSocketFactory() {
    Declarable d = createDeclarable();
    if (!(d instanceof SocketFactory)) {
      throw new CacheXmlException(String.format("A %s is not an instance of a %s",
          d.getClass().getName(), "SocketFactory"));

    }
    Object a = stack.peek();
    if (a instanceof PoolFactory) {
      PoolFactory poolFactory = (PoolFactory) a;
      poolFactory.setSocketFactory((SocketFactory) d);
    } else {
      throw new CacheXmlException(String.format("A %s must be defined in the context of a pool.",
          SOCKET_FACTORY));
    }

  }

  private void endGatewayTransportFilter() {
    Declarable d = createDeclarable();
    if (!(d instanceof GatewayTransportFilter)) {
      throw new CacheXmlException(String.format("A %s is not an instance of a %s",
          d.getClass().getName(), "GatewayTransportFilter"));

    }
    Object a = stack.peek();
    if (a instanceof GatewaySenderFactory) {
      GatewaySenderFactory senderFactory = (GatewaySenderFactory) a;
      senderFactory.addGatewayTransportFilter((GatewayTransportFilter) d);
    } else if (a instanceof GatewayReceiverFactory) {
      GatewayReceiverFactory receiverFactory = (GatewayReceiverFactory) a;
      receiverFactory.addGatewayTransportFilter((GatewayTransportFilter) d);
    } else {
      throw new CacheXmlException(
          String.format(
              "A %s must be defined in the context of gateway-sender or gateway-receiver.",
              GATEWAY_TRANSPORT_FILTER));
    }
  }

  private void endGatewayEventFilter() {
    Declarable d = createDeclarable();
    if (!(d instanceof GatewayEventFilter)) {
      throw new CacheXmlException(String.format("A %s is not an instance of a %s",
          new Object[] {d.getClass().getName(), "GatewayEventFilter"}));
    }
    Object obj = stack.peek();
    if (obj instanceof GatewaySenderFactory) {
      GatewaySenderFactory senderFactory = (GatewaySenderFactory) obj;
      senderFactory.addGatewayEventFilter((GatewayEventFilter) d);
    } else if (obj instanceof AsyncEventQueueCreation) {
      AsyncEventQueueCreation asyncEventQueueCreation = (AsyncEventQueueCreation) obj;
      asyncEventQueueCreation.addGatewayEventFilter((GatewayEventFilter) d);
    } else {
      throw new CacheXmlException(
          String.format(
              "A %s must be defined in the context of gateway-sender or async-event-queue.",
              "GatewayEventFilter"));
    }
  }

  private void endGatewayEventSubstitutionFilter() {
    Declarable d = createDeclarable();
    if (!(d instanceof GatewayEventSubstitutionFilter)) {
      throw new CacheXmlException(
          String.format("A %s is not an instance of a %s",
              new Object[] {d.getClass().getName(), "GatewayEventSubstitutionFilter"}));
    }
    Object obj = stack.peek();
    if (obj instanceof GatewaySenderFactory) {
      GatewaySenderFactory senderFactory = (GatewaySenderFactory) obj;
      senderFactory.setGatewayEventSubstitutionFilter((GatewayEventSubstitutionFilter) d);
    } else if (obj instanceof AsyncEventQueueCreation) {
      AsyncEventQueueCreation asyncEventQueueCreation = (AsyncEventQueueCreation) obj;
      asyncEventQueueCreation.setGatewayEventSubstitutionFilter((GatewayEventSubstitutionFilter) d);
    } else {
      throw new CacheXmlException(
          String.format(
              "A %s must be defined in the context of gateway-sender or async-event-queue.",
              "GatewayEventSubstitutionFilter"));
    }
  }

  private GatewaySenderFactory peekGatewaySender(String dependentElement) {
    Object a = stack.peek();
    if (!(a instanceof GatewaySenderFactory)) {
      throw new CacheXmlException(
          String.format("A %s must be defined in the context of gateway-sender.",
              dependentElement));
    }
    return (GatewaySenderFactory) a;
  }

  private void endPdxSerializer() {
    Declarable d = createDeclarable();
    if (!(d instanceof PdxSerializer)) {
      throw new CacheXmlException(
          String.format("A %s is not an instance of a PdxSerializer.",
              d.getClass().getName()));
    }
    PdxSerializer serializer = (PdxSerializer) d;
    this.cache.setPdxSerializer(serializer);
  }

  private void startInitializer() {

  }

  private void endInitializer() {
    Properties props = new Properties();
    Object top = stack.pop();
    while (top instanceof Parameter) {
      Parameter param = (Parameter) top;
      props.put(param.getName(), param.getValue());
      top = stack.pop();
    }
    Assert.assertTrue(top instanceof String);
    String className = (String) top;
    Object o;
    try {
      Class c = InternalDataSerializer.getCachedClass(className);
      o = c.newInstance();
    } catch (Exception ex) {
      throw new CacheXmlException(
          String.format("While instantiating a %s", className), ex);
    }
    if (!(o instanceof Declarable)) {
      throw new CacheXmlException(
          String.format("Class %s is not an instance of Declarable.",
              className));
    }
    Declarable d = (Declarable) o;
    this.cache.setInitializer(d, props);
  }

  /**
   * Do nothing
   *
   * @since GemFire 5.7
   */
  private void endClientHaQueue() {}

  /**
   * Process either the <code>local-properties</code> or <code>global-properties</code> for a
   * {@link org.apache.geode.internal.cache.PartitionedRegion}
   *
   * @param globalOrLocal either the string {@link CacheXml#LOCAL_PROPERTIES} or
   *        {@link CacheXml#GLOBAL_PROPERTIES}
   */
  private void endPartitionProperites(String globalOrLocal) {
    Properties props = new Properties();
    Object top = stack.pop();
    while (!top.equals(globalOrLocal)) {
      if (!(top instanceof Parameter)) {
        throw new CacheXmlException(
            String.format("Only a parameter is allowed in the context of %s",
                globalOrLocal));
      }
      Parameter param = (Parameter) top;
      props.put(param.getName(), param.getValue());
      top = stack.pop();
    }
    if (globalOrLocal.equals(GLOBAL_PROPERTIES)) {
      PartitionAttributesImpl pai = peekPartitionAttributesImpl(GLOBAL_PROPERTIES);
      pai.setGlobalProperties(props);
    } else if (globalOrLocal.equals(LOCAL_PROPERTIES)) {
      PartitionAttributesImpl pai = peekPartitionAttributesImpl(LOCAL_PROPERTIES);
      pai.setLocalProperties(props);
    } else {
      Assert.assertTrue(false, "Argument globalOrLocal has unexpected value " + globalOrLocal);
    }
  }

  @Override
  public void characters(char[] ch, int start, int length) throws SAXException {
    // This method needs to handle XML chunking, so its uses a
    // StringBuffer to uniquely identify previous calls and will
    // append to the existing StringBuffer for each subsequent call
    Object o = null;
    try {
      o = stack.peek();
    } catch (EmptyStackException firstTime) {
      // No entries on the stack, this is the first element that
      // performs any stack operations, initialize a StringBuffer (see
      // finally block)
    } finally {
      StringBuffer chars = null;
      if (o instanceof StringBuffer) {
        chars = (StringBuffer) o;
        chars.append(ch, start, length);
        if (logger.isTraceEnabled(LogMarker.CACHE_XML_PARSER_VERBOSE)) {
          logger.trace(LogMarker.CACHE_XML_PARSER_VERBOSE,
              "XML Parser characters, appended character data: {}",
              chars);
        }
      } else {
        chars = new StringBuffer(length);
        chars.append(ch, start, length);
        stack.push(chars);
        if (logger.isTraceEnabled(LogMarker.CACHE_XML_PARSER_VERBOSE)) {
          logger.trace(LogMarker.CACHE_XML_PARSER_VERBOSE,
              "XML Parser characters, new character data: {}", chars);
        }
      }
    }
  }

  ////////// Inherited methods that don't do anything //////////
  @Override
  public void setDocumentLocator(Locator locator) {
    this.documentLocator = locator;
  }

  @Override
  public void startDocument() throws SAXException {}

  @Override
  public void endDocument() throws SAXException {}

  @Override
  public void startPrefixMapping(String prefix, String uri) throws SAXException {}

  @Override
  public void endPrefixMapping(String prefix) throws SAXException {}

  @Override
  public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {}

  @Override
  public void processingInstruction(String target, String data) throws SAXException {}

  @Override
  public void skippedEntity(String name) throws SAXException {}

  /*
   * Binds a jndi name of datasource to a context. @param atts Attributes of <jndi-name> jndi name
   * and Datasource related information.
   *
   */
  private void mapJNDI(Attributes atts, Map gfSpecific) {
    int attsLen = atts.getLength();
    String key = "";
    String value = "";
    // put attributes into a Map
    for (int i = 0; i < attsLen; i++) {
      key = atts.getQName(i);
      value = atts.getValue(key);
      gfSpecific.put(key, value);
    }
  }

  /////////////////////// Inner Classes ///////////////////////
  /**
   * Class that delegates all of the methods of a {@link org.xml.sax.helpers.DefaultHandler} to a
   * {@link CacheXmlParser} that implements all of the methods of <code>DefaultHandler</code>, but
   * <B>is not </B> a <code>DefaultHandler</code>.
   */
  static class DefaultHandlerDelegate extends DefaultHandler2 {

    /** The <code>CacheXmlParser</code> that does the real work */
    private CacheXmlParser handler;

    /**
     * Creates a new <code>DefaultHandlerDelegate</code> that delegates to the given
     * <code>CacheXmlParser</code>.
     */
    public DefaultHandlerDelegate(CacheXmlParser handler) {
      this.handler = handler;
    }

    @Override
    public InputSource resolveEntity(String publicId, String systemId)
        throws SAXException, IOException {
      return handler.resolveEntity(publicId, systemId);
    }

    @Override
    public InputSource resolveEntity(String name, String publicId, String baseURI, String systemId)
        throws SAXException, IOException {
      return handler.resolveEntity(name, publicId, baseURI, systemId);
    }

    @Override
    public void setDocumentLocator(Locator locator) {
      handler.setDocumentLocator(locator);
    }

    @Override
    public void startDocument() throws SAXException {
      handler.startDocument();
    }

    @Override
    public void endDocument() throws SAXException {
      handler.endDocument();
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
      handler.startPrefixMapping(prefix, uri);
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
      handler.endPrefixMapping(prefix);
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes)
        throws SAXException {
      handler.startElement(uri, localName, qName, attributes);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
      handler.endElement(uri, localName, qName);
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
      handler.characters(ch, start, length);
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
      handler.ignorableWhitespace(ch, start, length);
    }

    @Override
    public void processingInstruction(String target, String data) throws SAXException {
      handler.processingInstruction(target, data);
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
      handler.skippedEntity(name);
    }

    @Override
    public void warning(SAXParseException e) throws SAXException {
      handler.warning(e);
    }

    @Override
    public void error(SAXParseException e) throws SAXException {
      handler.error(e);
    }

    @Override
    public void fatalError(SAXParseException e) throws SAXException {
      handler.fatalError(e);
    }
  }

  /**
   * Represents a parameter used to initialize a {@link Declarable}
   */
  static class Parameter {

    /** The name of the parameter */
    private String name;
    /** The value of the parameter */
    private Object value;

    /**
     * Creates a new <code>Parameter</code> with the given name and value.
     */
    public Parameter(String name, Object value) {
      this.name = name;
      this.value = value;
    }

    public String getName() {
      return this.name;
    }

    public Object getValue() {
      return this.value;
    }
  }
}

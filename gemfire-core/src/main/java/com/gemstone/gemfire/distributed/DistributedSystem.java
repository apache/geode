/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.distributed;

import java.io.File;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.tcp.ConnectionTable;
import com.gemstone.gemfire.internal.util.IOUtils;
import com.gemstone.gemfire.memcached.GemFireMemcachedServer;
import com.gemstone.gemfire.redis.GemFireRedisServer;
import com.gemstone.gemfire.security.GemFireSecurityException;

/**
 * A "connection" to a GemFire distributed system.  A
 * <code>DistributedSystem</code> is created by invoking the {@link
 * #connect} method with a configuration as described <a
 * href="#configuration">below</a>.  A <code>DistributedSystem</code>
 * is used when calling {@link
 * com.gemstone.gemfire.cache.CacheFactory#create}.  This class should
 * not be confused with the {@link
 * com.gemstone.gemfire.admin.AdminDistributedSystem
 * AdminDistributedSystem} interface that is used for administering a
 * distributed system.
 *
 * <P>
 *
 * When a program connects to the distributed system, a "distribution
 * manager" is started in this VM and the other members of the
 * distributed system are located.  This discovery can be performed
 * using either IP multicast (default) or by contacting "locators"
 * running on a given host and port.  All connections that are
 * configured to use the same multicast address/port and the same
 * locators are part of the same distributed system.
 *
 * <P>
 *
 * The current version of GemFire only supports creating one
 * <code>DistributedSystem</code> per virtual machine.  Attempts to
 * connect to multiple distributed systems (that is calling {@link
 * #connect} multiple times with different configuration
 * <code>Properties</code>) will result in an {@link
 * IllegalStateException} being thrown (if <code>connect</code> is
 * invoked multiple times with equivalent <code>Properties</code>,
 * then the same instance of <code>DistributedSystem</code> will be
 * returned).  A common practice is to connect to the distributed
 * system and store a reference to the <code>DistributedSystem</code>
 * object in a well-known location such as a <code>static</code>
 * variable.  This practice provides access to the
 * <code>DistributedSystem</code> slightly faster than invoking
 * <code>connect</code> multiple times.  Note that it is always
 * advisable to {@link #disconnect()} from the distributed system when a
 * program will no longer access it.  Disconnecting frees up certain
 * resources and allows your application to connect to a different
 * distributed system, if desirable.
 *
 * <P>
 *
 * <CENTER>
 * <B><a name="configuration">Configuration</a></B>
 * </CENTER>
 *
 * <P>
 *
 * There are a number of configuration properties that can be set when
 * a program {@linkplain #connect connects} to a distributed system.
 *
 * <P>
 *
 * <B>Distribution Configuration Properties</B>
 *
 * <dl>
 *   <a name="groups"><dt>groups</dt></a>
 *   <dd><U>Description</U>: Defines the list of groups this member belongs to.
 *   Use commas to separate group names.
 *   Note that anything defined by the roles gemfire property will also be considered a group.
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 7.0</dd>
 * </dl>
 * <dl>
 *   <a name="name"><dt>name</dt></a>
 *   <dd><U>Description</U>: Uniquely identifies a member in its distributed system.
 *   If two members with the same name try to join the same distributed system
 *   then the second join will fail.</dd>
 *   <dd><U>Default</U>: ""</dd>
 * </dl>

 * <dl>
 *   <a name="distributed-system-id"><dt>distributed-system-id</dt></a>
 *<dd> A number that uniquely identifies this distributed system, when
 * using the WAN gateway to share data between multiple distributed systems. This
 * setting is only required when using the WAN gateway in conjunction with
 * the Portable Data eXchange (PDX) serialization format.
 * 
 * If set, this setting must be the same for every member in this distributed
 * system. It must be different than the number in other distributed systems
 * that this one will connect to using the WAN.
 * 
 * -1 means no setting.</dd>
 * 
 *   <dd><U>Range</U>-1..255</dd>
 *   <dd><U>Default</U>: "-1"</dd>
 * </dl>
 *
 * <dl>
 *   <a name="mcast-port"><dt>mcast-port</dt></a>
 *   <dd><U>Description</U>: The port used for multicast networking.
 *   If zero, then multicast will be disabled and locators must be used to find the other members
 *   of the distributed system.
 *   If "mcast-port" is zero and "locators" is ""
 *   then this distributed system will be isolated from all other GemFire
 *   processes.
 *   </dd>
 *   <dd><U>Default</U>: "0" if locators is not ""; otherwise "10334"</dd>
 * </dl>
 *
 * <dl>
 *   <a name="mcast-address"><dt>mcast-address</dt></a>
 *   <dd><U>Description</U>: The IP address used for multicast
 *   networking.  If mcast-port is zero, then mcast-address is
 *   ignored.
 *   </dd>
 *   <dd><U>Default</U>: "239.192.81.1"</dd>
 * </dl>
 * <dl>
 *   <a name="mcast-ttl"><dt>mcast-ttl</dt></a>
 *   <dd><U>Description</U>: Determines how far through your network
 *   the multicast packets used by GemFire will propagate.
 *   <dd><U>Default</U>: "32"</dd>
 *   <dd><U>Allowed values</U>: 0..255</dd>
 *   <dd><U>Since</U>: 4.1</dd>
 * </dl>
 * <dl>
 *   <a name="mcast-send-buffer-size"><dt>mcast-send-buffer-size</dt></a>
 *   <dd><U>Description</U>: Sets the size of the socket buffer used for
 *    outgoing multicast transmissions.
 *   <dd><U>Default</U>: "65535"</dd>
 *   <dd><U>Allowed values</U>: 2048..Operating System maximum</dd>
 *   <dd><U>Since</U>: 5.0</dd>
 * </dl>
 * <dl>
 *   <a name="mcast-recv-buffer-size"><dt>mcast-recv-buffer-size</dt></a>
 *   <dd><U>Description</U>: Sets the size of the socket buffer used for
 *    incoming multicast transmissions.  You should set this high if there will be
 *    high volumes of messages.
 *   <dd><U>Default</U>: "1048576"</dd>
 *   <dd><U>Allowed values</U>: 2048..Operating System maximum</dd>
 *   <dd><U>Since</U>: 5.0</dd>
 * </dl>
 * <dl>
 *   <a name="mcast-flow-control"><dt>mcast-flow-control</dt></a>
 *   <dd><U>Description</U>: Configures the flow-of-control protocol for
 *    multicast messaging.  There are three settings that are separated
 *    by commas:  byteAllowance (integer), rechargeThreshold (float) and
 *    rechargeBlockMs (integer).  The byteAllowance determines how many bytes
 *    can be sent without a recharge from other processes.  The rechargeThreshold
 *    tells receivers how low the sender's initial to remaining allowance
 *    ratio should be before sending a recharge.  The rechargeBlockMs
 *    tells the sender how long to wait for a recharge before explicitly
 *    requesting one.</dd>
 *   <dd><U>Default</U>: "1048576,0.25,5000"</dd>
 *   <dd><U>Allowed values</U>: 100000-maxInt, 0.1-0.5, 500-60000</dd>
 *   <dd><U>Since</U>: 5.0</dd>
 * </dl>
 *
 * <dl>
 *   <a name="udp-send-buffer-size"><dt>udp-send-buffer-size</dt></a>
 *   <dd><U>Description</U>: Sets the size of the socket buffer used for
 *    outgoing udp point-to-point transmissions.
 *   <dd><U>Default</U>: "65535"</dd>
 *   <dd><U>Allowed values</U>: 2048..Operating System maximum</dd>
 *   <dd><U>Since</U>: 5.0</dd>
 * </dl>
 * <dl>
 *   <a name="udp-recv-buffer-size"><dt>udp-recv-buffer-size</dt></a>
 *   <dd><U>Description</U>: Sets the size of the socket buffer used for
 *    incoming udp point-to-point transmissions.  Note: if multicast is not
 *    enabled and disable-tcp is not enabled, a reduced default size of
 *    65535 is used.
 *   <dd><U>Default</U>: "1048576 if multicast is enabled or disable-tcp is true, 131071 if not"</dd>
 *   <dd><U>Allowed values</U>: 2048..Operating System maximum</dd>
 *   <dd><U>Since</U>: 5.0</dd>
 * </dl>
 * <dl>
 *   <a name="udp-fragment-size"><dt>udp-fragment-size</dt></a>
 *   <dd><U>Description</U>: When messages are sent over datagram sockets,
 *    GemFire breaks large messages down into fragments for transmission.
 *    This property sets the maximum fragment size for transmission.
 *   <dd><U>Default</U>: "60000"</dd>
 *   <dd><U>Allowed values</U>: 1000..60000</dd>
 *   <dd><U>Since</U>: 5.0</dd>
 * </dl>
 * <dl>
 *   <a name="disable-tcp"><dt>disable-tcp</dt></a>
 *   <dd><U>Description</U>: Turns off use of tcp/ip sockets, forcing the
 *    cache to use datagram sockets for all communication.  This is useful
 *    if you have a large number of processes in the distributed cache since
 *    it eliminates the per-connection reader-thread that is otherwise required.
 *    However, udp communications are somewhat slower than tcp/ip communications
 *    due to the extra work required in Java to break messages down to
 *    transmittable sizes, and the extra work required to guarantee
 *    message delivery.
 *   </dd>
 *   <dd><U>Default</U>: "false"</dd>
 *   <dd><U>Allowed values</U>: true or false</dd>
 *   <dd><U>Since</U>: 5.0</dd>
 * </dl>
 * <dl>
 *   <a name="tcp-port"><dt>tcp-port</dt></a>
 *   <dd><U>Description</U>: A 16-bit integer that determines the tcp/ip port number to listen on
 *     for cache communications.  If zero, the operating system will select
 *     an available port to listen on.  Each process on a machine must have
 *     its own tcp-port.  Note that some operating systems restrict the range
 *     of ports usable by non-privileged users, and using restricted port
 *     numbers can cause runtime errors in GemFire startup.
 *   </dd>
 *   <dd><U>Default</U>: "0"</dd>
 *   <dd><U>Allowed values</U>: 0..65535</dd>
 *   <dd><U>Since</U>: 5.0</dd>
 * </dl>
 * <dl>
 *   <a name="member-timeout"><dt>member-timeout</dt></a>
 *   <dd><U>Description</U>: Sets the timeout interval, in milliseconds, used
 *   to determine whether another process is alive or not.  When another process
 *   appears to be gone, GemFire sends it an ARE-YOU-DEAD message and waits
 *   for the member-timeout period for it to respond and declare it is not dead.
 *   </dd>
 *   <dd><U>Default</U>: "5000"</dd>
 *   <dd><U>Allowed values</U>: 1000-600000</dd>
 *   <dd><U>Since</U>: 5.0</dd>
 * </dl>
 * <dl>
 *   <a name="bind-address"><dt>bind-address</dt></a>
 *   <dd><U>Description</U>: The IP address that this distributed system's
 *   server sockets will listen on.
 *   If set to an empty string then the local machine's
 *   default address will be listened on.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 * </dl>
 * <!--
 * <dl>
 *   <a name="server-bind-address"><dt>server-bind-address</dt></a>
 *   <dd><U>Description</U>: The IP address that this distributed system's
 *   server sockets in a client-server topology will listen on.
 *   If set to an empty string then all of the local machine's
 *   addresses will be listened on.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 * </dl>
 * -->
 * <dl>
 *   <a name="membership-port-range"><dt>membership-port-range</dt></a>
 *   <dd><U>Description</U>: The allowed range of ports for use in forming an
 *   unique membership identifier (UDP), for failure detection purposes (TCP) and
 *   to listen on for peer connections (TCP). This range is given as two numbers
 *   separated by a minus sign. Minimum 3 values in range are required to
 *   successfully startup.
 *   </dd>
 *   <dd><U>Default</U>: 1024-65535</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="locators"><dt>locators</dt></a>
 *   <dd><U>Description</U>: A list of locators (host and port) that
 *   are used to find other member of the distributed system.  This
 *   attribute's value is a possibly empty comma separated list.  Each
 *   element must be of the form "hostName[portNum]" and may be of the
 *   form "host:bindAddress[port]" if a specific bind address is to be
 *   used on the locator machine.  The square
 *   brackets around the portNum are literal character and must be
 *   specified.<p>
 *   Since IPv6 bind addresses may contain colons, you may use an at symbol
 *   instead of a colon to separate the host name and bind address.
 *   For example, "server1@fdf0:76cf:a0ed:9449::5[12233]" specifies a locator
 *   running on "server1" and bound to fdf0:76cf:a0ed:9449::5 on port 12233.<p>
 *   If "mcast-port" is zero and "locators" is ""
 *   then this distributed system will be isolated from all other GemFire
 *   processes.<p>
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="locator-wait-time"><dt>locator-wait-time</dt></a>
 *   <dd><U>Description</U>: The number of seconds to wait for a locator to start
 *   if one is not available when attempting to join the distributed system.  This
 *   setting can be used when locators and peers are being started all at once in
 *   order to have the peers be patient and wait for the locators to finish starting
 *   up before attempting to join the distributed system..<p>
 *   </dd>
 *   <dd><U>Default</U>: "0"</dd>
 * 
 * <dl>
 *   <a name="remote-locators"><dt>remote-locators</dt></a>
 *   <dd><U>Description</U>: A list of locators (host and port) that a cluster
 *   will use in order to connect to a remote site in a multi-site (WAN) 
 *   configuration. This attribute's value is a possibly comma separated list.
 *   <p>For each remote locator, provide a hostname and/or address 
 *   (separated by '@', if you use both), followed by a port number in brackets.
 *   <p>Examples:
 *   remote-locators=address1[port1],address2[port2]
 *   <p>
 *   remote-locators=hostName1@address1[port1],hostName2@address2[port2]
 *   <p>
 *   remote-locators=hostName1[port1],hostName2[port2]<p>
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="start-locator"><dt>start-locator</dt></a>
 *   <dd><U>Description</U>: A host name or bind-address and port 
 *   ("host[port],peer=<true|false>,server=<true|false>")
 *   that are used to start a locator in the same process as the DistributedSystem.
 *   The locator is started when the DistributedSystem connects,
 *   and is stopped when the DistributedSystem disconnects.  To start a
 *   locator that is not tied to the DistributedSystem's lifecycle, see
 *   the {@link Locator} class in this same package.<p>
 *   
 *   The peer and server parameters are optional. They specify whether
 *   the locator can be used for peers to discover each other, or for clients
 *   to discover peers. By default both are true.
 *   </dd>
 *   <dd><U>Default</U>: "" (doesn't start a locator)</dd>
 * </dl>
 *
 * <dl>
 *   <a name="ssl-enabled"><dt>ssl-enabled</dt></a>
 *   <dd><U>Description</U>: If true, all gemfire socket communication is
 *   configured to use SSL through JSSE.
 *   </dd>
 *   <dd><U>Default</U>: "false"</dd>
 *   <dd><U>Deprecated</U>: as of 8.0 use <a href="#cluster-ssl-enabled"><code>cluster-ssl-enabled</code></a> instead.</dd>
 * </dl>
 *
 * <dl>
 *   <a name="ssl-protocols"><dt>ssl-protocols</dt></a>
 *   <dd><U>Description</U>: A space separated list of the SSL protocols to enable.
 *   Those listed must be supported by the available providers.
 *   </dd>
 *   <dd><U>Default</U>: "any"</dd>
 *   <dd><U>Deprecated</U>: as of 8.0 use <a href="#cluster-ssl-protocols"><code>cluster-ssl-protocols</code></a> instead.</dd>
 * </dl>
 *
 * <dl>
 *   <a name="ssl-ciphers"><dt>ssl-ciphers</dt></a>
 *   <dd><U>Description</U>: A space separated list of the SSL cipher suites to enable.
 *   Those listed must be supported by the available providers.
 *   </dd>
 *   <dd><U>Default</U>: "any"</dd>
 *   <dd><U>Deprecated</U>: as of 8.0 use <a href="#cluster-ssl-ciphers"><code>cluster-ssl-ciphers</code></a> instead.</dd>
 * </dl>
 *
 * <dl>
 *   <a name="ssl-require-authentication"><dt>ssl-require-authentication</dt></a>
 *   <dd><U>Description</U>: If false, allow ciphers that do not require the client
 *   side of the connection to be authenticated.
 *   </dd>
 *   <dd><U>Default</U>: "true"</dd>
 *   <dd><U>Deprecated</U>: as of 8.0 use <a href="#cluster-ssl-require-authentication"><code>cluster-ssl-require-authentication</code></a> instead.</dd>
 * </dl>
 *
 * <dl>
 *   <a name="cluster-ssl-enabled"><dt>cluster-ssl-enabled</dt></a>
 *   <dd><U>Description</U>: If true, all gemfire socket communication is
 *   configured to use SSL through JSSE. Preferably Use cluster-ssl-* properties
 *   rather than ssl-* properties.
 *   </dd>
 *   <dd><U>Default</U>: "false"</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="cluster-ssl-protocols"><dt>cluster-ssl-protocols</dt></a>
 *   <dd><U>Description</U>: A space separated list of the SSL protocols to
 *   enable. Those listed must be supported by the available providers.Preferably
 *   use cluster-ssl-* properties rather than ssl-* properties.
 *   </dd>
 *   <dd><U>Default</U>: "any"</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="cluster-ssl-ciphers"><dt>cluster-ssl-ciphers</dt></a>
 *   <dd><U>Description</U>: A space separated list of the SSL cipher suites to
 *   enable. Those listed must be supported by the available providers.Preferably
 *   use cluster-ssl-* properties rather than ssl-* properties.
 *   </dd>
 *   <dd><U>Default</U>: "any"</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="cluster-ssl-require-authentication"><dt>cluster-ssl-require-authentication</dt></a>
 *   <dd><U>Description</U>: If false, allow ciphers that do not require the
 *   client side of the connection to be authenticated.Preferably use
 *   cluster-ssl-* properties rather than ssl-* properties.
 *   </dd>
 *   <dd><U>Default</U>: "true"</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="cluster-ssl-keystore"><dt>cluster-ssl-keystore</dt></a>
 *   <dd><U>Description</U>Location of the Java keystore file containing
 *   certificate and private key.</dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="cluster-ssl-keystore-type"><dt>cluster-ssl-keystore-type</dt></a>
 *   <dd><U>Description</U>For Java keystore file format, this property has the
 *   value jks (or JKS).
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="cluster-ssl-keystore-password"><dt>cluster-ssl-keystore-password</dt></a>
 *   <dd><U>Description</U>Password to access the private key from the keystore
 *   file specified by javax.net.ssl.keyStore.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="cluster-ssl-truststore"><dt>cluster-ssl-truststore</dt></a>
 *   <dd><U>Description</U>Location of the Java keystore file containing the
 *   collection of CA certificates trusted by distributed member (trust store).
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="cluster-ssl-truststore-password"><dt>cluster-ssl-truststore-password</dt></a>
 *   <dd><U>Description</U>Password to unlock the keystore file (store password)
 *   specified by javax.net.ssl.trustStore.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="ack-wait-threshold"><dt>ack-wait-threshold</dt></a>
 *   <dd><U>Description</U>: The number of seconds the distributed
 *   system will wait for a message to be acknowledged before it sends
 *   a <i>warning</i> level alert to signal that something might be wrong with the system
 *   node that is unresponsive.  After sending this alert the waiter
 *   continues to wait. The alerts are logged in the log as warnings
 *   and will cause an alert notification in the Admin API and GemFire
 *   JMX Agent.</dd>
 *   <dd><U>Default</U>: "15"</dd>
 *   <dd><U>Allowed values</U>: 1..2147483647</dd>
 * </dl>
 *
 * <dl>
 *   <a name="ack-severe-alert-threshold"><dt>ack-severe-alert-threshold</dt></a>
 *   <dd><U>Description</U>:
 *   The number of seconds the distributed
 *   system will wait after the ack-wait-threshold for a message to be
 *   acknowledged before it issues an alert at <i>severe</i> level.  The
 *   default value is zero, which turns off this feature.<p>
 *   when ack-severe-alert-threshold is used, GemFire will also initiate
 *   additional checks to see if the process is alive.  These checks will
 *   begin when the ack-wait-threshold is reached and will continue until
 *   GemFire has been able to communicate with the process and ascertain its
 *   status.
 *   <dd><U>Default</U>: "0"</dd>
 *   <dd><U>Allowed values</U>: 0..2147483647</dd>
 * </dl>
 *
 * <dl>
 *   <a name="conserve-sockets"><dt>conserve-sockets</dt></a>
 *   <dd><U>Description</U>: If "true" then a minimal number of sockets
 *   will be used when connecting to the distributed system. This conserves
 *   resource usage but can cause performance to suffer.
 *   If "false" then every application thread that sends
 *   distribution messages to other members of the distributed system
 *   will own its own sockets and have exclusive access to them.
 *   The length of time a thread can have exclusive access to a socket
 *   can be configured with "socket-lease-time".
 *   <dd><U>Default</U>: "true"</dd>
 *   <dd><U>Allowed values</U>: true|false</dd>
 *   <dd><U>Since</U>: 4.1</dd>
 * </dl>
 * <dl>
 *   <a name="socket-lease-time"><dt>socket-lease-time</dt></a>
 *   <dd><U>Description</U>: The number of milliseconds a thread
 *   can keep exclusive access to a socket that it is not actively using.
 *   Once a thread loses its lease to a socket it will need to re-acquire
 *   a socket the next time it sends a message.
 *   A value of zero causes socket leases to never expire.
 *   This property is ignored if "conserve-sockets" is true.
 *   <dd><U>Default</U>: "15000"</dd>
 *   <dd><U>Allowed values</U>: 0..600000</dd>
 *   <dd><U>Since</U>: 4.1</dd>
 * </dl>
 * <dl>
 *   <a name="socket-buffer-size"><dt>socket-buffer-size</dt></a>
 *   <dd><U>Description</U>: The size of each socket buffer, in bytes.
 *   Smaller buffers conserve memory. Larger buffers can improve performance;
 *   in particular if large messages are being sent.
 *   <dd><U>Default</U>: "32768"</dd>
 *   <dd><U>Allowed values</U>: 128..16777215</dd>
 *   <dd><U>Since</U>: 4.1</dd>
 * </dl>
 * <dl>
 *   <a name="conflate-events"><dt>conflate-events</dt></a>
 *   <dd><U>Description</U>: This is a client-side property that is passed to
 *   the server. Allowable values are "server", "true", and "false". With the
 *   "server" setting, this client&apos;s servers use their own client queue
 *   conflation settings. With a "true" setting, the servers disregard their
 *   own configuration and enable conflation of events for all regions for the
 *   client. A "false" setting causes the client&apos;s servers to disable
 *   conflation for all regions for the client.
 *   <dd><U>Default</U>: "server"</dd>
 *   <dd><U>Since</U>: 5.7</dd>
 * </dl>
 * <dl>
 *   <a name="durable-client-id"><dt>durable-client-id</dt></a>
 *   <dd><U>Description</U>: The id to be used by this durable client. When a
 *   durable client connects to a server, this id is used by the server to
 *   identify it. The server will accumulate updates for a durable client
 *   while it is disconnected and deliver these events to the client when it
 *   reconnects.
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 5.5</dd>
 * </dl>
 * <dl>
 *   <a name="durable-client-timeout"><dt>durable-client-timeout</dt></a>
 *   <dd><U>Description</U>: The number of seconds a disconnected durable
 *   client is kept alive and updates are accumulated for it by the server
 *   before it is terminated.
 *   <dd><U>Default</U>: "300"</dd>
 *   <dd><U>Since</U>: 5.5</dd>
 * </dl>
 *<dl>
 *   <a name="security-"><dt>security-</dt></a>
 *   <dd><U>Description</U>: Mechanism to define client credentials.
 *   All tags with "security-" prefix is packaged together as security properties 
 *   and passed as an argument to getCredentials of Authentication module.
 *   These tags cannot have null values. 
 *   </dd>
 *   <dd><U>Default</U>: Optional</dd>
 *   <dd><U>Allowed values</U>: any string</dd>
 * </dl>
 * 
 *<dl>
 *   <a name="security-client-auth-init"><dt>security-client-auth-init</dt></a>
 *   <dd><U>Description</U>: Authentication module name for Clients that requires to act
 *   upon credentials read from the gemfire.properties file.
 *   Module must implement AuthInitialize interface.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Allowed values</U>: jar file:class name</dd>
 *</dl>
 * 
 * <dl>
 * <a name="delta-propagation">
 * <dt>delta-propagation</dt>
 * </a>
 * <dd><U>Description</U>: "true" indicates that server propagates delta
 * generated from {@link com.gemstone.gemfire.Delta} type of objects. If "false"
 * then server propagates full object but not delta. </dd>
 * <dd><U>Default</U>: "true"</dd>
 * <dd><U>Allowed values</U>: true|false</dd>
 * </dl> 
 * 
 * <b>Network Partitioning Detection</b>
 * 
 * <dl>
 *   <a name="enable-network-partition-detection"><dt>enable-network-partition-detection</dt></a>
 *   <dd><U>Description</U>: Turns on network partitioning detection algorithms, which
 *   detect loss of quorum and shuts down losing partitions.
 *   </dd>
 *   <dd><U>Default</U>: "false"</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="disable-auto-reconnect"><dt>disable-auto-reconnect</dt></a>
 *   <dd><U>Description</U>: By default GemFire will attempt to reconnect and
 *   reinitialize the cache when it has been forced out of the distributed system
 *   by a network-partition event or has otherwise been shunned by other members.
 *   This setting will turn off this behavior.</dd>
 *   <dd><U>Default</U>: "false"</dd>
 * </dl>
 * 
 * <b>Redundancy Management</b>
 * 
 * <dl>
 *   <a name="enforce-unique-host"><dt>enforce-unique-host</dt></a>
 *   <dd><U>Description</U>: Whether or not partitioned regions will
 *   put redundant copies of the same data in different JVMs running on the same physical host.
 *   
 *   By default, partitioned regions will try to put redundancy copies on different physical hosts, but it may 
 *   put them on the same physical host if no other hosts are available. Setting this property to true
 *   will prevent partitions regions from ever putting redundant copies of data on the same physical host.
 *   </dd>
 *   <dd><U>Default</U>: "false"</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="redundancy-zone"><dt>redundancy-zone</dt></a>
 *   <dd><u>Description</u>: Defines the redundancy zone from this member. If this property is set, partitioned
 *   regions will not put two redundant copies of data in two members with the same redundancy zone setting.   
 *   </dd>
 *   <dd><u>Default</u>: ""</dd>
 * </dl>
 *
 * <B>Logging Configuration Properties</B>
 *
 * <dl>
 *   <a name="log-file"><dt>log-file</dt></a>
 *   <dd><U>Description</U>: Name of the file to write logging
 *   messages to.  If the file name if "" (default) then messages are
 *   written to standard out.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 * </dl>
 *
 * <dl>
 *   <a name="log-level"><dt>log-level</dt></a>
 *   <dd><U>Description</U>:The type of log messages that will
 *   actually write to the log file.
 *   </dd>
 *   <dd><U>Default</U>: "config"</dd>
 *   <dd><U>Allowed values</U>: all|finest|finer|fine|config|info|warning|severe|none</dd>
 * </dl>
 *
 * <dl>
 *   <a name="statistic-sampling-enabled"><dt>statistic-sampling-enabled</dt></a>
 *   <dd><U>Description</U>: "true" causes the statistics to be
 *   sampled periodically and operating system statistics to be
 *   fetched each time a sample is taken.  "false" disables sampling
 *   which also disables operating system statistic collection.  Non
 *   OS statistics will still be recorded in memory and can be viewed
 *   by administration tools.  However, charts will show no activity
 *   and no statistics will be archived while sampling is
 *   disabled.
 *   Starting in 7.0 the default value has been changed to true.
 *   If statistic sampling is disabled it will also cause various
 *   metrics seen in gfsh and pulse to always be zero.
 *   </dd>
 *   <dd><U>Default</U>: "true"</dd>
 *   <dd><U>Allowed values</U>: true|false</dd>
 * </dl>
 *
 * <dl>
 *   <a name="statistic-sample-rate"><dt>statistic-sample-rate</dt></a>
 *   <dd><U>Description</U>:The rate, in milliseconds, at which samples
 *   of the statistics will be taken.
 *   If set to a value less than 1000 the rate will be set to 1000 because
 *   the VSD tool does not support sub-second sampling.
 *   </dd>
 *   <dd><U>Default</U>: "1000"</dd>
 *   <dd><U>Allowed values</U>: 100..60000</dd>
 * </dl>
 *
 * <dl>
 *   <a name="statistic-archive-file"><dt>statistic-archive-file</dt></a>
 *   <dd><U>Description</U>: The file that statistic samples are
 *   written to.  An empty string (default) disables statistic
 *   archival.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 * </dl>
 *
 * <dl>
 *   <a name="enable-time-statistics"><dt>enable-time-statistics</dt></a>
 *   <dd><U>Description</U>: "true" causes additional time-based statistics to be
 *   gathered for gemfire operations.  This can aid in discovering
 *   where time is going in cache operations, albeit at the expense of
 *   extra clock probes on every operation.  "false" disables the additional
 *   time-based statistics.
 *   </dd>
 *   <dd><U>Default</U>: "false"</dd>
 *   <dd><U>Allowed values</U>: true or false</dd>
 *   <dd><U>Since</U>: 5.0</dd>
 * </dl>
 *
 * <dl>
 *   <a name="log-file-size-limit"><dt>log-file-size-limit</dt></a>
 *   <dd><U>Description</U>: Limits, in megabytes, how large the current log
 *   file can grow before it is closed and logging rolls on to a new file.
 *   Set to zero to disable log rolling.
 *   </dd>
 *   <dd><U>Default</U>: "0"</dd>
 *   <dd><U>Allowed values</U>: 0..1000000</dd>
 * </dl>
 * <dl>
 *   <a name="log-disk-space-limit"><dt>log-disk-space-limit</dt></a>
 *   <dd><U>Description</U>: Limits, in megabytes, how much disk space can be
 *   consumed by old inactive log files. When the limit is
 *   exceeded the oldest inactive log file is deleted.
 *   Set to zero to disable automatic log file deletion.
 *   </dd>
 *   <dd><U>Default</U>: "0"</dd>
 *   <dd><U>Allowed values</U>: 0..1000000</dd>
 * </dl>
 *
 * <dl>
 *   <a name="archive-file-size-limit"><dt>archive-file-size-limit</dt></a>
 *   <dd><U>Description</U>: Limits, in megabytes, how large the current statistic archive
 *   file can grow before it is closed and archival rolls on to a new file.
 *   Set to zero to disable archive rolling.
 *   </dd>
 *   <dd><U>Default</U>: "0"</dd>
 *   <dd><U>Allowed values</U>: 0..1000000</dd>
 * </dl>
 * <dl>
 *   <a name="archive-disk-space-limit"><dt>archive-disk-space-limit</dt></a>
 *   <dd><U>Description</U>: Limits, in megabytes, how much disk space can be
 *   consumed by old inactive statistic archive files. When the limit is
 *   exceeded the oldest inactive archive is deleted.
 *   Set to zero to disable automatic archive deletion.
 *   </dd>
 *   <dd><U>Default</U>: "0"</dd>
 *   <dd><U>Allowed values</U>: 0..1000000</dd>
 * </dl>
 *
 * <dl>
 *   <a name="roles"><dt>roles</dt></a>
 *   <dd><U>Description</U>: Specifies the application roles that this member
 *   performs in the distributed system. This is a comma delimited list of
 *   user-defined strings. Any number of members can be configured to perform
 *   the same role, and a member can be configured to perform any number of
 *   roles.
 *   Note that anything defined by the groups gemfire property will also be considered a role.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 5.0</dd>
 *   <dd><U>Deprecated</U>: as of 7.0 use <a href="#groups"><code>groups</code></a> instead.</dd>
 * </dl>
 *
 * <dl>
 *   <a name="max-wait-time-reconnect"><dt>max-wait-time-reconnect</dt></a>
 *   <dd><U>Description</U>: Specifies the maximum number of milliseconds
 *   to wait for the distributed system to reconnect in case of required role
 *   loss or forced disconnect. The system will attempt to <a href="#max-num-reconnect-tries">reconnect
 *   more than once</a>, and this timeout period applies to each reconnection attempt.
 *   </dd>
 *   <dd><U>Default</U>: "60000"</dd>
 *   <dd><U>Since</U>: 5.0</dd>
 * </dl>
 *
 * <dl>
 *   <a name="max-num-reconnect-tries"><dt>max-num-reconnect-tries</dt></a>
 *   <dd><U>Description</U>: Specifies the maximum number or times to attempt
 *   to reconnect to the distributed system when required roles are missing.
 *   This does not apply to reconnect attempts due to a forced disconnect.
 *   </dd>
 *   <dd><U>Default</U>: "3"</dd>
 *   <dd><U>Since</U>: 5.0</dd>
 * </dl>
 *
 * <B>Cache Configuration Properties</B>
 *
 * <dl>
 *   <a name="cache-xml-file"><dt>cache-xml-file</dt></a>
 *   <dd><U>Description</U>: Specifies the name of the XML file or resource
 *   to initialize the cache with when it is
 *   {@linkplain com.gemstone.gemfire.cache.CacheFactory#create created}.
 *   Create will first look for a file that matches the value of this property.
 *   If a file is not found then it will be searched for using
 *   {@link java.lang.ClassLoader#getResource}.  If the value of this
 *   property is the empty string (<code>""</code>), then the cache
 *   will not be declaratively initialized.</dd>
 *   <dd><U>Default</U>: "cache.xml"</dd>
 * </dl>
 *
 * <dl>
 *   <a name="memcached-port"><dt>memcached-port</dt></a>
 *   <dd><U>Description</U>: Specifies the port used by {@link GemFireMemcachedServer}
 *   which enables memcached clients to connect and store data in GemFire distributed system.
 *   see {@link GemFireMemcachedServer} for other configuration options.</dd>
 *   <dd><U>Default</U>: "0" disables GemFireMemcachedServer</dd>
 *   <dd><U>Allowed values</U>: 0..65535</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="memcached-protocol"><dt>memcached-protocol</dt></a>
 *   <dd><U>Description</U>: Specifies the protocol used by {@link GemFireMemcachedServer}</dd>
 *   <dd><U>Default</U>: "ASCII"</dd>
 *   <dd><U>Allowed values</U>: "ASCII" "BINARY"</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="memcached-bind-address"><dt>memcached-bind-address</dt></a>
 *   <dd><U>Description</U>: Specifies the bind address used by {@link GemFireMemcachedServer}</dd>
 *   <dd><U>Default</U>: ""</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="redis-port"><dt>redis-port</dt></a>
 *   <dd><U>Description</U>: Specifies the port used by {@link GemFireRedisServer}
 *   which enables redis clients to connect and store data in GemFire distributed system.
 *   see {@link GemFireRedisServer} for other configuration options.</dd>
 *   <dd><U>Default</U>: "0" disables GemFireMemcachedServer</dd>
 *   <dd><U>Allowed values</U>: 0..65535</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="redis-bind-address"><dt>redis-bind-address</dt></a>
 *   <dd><U>Description</U>: Specifies the bind address used by {@link GemFireRedisServer}</dd>
 *   <dd><U>Default</U>: ""</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="redis-password"><dt>redis-password</dt></a>
 *   <dd><U>Description</U>: Specifies the password to authenticate a client of {@link GemFireRedisServer}</dd>
 *   <dd><U>Default</U>: ""</dd>
 * </dl>
 * 
 * <B>Asynchronous Message Properties</B>
 *
 * <dl>
 *   <a name="async-distribution-timeout"><dt>async-distribution-timeout</dt></a>
 *   <dd><U>Description</U>: The number of milliseconds before a
 *   publishing process should attempt to distribute a cache operation
 *   before switching over to asynchronous messaging for this process.
 *   To enable asynchronous messaging, the value must be set above
 *   zero. If a thread that is publishing to the cache exceeds this value
 *   when attempting to distribute to this process, it will switch to
 *   asynchronous messaging until this process catches up, departs, or
 *   some specified limit is reached, such as <a href="#async-queue-timeout">
 *   async-queue-timeout</a> or <a href="#async-max-queue-size">
 *   async-max-queue-size</a>.
 *   </dd>
 *   <dd><U>Default</U>: "0"</dd>
 *   <dd><U>Allowed values</U>: 0..60000</dd>
 * </dl>
 * <dl>
 *   <a name="async-queue-timeout"><dt>async-queue-timeout</dt></a>
 *   <dd><U>Description</U>: The number of milliseconds a queuing
 *   publisher may enqueue asynchronous messages without any distribution
 *   to this process before that publisher requests this process to
 *   depart. If a queuing publisher has not been able to send this process
 *   any cache operations prior to the timeout, this process will attempt
 *   to close its cache and disconnect from the distributed system.
 *   </dd>
 *   <dd><U>Default</U>: "60000"</dd>
 *   <dd><U>Allowed values</U>: 0..86400000</dd>
 * </dl>
 * <dl>
 *   <a name="async-max-queue-size"><dt>async-max-queue-size</dt></a>
 *   <dd><U>Description</U>: The maximum size in megabytes that a
 *   publishing process should be allowed to asynchronously enqueue
 *   for this process before asking this process to depart from the
 *   distributed system.
 *   </dd>
 *   <dd><U>Default</U>: "8"</dd>
 *   <dd><U>Allowed values</U>: 0..1024</dd>
 * </dl>
 * 
 * <b>JMX Management</b>
 * 
 * <dl>
 *   <a name="jmx-manager"><dt>jmx-manager</dt></a>
 *   <dd><U>Description</U>: If true then this member is willing to be a jmx-manager.
 *   All the other jmx-manager properties will be used when it does become a manager.
 *   If this property is false then all other jmx-manager properties are ignored.
 *   </dd>
 *   <dd><U>Default</U>: "false except on locators"</dd>
 * </dl>
 *
 * <dl>
 *   <a name="jmx-manager-start"><dt>jmx-manager-start</dt></a>
 *   <dd><U>Description</U>: If true then this member will start a jmx manager when
 *   it creates a cache. Management tools like gfsh can be configured to connect
 *   to the jmx-manager. In most cases you should not set this because a jmx manager will
 *   automatically be started when needed on a member that sets "jmx-manager" to true.
 *   Ignored if jmx-manager is false.
 *   </dd>
 *   <dd><U>Default</U>: "false"</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="jmx-manager-port"><dt>jmx-manager-port</dt></a>
 *   <dd><U>Description</U>: The port this jmx manager will listen to for client connections.
 *   If this property is set to zero then GemFire will not allow remote client connections 
 *   but you can alternatively use the standard system properties supported by the JVM 
 *   for configuring access from remote JMX clients.
 *   Ignored if jmx-manager is false.
 *   </dd>
 *   <dd><U>Default</U>: "1099"</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="jmx-manager-ssl"><dt>jmx-manager-ssl</dt></a>
 *   <dd><U>Description</U>: If true and jmx-manager-port is not zero then the jmx-manager
 *   will only accept ssl connections. Note that the ssl-enabled property does not apply to the jmx-manager
 *   but the other ssl properties do. This allows ssl to be configured for just the jmx-manager
 *   without needing to configure it for the other GemFire connections.
 *   Ignored if jmx-manager is false.
 *   </dd>
 *   <dd><U>Default</U>: "false"</dd>
 *   <dd><U>Deprecated</U>: as of 8.0 use <a href="#jmx-manager-ssl-enabled"><code>jmx-manager-ssl-enabled</code></a> instead.</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="jmx-manager-ssl-enabled"><dt>jmx-manager-ssl-enabled</dt></a>
 *   <dd><U>Description</U>: If true and jmx-manager-port is not zero then the jmx-manager
 *   will only accept ssl connections. Note that the ssl-enabled property does not apply to the jmx-manager
 *   but the other ssl properties do. This allows ssl to be configured for just the jmx-manager
 *   without needing to configure it for the other GemFire connections.
 *   Ignored if jmx-manager is false.
 *   </dd>
 *   <dd><U>Default</U>: "false"</dd>
 * </dl> 
 * 
 * <dl>
 *   <a name="jmx-manager-ssl-ciphers"><dt>jmx-manager-ssl-ciphers</dt></a>
 *   <dd><U>Description</U>: A space seperated list of the SSL cipher suites to enable.
 *   Those listed must be supported by the available providers.
 *   </dd>   
 *   <dd><U>Default</U>: "any"</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="jmx-manager-ssl-protocols"><dt>jmx-manager-ssl-protocols</dt></a>
 *   <dd><U>Description</U>: A space seperated list of the SSL protocols to enable.
 *   Those listed must be supported by the available providers.
 *   </dd>
 *   <dd><U>Default</U>: "any"</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="jmx-manager-ssl-require-authentication"><dt>jmx-manager-ssl-require-authentication</dt></a>
 *   <dd><U>Description</U>: If false, allow ciphers that do not require the client
 *   side of the connection to be authenticated.
 *   </dd>   
 *   <dd><U>Default</U>: "true"</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="jmx-manager-ssl-keystore"><dt>jmx-manager-ssl-keystore</dt></a>
 *   <dd><U>Description</U>Location of the Java keystore file containing
 *   certificate and private key.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="jmx-manager-ssl-keystore-type"><dt>jmx-manager-ssl-keystore-type</dt></a>
 *   <dd><U>Description</U>For Java keystore file format, this property has the
 *   value jks (or JKS).
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="jmx-manager-ssl-keystore-password"><dt>jmx-manager-ssl-keystore-password</dt></a>
 *   <dd><U>Description</U>Password to access the private key from the keystore
 *   file specified by javax.net.ssl.keyStore.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="jmx-manager-ssl-truststore"><dt>jmx-manager-ssl-truststore</dt></a>
 *   <dd><U>Description</U>Location of the Java keystore file containing the
 *   collection of CA certificates trusted by manager (trust store).
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="jmx-manager-ssl-truststore-password"><dt>jmx-manager-ssl-truststore-password</dt></a>
 *   <dd><U>Description</U>Password to unlock the keystore file (store password)
 *   specified by javax.net.ssl.trustStore.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="jmx-manager-bind-address"><dt>jmx-manager-bind-address</dt></a>
 *   <dd><U>Description</U>: By default the jmx-manager when configured with a port will listen
 *   on all the local host's addresses. You can use this property to configure what ip address
 *   or host name the jmx-manager will listen on. In addition, if the embedded http server is
 *   started, it will also bind to this address if it is set.
 *   Ignored if jmx-manager is false or jmx-manager-port is zero.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="jmx-manager-hostname-for-clients"><dt>jmx-manager-hostname-for-clients</dt></a>
 *   <dd><U>Description</U>: Lets you control what hostname will be given to clients that ask
 *   the locator for the location of a jmx manager. By default the ip address that the jmx-manager
 *   reports is used. But for clients on a different network this property allows you to configure
 *   a different hostname that will be given to clients.
 *   Ignored if jmx-manager is false or jmx-manager-port is zero.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="jmx-manager-password-file"><dt>jmx-manager-password-file</dt></a>
 *   <dd><U>Description</U>: By default the jmx-manager will allow clients without credentials to connect.
 *   If this property is set to the name of a file then only clients that connect with credentials that
 *   match an entry in this file will be allowed.
 *   Most JVMs require that the file is only readable by the owner.
 *   For more information about the format of this file see Oracle's documentation of the
 *   com.sun.management.jmxremote.password.file system property.
 *   Ignored if jmx-manager is false or if jmx-manager-port is zero.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 * </dl>
 *
 * <dl>
 *   <a name="jmx-manager-access-file"><dt>jmx-manager-access-file</dt></a>
 *   <dd><U>Description</U>: By default the jmx-manager will allow full access to all mbeans by any client.
 *   If this property is set to the name of a file then it can restrict clients to only being able to read
 *   mbeans; they will not be able to modify mbeans. The access level can be configured differently in this
 *   file for each user name defined in the password file.
 *   For more information about the format of this file see Oracle's documentation of the
 *   com.sun.management.jmxremote.access.file system property.
 *   Ignored if jmx-manager is false or if jmx-manager-port is zero.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 * </dl>
 *
 * <dl>
 *   <a name="jmx-manager-http-port"><dt>jmx-manager-http-port</dt></a>
 *   <dd><U>Description</U>: If non-zero then when the jmx manager is started, an embedded
 *   web server will also be started and will listen on this port.
 *   The web server is used to host the GemFire Pulse application.
 *   If you are hosting the Pulse web app in your own web server, then disable
 *   this embedded server by setting this property to zero.
 *   Ignored if jmx-manager is false.
 *   </dd>
 *   <dd><U>Default</U>: "7070"</dd>
 *   <dd><U>Deprecated</U>: as of 8.0 use <a href="#http-service-port"><code>http-service-port</code></a> instead.</dd>
 * </dl>
 *
 * <dl>
 *   <a name="jmx-manager-update-rate"><dt>jmx-manager-update-rate</dt></a>
 *   <dd><U>Description</U>: The rate, in milliseconds, at which this member will push updates
 *   to any jmx managers. Currently this value should be greater than or equal to the
 *   statistic-sample-rate. Setting this value too high will cause stale values to be
 *   seen by gfsh and pulse.
 *   </dd>
 *   <dd><U>Default</U>: "2000"</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="http-service-port"><dt>http-service-port</dt></a>
 *   <dd><U>Description</U>: Specifies the port used by the GemFire HTTP service. If
 *   configured with non-zero value, then an HTTP service will listen on this port.
 *   A value of "0" disables Gemfire HTTP service.
 *   </dd>
 *   <dd><U>Default</U>: "7070" </dd>
 *   <dd><U>Allowed values</U>: 0..65535</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 *  
 * <dl>
 *   <a name="http-service-bind-address"><dt>http-service-bind-address</dt></a>
 *   <dd><U>Description</U>: The address where the GemFire HTTP service will listen
 *   for remote connections. One can use this property to configure what ip
 *   address or host name the HTTP service will listen on. When not set, by
 *   default the HTTP service will listen on the local host's address.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <b> HTTP Service SSL Configuration </b>
 * 
 * <dl>
 *   <a name="http-service-ssl-enabled"><dt>http-service-ssl-enabled</dt></a>
 *   <dd><U>Description</U>: Specifies if http service is started with separate ssl configuration.
 *   If not specified, global property cluster-ssl-enabled (and its other related properties) are used
 *   to secure http service. All http-service-ssl-* properties are inherited from cluster-ssl-* properties. 
 *   User can ovverride them using specific http-service-ssl-* property.
 *   </dd>
 *   <dd><U>Default</U>: <code>false</code></dd>
 *   <dd><U>Since</U>: 8.1</dd>
 * </dl> 
 * 
 * <dl>
 *   <a name="http-service-ssl-ciphers"><dt>http-service-ssl-ciphers</dt></a>
 *   <dd><U>Description</U>: A space separated list of the SSL cipher suites to enable.
 *   Those listed must be supported by the available providers.
 *   </dd>
 *   <dd><U>Default</U>: <code>any</code></dd>
 *   <dd><U>Since</U>: 8.1</dd>
 * </dl>
 *  
 * <dl>
 *   <a name="http-service-ssl-protocols"><dt>http-service-ssl-protocols</dt></a>
 *   <dd><U>Description</U>: A space separated list of the SSL protocols to enable.
 *   Those listed must be supported by the available providers.
 *   </dd>
 *   <dd><U>Default</U>: <code>any</code></dd>
 *   <dd><U>Since</U>: 8.1</dd>
 * </dl>
 *  
 * <dl>
 *   <a name="http-service-ssl-require-authentication"><dt>http-service-ssl-require-authentication</dt></a>
 *   <dd><U>Description</U>: If false, allow ciphers that do not require the client
 *   side of the connection to be authenticated.
 *   </dd>
 *   <dd><U>Default</U>: <code>false</code></dd>
 *   <dd><U>Since</U>: 8.1</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="http-service-ssl-keystore"><dt>http-service-ssl-keystore</dt></a>
 *   <dd><U>Description</U>Location of the Java keystore file containing
 *   certificate and private key.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.1</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="http-service-ssl-keystore-type"><dt>http-service-ssl-keystore-type</dt></a>
 *   <dd><U>Description</U>For Java keystore file format, this property has the
 *   value jks (or JKS).
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.1</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="http-service-ssl-keystore-password"><dt>http-service-ssl-keystore-password</dt></a>
 *   <dd><U>Description</U>Password to access the private key from the keystore
 *   file specified by javax.net.ssl.keyStore.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.1</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="http-service-ssl-truststore"><dt>http-service-ssl-truststore</dt></a>
 *   <dd><U>Description</U>Location of the Java keystore file containing the
 *   collection of CA certificates trusted by server (trust store).
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.1</dd>
 * </dl>
 *  
 * <dl>
 *   <a name="http-service-ssl-truststore-password"><dt>http-service-ssl-truststore-password</dt></a>
 *   <dd><U>Description</U>Password to unlock the keystore file (store password)
 *   specified by javax.net.ssl.trustStore.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.1</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="start-dev-rest-api"><dt>start-dev-rest-api</dt></a>
 *   <dd><U>Description</U>: If true then developer(API) REST service will be
 *   started when cache is created. REST service can be configured using
 *   <code>http-service-port</code> and <code>http-service-bind-address</code>
 *   properties.</dd>
 *   <dd><U>Default</U>: "false"</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <b>Off-Heap Memory</b>
 * 
 * <dl>
 *   <a name="off-heap-memory-size"><dt>off-heap-memory-size</dt></a>
 *   <dd><U>Description</U>: The total size of off-heap memory specified as
 *   off-heap-memory-size=<n>[g|m]. <n> is the size. [g|m] indicates
 *   whether the size should be interpreted as gigabytes or megabytes.
 *   By default no off-heap memory is allocated.
 *   A non-zero value will cause that much memory to be allocated from the operating
 *   system and reserved for off-heap use.
 *   </dd>
 *   <dd><U>Default</U>: <code>""</code></dd>
 *   <dd><U>Since</U>: 9.0</dd>
 * </dl>
 * 
 * <b>Cluster Configuration Service</b>
 * <dl>
 *   <a name="enable-cluster-configuration"><dt>enable-cluster-configuration</dt></a>
 *   <dd><U>Description</U>: "true" causes creation of cluster configuration service on dedicated locators. The cluster configuration service on dedicated locator(s) 
 *   would serve the configuration to new members joining the distributed system and also save the configuration changes caused by the Gfsh commands.
 *   This property is only applicable to dedicated locators. 
 *   </dd>
 *   <dd><U>Default</U>: "true"</dd> 
 *   <dd><U>Allowed values</U>: true or false</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="use-cluster-configuration"><dt>use-cluster-configuration</dt></a>
 *   <dd><U>Description</U>: This property is only applicable for data members (non client and non locator) 
 *   "true" causes a member to request and uses the configuration from cluster configuration services running on dedicated locators. 
 *   "false" causes a member to not request the configuration from the configuration services running on the locator(s).
 *   </dd>
 *   <dd><U>Default</U>: "true"</dd> 
 *   <dd><U>Allowed values</U>: true or false</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="load-cluster-configuration-from-dir"><dt>load-cluster-configuration-from-dir</dt></a>
 *   <dd><U>Description</U>: "true" causes loading of cluster configuration from "cluster_config" directory in the locator. 
 *   This property is only applicable to dedicated locators which have "enable-cluster-configuration" set to true.
 *   </dd>
 *   <dd><U>Default</U>: "false"</dd>
 *   <dd><U>Allowed values</U>: true or false</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 *  * <dl>
 *   <a name="cluster-configuration-dir"><dt>cluster-configuration-dir</dt></a>
 *   <dd><U>Description</U>: This property specifies the directory in which the cluster configuration related disk-store and artifacts are stored  
 *   This property is only applicable to dedicated locators which have "enable-cluster-configuration" set to true.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Allowed values</U></dd>
 *   <dd><U>Since</U>: 8.1</dd>
 * </dl>
 * 
 * <b> Server SSL Configuration </b>
 * 
 * <dl>
 *   <a name="server-ssl-enabled"><dt>server-ssl-enabled</dt></a>
 *   <dd><U>Description</U>: Specifies if server is started with separate ssl configuration.
 *   If not specified global property ssl-enabled (and its other related properties) are used
 *   to create server socket
 *   </dd>
 *   <dd><U>Default</U>: <code>false</code></dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl> 
 * 
 * <dl>
 *   <a name="server-ssl-ciphers"><dt>server-ssl-ciphers</dt></a>
 *   <dd><U>Description</U>: A space seperated list of the SSL cipher suites to enable.
 *   Those listed must be supported by the available providers.
 *   </dd>
 *   <dd><U>Default</U>: <code>any</code></dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 *  
 * <dl>
 *   <a name="server-ssl-protocols"><dt>server-ssl-protocols</dt></a>
 *   <dd><U>Description</U>: A space seperated list of the SSL protocols to enable.
 *   Those listed must be supported by the available providers.
 *   </dd>
 *   <dd><U>Default</U>: <code>any</code></dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 *  
 * <dl>
 *   <a name="server-ssl-require-authentication"><dt>server-ssl-require-authentication</dt></a>
 *   <dd><U>Description</U>: If false, allow ciphers that do not require the client
 *   side of the connection to be authenticated.
 *   </dd>
 *   <dd><U>Default</U>: <code>any</code></dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="server-ssl-keystore"><dt>server-ssl-keystore</dt></a>
 *   <dd><U>Description</U>Location of the Java keystore file containing
 *   certificate and private key.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="server-ssl-keystore-type"><dt>server-ssl-keystore-type</dt></a>
 *   <dd><U>Description</U>For Java keystore file format, this property has the
 *   value jks (or JKS).
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="server-ssl-keystore-password"><dt>server-ssl-keystore-password</dt></a>
 *   <dd><U>Description</U>Password to access the private key from the keystore
 *   file specified by javax.net.ssl.keyStore.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="server-ssl-truststore"><dt>server-ssl-truststore</dt></a>
 *   <dd><U>Description</U>Location of the Java keystore file containing the
 *   collection of CA certificates trusted by server (trust store).
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="server-ssl-truststore-password"><dt>server-ssl-truststore-password</dt></a>
 *   <dd><U>Description</U>Password to unlock the keystore file (store password)
 *   specified by javax.net.ssl.trustStore.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <b> Gateway SSL Configuration </b>
 * 
 * <dl>
 *   <a name="gateway-ssl-enabled"><dt>gateway-ssl-enabled</dt></a>
 *   <dd><U>Description</U>: Specifies if gateway is started with separate ssl configuration.
 *   If not specified global property ssl-enabled (and its other related properties) are used
 *   to create gateway socket
 *   </dd>
 *   <dd><U>Default</U>: <code>false</code></dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl> 
 * 
 * <dl>
 *   <a name="gateway-ssl-ciphers"><dt>gateway-ssl-ciphers</dt></a>
 *   <dd><U>Description</U>: A space seperated list of the SSL cipher suites to enable.
 *   Those listed must be supported by the available providers.
 *   </dd>
 *   <dd><U>Default</U>: <code>any</code></dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 *  
 * <dl>
 *   <a name="gateway-ssl-protocols"><dt>gateway-ssl-protocols</dt></a>
 *   <dd><U>Description</U>: A space seperated list of the SSL protocols to enable.
 *   Those listed must be supported by the available providers.
 *   </dd>
 *   <dd><U>Default</U>: <code>any</code></dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 *  
 * <dl>
 *   <a name="gateway-ssl-require-authentication"><dt>gateway-ssl-require-authentication</dt></a>
 *   <dd><U>Description</U>: If false, allow ciphers that do not require the Gateway Sender
 *   side of the connection to be authenticated.
 *   </dd>
 *   <dd><U>Default</U>: <code>any</code></dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="gateway-ssl-keystore"><dt>gateway-ssl-keystore</dt></a>
 *   <dd><U>Description</U>Location of the Java keystore file containing
 *   certificate and private key.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="gateway-ssl-keystore-type"><dt>gateway-ssl-keystore-type</dt></a>
 *   <dd><U>Description</U>For Java keystore file format, this property has the
 *   value jks (or JKS).
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="gateway-ssl-keystore-password"><dt>gateway-ssl-keystore-password</dt></a>
 *   <dd><U>Description</U>Password to access the private key from the keystore
 *   file specified by javax.net.ssl.keyStore.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="gateway-ssl-truststore"><dt>gateway-ssl-truststore</dt></a>
 *   <dd><U>Description</U>Location of the Java keystore file containing the
 *   collection of CA certificates trusted by server (trust store).
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="gateway-ssl-truststore-password"><dt>gateway-ssl-truststore-password</dt></a>
 *   <dd><U>Description</U>Password to unlock the keystore file (store password)
 *   specified by javax.net.ssl.trustStore.
 *   </dd>
 *   <dd><U>Default</U>: ""</dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * <b>Miscellaneous</b> 
 *
 * <dl>
 *   <a name="lock-memory"><dt>lock-memory</dt></a>
 *   <dd><U>Description</U>: Include this option to lock GemFire heap and off-heap memory pages into RAM.
 *   This prevents the operating system from swapping the pages out to disk, which can cause sever
 *   performance degradation. When you use this command, also configure the operating system limits for
 *   locked memory.
 *   </dd>
 *   <dd><U>Default</U>: <code>"false"</code></dd>
 *   <dd><U>Since</U>: 9.0</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="deploy-working-dir"><dt>deploy-working-dir</dt></a>
 *   <dd><U>Description</U>: Specifies the working directory which this
 *   distributed member will use to persist deployed JAR files.  This directory
 *   should be unchanged when restarting the member so that it can read what
 *   was previously persisted. The default is the current working directory
 *   as determined by <code>System.getProperty("user.dir")</code>.
 *   </dd>
 *   <dd><U>Default</U>: <code>System.getProperty("user.dir")</code></dd>
 *   <dd><U>Since</U>: 7.0</dd>
 * </dl>
 *
 * <dl>
 *   <a name="user-command-packages"><dt>user-command-packages</dt></a>
 *   <dd><U>Description</U>: A comma separated list of Java packages that
 *   contain classes implementing the <code>CommandMarker</code> interface.
 *   Matching classes will be loaded when the VM starts and will be available
 *   in the GFSH command-line utility.
 *   </dd>
 *   <dd><U>Default</U>: <code>""</code></dd>
 *   <dd><U>Since</U>: 8.0</dd>
 * </dl>
 * 
 * @author Darrel Schneider
 * @author Bruce Schuchardt
 *
 * @since 3.0
 */
public abstract class DistributedSystem implements StatisticsFactory {

  /** 
   * The instances of <code>DistributedSystem</code> created in this
   * VM. Presently only one connect to a distributed system is allowed in a VM.
   * This set is never modified in place (it is always read only) but
   * the reference can be updated by holders of {@link #existingSystemsLock}.
   */
  protected static volatile List existingSystems = Collections.EMPTY_LIST;
  /**
   * This lock must be changed to add or remove a system.
   * It is notified when a system is removed.
   * 
   * @see #existingSystems
   */
  protected static final Object existingSystemsLock = new Object();

  //public static Properties props = new Properties();

  /**
   * Used to indicate a reconnect is tried in case of required role
   * loss.
   * */

 // public static boolean reconnect = false;

  ////////////////////////  Static Methods  ////////////////////////

  /**
   * Connects to a GemFire distributed system with a configuration
   * supplemented by the given properties.
   * <P>The actual configuration attribute values used to connect comes
   * from the following sources:
   * <OL>
   * <LI>System properties. If a system property named
   *     "<code>gemfire.</code><em>propertyName</em>" is defined
   *     and its value is not an empty string
   *     then its value will be used for the named configuration attribute.
   *
   * <LI>Code properties. Otherwise if a property is defined in the <code>config</code>
   *     parameter object and its value is not an empty string
   *     then its value will be used for that configuration attribute.
   * <LI>File properties. Otherwise if a property is defined in a configuration property
   *     file found by this application and its value is not an empty string
   *     then its value will be used for that configuration attribute.
   *     A configuration property file may not exist.
   *     See the following section for how configuration property files are found.
   * <LI>Defaults. Otherwise a default value is used.
   * </OL>
   * <P>
   * The name of the property file can be
   * specified using the "gemfirePropertyFile" system property.
   * If the system property is set to a relative file name then it
   * is searched for in following locations.
   * If the system property is set to an absolute file name then that
   * file is used as the property file.
   * If the system property is not set, then the name of the property
   * file defaults to "gemfire.properties".  The configuration file is
   * searched for in the following locations:
   *
   * <OL>
   * <LI>Current directory (directory in which the VM was
   *     launched)</LI>
   * <LI>User's home directory</LI>
   * <LI>Class path (loaded as a {@linkplain
   *     ClassLoader#getResource(String) system resource})</LI>
   * </OL>
   *
   * If the configuration file cannot be located, then the property
   * will have its default value as described <a
   * href="#configuration">above</a>.
   *
   * @param config
   *        The <a href="#configuration">configuration properties</a>
   *        used when connecting to the distributed system
   *
   * @throws IllegalArgumentException
   *         If <code>config</code> contains an unknown configuration
   *         property or a configuration property does not have an
   *         allowed value.  Note that the values of boolean
   *         properties are parsed using {@link Boolean#valueOf(java.lang.String)}.
   *         Therefore all values other than "true" values will be
   *         considered <code>false</code> -- an exception will not be
   *         thrown.
   * @throws IllegalStateException
   *         If a <code>DistributedSystem</code> with a different
   *         configuration has already been created in this VM or if
   *         this VM is {@link
   *         com.gemstone.gemfire.admin.AdminDistributedSystem
   *         administering} a distributed system.
   * @throws com.gemstone.gemfire.GemFireIOException
   *         Problems while reading configuration properties file or
   *         while opening the log file.
   * @throws com.gemstone.gemfire.GemFireConfigException
   *         The distribution transport is not configured correctly
   *
   * @deprecated as of 6.5 use {@link CacheFactory#create} or {@link ClientCacheFactory#create} instead.
   *
   * */
  public static DistributedSystem connect(Properties config) {
    if (config == null) {
      // fix for bug 33992
      config = new Properties();
    }
//     {
//       LogWriterI18n logger =
//         new LocalLogWriter(LocalLogWriter.ALL_LEVEL, System.out);
//       logger.info("DistributedSystem: Connecting with " + config,
//                   new Exception("Stack trace"));
//     }
    synchronized (existingSystemsLock) {
      if (DistributionManager.isDedicatedAdminVM) {
        // For a dedicated admin VM, check to see if there is already
        // a connect that will suit our purposes.
        DistributedSystem existingSystem = getConnection(config);
        if (existingSystem != null) {
          return existingSystem;
        }

      } else {
        boolean existingSystemDisconnecting = true;
        while (!existingSystems.isEmpty() && existingSystemDisconnecting) {
          Assert.assertTrue(existingSystems.size() == 1);

          InternalDistributedSystem existingSystem =
              (InternalDistributedSystem) existingSystems.get(0);
          existingSystemDisconnecting = existingSystem.isDisconnecting();
          if (existingSystemDisconnecting) {
            boolean interrupted = Thread.interrupted();
            try {
              // no notify for existingSystemsLock, just to release the sync
              existingSystemsLock.wait(50);
            } 
            catch (InterruptedException ex) {
              interrupted = true;
            }
            finally {
              if (interrupted) {
                Thread.currentThread().interrupt();
              }
            }
          } else if (existingSystem.isConnected()) {
            existingSystem.validateSameProperties(config,
                existingSystem.isConnected());
            return existingSystem;
          } else {
          // This should not happen: existingSystem.isConnected()==false && existingSystem.isDisconnecting()==false 
            throw new AssertionError("system should not be disconnecting==false and isConnected==falsed");
          }
        }
      }

      // Make a new connection to the distributed system
      try {
        InternalDistributedSystem newSystem =
          InternalDistributedSystem.newInstance(config);
        addSystem(newSystem);
        return newSystem;

      }
      catch (GemFireSecurityException ex) {
        ex.fillInStackTrace(); // Make it harder to figure out where jgroups code lives.
        throw ex;
      }
    }
  }

  protected static void addSystem(InternalDistributedSystem newSystem) {
    synchronized (existingSystemsLock) {
      int size = existingSystems.size();
      if (size == 0) {
        existingSystems = Collections.singletonList(newSystem);
      }
      else {
        ArrayList l = new ArrayList(size+1);
        l.addAll(existingSystems);
        l.add(0, newSystem);
        existingSystems = Collections.unmodifiableList(l);
      }
    }
  }
  protected static boolean removeSystem(InternalDistributedSystem oldSystem) {
    synchronized (existingSystemsLock) {
      ArrayList l = new ArrayList(existingSystems);
      boolean result = l.remove(oldSystem);
      if (result) {
        int size = l.size();
        if (size == 0) {
          existingSystems = Collections.EMPTY_LIST;
        }
        else if (size == 1) {
          existingSystems = Collections.singletonList(l.get(0));
        }
        else {
          existingSystems = Collections.unmodifiableList(l);
        }
      }
      return result;
    }
  }
  
  /**
   * Sets the calling thread's socket policy.
   * This value will override that default set by the
   * <code>conserve-sockets</code> configuration property.
   * @param conserveSockets If <code>true</code> then calling thread will share
   *   socket connections with other threads.
   *   If <code>false</code> then calling thread will have its own sockets.
   * @since 4.1
   */
  public static void setThreadsSocketPolicy(boolean conserveSockets) {
    if (conserveSockets) {
      ConnectionTable.threadWantsSharedResources();
    } else {
      ConnectionTable.threadWantsOwnResources();
    }
  }



  /**
   * Frees up any socket resources owned by the calling thread.
   * @since 4.1
   */
  public static void releaseThreadsSockets() {
    ConnectionTable.releaseThreadsSockets();
  }

  /**
   * Returns an existing connection to the distributed system
   * described by the given properties.
   *
   * @since 4.0
   */
  private static DistributedSystem getConnection(Properties config) {
    // In an admin VM you can have a connection to more than one
    // distributed system.  If we are already connected to the desired
    // distributed system, return that connection.
    List l = existingSystems;
    for (Iterator iter = l.iterator(); iter.hasNext(); ) {
      InternalDistributedSystem existingSystem =
        (InternalDistributedSystem) iter.next();
      if (existingSystem.sameSystemAs(config)) {
        Assert.assertTrue(existingSystem.isConnected());
        return existingSystem;
      }
    }

    return null;
  }

  /**
   * Returns a connection to the distributed system that is
   * appropriate for administration.  This method is for internal use
   * only by the admin API.
   *
   * @since 4.0
   */
  protected static DistributedSystem connectForAdmin(Properties props) {
    DistributedSystem existing = getConnection(props);
    if (existing != null) {
      return existing;

    } else {
      //logger.info("creating new distributed system for admin");
      //for (java.util.Enumeration en=props.propertyNames(); en.hasMoreElements(); ) {
      //  String prop=(String)en.nextElement();
      //  logger.info(prop + "=" + props.getProperty(prop));
      //}
      props.setProperty(DistributionConfig.CONSERVE_SOCKETS_NAME, "true");
      // LOG: no longer using the LogWriter that was passed in
      return connect(props);
    }
  }

  /**
   * see {@link com.gemstone.gemfire.admin.AdminDistributedSystemFactory}
   * @since 5.7
   */
  protected static void setEnableAdministrationOnly(boolean adminOnly) {
    synchronized (existingSystemsLock) {
      if( existingSystems != null && !existingSystems.isEmpty()) {
        throw new IllegalStateException(
          LocalizedStrings.DistributedSystem_THIS_VM_ALREADY_HAS_ONE_OR_MORE_DISTRIBUTED_SYSTEM_CONNECTIONS_0
            .toLocalizedString(existingSystems));
      }
      DistributionManager.isDedicatedAdminVM = adminOnly;
    }
  }

//   /**
//    * Connects to a GemFire distributed system with a configuration
//    * supplemented by the given properties.
//    *
//    * @param config
//    *        The <a href="#configuration">configuration properties</a>
//    *        used when connecting to the distributed system
//    * @param callback
//    *        A user-specified object that is delivered with the {@link
//    *        com.gemstone.gemfire.admin.SystemMembershipEvent}
//    *        triggered by connecting.
//    *
//    * @see #connect(Properties)
//    * @see com.gemstone.gemfire.admin.SystemMembershipListener#memberJoined
//    *
//    * @since 4.0
//    */
//   public static DistributedSystem connect(Properties config,
//                                           Object callback) {
//     throw new UnsupportedOperationException("Not implemented yet");
//   }

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new instance of <code>DistributedSystem</code>.  This
   * constructor is protected so that it may only be invoked by
   * subclasses.
   */
  protected DistributedSystem() {

  }

  ////////////////////  Instance Methods  ////////////////////

  /**
   * Returns the <code>LogWriter</code> used for logging information.
   * See <A href="#logFile">logFile</A>.
   *
   * @throws IllegalStateException
   *         This VM has {@linkplain #disconnect() disconnected} from the
   *         distributed system.
   */
  public abstract LogWriter getLogWriter();

  /**
   * Returns the <code>LogWriter</code> used for logging security related
   * information. See <A href="#logFile">logFile</A>.
   * 
   * @throws IllegalStateException
   *                 This VM has {@linkplain #disconnect() disconnected} from
   *                 the distributed system.
   * @since 5.5
   */
  public abstract LogWriter getSecurityLogWriter();

  /**
   * Returns the configuration properties.
   * @return the configuration Properties
   */
  public abstract Properties getProperties();

  /**
   * Returns the security specific configuration properties.
   * @return the configuration Properties
   * @since 5.5
   */
  public abstract Properties getSecurityProperties();

  /**
   * 
   * @return the cancel criterion for this system
   */
  public abstract CancelCriterion getCancelCriterion();
  
  /**
   * Disconnects from this distributed system.  This operation will
   * close the distribution manager and render the {@link
   * com.gemstone.gemfire.cache.Cache Cache} and all distributed
   * collections obtained from this distributed system inoperable.
   * After a disconnect has completed, a VM may connect to another
   * distributed system.
   *
   * <P>
   *
   * Attempts to access a distributed system after a VM has
   * disconnected from it will result in an {@link
   * IllegalStateException} being thrown.
   * @deprecated as of 6.5 use {@link Cache#close} or {@link ClientCache#close} instead.
   */
  public abstract void disconnect();
  /**
   * Returns whether or not this <code>DistributedSystem</code> is
   * connected to the distributed system.
   *
   * @see #disconnect()
   */
  public abstract boolean isConnected();

  /**
   * Returns the id of this connection to the distributed system.
   *
   * @deprecated {@link #getDistributedMember} provides an identity for
   *             this connection that is unique across the entire
   *             distributed system.
   */
  @Deprecated
  public abstract long getId();

  /**
   * Returns a string that uniquely identifies this connection to the
   * distributed system.
   *
   * @see com.gemstone.gemfire.admin.SystemMembershipEvent#getMemberId
   *
   * @since 4.0
   * @deprecated as of GemFire 5.0, use {@link #getDistributedMember} instead
   */
  @Deprecated
  public abstract String getMemberId();

  /**
   * Returns the {@link DistributedMember} that identifies this connection to
   * the distributed system.
   * @return the member that represents this distributed system connection.
   * @since 5.0
   */
  public abstract DistributedMember getDistributedMember();
  
  /**
   * Returns a set of all the other members in this distributed system.
   * @return returns a set of all the other members in this distributed system.
   * @since 7.0
   */
  public abstract Set<DistributedMember> getAllOtherMembers();

  /**
   * Returns a set of all the members in the given group.
   * Members join a group be setting the "groups" gemfire property.
   * @return returns a set of all the member in a group.
   * @since 7.0
   */
  public abstract Set<DistributedMember> getGroupMembers(String group);
  
  
  /**
   * Find the set of distributed members running on a given address
   * 
   * @return a set of all DistributedMembers that have any interfaces
   * that match the given IP address. May be empty if there are no members.
   * 
   * @since 7.1
   */ 
   public abstract Set<DistributedMember> findDistributedMembers(InetAddress address);

   /**
   * Find the distributed member with the given name
   * 
   * @return the distributed member that has the given name, or null if
   * no member is currently running with the given name.
   * 
   * @since 7.1
   */
   public abstract DistributedMember findDistributedMember(String name);

  /**
   * Returns the <a href="#name">name</a> of this connection to the
   * distributed system.
   */
  public abstract String getName();

//   /**
//    * Fires an "informational" <code>SystemMembershipEvent</code> that
//    * is delivered to all {@link
//    * com.gemstone.gemfire.admin.SystemMembershipListener}s.
//    *
//    * @param callback
//    *        A user-specified object that is delivered with the {@link
//    *        com.gemstone.gemfire.admin.SystemMembershipEvent}
//    *        triggered by invoking this method.
//    *
//    * @see com.gemstone.gemfire.admin.SystemMembershipListener#memberInfo
//    *
//    * @since 4.0
//    */
//   public abstract void fireInfoEvent(Object callback);

  /**
   * The <code>PROPERTY_FILE</code> is the name of the
   * property file that the connect method will check for when
   * it looks for a property file.
   * The file will be searched for, in order, in the following directories:
   * <ol>
   * <li> the current directory
   * <li> the home directory
   * <li> the class path
   * </ol>
   * Only the first file found will be used.
   * <p>
   * The default value of PROPERTY_FILE is
   * <code>"gemfire.properties"</code>.  However if the
   * "gemfirePropertyFile" system property is set then its value is
   * the value of PROPERTY_FILE. If this value is a relative file
   * system path then the above search is done.  If it is an absolute
   * file system path then that file must exist; no search for it is
   * done.
   * @since 5.0
   *  */
  public static final String PROPERTY_FILE = System.getProperty("gemfirePropertyFile", "gemfire.properties");

  /**
   * The <code>SECURITY_PROPERTY_FILE</code> is the name of the
     * property file that the connect method will check for when
     * it looks for a security property file.
     * The file will be searched for, in order, in the following directories:
     * <ol>
     * <li> the current directory
     * <li> the home directory
     * <li> the class path
     * </ol>
     * Only the first file found will be used.
     * <p>
     * The default value of SECURITY_PROPERTY_FILE is
     * <code>"gfsecurity.properties"</code>.  However if the
     * "gemfireSecurityPropertyFile" system property is set then its value is
     * the value of SECURITY_PROPERTY_FILE. If this value is a relative file
     * system path then the above search is done.  If it is an absolute
     * file system path then that file must exist; no search for it is
     * done.
     * @since 6.6.2
   */
  public static final String SECURITY_PROPERTY_FILE = System.getProperty("gemfireSecurityPropertyFile",
    "gfsecurity.properties");

  /**
   * Gets an <code>URL</code> for the property file, if one can be found,
   * that the connect method will use as its property file.
   * <p>
   * See {@link #PROPERTY_FILE} for information on the name of
   * the property file and what locations it will be looked for in.
   * @return a <code>URL</code> that names the GemFire property file.
   *    Null is returned if no property file was found.
   * @since 5.0
   */
  public static URL getPropertyFileURL() {
    return getFileURL(PROPERTY_FILE);
  }

  /**
   * Gets an <code>URL</code> for the security property file, if one can be found,
   * that the connect method will use as its property file.
   * <p>
   * See {@link #SECURITY_PROPERTY_FILE} for information on the name of
   * the property file and what locations it will be looked for in.
   * @return a <code>URL</code> that names the GemFire security property file.
   *    Null is returned if no property file was found.
   * @since 6.6.2
   */
  public static URL getSecurityPropertiesFileURL() {
    return getFileURL(SECURITY_PROPERTY_FILE);
  }

  private static URL getFileURL(String fileName) {
    File file = new File(fileName);

    if (file.exists()) {
      try {
        return IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(file).toURI().toURL();
      } catch (MalformedURLException ignore) {
      }
    }

    file = new File(System.getProperty("user.home"), fileName);

    if (file.exists()) {
      try {
        return IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(file).toURI().toURL();
      } catch (MalformedURLException ignore) {
      }
    }

    return ClassPathLoader.getLatest().getResource(DistributedSystem.class, fileName);
  }
  
  /**
   * Test to see whether the DistributedSystem is in the process of reconnecting
   * and recreating the cache after it has been removed from the system
   * by other members or has shut down due to missing Roles and is reconnecting.<p>
   * This will also return true if the DistributedSystem has finished reconnecting.
   * When reconnect has completed you can use {@link DistributedSystem#getReconnectedSystem} to
   * retrieve the new distributed system.
   * 
   * @return true if the DistributedSystem is attempting to reconnect or has finished reconnecting
   */
  abstract public boolean isReconnecting();
  
  /**
   * Wait for the DistributedSystem to finish reconnecting to the system
   * and recreate the cache.
   * 
   * @param time amount of time to wait, or -1 to wait forever
   * @param units
   * @return true if the system was reconnected
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  abstract public boolean waitUntilReconnected(long time, TimeUnit units) throws InterruptedException;
  
  /**
   * Force the DistributedSystem to stop reconnecting.  If the DistributedSystem
   * is currently connected this will disconnect it and close the cache.
   * 
   */
  abstract public void stopReconnecting();
  
  /**
   * Returns the new DistributedSystem if there was an auto-reconnect
   */
  abstract public DistributedSystem getReconnectedSystem();

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.admin.jmx;

import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
//import com.gemstone.gemfire.admin.internal.InetAddressUtil;

/**
 * A configuration object for a JMX administration {@linkplain Agent
 * agent} that is hosted by a GemFire application VM.  A file named
 * {@link #DEFAULT_PROPERTY_FILE "agent.properties"} can be used to
 * override the default values of <code>AgentConfig</code> attributes.
 * The "gfAgentPropertyFile" {@linkplain System#getProperty(java.lang.String) system
 * property} can be used to specify an agent properties file other
 * than "agent.properties".  System properties prefixed with
 * {@linkplain #SYSTEM_PROPERTY_PREFIX "gemfire.agent."} can be used to
 * override the values in the properties file.  For instance
 * "-Dgemfire.agent.http-port=8081" can be used to override the
 * default port for the HTTP adapter.  Configuration related to the
 * distributed system that the JMX agent administers is inherited from
 * and described in <code>AgentConfig</code>'s superinterface, {@link
 * DistributedSystemConfig}.
 *
 * <P>
 *
 * An <code>AgentConfig</code> can be modified using a number of
 * mutator methods until it is used to create an <code>Agent</code>.
 * After that, attempts to modify most attributes in the
 * <code>AgentConfig</code> will result in an {@link
 * IllegalStateException} being thrown.  If you wish to use the same
 * <code>AgentConfig</code> to configure multiple <code>Agent</code>s,
 * a copy of the <code>AgentConfig</code> object can be made by
 * invoking its {@link #clone} method.
 *
 * <P>
 *
 * <B>JMX Administation Agent Configuration Properties</B>
 *
 * <dl>
 *   <a name="auto-connect"><dt>{@linkplain #AUTO_CONNECT_NAME auto-connect}</dt></a>
 *   <dd><U>Description</U>: whether or not a JMX agent will
 *   automatically connect to the distributed system it is configured to
 *   administer.</dd>
 *   <dd><U>{@linkplain #DEFAULT_AUTO_CONNECT Default}</U>: false</dd>
 * </dl>
 *
 * <B>JMX Agent SSL Configuration Properties</B>
 *
 * <P>
 *
 * These parameters configure sockets that are created by the GemFire
 * JMX Agent regardless of which adapters are enabled.  These setting
 * apply to all adapters.  For example, if clients connect to the RMI
 * adapter using SSL, then clients must also connect to the HTTP
 * adapter using SSL (HTTPS).  Note that these configuration
 * attributes do <b>not</b> effect how the agent connects to the
 * distributed system it administers, only how JMX clients connect to
 * the agent.
 *
 * <dl>
 *   <a name="agent-ssl-enabled"><dt>{@linkplain #AGENT_SSL_ENABLED_NAME agent-ssl-enabled}</dt></a>
 *   <dd><U>Description</U>: whether or not connections to the JMX agent
 *   require SSL
 *   </dd>
 *   <dd><U>{@linkplain #DEFAULT_AGENT_SSL_ENABLED Default}</U>: false</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="agent-ssl-protocols"><dt>{@linkplain #AGENT_SSL_PROTOCOLS_NAME agent-ssl-protocols}</dt></a>
 *   <dd><U>Description</U>: the SSL protocols to be used when connecting
 *   to the JMX agent
 *   </dd>
 *   <dd><U>{@linkplain #DEFAULT_AGENT_SSL_PROTOCOLS Default}</U>: any</dd>
 * </dl>
 *
 * <dl>
 *   <a name="agent-ssl-ciphers"><dt>{@linkplain #AGENT_SSL_CIPHERS_NAME agent-ssl-ciphers}</dt></a>
 *   <dd><U>Description</U>: the SSL ciphers to be used when connecting
 *   to the JMX agent
 *   </dd>
 *   <dd><U>{@linkplain #DEFAULT_AGENT_SSL_CIPHERS Default}</U>: any</dd>
 * </dl>
 *
 * <dl>
 *   <a name="agent-ssl-require-authentication"><dt>{@linkplain #AGENT_SSL_REQUIRE_AUTHENTICATION_NAME agent-ssl-require-authentication}</dt></a>
 *   <dd><U>Description</U>: whether or not SSL connections to the RMI
 *   adapter require authentication
 *   </dd>
 *   <dd><U>{@linkplain #DEFAULT_AGENT_SSL_REQUIRE_AUTHENTICATION Default}</U>: true</dd>
 * </dl>
 *
 * <dl>
 *   <a name="http-ssl-require-authentication"><dt>{@linkplain #HTTP_SSL_REQUIRE_AUTHENTICATION_NAME http-ssl-require-authentication}</dt></a>
 *   <dd><U>Description</U>: whether or not SSL connections to the HTTP
 *   adapter require authentication
 *   </dd>
 *   <dd><U>{@linkplain #DEFAULT_HTTP_SSL_REQUIRE_AUTHENTICATION Default}</U>: false</dd>
 * </dl>
 *
 * <B>HTTP Adapter Configuration</B>
 *
 * <dl>
 *   <a name="http-enabled"><dt>{@linkplain #HTTP_ENABLED_NAME http-enabled}</dt></a>
 *   <dd><U>Description</U>: whether or not the HTTP adapter is
 *   enabled in the JMX agent.</dd>
 *   <dd><U>{@linkplain #DEFAULT_HTTP_ENABLED Default}</U>: true</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="http-port"><dt>{@linkplain #HTTP_PORT_NAME http-port}</dt></a>
 *   <dd><U>Description</U>: the port on which the HTTP adapter should
 *   listen for client connections.
 *   </dd>
 *   <dd><U>{@linkplain #DEFAULT_HTTP_PORT Default}</U>: 8080</dd>
 * </dl>
 *
 * <dl>
 *   <a name="http-bind-address"><dt>{@linkplain #HTTP_BIND_ADDRESS_NAME http-bind-address}</dt></a>
 *   <dd><U>Description</U>: the machine name or IP address to which
 *   the HTTP listening socket should be bound.  If this value is
 *   "localhost", then the socket will be bound to the loopback
 *   address (127.0.0.1) and the adapter will only be accessible via
 *   the URL <code>http://localhost:8080</code>.</dd>
 *   <dd><U>{@linkplain #DEFAULT_HTTP_BIND_ADDRESS Default}</U>: ""
 *   (all network addresses)</dd>
 * </dl>
 *
 * <dl>
 *   <a name="http-authentication-enabled"><dt>{@linkplain #HTTP_AUTHENTICATION_ENABLED_NAME http-authentication-enabled}</dt></a>
 *   <dd><U>Description</U>: Whether or not connections to the HTTP
 *   adapter should be authenticated with a user name and password.
 *   </dd>
 *   <dd><U>{@linkplain #DEFAULT_HTTP_AUTHENTICATION_ENABLED Default}</U>: false</dd>
 * </dl>
 *
 * <dl>
 *   <a name="http-authentication-user"><dt>{@linkplain #HTTP_AUTHENTICATION_USER_NAME http-authentication-user}</dt></a>
 *   <dd><U>Description</U>: the user name for authenticating secure
 *   communication. 
 *   </dd>
 *   <dd><U>{@linkplain #DEFAULT_HTTP_AUTHENTICATION_USER Default}</U>: admin</dd>
 * </dl>
 *
 * <dl>
 *   <a name="http-authentication-password"><dt>{@linkplain #HTTP_AUTHENTICATION_PASSWORD_NAME http-authentication-password}</dt></a>
 *   <dd><U>Description</U>: the password for authenticating secure
 *   communication. 
 *   </dd>
 *   <dd><U>{@linkplain #DEFAULT_HTTP_AUTHENTICATION_PASSWORD Default}</U>: password</dd>
 * </dl>
 *
 * <B>RMI Adapter Configuration Properties</B>
 *
 * <dl>
 *   <a name="rmi-enabled"><dt>{@linkplain #RMI_ENABLED_NAME rmi-enabled}</dt></a>
 *   <dd><U>Description</U>: whether or not the RMI JMX adapter is enabled
 *   </dd>
 *   <dd><U>{@linkplain #DEFAULT_RMI_ENABLED Default}</U>: true</dd>
 * </dl>
 *
 * <dl>
 *   <a name="rmi-registry-enabled"><dt>{@linkplain #RMI_REGISTRY_ENABLED_NAME rmi-registry-enabled}</dt></a>
 *   <dd><U>Description</U>: whether or not the JMX agent should start
 *   an RMI registry.  Alternatively, a registry outside of the JMX
 *   agent VM can be used.
 *   </dd>
 *   <dd><U>{@linkplain #DEFAULT_RMI_REGISTRY_ENABLED Default}</U>: true</dd>
 * </dl>
 *
 * <dl>
 *   <a name="rmi-port"><dt>{@linkplain #RMI_PORT_NAME rmi-port}</dt></a>
 *   <dd><U>Description</U>: the port of the RMI registry in which the
 *   JMX Agent should bind remote objects.
 *   </dd>
 *   <dd><U>{@linkplain #DEFAULT_RMI_PORT Default}</U>: 1099</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="rmi-server-port"><dt>{@linkplain #RMI_PORT_NAME rmi-server-port}</dt></a>
 *   <dd><U>Description</U>: the port to be used by the RMI Server started by
 *   JMX Agent.
 *   </dd>
 *   <dd><U>{@linkplain #DEFAULT_RMI_SERVER_PORT Default}</U>: 0</dd>
 * </dl> 
 *
 * <dl>
 *   <a name="rmi-bind-address"><dt>{@linkplain #RMI_BIND_ADDRESS_NAME rmi-bind-address}</dt></a>
 *   <dd><U>Description</U>: the bind address on which the RMI
 *   registry binds its sockets.
 *   </dd>
 *   <dd><U>{@linkplain #DEFAULT_RMI_BIND_ADDRESS Default}</U>: ""
 *   (all network addresses)</dd>
 * </dl>
 *
 * <B>AdventNet SNMP Adapter Configuration Properties</B>
 *
 * <dl>
 *   <a name="snmp-enabled"><dt>{@linkplain #SNMP_ENABLED_NAME snmp-enabled}</dt></a>
 *   <dd><U>Description</U>: whether or not the SNMP JMX adapter is
 *   enabled 
 *   </dd>
 *   <dd><U>{@linkplain #DEFAULT_SNMP_ENABLED Default}</U>: false</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="snmp-bind-address"><dt>{@linkplain #SNMP_BIND_ADDRESS_NAME snmp-bind-address}</dt></a>
 *   <dd><U>Description</U>: the host name to which sockets used by the
 *   SNMP adapter should be bound.
 *   </dd>
 *   <dd><U>{@linkplain #DEFAULT_SNMP_BIND_ADDRESS Default}</U>: the name of
 *   the local machine (not <code>localhost</code>)</dd>
 * </dl>
 *
 * <dl>
 *   <a name="snmp-directory"><dt>{@linkplain #SNMP_DIRECTORY_NAME snmp-directory}</dt></a>
 *   <dd><U>Description</U>: the deployment directory for AdventNet
 *   SNMP Adaptor 
 *   </dd>
 *   <dd><U>{@linkplain #DEFAULT_SNMP_DIRECTORY Default}</U>: ""</dd>
 * </dl>
 * 
 * <B>JMX Agent Email Notification Properties (for statistics alerts)</B>
 * 
 * <dl>
 *   <a name="email-notification-enabled"><dt>{@linkplain #EMAIL_NOTIFICATIONS_ENABLED_NAME email-notification-enabled}</dt></a>
 *   <dd><U>Description</U>: Whether or not email notifications are enabled for statistics alerts.
 *   </dd>
 *   <dd><U>{@linkplain #DEFAULT_EMAIL_NOTIFICATIONS_ENABLED Default}</U>: false</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="email-notification-from"><dt>{@linkplain #EMAIL_NOTIFICATIONS_FROM_NAME email-notification-from}</dt></a>
 *   <dd><U>Description</U>: Email address to be used to send email notifications.
 *   </dd>
 *   <dd><U>{@linkplain #DEFAULT_EMAIL_FROM Default}</U>: ""</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="email-notification-host"><dt>{@linkplain #EMAIL_NOTIFICATIONS_HOST_NAME email-notification-host}</dt></a>
 *   <dd><U>Description</U>: The host name of the mail server to be used for email communication. 
 *   </dd>
 *   <dd><U>{@linkplain #DEFAULT_EMAIL_HOST Default}</U>: ""</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="email-notification-to"><dt>{@linkplain #EMAIL_NOTIFICATIONS_TO_LIST_NAME email-notification-to}</dt></a>
 *   <dd><U>Description</U>: Email address where the email notifications should be sent.
 *   </dd>
 *   <dd><U>{@linkplain #DEFAULT_EMAIL_TO_LIST Default}</U>: ""</dd>
 * </dl>
 * 
 * <dl>
 *   <a name="state-save-file"><dt>{@linkplain #STATE_SAVE_FILE_NAME state-save-file}</dt></a>
 *   <dd><U>Description</U>: The name of the file to be used for saving agent state. The file
         is stored in the same directory in which the agent.properties file is located
 *   </dd>
 *   <dd><U>{@linkplain #DEFAULT_STATE_SAVE_FILE Default}</U>: ""</dd>
 * </dl>
 * 
 *
 * @since GemFire 4.0
 * @deprecated as of 7.0 use the <code><a href="{@docRoot}/com/gemstone/gemfire/management/package-summary.html">management</a></code> package instead
 */
public interface AgentConfig extends DistributedSystemConfig {

  /** The prefix for JMX Agent configuration system properties */
  public static final String SYSTEM_PROPERTY_PREFIX = DistributionConfig.GEMFIRE_PREFIX + "agent.";

  /** The default "propertyFile" value */
  public static final String DEFAULT_PROPERTY_FILE = "agent.properties";

  /** The default name for file that has "agent state saved serialized" */
  public static final String DEFAULT_STATE_SAVE_FILE = "agent.ser";
  
  /** The name of the "auto-connect" property */
  public static final String AUTO_CONNECT_NAME = "auto-connect";

  /** The default value of the "auto-connect" property  */
  public static final boolean DEFAULT_AUTO_CONNECT = true;

  // -------------------------------------------------------------------------
  //   HttpAdaptor properties...
  // -------------------------------------------------------------------------
  
  /** The name of the "httpEnabled" property */
  public static final String HTTP_ENABLED_NAME = "http-enabled";

  /** The default value of the "httpEnabled" property  */
  public static final boolean DEFAULT_HTTP_ENABLED = true;
  
  /** The name of the "httpBindAddress" property */
  public static final String HTTP_BIND_ADDRESS_NAME = "http-bind-address";

  /** The default value of the "httpBindAddress" property */
  public static final String DEFAULT_HTTP_BIND_ADDRESS = "";
  
  /** The name of the "httpPort" property */
  public static final String HTTP_PORT_NAME = "http-port";

  /** The default value of the "httpPort" property (8080) */
  public static final int DEFAULT_HTTP_PORT = 8080;

  /** The minimum httpPort (0) */
  public static final int MIN_HTTP_PORT = 0;

  /** The maximum httpPort (65535) */
  public static final int MAX_HTTP_PORT = 65535;

  /** The name of the "state-save-file-name" property */
  public static final String STATE_SAVE_FILE_NAME =
    "state-save-file";

  /** The name of the "http-authentication-enabled" property */
  public static final String HTTP_AUTHENTICATION_ENABLED_NAME =
    "http-authentication-enabled";

  /** The default value of the "http-authentication-enabled" property
   *  */ 
  public static final boolean DEFAULT_HTTP_AUTHENTICATION_ENABLED = false;

  /** The name of the "http-authentication-user" property */
  public static final String HTTP_AUTHENTICATION_USER_NAME =
    "http-authentication-user";

  /** The default value of the "http-authentication-user" property  */
  public static final String DEFAULT_HTTP_AUTHENTICATION_USER = "admin";

  /** The name of the "http-authentication-password" property */
  public static final String HTTP_AUTHENTICATION_PASSWORD_NAME =
    "http-authentication-password";

  /** The default value of the "http-authentication-password" property
   *  */
  public static final String DEFAULT_HTTP_AUTHENTICATION_PASSWORD =
    "password";

  /** The name of the "email-notification-enabled" property */
  public static final String EMAIL_NOTIFICATIONS_ENABLED_NAME =
    "email-notification-enabled";

  /** The default value of the "email-notification-enabled" property
   *  */ 
  public static final boolean DEFAULT_EMAIL_NOTIFICATIONS_ENABLED = false;

  /** The name of the "email-notification-from" property */
  public static final String EMAIL_NOTIFICATIONS_FROM_NAME =
    "email-notification-from";

  /** The default value of the "email-notification-from" property
   *  */ 
  public static final String DEFAULT_EMAIL_FROM = "";

  /** The name of the "email-notification-host" property */
  public static final String EMAIL_NOTIFICATIONS_HOST_NAME =
    "email-notification-host";

  /** The default value of the "email-notification-host" property
   *  */ 
  public static final String DEFAULT_EMAIL_HOST = "";

  /** The name of the "email-notification-to" property */
  public static final String EMAIL_NOTIFICATIONS_TO_LIST_NAME =
    "email-notification-to";

  /** The default value of the "email-notification-to" property
   *  */ 
  public static final String DEFAULT_EMAIL_TO_LIST = "";

  // -------------------------------------------------------------------------
  //   RMIConnectorServer properties...
  // -------------------------------------------------------------------------
  
  /** The name of the "rmiEnabled" property */
  public static final String RMI_ENABLED_NAME = "rmi-enabled";

  /** The default value of the {@linkplain #RMI_ENABLED_NAME rmi-enabled} property  */
  public static final boolean DEFAULT_RMI_ENABLED = true;
  
  /** The name of the "rmi-registry-enabled" property */
  public static final String RMI_REGISTRY_ENABLED_NAME =
    "rmi-registry-enabled"; 

  /** The default value of the {@linkplain #RMI_REGISTRY_ENABLED_NAME rmi-registry-enabled} property*/
  public static final boolean DEFAULT_RMI_REGISTRY_ENABLED = true;
  
  /** The name of the "rmiBindAddress" property */
  public static final String RMI_BIND_ADDRESS_NAME = "rmi-bind-address";

  /** The default value of the {@linkplain #RMI_BIND_ADDRESS_NAME rmi-bind-address} property  */
  public static final String DEFAULT_RMI_BIND_ADDRESS = "";

  /** The name of the "rmiPort" property */
  public static final String RMI_PORT_NAME = "rmi-port";

  /** The default value of the {@linkplain #RMI_PORT_NAME rmi-port} property (1099) */
  public static final int DEFAULT_RMI_PORT = 1099;
  
  /** 
   * The name of the "rmi-server-port" property
   * 
   * @since GemFire 6.5
   */
  public static final String RMI_SERVER_PORT_NAME = "rmi-server-port";

  /** 
   * The default value of the {@linkplain #RMI_SERVER_PORT_NAME rmi-server-port} 
   * property (0)
   * 
   * @since GemFire 6.5
   */
  public static final int DEFAULT_RMI_SERVER_PORT = 0;

  /**
   * The minimum value for {@linkplain #RMI_PORT_NAME rmi-port} or
   * {@linkplain #RMI_SERVER_PORT_NAME rmi-server-port} (0)
   */
  public static final int MIN_RMI_PORT = 0;

  /**
   * The maximum value for {@linkplain #RMI_PORT_NAME rmi-port} or
   * {@linkplain #RMI_SERVER_PORT_NAME rmi-server-port} (65535)
   */
  public static final int MAX_RMI_PORT = 65535;
  
  // -------------------------------------------------------------------------
  //   AdventNetSNMPAdaptor properties...
  // -------------------------------------------------------------------------
  
  /** The name of the "snmpEnabled" property */
  public static final String SNMP_ENABLED_NAME = "snmp-enabled";

  /** The default value of the "snmpEnabled" property  */
  public static final boolean DEFAULT_SNMP_ENABLED = false;
  
  /** The name of the "snmpBindAddress" property */
  public static final String SNMP_BIND_ADDRESS_NAME = "snmp-bind-address";

  /** The default value of the "snmpBindAddress" property */
  public static final String DEFAULT_SNMP_BIND_ADDRESS = "";

  /** The name of the "snmpDirectory" property */
  public static final String SNMP_DIRECTORY_NAME = "snmp-directory";

  /** The default value of the "snmpDirectory" property  */
  public static final String DEFAULT_SNMP_DIRECTORY = "";

  // -------------------------------------------------------------------------
  //   JMX SSL properties...
  // -------------------------------------------------------------------------

  /** The name of the "agent-ssl-enabled" property */
  public static final String AGENT_SSL_ENABLED_NAME = "agent-ssl-enabled";

  /** The default value of the "agent-ssl-enabled" property  */
  public static final boolean DEFAULT_AGENT_SSL_ENABLED = false;

  /** The name of the "agent-ssl-protocols" property */
  public static final String AGENT_SSL_PROTOCOLS_NAME = "agent-ssl-protocols";

   /** The default value of the "agent-ssl-protocols" property  */
  public static final String DEFAULT_AGENT_SSL_PROTOCOLS = "any";

  /** The name of the "agent-ssl-ciphers" property */
  public static final String AGENT_SSL_CIPHERS_NAME = "agent-ssl-ciphers";

   /** The default value of the "agent-ssl-ciphers" property  */
  public static final String DEFAULT_AGENT_SSL_CIPHERS = "any";

  /** The name of the "agent-ssl-require-authentication" property */
  public static final String AGENT_SSL_REQUIRE_AUTHENTICATION_NAME =
    "agent-ssl-require-authentication";

   /** The default value of the "agent-ssl-require-authentication"
    * property  */
  public static final boolean DEFAULT_AGENT_SSL_REQUIRE_AUTHENTICATION = true;
    
  /** The name of the "http-ssl-require-authentication" property */
  public static final String HTTP_SSL_REQUIRE_AUTHENTICATION_NAME =
    "http-ssl-require-authentication";

   /** The default value of the "http-ssl-require-authentication"
    * property  */
  public static final boolean DEFAULT_HTTP_SSL_REQUIRE_AUTHENTICATION = false;

  //////////////////////  Instance Methods  //////////////////////

  /**
   * Returns whether or not the JMX agent will automatically connect
   * to the distributed system it administers.
   *
   * See <a href="#auto-connect">description</a> above.
   */
  public boolean getAutoConnect();

  /**
   * Sets whether or not the JMX agent will automatically connect
   * to the distributed system it administers.
   *
   * See <a href="#auto-connect">description</a> above.
   */
  public void setAutoConnect(boolean autoConnect);

  /**
   * Returns whether or not the HTTP adapter is enabled.
   *
   * See <a href="#http-enabled">description</a> above.
   */
  public boolean isHttpEnabled();

  /**
   * Sets whether or not the HTTP adapter is enabled.
   *
   * See <a href="#http-enabled">description</a> above.
   */
  public void setHttpEnabled(boolean httpEnabled);

  /**
   * Returns the port of the HTTP adapter.
   *
   * See <a href="#http-port">description</a> above.
   */
  public int getHttpPort();

  /**
   * Sets the port of the HTTP adapter.
   *
   * See <a href="#http-port">description</a> above.
   */
  public void setHttpPort(int port);

  /**
   * Returns the bind address to which the HTTP adapter's listening
   * socket is bound.
   *
   * See <a href="#http-bind-address">description</a> above.
   */
  public String getHttpBindAddress();

  /**
   * Sets the bind address to which the HTTP adapter's listening
   * socket is bound.
   *
   * See <a href="#http-bind-address">description</a> above.
   */
  public void setHttpBindAddress(String address);

  /**
   * Returns whether or not the HTTP adapter authenticates
   * connections.
   *
   * See <a href="#http-authentication-enabled">description</a>
   * above.
   */ 
  public boolean isHttpAuthEnabled();

  /**
   * Sets whether or not the HTTP adapter authenticates connections.
   *
   * See <a href="#http-authentication-enabled">description</a>
   * above.
   */
  public void setHttpAuthEnabled(boolean enabled);

  /**
   * Returns the user name for HTTP adapter authentication.
   *
   * See <a href="#http-authentication-user">description</a>
   * above.
   */
  public String getHttpAuthUser();
  
  /**
   * Sets the user name for HTTP adapter authentication.
   *
   * See <a href="#http-authentication-user">description</a>
   * above.
   */
  public void setHttpAuthUser(String user);

  /**
   * Returns the password for HTTP adapter authentication.
   *
   * See <a href="#http-authentication-password">description</a>
   * above.
   */
  public String getHttpAuthPassword();
  
  /**
   * Sets the password for HTTP adapter authentication.
   *
   * See <a href="#http-authentication-password">description</a>
   * above.
   */
  public void setHttpAuthPassword(String password);

  /**
   * Returns whether or not the RMI adapter is enabled.
   *
   * See <a href="#rmi-enabled">description</a> above.
   */
  public boolean isRmiEnabled();

  /**
   * Sets whether or not the RMI adapter is enabled.
   *
   * See <a href="#rmi-enabled">description</a> above.
   */
  public void setRmiEnabled(boolean rmiEnabled);

  /**
   * Returns whether or not the agent hosts an RMI registry.
   *
   * See <a href="#rmi-registry-enabled">description</a> above.
   */
  public boolean isRmiRegistryEnabled();

  /**
   * Sets whether or not the agent hosts an RMI registry.
   *
   * See <a href="#rmi-registry-enabled">description</a> above.
   */
  public void setRmiRegistryEnabled(boolean enabled);

  /**
   * Returns the port of the RMI adapter.
   *
   * See <a href="#rmi-port">description</a> above.
   */
  public int getRmiPort();

  /**
   * Sets the port of the RMI adapter.
   *
   * See <a href="#rmi-port">description</a> above.
   */
  public void setRmiPort(int port);
  
  /**
   * Returns the port of the RMI Connector Server.
   *
   * See <a href="#rmi-server-port">description</a> above.
   * 
   * @return the value set for rmi-server-port
   * @since GemFire 6.5
   */
  public int getRmiServerPort();

  /**
   * Sets the port of the RMI Connector Server.
   *
   * See <a href="#rmi-server-port">description</a> above.
   * 
   * @param port rmi-server-port to set. 
   * @since GemFire 6.5
   */
  public void setRmiServerPort(int port);

  /**
   * Returns the bind address to which the RMI adapter's listening
   * sockets are bound.
   *
   * See <a href="#rmi-bind-address">description</a> above.
   */
  public String getRmiBindAddress();

  /**
   * Sets the bind address to which the RMI adapter's listening
   * sockets are bound.
   *
   * See <a href="#rmi-bind-address">description</a> above.
   */
  public void setRmiBindAddress(String address);

  /**
   * Returns whether or not the SNMP adapter is enabled.
   *
   * See <a href="#snmp-enabled">description</a> above.
   */
  public boolean isSnmpEnabled();

  /**
   * Sets whether or not the SNMP adapter is enabled.
   *
   * See <a href="#snmp-enabled">description</a> above.
   */
  public void setSnmpEnabled(boolean enabled);

  /**
   * Returns the bind address used with the SNMP adapter.
   *
   * See <a href="#snmp-bind-address">description</a> above.
   */
  public String getSnmpBindAddress();

  /**
   * Sets the bind address used with the SNMP adapter.
   *
   * See <a href="#snmp-bind-address">description</a> above.
   */
  public void setSnmpBindAddress(String address);

  /**
   * Returns the directory for the SNMP adapater.
   *
   * See <a href="#snmp-directory">description</a> above.
   */
  public String getSnmpDirectory();

  /**
   * Sets the directory for the SNMP adapater.
   *
   * See <a href="#snmp-directory">description</a> above.
   */
  public void setSnmpDirectory(String snmpDirectory);
  
  /**
   * Returns whether or not SSL is required for the JMX agent.
   *
   * See <a href="#agent-ssl-enabled">description</a> above.
   */
  public boolean isAgentSSLEnabled();

  /**
   * Sets whether or not SSL is required for the JMX agent.
   *
   * See <a href="#agent-ssl-enabled">description</a> above.
   */
  public void setAgentSSLEnabled(boolean enabled);

  /**
   * Returns the SSL protocols used when connecting to the JMX agent.
   *
   * See <a href="#agent-ssl-protocols">description</a> above.
   */
  public String getAgentSSLProtocols();

  /**
   * Sets the SSL protocols used when connecting to the JMX agent.
   *
   * See <a href="#agent-ssl-protocols">description</a> above.
   */
  public void setAgentSSLProtocols(String protocols);

  /**
   * Returns the SSL ciphers used when connecting to the JMX agent.
   *
   * See <a href="#agent-ssl-ciphers">description</a> above.
   */
  public String getAgentSSLCiphers();

  /**
   * Sets the SSL ciphers used when connecting to the JMX agent.
   *
   * See <a href="#agent-ssl-ciphers">description</a> above.
   */
  public void setAgentSSLCiphers(String ciphers);

  /**
   * Returns whether SSL authentication is used when connecting to the
   * RMI connector.
   *
   * See <a href="#agent-ssl-require-authentication">description</a> above.
   */
  public boolean isAgentSSLRequireAuth();
  
  /**
   * Sets whether SSL authentication is used when connecting to the
   * RMI connector.
   *
   * See <a href="#agent-ssl-require-authentication">description</a> above.
   */
  public void setAgentSSLRequireAuth(boolean require);

  /**
   * Returns whether SSL authentication is used when connecting to the
   * HTTP connector.
   *
   * See <a href="#http-ssl-require-authentication">description</a> above.
   */
  public boolean isHttpSSLRequireAuth();
  
  /**
   * Sets whether SSL authentication is used when connecting to the
   * HTTP connector.
   *
   * See <a href="#http-ssl-require-authentication">description</a> above.
   */
  public void setHttpSSLRequireAuth(boolean require);

  /**
   * Returns whether Emails for Notifications is enabled
   *
   * See <a href="#email-notification-enabled">description</a> above.
   */
  public boolean isEmailNotificationEnabled();

  /**
   * Sets whether Emails for Notifications is enabled
   *
   * See <a href="#email-notification-enabled">description</a> above.
   */
  public void setEmailNotificationEnabled(boolean enabled);

  /**
   * Returns the EmailID from whom notification emails are sent.
   *
   * See <a href="#email-notification-from">description</a> above.
   */
  public String getEmailNotificationFrom();
  
  /**
   * Sets the EmailID from whom notification emails are sent.
   *
   * See <a href="#email-notification-from">description</a> above.
   */
  public void setEmailNotificationFrom(String emailID);

  /**
   * Returns the Host Name using which notification emails are sent.
   *
   * See <a href="#email-notification-host">description</a> above.
   */
  public String getEmailNotificationHost();
  
  /**
   * Sets the Host Name from whom notification emails are sent.
   *
   * See <a href="#email-notification-host">description</a> above.
   */
  public void setEmailNotificationHost(String hostName);

  /**
   * Returns the comma separated EmailID list to whom notification
   * emails are sent.
   *
   * See <a href="#email-notification-to">description</a> above.
   */
  public String getEmailNotificationToList();
  
  /**
   * Sets the EmailID from whom notification emails are sent as a
   * comma separated list.
   *
   * See <a href="#email-notification-to">description</a> above.
   */
  public void setEmailNotificationToList(String emailIDs);
  
  /**
   * Returns the name of the file to be used for saving agent state
   *
   * See <a href="#state-save-file">description</a> above.
   */
  public String getStateSaveFile();

  /**
   * Sets the name of the file to be used for saving agent state
   *
   * See <a href="#state-save-file">description</a> above.
   */
  public void setStateSaveFile(String file);

  /**
   * Returns an <code>AgentConfig</code> with the same configuration
   * as this <code>AgentConfig</code>.
   */
  public Object clone() throws CloneNotSupportedException; 

}

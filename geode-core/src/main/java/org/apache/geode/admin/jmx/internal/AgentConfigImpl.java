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
package org.apache.geode.admin.jmx.internal;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.distributed.internal.DistributionConfig.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.URL;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.geode.GemFireIOException;
import org.apache.geode.admin.DistributedSystemConfig;
import org.apache.geode.admin.DistributionLocatorConfig;
import org.apache.geode.admin.internal.DistributedSystemConfigImpl;
import org.apache.geode.admin.internal.InetAddressUtil;
import org.apache.geode.admin.jmx.Agent;
import org.apache.geode.admin.jmx.AgentConfig;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.util.IOUtils;

/**
 * Provides the JMX Agent configuration properties.
 * <p>
 * Supports importing of persisted properties from an external configuration file.
 * <p>
 * Select values can also be overridden with command line arguments. See remarks on individual
 * properties for further information.
 * <p>
 * Extends and implements DistributedSystemConfig.
 * 
 * @since GemFire 3.5 (in which it was named AgentConfig)
 */
public class AgentConfigImpl extends DistributedSystemConfigImpl implements AgentConfig {

  // -------------------------------------------------------------------------
  // Static class variable(s)
  // -------------------------------------------------------------------------

  /**
   * Command-line arg to enable agent debugging
   */
  public static final String AGENT_DEBUG = "agent-debug";

  /**
   * The name of the "propertyFile" property. May specify as cmd-line arg
   */
  public static final String PROPERTY_FILE_NAME = "property-file";

  /**
   * The name of the "gfAgentPropertyFile" property, can be specified as System Property
   */
  public static final String AGENT_PROPSFILE_PROPERTY_NAME = "gfAgentPropertyFile";

  // -------------------------------------------------------------------------
  // DistributionLocator properties...
  // -------------------------------------------------------------------------

  /**
   * The name of the "locator.host-" property
   */
  public static final String LOCATOR_HOST_NAME = "locator.host-";
  /**
   * The name of the "locator.port-" property
   */
  public static final String LOCATOR_PORT_NAME = "locator.port-";
  /**
   * The name of the "locator.product-directory-" property
   */
  public static final String LOCATOR_PRODUCT_DIRECTORY_NAME = "locator.product-directory-";
  /**
   * The name of the "locator.working-directory-" property
   */
  public static final String LOCATOR_WORKING_DIRECTORY_NAME = "locator.working-directory-";
  /**
   * The name of the "locator.remote-command-" property
   */
  public static final String LOCATOR_REMOTE_COMMAND = "locator.remote-command-";
  /**
   * The name of the "locator.bind-address-" property
   */
  public static final String LOCATOR_BIND_ADDRESS = "locator.bind-address-";
  /**
   * the properties used in configuring a locator's distributed system
   */
  public static final String LOCATOR_DS_PROPERTIES = "locator.ds-properties";

  /**
   * The default log file for stand-alone JMX agents
   */
  /* package scope */
  static final String DEFAULT_LOG_FILE = "agent.log";

  /**
   * The default startup log file to be used by agent launcher
   */
  /* package scope */
  static final String DEFAULT_STARTUP_LOG_FILE = "start_agent.log";

  private static String OBFUSCATED_STRING = "********";

  ////////////////////// Static Methods //////////////////////

  /**
   * The <code>propertyFile</code> is the name of the property file that will be loaded on startup
   * of the Agent.
   * <p>
   * The file will be searched for, in order, in the following directories:
   * <ol>
   * <li>the current directory
   * <li>the home directory
   * <li>the class path
   * </ol>
   * Only the first file found will be used.
   * <p>
   * The default value of propertyFile is <code>"agent.properties"</code>. However if the
   * "gfAgentPropertyFile" system property is set then its value is the value of propertyFile. If
   * this value is a relative file system path then the above search is done. If its an absolute
   * file system path then that file must exist; no search for it is done.
   */
  static String retrievePropertyFile() {
    return System.getProperty(AGENT_PROPSFILE_PROPERTY_NAME, DEFAULT_PROPERTY_FILE);
  }

  /**
   * Creates a new <code>Properties</code> object that contains all of the default values.
   */
  private static Properties getDefaultProperties() {
    Properties props = new Properties();

    props.setProperty(AUTO_CONNECT_NAME, String.valueOf(DEFAULT_AUTO_CONNECT));

    props.setProperty(HTTP_ENABLED_NAME, String.valueOf(DEFAULT_HTTP_ENABLED));
    props.setProperty(HTTP_BIND_ADDRESS_NAME, String.valueOf(DEFAULT_HTTP_BIND_ADDRESS));
    props.setProperty(HTTP_PORT_NAME, String.valueOf(DEFAULT_HTTP_PORT));
    props.setProperty(HTTP_AUTHENTICATION_ENABLED_NAME,
        String.valueOf(DEFAULT_HTTP_AUTHENTICATION_ENABLED));
    props.setProperty(HTTP_AUTHENTICATION_USER_NAME,
        String.valueOf(DEFAULT_HTTP_AUTHENTICATION_USER));
    props.setProperty(HTTP_AUTHENTICATION_PASSWORD_NAME,
        String.valueOf(DEFAULT_HTTP_AUTHENTICATION_PASSWORD));

    props.setProperty(RMI_ENABLED_NAME, String.valueOf(DEFAULT_RMI_ENABLED));
    props.setProperty(RMI_REGISTRY_ENABLED_NAME, String.valueOf(DEFAULT_RMI_REGISTRY_ENABLED));
    props.setProperty(RMI_BIND_ADDRESS_NAME, String.valueOf(DEFAULT_RMI_BIND_ADDRESS));
    props.setProperty(RMI_PORT_NAME, String.valueOf(DEFAULT_RMI_PORT));
    props.setProperty(RMI_SERVER_PORT_NAME, String.valueOf(DEFAULT_RMI_SERVER_PORT));

    props.setProperty(SNMP_ENABLED_NAME, String.valueOf(DEFAULT_SNMP_ENABLED));
    props.setProperty(SNMP_DIRECTORY_NAME, String.valueOf(DEFAULT_SNMP_DIRECTORY));

    props.setProperty(AGENT_SSL_ENABLED_NAME, String.valueOf(DEFAULT_AGENT_SSL_ENABLED));
    props.setProperty(AGENT_SSL_PROTOCOLS_NAME, String.valueOf(DEFAULT_AGENT_SSL_PROTOCOLS));
    props.setProperty(AGENT_SSL_CIPHERS_NAME, String.valueOf(DEFAULT_AGENT_SSL_CIPHERS));
    props.setProperty(AGENT_SSL_REQUIRE_AUTHENTICATION_NAME,
        String.valueOf(DEFAULT_AGENT_SSL_REQUIRE_AUTHENTICATION));
    props.setProperty(HTTP_SSL_REQUIRE_AUTHENTICATION_NAME,
        String.valueOf(DEFAULT_HTTP_SSL_REQUIRE_AUTHENTICATION));

    return props;
  }

  /**
   * Returns default values for all valid agent properties as a Properties object.
   * 
   * @return default values for all valid agent properties
   */
  static Properties getDefaultValuesForAllProperties() {
    Properties props = new Properties();

    props.setProperty(AUTO_CONNECT_NAME, String.valueOf(DEFAULT_AUTO_CONNECT));

    props.setProperty(HTTP_ENABLED_NAME, String.valueOf(DEFAULT_HTTP_ENABLED));

    props.setProperty(HTTP_BIND_ADDRESS_NAME, String.valueOf(DEFAULT_HTTP_BIND_ADDRESS));

    props.setProperty(HTTP_PORT_NAME, String.valueOf(DEFAULT_HTTP_PORT));

    props.setProperty(HTTP_AUTHENTICATION_ENABLED_NAME,
        String.valueOf(DEFAULT_HTTP_AUTHENTICATION_ENABLED));

    props.setProperty(HTTP_AUTHENTICATION_USER_NAME,
        String.valueOf(DEFAULT_HTTP_AUTHENTICATION_USER));

    props.setProperty(HTTP_AUTHENTICATION_PASSWORD_NAME,
        String.valueOf(DEFAULT_HTTP_AUTHENTICATION_PASSWORD));

    props.setProperty(RMI_ENABLED_NAME, String.valueOf(DEFAULT_RMI_ENABLED));

    props.setProperty(RMI_REGISTRY_ENABLED_NAME, String.valueOf(DEFAULT_RMI_REGISTRY_ENABLED));

    props.setProperty(RMI_BIND_ADDRESS_NAME, String.valueOf(DEFAULT_RMI_BIND_ADDRESS));

    props.setProperty(RMI_PORT_NAME, String.valueOf(DEFAULT_RMI_PORT));

    props.setProperty(RMI_SERVER_PORT_NAME, String.valueOf(DEFAULT_RMI_SERVER_PORT));

    props.setProperty(SNMP_ENABLED_NAME, String.valueOf(DEFAULT_SNMP_ENABLED));

    props.setProperty(SNMP_DIRECTORY_NAME, String.valueOf(DEFAULT_SNMP_DIRECTORY));

    props.setProperty(AGENT_SSL_ENABLED_NAME, String.valueOf(DEFAULT_AGENT_SSL_ENABLED));

    props.setProperty(AGENT_SSL_PROTOCOLS_NAME, String.valueOf(DEFAULT_AGENT_SSL_PROTOCOLS));

    props.setProperty(AGENT_SSL_CIPHERS_NAME, String.valueOf(DEFAULT_AGENT_SSL_CIPHERS));

    props.setProperty(AGENT_SSL_REQUIRE_AUTHENTICATION_NAME,
        String.valueOf(DEFAULT_AGENT_SSL_REQUIRE_AUTHENTICATION));

    props.setProperty(HTTP_SSL_REQUIRE_AUTHENTICATION_NAME,
        String.valueOf(DEFAULT_HTTP_SSL_REQUIRE_AUTHENTICATION));

    props.setProperty(SNMP_BIND_ADDRESS_NAME, String.valueOf(DEFAULT_SNMP_BIND_ADDRESS));

    props.setProperty(EMAIL_NOTIFICATIONS_ENABLED_NAME,
        String.valueOf(DEFAULT_EMAIL_NOTIFICATIONS_ENABLED));

    props.setProperty(EMAIL_NOTIFICATIONS_HOST_NAME, String.valueOf(DEFAULT_EMAIL_HOST));

    props.setProperty(EMAIL_NOTIFICATIONS_FROM_NAME, String.valueOf(DEFAULT_EMAIL_FROM));

    props.setProperty(EMAIL_NOTIFICATIONS_TO_LIST_NAME, String.valueOf(DEFAULT_EMAIL_TO_LIST));

    props.setProperty(STATE_SAVE_FILE_NAME, String.valueOf(DEFAULT_STATE_SAVE_FILE));

    props.setProperty(CLUSTER_SSL_ENABLED, String.valueOf(DEFAULT_SSL_ENABLED));

    props.setProperty(CLUSTER_SSL_PROTOCOLS, String.valueOf(DEFAULT_SSL_PROTOCOLS));

    props.setProperty(CLUSTER_SSL_CIPHERS, String.valueOf(DEFAULT_SSL_CIPHERS));

    props.setProperty(CLUSTER_SSL_REQUIRE_AUTHENTICATION,
        String.valueOf(DEFAULT_SSL_REQUIRE_AUTHENTICATION));

    props.setProperty(ENTITY_CONFIG_XML_FILE_NAME, String.valueOf(DEFAULT_ENTITY_CONFIG_XML_FILE));

    props.setProperty(MCAST_PORT, String.valueOf(DEFAULT_MCAST_PORT));

    props.setProperty(MCAST_ADDRESS, String.valueOf(DEFAULT_MCAST_ADDRESS));

    props.setProperty(LOCATORS, String.valueOf(DEFAULT_LOCATORS));

    props.setProperty(BIND_ADDRESS, String.valueOf(DEFAULT_BIND_ADDRESS));

    props.setProperty(REMOTE_COMMAND_NAME, String.valueOf(DEFAULT_REMOTE_COMMAND));

    props.setProperty(LOG_FILE_NAME, String.valueOf(DEFAULT_LOG_FILE));

    props.setProperty(LOG_LEVEL_NAME, String.valueOf(DEFAULT_LOG_LEVEL));

    props.setProperty(LOG_DISK_SPACE_LIMIT_NAME, String.valueOf(DEFAULT_LOG_DISK_SPACE_LIMIT));

    props.setProperty(LOG_FILE_SIZE_LIMIT_NAME, String.valueOf(DEFAULT_LOG_FILE_SIZE_LIMIT));

    props.setProperty(REFRESH_INTERVAL_NAME, String.valueOf(DEFAULT_REFRESH_INTERVAL));

    return props;

  }

  // -------------------------------------------------------------------------
  // Member variable(s)
  // -------------------------------------------------------------------------

  /**
   * Does agent automatically connect to the distributed system?
   */
  private boolean autoConnect;

  /**
   * True if Agent adaptors should use SSL
   */
  private boolean agentSSLEnabled;

  /**
   * The SSL Protocols that the Agent adaptors will use
   */
  private String agentSSLProtocols;

  /**
   * The SSL Ciphers that the Agent adaptors will use
   */
  private String agentSSLCiphers;

  /**
   * True if Agent adaptors require authentication when SSL is enabled
   */
  private boolean agentSSLRequireAuth;

  /**
   * HttpAdaptor won't work with ssl authentication required, so this attribute allows this option
   * to be turned on for RMI but off for HTTP in the event that both adaptors are being used with
   * ssl.
   */
  private boolean httpSSLRequireAuth;

  /**
   * True if HttpAdaptor authentication is enabled
   */
  private boolean httpAuthEnabled;

  /**
   * The login user for HttpAdaptor authentication
   */
  private String httpAuthUser;

  /**
   * The login password for HttpAdaptor authentication
   */
  private String httpAuthPassword;

  /**
   * True if the HttpAdaptor is enabled
   */
  private boolean httpEnabled;

  /**
   * The port for the MX4J HttpAdatper
   */
  private int httpPort;

  /**
   * The host for the MX4J HttpAdatper
   */
  private String httpBindAddress;

  /**
   * True if the RMIConnectorServer is enabled
   */
  private boolean rmiEnabled;

  /**
   * True if the Agent is to create its own RMI registry
   */
  private boolean rmiRegistryEnabled;

  /**
   * The host for the MX4J RMIConnectorServer
   */
  private String rmiBindAddress;

  /**
   * The port for the RMI Registry created by the Agent
   */
  private int rmiPort;

  /**
   * The port for the MX4J RMIConnectorServer
   */
  private int rmiServerPort;

  /**
   * True if the SnmpAdaptor is enabled
   */
  private boolean snmpEnabled;

  /**
   * The bind address for sockets used by the SNMP adapter
   */
  private String snmpBindAddress;

  /**
   * Path to the directory containing the SNMP Adaptor and its sub-dirs
   */
  private String snmpDirectory;

  /**
   * Is Email notification enabled
   */
  private boolean isEmailNotificationEnabled;

  /**
   * Email notification from: emailID
   */
  private String emailNotificationFrom;

  /**
   * The host name of the mail server to be used for email communication.
   */
  private String emailNotificationHostName;

  /**
   * Email notification to: emailIDs list
   */
  private String emailNotificationToList;

  /**
   * State Save File Name
   */
  private String stateSaveFile;

  /**
   * Describes the property file used to load configuration from. Null if no file was found.
   */
  private URL url;

  /**
   * Original command line arguments
   */
  private String[] originalCmdLineArgs = null;

  /**
   * The <code>Agent</code> that is configured by this <code>AgentConfigImpl</code>
   */
  private Agent agent;

  // -------------------------------------------------------------------------
  // Constructor(s)
  // -------------------------------------------------------------------------

  /**
   * Constructs new instance of <code>AgentConfigImpl</code> with the default configuration.
   */
  public AgentConfigImpl() {
    this(getDefaultProperties());
  }

  /**
   * Constructs new instance of AgentConfig. Supplied command-line arguments are used to create a
   * set of non-default properties for initializing this AgentConfig.
   * 
   * @param args array of non-default configuration arguments
   */
  public AgentConfigImpl(String[] args) {
    this(toProperties(args));
    this.originalCmdLineArgs = args;
  }

  /**
   * Creates a new <code>AgentConfig</code> with the given non-default configuration properties.
   * 
   * @param props overriding non-default configuration properties
   */
  public AgentConfigImpl(Properties props) {
    // for admin bug #40434
    super(filterOutAgentProperties(appendOptionalPropertyFileProperties(props)),
        true/* ignore gemfire.properties */);

    // first get any property values set in the optional property file
    this.url = getPropertyFileURL(retrievePropertyFile());

    initialize(appendOptionalPropertyFileProperties(props));
  }

  /**
   * Constructs new instance of AgentConfig using the specified property file.
   * 
   * @param propFile the file to load configuration properties from
   */
  public AgentConfigImpl(File propFile) {
    // Initialize default values
    this();

    Properties props = new Properties();
    if (propFile.exists()) {
      try {
        FileInputStream in = new FileInputStream(propFile);
        props.load(in);
        in.close();
      } catch (java.io.IOException e) {
        throw new GemFireIOException(
            LocalizedStrings.AgentConfigImpl_FAILED_READING_0.toLocalizedString(propFile), e);
      }
    } else {
      throw new IllegalArgumentException(
          LocalizedStrings.AgentConfigImpl_SPECIFIED_PROPERTIES_FILE_DOES_NOT_EXIST_0
              .toLocalizedString(propFile));
    }

    initialize(props);
  }

  ////////////////////// Instance Methods //////////////////////

  /**
   * Sets the <code>Agent</code> associated with this <code>AgentConfigImpl</code>.
   */
  void setAgent(Agent agent) {
    this.agent = agent;
  }

  /**
   * Checks to see if this config object is "read only". If it is, then an
   * {@link IllegalStateException} is thrown.
   * 
   * @since GemFire 4.0
   */
  @Override
  protected void checkReadOnly() {
    if (this.agent != null) {
      throw new IllegalStateException(
          LocalizedStrings.AgentConfigImpl_AN_AGENTCONFIG_OBJECT_CANNOT_BE_MODIFIED_AFTER_IT_HAS_BEEN_USED_TO_CREATE_AN_AGENT
              .toLocalizedString());
    }

    super.checkReadOnly();
  }

  // -------------------------------------------------------------------------
  // Methods for handling Properties and the Properties file
  // -------------------------------------------------------------------------

  /**
   * Returns a description of the property file used to load this config. If no property file was
   * used then the description will say so.
   */
  public String getPropertyFileDescription() {
    /*
     * Checking if the specified or the default properties file exists. If not, just log this as an
     * information.
     */
    if (this.url == null) {
      return LocalizedStrings.AgentConfigImpl_USING_DEFAULT_CONFIGURATION_BECAUSE_PROPERTY_FILE_WAS_FOUND
          .toLocalizedString();
    } else {
      return LocalizedStrings.AgentConfigImpl_CONFIGURATION_LOADED_FROM_0
          .toLocalizedString(this.url);
    }
  }

  /**
   * Returns the default property file that will be used when configuration is saved.
   */
  public File getPropertyFile() {
    File f;
    if (this.url == null) {
      f = new File(retrievePropertyFile());
      if (!f.isAbsolute()) {
        // save to <cwd>/propertyFile
        f = new File(System.getProperty("user.dir"), retrievePropertyFile());
      }
    } else {
      f = new File(this.url.getFile());
    }
    return f;
  }

  /**
   * Converts the contents of this config to a property instance and Stringifies it
   * 
   * @return contents of this config as String
   */
  public String toPropertiesAsString() {
    Properties p = toProperties(true /* include DS properties */);

    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    pw.println(LocalizedStrings.AgentConfigImpl_AGENT_CONFIGURATION.toLocalizedString());
    Enumeration e = p.propertyNames();
    while (e.hasMoreElements()) {
      String pn = (String) e.nextElement();
      String pv = p.getProperty(pn);
      pw.println("  " + pn + " = " + pv);
    }
    pw.close();

    return sw.toString();
  }

  /**
   * Converts the contents of this config to a property instance.
   * 
   * @return contents of this config as java.util.Properties
   */
  public Properties toProperties() {
    return toProperties(false /* include DS properties */);
  }

  /**
   * Converts the contents of this config to a property instance.
   * 
   * @param includeDSProperties Should distributed system properties be included in the
   *        <code>Properties</code> object? See bug 32682.
   *
   * @return contents of this config as java.util.Properties
   */
  public Properties toProperties(boolean includeDSProperties) {
    Properties props = new Properties();

    props.setProperty(AUTO_CONNECT_NAME, toString(AUTO_CONNECT_NAME, getAutoConnect()));

    props.setProperty(HTTP_ENABLED_NAME, toString(HTTP_ENABLED_NAME, isHttpEnabled()));
    props.setProperty(HTTP_BIND_ADDRESS_NAME,
        toString(HTTP_BIND_ADDRESS_NAME, getHttpBindAddress()));
    props.setProperty(HTTP_PORT_NAME, toString(HTTP_PORT_NAME, getHttpPort()));

    props.setProperty(RMI_ENABLED_NAME, toString(RMI_ENABLED_NAME, isRmiEnabled()));
    props.setProperty(RMI_REGISTRY_ENABLED_NAME,
        toString(RMI_REGISTRY_ENABLED_NAME, isRmiRegistryEnabled()));
    props.setProperty(RMI_BIND_ADDRESS_NAME, toString(RMI_BIND_ADDRESS_NAME, getRmiBindAddress()));
    props.setProperty(RMI_PORT_NAME, toString(RMI_PORT_NAME, getRmiPort()));
    props.setProperty(RMI_SERVER_PORT_NAME, toString(RMI_SERVER_PORT_NAME, getRmiServerPort()));

    props.setProperty(SNMP_ENABLED_NAME, toString(SNMP_ENABLED_NAME, isSnmpEnabled()));
    props.setProperty(SNMP_BIND_ADDRESS_NAME,
        toString(SNMP_BIND_ADDRESS_NAME, getSnmpBindAddress()));
    props.setProperty(SNMP_DIRECTORY_NAME, toString(SNMP_DIRECTORY_NAME, getSnmpDirectory()));

    props.setProperty(AGENT_SSL_ENABLED_NAME,
        toString(AGENT_SSL_ENABLED_NAME, isAgentSSLEnabled()));
    props.setProperty(AGENT_SSL_PROTOCOLS_NAME,
        toString(AGENT_SSL_PROTOCOLS_NAME, getAgentSSLProtocols()));
    props.setProperty(AGENT_SSL_CIPHERS_NAME,
        toString(AGENT_SSL_CIPHERS_NAME, getAgentSSLCiphers()));
    props.setProperty(AGENT_SSL_REQUIRE_AUTHENTICATION_NAME,
        toString(AGENT_SSL_REQUIRE_AUTHENTICATION_NAME, isAgentSSLRequireAuth()));
    props.setProperty(HTTP_SSL_REQUIRE_AUTHENTICATION_NAME,
        toString(HTTP_SSL_REQUIRE_AUTHENTICATION_NAME, isHttpSSLRequireAuth()));

    props.setProperty(HTTP_AUTHENTICATION_ENABLED_NAME,
        toString(HTTP_AUTHENTICATION_ENABLED_NAME, isHttpAuthEnabled()));
    props.setProperty(HTTP_AUTHENTICATION_USER_NAME,
        toString(HTTP_AUTHENTICATION_USER_NAME, getHttpAuthUser()));
    props.setProperty(HTTP_AUTHENTICATION_PASSWORD_NAME,
        toString(HTTP_AUTHENTICATION_PASSWORD_NAME, getHttpAuthPassword()));

    props.setProperty(EMAIL_NOTIFICATIONS_ENABLED_NAME,
        toString(EMAIL_NOTIFICATIONS_ENABLED_NAME, isEmailNotificationEnabled()));
    props.setProperty(EMAIL_NOTIFICATIONS_HOST_NAME,
        toString(EMAIL_NOTIFICATIONS_HOST_NAME, getEmailNotificationHost()));
    props.setProperty(EMAIL_NOTIFICATIONS_FROM_NAME,
        toString(EMAIL_NOTIFICATIONS_FROM_NAME, getEmailNotificationFrom()));
    props.setProperty(EMAIL_NOTIFICATIONS_TO_LIST_NAME,
        toString(EMAIL_NOTIFICATIONS_TO_LIST_NAME, getEmailNotificationToList()));

    props.setProperty(STATE_SAVE_FILE_NAME, toString(STATE_SAVE_FILE_NAME, getStateSaveFile()));

    props.setProperty(EMAIL_NOTIFICATIONS_ENABLED_NAME,
        toString(EMAIL_NOTIFICATIONS_ENABLED_NAME, isEmailNotificationEnabled()));
    props.setProperty(EMAIL_NOTIFICATIONS_HOST_NAME,
        toString(EMAIL_NOTIFICATIONS_HOST_NAME, getEmailNotificationHost()));
    props.setProperty(EMAIL_NOTIFICATIONS_FROM_NAME,
        toString(EMAIL_NOTIFICATIONS_FROM_NAME, getEmailNotificationFrom()));
    props.setProperty(EMAIL_NOTIFICATIONS_TO_LIST_NAME,
        toString(EMAIL_NOTIFICATIONS_TO_LIST_NAME, getEmailNotificationToList()));

    props.setProperty(CLUSTER_SSL_ENABLED, toString(CLUSTER_SSL_ENABLED, isSSLEnabled()));
    props.setProperty(CLUSTER_SSL_PROTOCOLS, toString(CLUSTER_SSL_PROTOCOLS, getSSLProtocols()));
    props.setProperty(CLUSTER_SSL_CIPHERS, toString(CLUSTER_SSL_CIPHERS, getSSLCiphers()));
    props.setProperty(CLUSTER_SSL_REQUIRE_AUTHENTICATION,
        toString(CLUSTER_SSL_REQUIRE_AUTHENTICATION, isSSLAuthenticationRequired()));

    Properties sslProps = getSSLProperties();
    if (sslProps.size() > 0) {
      int sequence = 0;
      for (Iterator iter = sslProps.keySet().iterator(); iter.hasNext();) {
        String key = (String) iter.next();
        String value = sslProps.getProperty(key);
        props.setProperty("ssl-property-" + sequence, key + "=" + OBFUSCATED_STRING);
        sequence++;
      }
    }

    if (this.getDistributionLocatorConfigs().length > 0) {
      DistributionLocatorConfig[] configs = this.getDistributionLocatorConfigs();
      for (int i = 0; i < configs.length; i++) {
        DistributionLocatorConfig locator = configs[i];
        props.setProperty(LOCATOR_HOST_NAME + i, toString(LOCATOR_HOST_NAME, locator.getHost()));
        props.setProperty(LOCATOR_PORT_NAME + i, toString(LOCATOR_PORT_NAME, locator.getPort()));
        props.setProperty(LOCATOR_PRODUCT_DIRECTORY_NAME + i,
            toString(LOCATOR_PRODUCT_DIRECTORY_NAME, locator.getProductDirectory()));
        props.setProperty(LOCATOR_WORKING_DIRECTORY_NAME + i,
            toString(LOCATOR_WORKING_DIRECTORY_NAME, locator.getWorkingDirectory()));
        props.setProperty(LOCATOR_REMOTE_COMMAND + i,
            toString(LOCATOR_REMOTE_COMMAND, locator.getRemoteCommand()));
        props.setProperty(LOCATOR_BIND_ADDRESS + i,
            toString(LOCATOR_BIND_ADDRESS, locator.getBindAddress()));
        // props.setProperty(LOCATOR_DS_PROPERTIES + i,
        // getdsPropertiesString(locator));
      }
    }

    if (includeDSProperties) {
      props.setProperty(ENTITY_CONFIG_XML_FILE_NAME,
          toString(ENTITY_CONFIG_XML_FILE_NAME, getEntityConfigXMLFile()));
      // This could be different each time agent is started
      // props.setProperty(SYSTEM_ID_NAME, toString(getSystemId()));
      props.setProperty(MCAST_PORT, toString(MCAST_PORT, getMcastPort()));
      props.setProperty(MCAST_ADDRESS, toString(MCAST_ADDRESS, getMcastAddress()));
      props.setProperty(LOCATORS, toString(LOCATORS, getLocators()));
      props.setProperty(MEMBERSHIP_PORT_RANGE_NAME, getMembershipPortRange());
      props.setProperty(TCP_PORT, "" + getTcpPort());
      props.setProperty(BIND_ADDRESS, toString(BIND_ADDRESS, getBindAddress()));
      props.setProperty(REMOTE_COMMAND_NAME, toString(REMOTE_COMMAND_NAME, getRemoteCommand()));
      props.setProperty(LOG_FILE_NAME, toString(LOG_FILE_NAME, getLogFile()));
      props.setProperty(LOG_LEVEL_NAME, toString(LOG_LEVEL_NAME, getLogLevel()));
      props.setProperty(LOG_DISK_SPACE_LIMIT_NAME,
          toString(LOG_DISK_SPACE_LIMIT_NAME, getLogDiskSpaceLimit()));
      props.setProperty(LOG_FILE_SIZE_LIMIT_NAME,
          toString(LOG_FILE_SIZE_LIMIT_NAME, getLogFileSizeLimit()));
      props.setProperty(REFRESH_INTERVAL_NAME,
          toString(REFRESH_INTERVAL_NAME, getRefreshInterval()));
    }

    return props;
  }


  // -------------------------------------------------------------------------
  // Agent specific properties
  // -------------------------------------------------------------------------

  public boolean isAgentSSLEnabled() {
    return this.agentSSLEnabled;
  }

  public void setAgentSSLEnabled(boolean agentSSLEnabled) {
    checkReadOnly();
    this.agentSSLEnabled = agentSSLEnabled;
    configChanged();
  }

  public String getAgentSSLProtocols() {
    return this.agentSSLProtocols;
  }

  public void setAgentSSLProtocols(String agentSSLProtocols) {
    checkReadOnly();
    this.agentSSLProtocols = agentSSLProtocols;
    configChanged();
  }

  public String getAgentSSLCiphers() {
    return this.agentSSLCiphers;
  }

  public void setAgentSSLCiphers(String agentSSLCiphers) {
    checkReadOnly();
    this.agentSSLCiphers = agentSSLCiphers;
    configChanged();
  }

  public boolean isAgentSSLRequireAuth() {
    return this.agentSSLRequireAuth;
  }

  public void setAgentSSLRequireAuth(boolean agentSSLRequireAuth) {
    checkReadOnly();
    this.agentSSLRequireAuth = agentSSLRequireAuth;
    configChanged();
  }

  public boolean isHttpSSLRequireAuth() {
    return this.httpSSLRequireAuth;
  }

  public void setHttpSSLRequireAuth(boolean httpSSLRequireAuth) {
    checkReadOnly();
    this.httpSSLRequireAuth = httpSSLRequireAuth;
    configChanged();
  }

  public boolean isHttpAuthEnabled() {
    return this.httpAuthEnabled;
  }

  public void setHttpAuthEnabled(boolean httpAuthEnabled) {
    checkReadOnly();
    this.httpAuthEnabled = httpAuthEnabled;
    configChanged();
  }

  public String getHttpAuthUser() {
    return this.httpAuthUser;
  }

  public void setHttpAuthUser(String httpAuthUser) {
    checkReadOnly();
    this.httpAuthUser = httpAuthUser;
    configChanged();
  }

  public String getHttpAuthPassword() {
    return this.httpAuthPassword;
  }

  public void setHttpAuthPassword(String httpAuthPassword) {
    checkReadOnly();
    this.httpAuthPassword = httpAuthPassword;
    configChanged();
  }

  public boolean isSnmpEnabled() {
    return this.snmpEnabled;
  }

  public void setSnmpEnabled(boolean snmpEnabled) {
    checkReadOnly();
    this.snmpEnabled = snmpEnabled;
    configChanged();
  }

  public String getSnmpBindAddress() {
    return this.snmpBindAddress;
  }

  public void setSnmpBindAddress(String snmpBindAddress) {
    checkReadOnly();
    this.snmpBindAddress = validateSnmpBindAddress(snmpBindAddress);
    configChanged();
  }

  public String getSnmpDirectory() {
    return this.snmpDirectory;
  }

  public void setSnmpDirectory(String snmpDirectory) {
    checkReadOnly();
    this.snmpDirectory = validateSnmpDirectory(snmpDirectory);
    configChanged();
  }

  public boolean isRmiEnabled() {
    return this.rmiEnabled;
  }

  public void setRmiEnabled(boolean rmiEnabled) {
    checkReadOnly();
    this.rmiEnabled = rmiEnabled;
    configChanged();
  }

  public boolean isRmiRegistryEnabled() {
    return this.rmiRegistryEnabled;
  }

  public void setRmiRegistryEnabled(boolean rmiRegistryEnabled) {
    checkReadOnly();
    this.rmiRegistryEnabled = rmiRegistryEnabled;
    configChanged();
  }

  public String getRmiBindAddress() {
    return this.rmiBindAddress;
  }

  public void setRmiBindAddress(String rmiBindAddress) {
    checkReadOnly();
    this.rmiBindAddress = validateRmiBindAddress(rmiBindAddress);
    configChanged();
  }

  public int getRmiPort() {
    return this.rmiPort;
  }

  public void setRmiPort(int rmiPort) {
    checkReadOnly();
    this.rmiPort = validateRmiPort(rmiPort);
    configChanged();
  }

  /**
   * Returns the port of the RMI Connector Server.
   * <p>
   * See <a href="#rmi-server-port">description</a> above.
   * 
   * @return the value set for rmi-server-port
   *
   * @since GemFire 6.5
   */
  public int getRmiServerPort() {
    return this.rmiServerPort;
  }

  /**
   * Sets the port of the RMI Connector Server.
   * 
   * @param port rmi-server-port to set.
   *
   * @since GemFire 6.5
   */
  public void setRmiServerPort(int port) {
    checkReadOnly();
    this.rmiServerPort = validateRmiServerPort(rmiServerPort);
    configChanged();
  }

  public boolean isHttpEnabled() {
    return this.httpEnabled;
  }

  public void setHttpEnabled(boolean httpEnabled) {
    checkReadOnly();
    this.httpEnabled = httpEnabled;
    configChanged();
  }

  public int getHttpPort() {
    return this.httpPort;
  }

  public void setHttpPort(int httpPort) {
    checkReadOnly();
    this.httpPort = validateHttpPort(httpPort);
    configChanged();
  }

  public String getHttpBindAddress() {
    return this.httpBindAddress;
  }

  public void setHttpBindAddress(String httpBindAddress) {
    checkReadOnly();
    this.httpBindAddress = validateHttpBindAddress(httpBindAddress);
    configChanged();
  }

  public void setHttpBindAddress(InetAddress httpBindAddress) {
    checkReadOnly();
    this.httpBindAddress = validateHttpBindAddress(httpBindAddress);
    configChanged();
  }

  public boolean getAutoConnect() {
    return this.autoConnect;
  }

  public void setAutoConnect(boolean v) {
    checkReadOnly();
    this.autoConnect = v;
    configChanged();
  }

  // -------------------------------------------------------------------------
  // Implementation methods
  // -------------------------------------------------------------------------

  /**
   * Initialize the values of this AgentConfig.
   * 
   * @param props the configuration values to use
   */
  private void initialize(Properties props) {
    this.autoConnect = validateBoolean(props.getProperty(AUTO_CONNECT_NAME), DEFAULT_AUTO_CONNECT);

    this.httpEnabled = validateBoolean(props.getProperty(HTTP_ENABLED_NAME), DEFAULT_HTTP_ENABLED);
    this.httpBindAddress = validateHttpBindAddress(props.getProperty(HTTP_BIND_ADDRESS_NAME));
    this.httpPort = validateHttpPort(props.getProperty(HTTP_PORT_NAME));

    this.rmiEnabled = validateBoolean(props.getProperty(RMI_ENABLED_NAME), DEFAULT_RMI_ENABLED);
    this.rmiRegistryEnabled =
        validateBoolean(props.getProperty(RMI_REGISTRY_ENABLED_NAME), DEFAULT_RMI_REGISTRY_ENABLED);

    this.rmiBindAddress = validateRmiBindAddress(props.getProperty(RMI_BIND_ADDRESS_NAME));
    this.rmiPort = validateRmiPort(props.getProperty(RMI_PORT_NAME));
    this.rmiServerPort = validateRmiServerPort(props.getProperty(RMI_SERVER_PORT_NAME));

    this.snmpEnabled = validateBoolean(props.getProperty(SNMP_ENABLED_NAME), DEFAULT_SNMP_ENABLED);
    this.snmpDirectory = validateSnmpDirectory(props.getProperty(SNMP_DIRECTORY_NAME));

    this.agentSSLEnabled =
        validateBoolean(props.getProperty(AGENT_SSL_ENABLED_NAME), DEFAULT_AGENT_SSL_ENABLED);
    this.agentSSLProtocols = validateNonEmptyString(props.getProperty(AGENT_SSL_PROTOCOLS_NAME),
        DEFAULT_AGENT_SSL_PROTOCOLS);
    this.agentSSLCiphers = validateNonEmptyString(props.getProperty(AGENT_SSL_CIPHERS_NAME),
        DEFAULT_AGENT_SSL_CIPHERS);
    this.agentSSLRequireAuth =
        validateBoolean(props.getProperty(AGENT_SSL_REQUIRE_AUTHENTICATION_NAME),
            DEFAULT_AGENT_SSL_REQUIRE_AUTHENTICATION);
    this.httpSSLRequireAuth =
        validateBoolean(props.getProperty(HTTP_SSL_REQUIRE_AUTHENTICATION_NAME),
            DEFAULT_HTTP_SSL_REQUIRE_AUTHENTICATION);

    this.httpAuthEnabled = validateBoolean(props.getProperty(HTTP_AUTHENTICATION_ENABLED_NAME),
        DEFAULT_HTTP_AUTHENTICATION_ENABLED);
    this.httpAuthUser = validateNonEmptyString(props.getProperty(HTTP_AUTHENTICATION_USER_NAME),
        DEFAULT_HTTP_AUTHENTICATION_USER);
    this.httpAuthPassword = validateNonEmptyString(
        props.getProperty(HTTP_AUTHENTICATION_PASSWORD_NAME), DEFAULT_HTTP_AUTHENTICATION_PASSWORD);

    this.sslEnabled = validateBoolean(props.getProperty(CLUSTER_SSL_ENABLED), DEFAULT_SSL_ENABLED);
    this.sslProtocols =
        validateNonEmptyString(props.getProperty(CLUSTER_SSL_PROTOCOLS), DEFAULT_SSL_PROTOCOLS);
    this.sslCiphers =
        validateNonEmptyString(props.getProperty(CLUSTER_SSL_CIPHERS), DEFAULT_SSL_CIPHERS);
    this.sslAuthenticationRequired = validateBoolean(
        props.getProperty(CLUSTER_SSL_REQUIRE_AUTHENTICATION), DEFAULT_SSL_REQUIRE_AUTHENTICATION);
    this.sslProperties = new Properties();
    for (int i = 0; true; i++) {
      String key = "ssl-property-" + i;
      String value = props.getProperty(key);
      if (value == null) {
        break;
      }
      StringTokenizer st = new StringTokenizer(value, "=");
      if (!st.hasMoreTokens()) {
        break;
      }
      String propKey = st.nextToken();
      if (!st.hasMoreTokens()) {
        break;
      }
      String propValue = st.nextToken();
      this.sslProperties.put(propKey, propValue);
    }

    this.isEmailNotificationEnabled =
        validateBoolean(props.getProperty(AgentConfig.EMAIL_NOTIFICATIONS_ENABLED_NAME),
            DEFAULT_EMAIL_NOTIFICATIONS_ENABLED);
    this.emailNotificationHostName = validateNonEmptyString(
        props.getProperty(AgentConfig.EMAIL_NOTIFICATIONS_HOST_NAME), DEFAULT_EMAIL_HOST);
    this.emailNotificationFrom = validateNonEmptyString(
        props.getProperty(AgentConfig.EMAIL_NOTIFICATIONS_FROM_NAME), DEFAULT_EMAIL_FROM);
    this.emailNotificationToList = validateNonEmptyString(
        props.getProperty(AgentConfig.EMAIL_NOTIFICATIONS_TO_LIST_NAME), DEFAULT_EMAIL_TO_LIST);

    this.stateSaveFile = validateNonEmptyString(props.getProperty(AgentConfig.STATE_SAVE_FILE_NAME),
        DEFAULT_STATE_SAVE_FILE);

    try {
      for (int i = 0; true; i++) {
        String hostProp = props.getProperty(LOCATOR_HOST_NAME + i);
        if (isEmpty(hostProp)) {
          break;
        }
        String host = hostProp;
        int port = Integer.parseInt(props.getProperty(LOCATOR_PORT_NAME + i));
        File workDir =
            validateWorkingDirectory(props.getProperty(LOCATOR_WORKING_DIRECTORY_NAME + i));
        File prodDir = new File(
            validateProductDirectory(props.getProperty(LOCATOR_PRODUCT_DIRECTORY_NAME + i)));
        String remoteCmd = props.getProperty(LOCATOR_REMOTE_COMMAND + i);
        String bindAddr = props.getProperty(LOCATOR_BIND_ADDRESS + i);

        DistributionLocatorConfig config = this.createDistributionLocatorConfig();
        config.setHost(host);
        config.setPort(port);
        config.setBindAddress(bindAddr);
        config.setWorkingDirectory(workDir.getAbsolutePath());
        config.setProductDirectory(prodDir.getAbsolutePath());
        config.setRemoteCommand(remoteCmd);
      }
    } catch (IllegalArgumentException e) {
      // This is how we break out of the loop? Yuck!
      /*
       * LogWriter is initialized afterwards. Hence printing the stack trace. This is done to avoid
       * creation of duplicate log writer.
       */
      e.printStackTrace();
    }
  }

  /**
   * Filter all agent configuration attributes out of the given <code>Properties</code> object.
   * <p/>
   * 
   * @param props the <code>Properties</code> object of filter agent configuration attributes out
   *        of.
   *
   * @see AgentConfigImpl#_getPropertyDescription(String)
   */
  private static Properties filterOutAgentProperties(final Properties props) {
    final Properties filteredProps = new Properties();

    for (final Object key : props.keySet()) {
      if (_getPropertyDescription(key.toString()) == null) {
        final String value = props.getProperty(key.toString());
        if (value != null) {
          filteredProps.setProperty(key.toString(), value);
        }
      }
    }

    appendLogFileProperty(filteredProps);

    return filteredProps;
  }

  /**
   * Appends the log-file property to the Properties object if set of properties does not already
   * define the log-file property or the gemfire.agent.log-file property.
   * <p/>
   * 
   * @param props the <code>Properties</code> to append the log-file property to if the property
   *        does not exist.
   */
  private static void appendLogFileProperty(final Properties props) {
    if (!(props.containsKey(DistributedSystemConfig.LOG_FILE_NAME)
        || props.containsKey(SYSTEM_PROPERTY_PREFIX + DistributedSystemConfig.LOG_FILE_NAME))) {
      props.put(DistributedSystemConfig.LOG_FILE_NAME, DEFAULT_LOG_FILE);
    }
  }

  /**
   * Appends any additional property-file specified properties to the supplied Properties. If the
   * supplied property overrides the property in the property-file, then property-file value is
   * ignored. System Properties always override the supplied properties
   * 
   * @return appendedProps Properties appened to from the property-file if any
   */
  private static Properties appendOptionalPropertyFileProperties(final Properties props) {
    final URL url = getPropertyFileURL(retrievePropertyFile());
    final Properties appendedProps = new Properties();

    appendedProps.putAll(props);

    // first, get any property values set in the optional property file
    if (url != null) {
      InputStream in = null;

      try {
        in = url.openStream();

        final Properties agentPropertyFileProperties = new Properties();

        agentPropertyFileProperties.load(in);

        // don't let any properties from the file override those on the command-line
        for (final Object key : agentPropertyFileProperties.keySet()) {
          if (props.getProperty(key.toString()) == null) {
            appendedProps.setProperty(key.toString(),
                agentPropertyFileProperties.getProperty(key.toString()));
          }
        }
      } catch (IOException e) {
        throw new GemFireIOException(
            LocalizedStrings.AgentConfigImpl_FAILED_READING_0.toLocalizedString(url.toString()), e);
      } finally {
        IOUtils.close(in);
      }
    }

    // last override values with those from the system properties
    // TODO this is not exactly overriding!
    for (final Object propSuffix : props.keySet()) {
      final String key = SYSTEM_PROPERTY_PREFIX + propSuffix;
      final String value = System.getProperty(key);

      if (value != null) {
        appendedProps.put(key, value);
      }
    }

    return appendedProps;
  }

  /**
   * Returns a description of the given agent config property
   * 
   * @throws IllegalArgumentException If <code>prop</code> is not a recognized agent configuration
   *         property
   */
  public static String getPropertyDescription(String prop) {
    if (prop.equals(LOG_FILE_NAME)) {
      return LocalizedStrings.AgentConfigImpl_NAME_OF_THE_AGENTS_LOG_FILE.toLocalizedString();
    } else if (prop.equals(LOG_LEVEL_NAME)) {
      return LocalizedStrings.AgentConfigImpl_MINIMUM_LEVEL_OF_LOGGING_PERFORMED_BY_AGENT
          .toLocalizedString();
    } else if (prop.equals(AGENT_DEBUG)) {
      return LocalizedStrings.AgentConfigImpl_WHETHER_THE_AGENT_SHOULD_PRINT_DEBUGGING_INFORMATION
          .toLocalizedString();
    } else if (prop.equals(LOG_DISK_SPACE_LIMIT_NAME)) {
      return LocalizedStrings.AgentConfigImpl_LIMIT_IN_MEGABYTES_OF_HOW_MUCH_DISK_SPACE_CAN_BE_CONSUMED_BY_OLD_INACTIVE_LOG_FILES
          .toLocalizedString();
    } else if (prop.equals(LOG_FILE_SIZE_LIMIT_NAME)) {
      return LocalizedStrings.AgentConfigImpl_LIMIT_IN_MEGABYTES_OF_HOW_LARGE_THE_CURRENT_STATISTIC_ARCHIVE_FILE_CAN_GROW_BEFORE_IT_IS_CLOSED_AND_ARCHIVAL_ROLLS_ON_TO_A_NEW_FILE
          .toLocalizedString();
    } else if (prop.equals(MCAST_PORT)) {
      return LocalizedStrings.AgentConfigImpl_MULTICAST_PORT_USED_TO_CONNECT_TO_DISTRIBUTED_SYSTEM
          .toLocalizedString();
    } else if (prop.equals(MCAST_ADDRESS)) {
      return LocalizedStrings.AgentConfigImpl_MULTICAST_ADDRESS_USED_TO_CONNECT_TO_DISTRIBUTED_SYSTEM
          .toLocalizedString();
    } else if (prop.equals(BIND_ADDRESS)) {
      return LocalizedStrings.AgentConfigImpl_IP_ADDRESS_OF_THE_AGENTS_DISTRIBUTED_SYSTEM
          .toLocalizedString();
    } else if (prop.equals(TCP_PORT)) {
      return LocalizedStrings.AgentConfigImpl_TCP_PORT.toLocalizedString();
    } else if (prop.equals(LOCATORS)) {
      return LocalizedStrings.AgentConfigImpl_ADDRESSES_OF_THE_LOCATORS_OF_THE_DISTRIBUTED_SYSTEM
          .toLocalizedString();
    } else if (prop.equals(MEMBERSHIP_PORT_RANGE_NAME)) {
      return LocalizedStrings.AgentConfigImpl_ALLOWED_RANGE_OF_UDP_PORTS_TO_FORM_UNIQUE_MEMBERSHIP_ID
          .toLocalizedString();
      // } else if (prop.equals(SYSTEM_ID_NAME)) {
      // return "The id of the distributed system";
    } else if (prop.equals(ENTITY_CONFIG_XML_FILE_NAME)) {
      return LocalizedStrings.AgentConfigImpl_XML_CONFIGURATION_FILE_FOR_MANAGED_ENTITIES
          .toLocalizedString();
    } else if (prop.equals(REFRESH_INTERVAL_NAME)) {
      return LocalizedStrings.AgentConfigImpl_REFRESH_INTERVAL_IN_SECONDS_FOR_AUTOREFRESH_OF_MEMBERS_AND_STATISTIC_RESOURCES
          .toLocalizedString();
    } else if (prop.equals(REMOTE_COMMAND_NAME)) {
      return LocalizedStrings.AgentConfigImpl_COMMAND_PREFIX_USED_FOR_LAUNCHING_MEMBERS_OF_THE_DISTRIBUTED_SYSTEM
          .toLocalizedString();
    } else if (prop.equals(CLUSTER_SSL_ENABLED)) {
      return LocalizedStrings.AgentConfigImpl_DOES_THE_DISTRIBUTED_SYSTEM_COMMUNICATE_USING_SSL
          .toLocalizedString();
    } else if (prop.equals(CLUSTER_SSL_PROTOCOLS)) {
      return LocalizedStrings.AgentConfigImpl_SSL_PROTOCOLS_USED_TO_COMMUNICATE_WITH_DISTRIBUTED_SYSTEM
          .toLocalizedString();
    } else if (prop.equals(CLUSTER_SSL_CIPHERS)) {
      return LocalizedStrings.AgentConfigImpl_SSL_CIPHERS_USED_TO_COMMUNICATE_WITH_DISTRIBUTED_SYSTEM
          .toLocalizedString();
    } else if (prop.equals(CLUSTER_SSL_REQUIRE_AUTHENTICATION)) {
      return LocalizedStrings.AgentConfigImpl_DOES_CONNECTING_TO_THE_DISTRIBUTED_SYSTEM_REQUIRE_SSL_AUTHENTICATION
          .toLocalizedString();
    } else {
      String description = _getPropertyDescription(prop);
      if (description == null) {
        throw new IllegalArgumentException(
            LocalizedStrings.AgentConfigImpl_UNKNOWN_CONFIG_PROPERTY_0.toLocalizedString(prop));

      } else {
        return description;
      }
    }
  }

  /**
   * Returns a description of the given agent config property or <code>null</code> if
   * <code>prop</code> is not a recognized agent property.
   */
  public static String _getPropertyDescription(String prop) {
    if (prop.equals(AUTO_CONNECT_NAME)) {
      return LocalizedStrings.AgentConfigImpl_WILL_THE_AGENT_AUTOMATICALLY_CONNECT_TO_THE_DISTRIBUTED_SYSTEM
          .toLocalizedString();

      // } else if (prop.equals(SYSTEM_NAME_NAME)) {
      // return "The logical name of the distributed system";

    } else if (prop.equals(HTTP_ENABLED_NAME)) {
      return LocalizedStrings.AgentConfigImpl_WILL_THE_AGENT_START_THE_HTTP_JMX_ADAPTER
          .toLocalizedString();

    } else if (prop.equals(HTTP_BIND_ADDRESS_NAME)) {
      return LocalizedStrings.AgentConfigImpl_BIND_ADDRESS_OF_HTTP_ADAPTERS_SOCKETS
          .toLocalizedString();

    } else if (prop.equals(HTTP_PORT_NAME)) {
      return LocalizedStrings.AgentConfigImpl_THE_PORT_ON_WHICH_THE_HTTP_ADAPTER_WILL_BE_STARTED
          .toLocalizedString();

    } else if (prop.equals(RMI_ENABLED_NAME)) {
      return LocalizedStrings.AgentConfigImpl_WILL_THE_AGENT_START_THE_RMI_JMX_ADAPTER
          .toLocalizedString();

    } else if (prop.equals(RMI_REGISTRY_ENABLED_NAME)) {
      return LocalizedStrings.AgentConfigImpl_WILL_THE_AGENT_HOST_AN_RMI_REGISTRY
          .toLocalizedString();

    } else if (prop.equals(RMI_BIND_ADDRESS_NAME)) {
      return LocalizedStrings.AgentConfigImpl_BIND_ADDRESS_OF_RMI_ADAPTERS_SOCKETS
          .toLocalizedString();

    } else if (prop.equals(RMI_PORT_NAME)) {
      return LocalizedStrings.AgentConfigImpl_THE_PORT_ON_WHICH_TO_CONTACT_THE_RMI_REGISTER
          .toLocalizedString();

    } else if (prop.equals(RMI_SERVER_PORT_NAME)) {
      return LocalizedStrings.AgentConfigImpl_THE_PORT_USED_TO_CONFIGURE_RMI_CONNECTOR_SERVER
          .toLocalizedString();

    } else if (prop.equals(SNMP_ENABLED_NAME)) {
      return LocalizedStrings.AgentConfigImpl_WILL_THE_AGENT_START_THE_SNMP_JMX_ADAPTER
          .toLocalizedString();

    } else if (prop.equals(SNMP_BIND_ADDRESS_NAME)) {
      return LocalizedStrings.AgentConfigImpl_BIND_ADDRESS_OF_SNMP_ADAPTERS_SOCKETS
          .toLocalizedString();

    } else if (prop.equals(SNMP_DIRECTORY_NAME)) {
      return LocalizedStrings.AgentConfigImpl_THE_DIRECTORY_IN_WHICH_SNMP_CONFIGURATION_RESIDES
          .toLocalizedString();

    } else if (prop.equals(AGENT_SSL_ENABLED_NAME)) {
      return LocalizedStrings.AgentConfigImpl_WILL_THE_AGENT_COMMUNICATE_USING_SSL
          .toLocalizedString();

    } else if (prop.equals(AGENT_SSL_PROTOCOLS_NAME)) {
      return LocalizedStrings.AgentConfigImpl_THE_SSL_PROTOCOLS_USED_BY_THE_AGENT
          .toLocalizedString();

    } else if (prop.equals(AGENT_SSL_CIPHERS_NAME)) {
      return LocalizedStrings.AgentConfigImpl_THE_SSL_CIPHERS_USED_BY_THE_AGENT.toLocalizedString();

    } else if (prop.equals(AGENT_SSL_REQUIRE_AUTHENTICATION_NAME)) {
      return LocalizedStrings.AgentConfigImpl_WILL_THE_AGENT_REQUIRE_SSL_AUTHENTICATION
          .toLocalizedString();

    } else if (prop.equals(HTTP_SSL_REQUIRE_AUTHENTICATION_NAME)) {
      return LocalizedStrings.AgentConfigImpl_WILL_THE_HTTP_ADAPTER_REQUIRE_SSL_AUTHENTICATION
          .toLocalizedString();

    } else if (prop.equals(HTTP_AUTHENTICATION_ENABLED_NAME)) {
      return LocalizedStrings.AgentConfigImpl_WILL_THE_HTTP_JMX_ADAPTER_USE_HTTP_AUTHENTICATION
          .toLocalizedString();

    } else if (prop.equals(HTTP_AUTHENTICATION_USER_NAME)) {
      return LocalizedStrings.AgentConfigImpl_THE_USER_NAME_FOR_AUTHENTICATION_IN_THE_HTTP_JMX_ADAPTER
          .toLocalizedString();

    } else if (prop.equals(HTTP_AUTHENTICATION_PASSWORD_NAME)) {
      return LocalizedStrings.AgentConfigImpl_THE_PASSWORD_FOR_AUTHENTICATION_IN_THE_HTTP_JMX_ADAPTER
          .toLocalizedString();

    } else if (prop.equals(PROPERTY_FILE_NAME)) {
      return LocalizedStrings.AgentConfigImpl_PROPERTY_FILE_FROM_WHICH_AGENT_READS_CONFIGURATION
          .toLocalizedString();

    } else if (prop.equals(LOCATOR_HOST_NAME)) {
      return LocalizedStrings.AgentConfigImpl_HOST_ON_WHICH_THE_DISTRIBUTED_SYSTEMS_LOCATOR_RUNS
          .toLocalizedString();

    } else if (prop.equals(LOCATOR_PORT_NAME)) {
      return LocalizedStrings.AgentConfigImpl_HOST_ON_WHICH_THE_DISTRIBUTED_SYSTEMS_LOCATOR_RUNS
          .toLocalizedString();

    } else if (prop.equals(LOCATOR_PRODUCT_DIRECTORY_NAME)) {
      return LocalizedStrings.AgentConfigImpl_GEMFIRE_PRODUCT_DIRECTORY_USED_TO_LAUNCH_A_LOCATOR
          .toLocalizedString();

    } else if (prop.equals(LOCATOR_WORKING_DIRECTORY_NAME)) {
      return LocalizedStrings.AgentConfigImpl_DIRECTORY_IN_WHICH_A_LOCATOR_WILL_BE_LAUNCHED
          .toLocalizedString();

    } else if (prop.equals(LOCATOR_REMOTE_COMMAND)) {
      return LocalizedStrings.AgentConfigImpl_COMMAND_PREFIX_USED_WHEN_LAUNCHING_A_LOCATOR
          .toLocalizedString();

    } else if (prop.equals(LOCATOR_BIND_ADDRESS)) {
      return LocalizedStrings.AgentConfigImpl_IP_ADDRESS_TO_USE_WHEN_CONTACTING_LOCATOR
          .toLocalizedString();

    } else if (prop.equals(LOCATOR_DS_PROPERTIES)) {
      return LocalizedStrings.AgentConfigImpl_PROPERTIES_FOR_CONFIGURING_A_LOCATORS_DISTRIBUTED_SYSTEM
          .toLocalizedString();

    } else if (prop.equals(EMAIL_NOTIFICATIONS_ENABLED_NAME)) {
      return LocalizedStrings.AgentConfigImpl_IDENTIFY_IF_EMAIL_NOTIFICATIONS_ARE_ENABLED_OR_NOT
          .toLocalizedString();

    } else if (prop.equals(EMAIL_NOTIFICATIONS_FROM_NAME)) {
      return LocalizedStrings.AgentConfigImpl_IDENTIFY_THE_EMAIL_ADDRESS_USING_WHICH_EMAIL_NOTIFICATIONS_ARE_SENT
          .toLocalizedString();

    } else if (prop.equals(EMAIL_NOTIFICATIONS_HOST_NAME)) {
      return LocalizedStrings.AgentConfigImpl_IDENTIFY_THE_EMAIL_SERVER_HOST_USING_WHICH_EMAIL_NOTIFICATIONS_ARE_SENT
          .toLocalizedString();

    } else if (prop.equals(EMAIL_NOTIFICATIONS_TO_LIST_NAME)) {
      return LocalizedStrings.AgentConfigImpl_IDENTIFY_THE_COMMA_SEPARATED_EMAIL_ADDRESSES_LIST_TO_WHICH_EMAIL_NOTIFICATIONS_ARE_SENT
          .toLocalizedString();

    } else if (prop.equals(STATE_SAVE_FILE_NAME)) {
      return LocalizedStrings.AgentConfigImpl_IDENTIFY_THE_NAME_OF_THE_FILE_TO_BE_USED_FOR_SAVING_AGENT_STATE
          .toLocalizedString();

    } else {
      return null;
    }
  }

  /**
   * Parses the array of command-line arguments (format: key=value) into an instance of Properties.
   * 
   * @param args the command-line arguments to convert into a Properties
   */
  private static Properties toProperties(String[] args) {
    Properties props = new Properties();
    // loop all args and pick out key=value pairs...
    for (int i = 0; i < args.length; i++) {
      // VM args...
      if (args[i].startsWith("-J")) {
        int eq = args[i].indexOf("=");
        String key = args[i].substring(2, eq);
        String value = args[i].substring(eq + 1);
        System.setProperty(key, value);
      } else if (args[i].indexOf(AGENT_DEBUG) > 0) {
        int eq = args[i].indexOf("=");
        String key = args[i].substring(2, eq);
        String value = args[i].substring(eq + 1);
        System.setProperty(key, value);
      }

      // all other args
      else if (args[i].indexOf("=") > 0) {
        int eq = args[i].indexOf("=");
        String key = args[i].substring(0, eq);
        String value = args[i].substring(eq + 1);
        props.setProperty(key, value);
      }
    }

    return props;
  }

  /**
   * Returns the original command-line arguments.
   */
  public String[] getOriginalArgs() {
    return this.originalCmdLineArgs;
  }

  // -------------------------------------------------------------------------
  // Validation methods for configuration options
  // -------------------------------------------------------------------------

  /**
   * Makes sure that the mcast port and locators are correct and consistent.
   * 
   * @throws IllegalArgumentException If configuration is not valid
   */
  @Override
  public void validate() {
    super.validate();

    if (this.httpPort < 0 || this.httpPort > MAX_HTTP_PORT) {
      throw new IllegalArgumentException(
          LocalizedStrings.AgentConfigImpl_0_MUST_BE_ZERO_OR_AN_INTEGER_BETWEEN_1_AND_2
              .toLocalizedString(new Object[] {HTTP_PORT_NAME, Integer.valueOf(MIN_HTTP_PORT),
                  Integer.valueOf(MAX_HTTP_PORT)}));
    }

    if (this.rmiPort < 0 || this.rmiPort > MAX_RMI_PORT) {
      throw new IllegalArgumentException(
          LocalizedStrings.AgentConfigImpl_0_MUST_BE_ZERO_OR_AN_INTEGER_BETWEEN_1_AND_2
              .toLocalizedString(new Object[] {RMI_PORT_NAME, Integer.valueOf(MIN_RMI_PORT),
                  Integer.valueOf(MAX_RMI_PORT)}));
    }

    if (this.rmiServerPort < 0 || this.rmiServerPort > MAX_RMI_PORT) {
      throw new IllegalArgumentException(
          LocalizedStrings.AgentConfigImpl_0_MUST_BE_ZERO_OR_AN_INTEGER_BETWEEN_1_AND_2
              .toLocalizedString(new Object[] {RMI_SERVER_PORT_NAME, Integer.valueOf(MIN_RMI_PORT),
                  Integer.valueOf(MAX_RMI_PORT)}));
    }

  }

  /**
   * Returns defaultValue if value is empty.
   */
  private String validateNonEmptyString(String value, String defaultValue) {
    return isEmpty(value) ? defaultValue : value;
  }

  /**
   * Validates that systemHost can be used for an InetAddress.
   */
  private String validateSystemHost(String systemHost) {
    return InetAddressUtil.validateHost(systemHost);
  }

  /**
   * Returns null if productDir is empty; else converts it to File.
   */
  private String validateProductDirectory(String productDir) {
    if (isEmpty(productDir)) {
      return null;
    }
    return productDir;
  }

  /**
   * Returns true if value parses as true; null value returns defaultValue.
   */
  private boolean validateBoolean(String value, boolean defaultValue) {
    if (isEmpty(value)) {
      return defaultValue;
    }
    return Boolean.valueOf(value).booleanValue();
  }

  // HttpAdaptor property validators...

  /**
   * Returns {@link org.apache.geode.admin.jmx.AgentConfig#DEFAULT_HTTP_PORT} if httpPort is empty;
   * else validates that it's an integer and returns the int form.
   */
  private int validateHttpPort(String val) {
    if (isEmpty(val)) {
      return DEFAULT_HTTP_PORT;
    } else {
      return validateHttpPort(Integer.parseInt(val));
    }
  }

  /**
   * Validates that httpPort is either zero or within the
   * {@link org.apache.geode.admin.jmx.AgentConfig#MIN_HTTP_PORT} and
   * {@link org.apache.geode.admin.jmx.AgentConfig#MAX_HTTP_PORT} values.
   */
  private int validateHttpPort(int val) {
    if (val < 0 || val > MAX_HTTP_PORT) {
      throw new IllegalArgumentException(
          LocalizedStrings.AgentConfigImpl_0_MUST_BE_ZERO_OR_AN_INTEGER_BETWEEN_1_AND_2
              .toLocalizedString(new Object[] {HTTP_PORT_NAME, Integer.valueOf(MIN_HTTP_PORT),
                  Integer.valueOf(MAX_HTTP_PORT)}));
    }
    return val;
  }

  /**
   * Returns {@link org.apache.geode.admin.jmx.AgentConfig#DEFAULT_HTTP_BIND_ADDRESS} unless
   * httpBindAddress can be used to create a valid InetAddress.
   */
  private String validateHttpBindAddress(String val) {
    String value = InetAddressUtil.validateHost(val);
    if (value == null) {
      return DEFAULT_HTTP_BIND_ADDRESS;
    } else {
      return value;
    }
  }

  /**
   * Validates that httpBindAddress is not null and then returns the string form of it.
   */
  private String validateHttpBindAddress(InetAddress val) {
    if (val == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.AgentConfigImpl_HTTPBINDADDRESS_MUST_NOT_BE_NULL.toLocalizedString());
    }
    return toString("", val);
  }

  // SnmpAdaptor property validators...

  /**
   * Returns {@link org.apache.geode.admin.jmx.AgentConfig#DEFAULT_SNMP_BIND_ADDRESS} unless
   * snmpBindAddress can be used to create a valid InetAddress.
   */
  private String validateSnmpBindAddress(String val) {
    String value = InetAddressUtil.validateHost(val);
    if (value == null) {
      return DEFAULT_SNMP_BIND_ADDRESS;
    } else {
      return value;
    }
  }

  // /**
  // * Validates that snmpBindAddress is not null and then returns the string form of it.
  // */
  // private String validateSnmpBindAddress(InetAddress snmpBindAddress) {
  // if (snmpBindAddress == null) {
  // throw new IllegalArgumentException("SnmpBindAddress must not be null");
  // }
  // return toString(snmpBindAddress);
  // }

  /**
   * SnmpDirectory must be specified if SNMP is enabled. This directory must also exist.
   */
  private String validateSnmpDirectory(String snmpDir) {
    /*
     * if (isSnmpEnabled() && isEmpty(snmpDir)) { throw new
     * IllegalArgumentException(LocalizedStrings.
     * AgentConfigImpl_SNMPDIRECTORY_MUST_BE_SPECIFIED_BECAUSE_SNMP_IS_ENABLED.toLocalizedString());
     * } File root new File(snmpDir); if (!root.exists()) throw new
     * IllegalArgumentException(LocalizedStrings.AgentConfigImpl_SNMPDIRECTORY_DOES_NOT_EXIST.
     * toLocalizedString());
     */

    return snmpDir;
  }

  // RMIConnectorServer property validators...

  /**
   * Returns {@link org.apache.geode.admin.jmx.AgentConfig#DEFAULT_RMI_PORT} if rmiPort is empty;
   * else validates that it's an integer and returns the int form.
   */
  private int validateRmiPort(String val) {
    if (isEmpty(val)) {
      return DEFAULT_RMI_PORT;
    } else {
      return validateRmiPort(Integer.parseInt(val));
    }
  }

  /**
   * Validates that rmiPort is either zero or within the
   * {@link org.apache.geode.admin.jmx.AgentConfig#MIN_RMI_PORT} and
   * {@link org.apache.geode.admin.jmx.AgentConfig#MAX_RMI_PORT} values.
   */
  private int validateRmiPort(int val) {
    if (val < MIN_RMI_PORT || val > MAX_RMI_PORT) {
      throw new IllegalArgumentException(
          LocalizedStrings.AgentConfigImpl_0_MUST_BE_ZERO_OR_AN_INTEGER_BETWEEN_1_AND_2
              .toLocalizedString(new Object[] {RMI_PORT_NAME, Integer.valueOf(MIN_RMI_PORT),
                  Integer.valueOf(MAX_RMI_PORT)}));
    }
    return val;
  }

  /**
   * Returns {@link org.apache.geode.admin.jmx.AgentConfig#DEFAULT_RMI_SERVER_PORT} if
   * rmi-server-port is empty; else validates that it's an integer within the allowed range and
   * returns the int form.
   */
  private int validateRmiServerPort(String val) {
    if (isEmpty(val)) {
      return DEFAULT_RMI_SERVER_PORT;
    } else {
      return validateRmiServerPort(Integer.parseInt(val));
    }
  }

  /**
   * Validates that rmiPort is either zero or within the
   * {@link org.apache.geode.admin.jmx.AgentConfig#MIN_RMI_PORT} and
   * {@link org.apache.geode.admin.jmx.AgentConfig#MAX_RMI_PORT} values.
   */
  private int validateRmiServerPort(int val) {
    if (val < MIN_RMI_PORT || val > MAX_RMI_PORT) {
      throw new IllegalArgumentException(
          LocalizedStrings.AgentConfigImpl_0_MUST_BE_ZERO_OR_AN_INTEGER_BETWEEN_1_AND_2
              .toLocalizedString(new Object[] {RMI_SERVER_PORT_NAME, Integer.valueOf(MIN_RMI_PORT),
                  Integer.valueOf(MAX_RMI_PORT)}));
    }
    return val;
  }

  /**
   * Returns {@link org.apache.geode.admin.jmx.AgentConfig#DEFAULT_RMI_BIND_ADDRESS} unless
   * rmiBindAddress can be used to create a valid InetAddress.
   */
  private String validateRmiBindAddress(String val) {
    String value = InetAddressUtil.validateHost(val);
    if (value == null) {
      return DEFAULT_RMI_BIND_ADDRESS;
    } else {
      return value;
    }
  }
  // /**
  // * Validates that rmiBindAddress is not null and then returns the string form of it.
  // */
  // private String validateRmiBindAddress(InetAddress rmiBindAddress) {
  // if (rmiBindAddress == null) {
  // throw new IllegalArgumentException("RmiBindAddress must not be null");
  // }
  // return toString(rmiBindAddress);
  // }

  /**
   * Validates working directory is not null or empty.
   */
  private File validateWorkingDirectory(String workingDir) {
    if (isEmpty(workingDir)) {
      throw new IllegalArgumentException(
          LocalizedStrings.AgentConfigImpl_LOCATOR_WORKINGDIRECTORY_MUST_NOT_BE_NULL
              .toLocalizedString());
    }
    return new File(workingDir);
  }

  // -------------------------------------------------------------------------
  // Static utility methods
  // -------------------------------------------------------------------------

  /**
   * Gets an <code>URL</code> for the property file, if one can be found, that the create method
   * would use to determine the systemName and product home.
   * <p>
   * The file will be searched for, in order, in the following locations:
   * <ol>
   * <li>the current directory
   * <li>the home directory
   * <li>the class path
   * </ol>
   * Only the first file found will be used.
   * 
   * @return a <code>URL</code> that names the property file; otherwise Null if no property file was
   *         found.
   */
  public static URL getPropertyFileURL(final String propFileLocation) {
    File propFile = new File(propFileLocation);

    // first, try the current directory...
    if (propFile.exists()) {
      propFile = IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(propFile);

      try {
        return propFile.toURI().toURL();
      } catch (java.net.MalformedURLException ignore) {
      }
    }

    // next, try the user's home directory...
    if (propFileLocation != null && propFileLocation.length() > 0) {
      propFile = new File(System.getProperty("user.home"), propFileLocation);

      if (propFile.exists()) {
        propFile = IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(propFile);

        try {
          return propFile.toURI().toURL();
        } catch (java.net.MalformedURLException ignore) {
        }
      }
    }

    // finally, try the classpath...
    return ClassPathLoader.getLatest().getResource(AgentConfigImpl.class, propFileLocation);
  }

  private static boolean okToDisplayPropertyValue(String attName) {
    if (attName.startsWith(HTTP_AUTHENTICATION_USER_NAME)) {
      return false;
    }
    if (attName.startsWith(HTTP_AUTHENTICATION_PASSWORD_NAME)) {
      return false;
    }
    if (attName.startsWith(AGENT_SSL_PROTOCOLS_NAME)) {
      return false;
    }
    if (attName.startsWith(AGENT_SSL_CIPHERS_NAME)) {
      return false;
    }
    if (attName.toLowerCase().contains("javax.net.ssl")) {
      return false;
    }
    if (attName.toLowerCase().contains("password")) {
      return false;
    }
    return true;
  }

  /**
   * Returns string representation of the specified object with special handling for InetAddress.
   * 
   * @param obj the object to convert to string
   *
   * @return string representation of the specified object
   */
  private static String toString(String attName, java.lang.Object obj) {
    if (okToDisplayPropertyValue(attName)) {
      if (obj == null) {
        return "";
      }
      if (obj instanceof InetAddress) {
        return InetAddressUtil.toString(obj);
      }
      return obj.toString();
    } else {
      return OBFUSCATED_STRING;
    }
  }

  /**
   * Returns string representation of the int.
   */
  private static String toString(String attName, int num) {
    if (okToDisplayPropertyValue(attName)) {
      return String.valueOf(num);
    } else {
      return OBFUSCATED_STRING;
    }
  }

  /**
   * Returns string representation of the boolean value.
   */
  private static String toString(String attName, boolean v) {
    if (okToDisplayPropertyValue(attName)) {
      return String.valueOf(v);
    } else {
      return OBFUSCATED_STRING;
    }
  }

  /**
   * Returns true if the string is null or empty.
   */
  public static boolean isEmpty(String string) {
    return string == null || string.length() == 0;
  }

  // -------------------------------------------------------------------------
  // SSL support...
  // -------------------------------------------------------------------------
  private boolean sslEnabled = DEFAULT_SSL_ENABLED;
  private String sslProtocols = DEFAULT_SSL_PROTOCOLS;
  private String sslCiphers = DEFAULT_SSL_CIPHERS;
  private boolean sslAuthenticationRequired = DEFAULT_SSL_REQUIRE_AUTHENTICATION;
  private Properties sslProperties = new Properties();

  @Override
  public boolean isSSLEnabled() {
    return this.sslEnabled;
  }

  @Override
  public void setSSLEnabled(boolean enabled) {
    this.sslEnabled = enabled;
    configChanged();
  }

  @Override
  public String getSSLProtocols() {
    return this.sslProtocols;
  }

  @Override
  public void setSSLProtocols(String protocols) {
    this.sslProtocols = protocols;
    configChanged();
  }

  @Override
  public String getSSLCiphers() {
    return this.sslCiphers;
  }

  @Override
  public void setSSLCiphers(String ciphers) {
    this.sslCiphers = ciphers;
    configChanged();
  }

  @Override
  public boolean isSSLAuthenticationRequired() {
    return this.sslAuthenticationRequired;
  }

  @Override
  public void setSSLAuthenticationRequired(boolean authRequired) {
    this.sslAuthenticationRequired = authRequired;
    configChanged();
  }

  @Override
  public Properties getSSLProperties() {
    return this.sslProperties;
  }

  @Override
  public void setSSLProperties(Properties sslProperties) {
    this.sslProperties = sslProperties;
    if (this.sslProperties == null) {
      this.sslProperties = new Properties();
    }
    configChanged();
  }

  public String getStateSaveFile() {
    return this.stateSaveFile;
  }

  public void setStateSaveFile(String file) {
    checkReadOnly();
    this.stateSaveFile = file;
    configChanged();
  }

  public boolean isEmailNotificationEnabled() {
    return this.isEmailNotificationEnabled;
  }

  public void setEmailNotificationEnabled(boolean enabled) {
    checkReadOnly();
    this.isEmailNotificationEnabled = enabled;
    configChanged();
  }

  public String getEmailNotificationFrom() {
    return this.emailNotificationFrom;
  }

  public void setEmailNotificationFrom(String emailID) {
    this.emailNotificationFrom = emailID;
    configChanged();
  }

  public String getEmailNotificationHost() {
    return this.emailNotificationHostName;
  }

  public void setEmailNotificationHost(String hostName) {
    this.emailNotificationHostName = hostName;
    configChanged();
  }

  public String getEmailNotificationToList() {
    return this.emailNotificationToList;
  }

  public void setEmailNotificationToList(String emailIDs) {
    this.emailNotificationToList = emailIDs;
    configChanged();
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }
}


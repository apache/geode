/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.gopivotal.com/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.distributed.internal;

import java.io.File;
import java.net.InetAddress;
import java.util.Properties;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.Config;
import com.gemstone.gemfire.internal.ConfigSource;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LogConfig;
import com.gemstone.gemfire.internal.tcp.Connection;
import com.gemstone.gemfire.memcached.GemFireMemcachedServer;

/**
 * Provides accessor (and in some cases mutator) methods for the
 * various GemFire distribution configuration properties.  The
 * interface also provides constants for the names of properties and
 * their default values.
 *
 * <P>
 *
 * Decriptions of these properties can be found <a
 * href="../DistributedSystem.html#configuration">here</a>.
 *
 * @see com.gemstone.gemfire.internal.Config
 *
 * @author David Whitlock
 * @author Darrel Schneider
 *
 * @since 2.1
 */
public interface DistributionConfig extends Config, LogConfig {


  ////////////////////  Instance Methods  ////////////////////

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#name">"name"</a> property
   * Gets the member's name.
   * A name is optional and by default empty.
   * If set it must be unique in the ds.
   * When set its used by tools to help identify the member.
   * <p> The default value is: {@link #DEFAULT_NAME}.
   * @return the system's name.
   */
  public String getName();
  /**
   * Sets the member's name.
   * <p> The name can not be changed while the system is running.
   * @throws IllegalArgumentException if the specified value is not acceptable.
   * @throws com.gemstone.gemfire.UnmodifiableException if this attribute can not be modified.
   * @throws com.gemstone.gemfire.GemFireIOException if the set failure is caused by an error
   *   when writing to the system's configuration file.
   */
  public void setName(String value);
  /**
   * Returns true if the value of the <code>name</code> attribute can currently
   * be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isNameModifiable();
  /** The name of the "name" property */
  public static final String NAME_NAME = "name";

  /**
   * The default system name.
   * <p> Actual value of this constant is <code>""</code>.
   */
  public static final String DEFAULT_NAME = "";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#mcast-port">"mcast-port"</a>
   * property
   */
  public int getMcastPort();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#mcast-port">"mcast-port"</a>
   * property
   */
  public void setMcastPort(int value);
  /**
   * Returns true if the value of the
   * attribute can currently be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isMcastPortModifiable();

  /** The name of the "mcastPort" property */
  public static final String MCAST_PORT_NAME = "mcast-port";

  /** The default value of the "mcastPort" property */
  public static final int DEFAULT_MCAST_PORT = 10334;
  /**
   * The minimum mcastPort.
   * <p> Actual value of this constant is <code>0</code>.
   */
  public static final int MIN_MCAST_PORT = 0;
  /**
   * The maximum mcastPort.
   * <p> Actual value of this constant is <code>65535</code>.
   */
  public static final int MAX_MCAST_PORT = 65535;


  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#tcp-port">"tcp-port"</a>
   * property
   */
  public int getTcpPort();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#tcp-port">"tcp-port"</a>
   * property
   */
  public void setTcpPort(int value);
  /**
   * Returns true if the value of the
   * attribute can currently be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isTcpPortModifiable();
  /** The name of the "tcpPort" property */
  public static final String TCP_PORT_NAME = "tcp-port";
  /** The default value of the "tcpPort" property */
  public static final int DEFAULT_TCP_PORT = 0;
  /**
   * The minimum tcpPort.
   * <p> Actual value of this constant is <code>0</code>.
   */
  public static final int MIN_TCP_PORT = 0;
  /**
   * The maximum tcpPort.
   * <p> Actual value of this constant is <code>65535</code>.
   */
  public static final int MAX_TCP_PORT = 65535;


  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#mcast-address">"mcast-address"</a>
   * property
   */
  public InetAddress getMcastAddress();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#mcast-address">"mcast-address"</a>
   * property
   */
  public void setMcastAddress(InetAddress value);
  /**
   * Returns true if the value of the
   * attribute can currently be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isMcastAddressModifiable();

  /** The name of the "mcastAddress" property */
  public static final String MCAST_ADDRESS_NAME = "mcast-address";

  /** The default value of the "mcastAddress" property.
   * Current value is <code>239.192.81.1</code>
   */
  public static final InetAddress DEFAULT_MCAST_ADDRESS = AbstractDistributionConfig._getDefaultMcastAddress();

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#mcast-ttl">"mcast-ttl"</a>
   * property
   */
  public int getMcastTtl();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#mcast-ttl">"mcast-ttl"</a>
   * property
   */
  public void setMcastTtl(int value);
  /**
   * Returns true if the value of the
   * attribute can currently be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isMcastTtlModifiable();

  /** The name of the "mcastTtl" property */
  public static final String MCAST_TTL_NAME = "mcast-ttl";

  /** The default value of the "mcastTtl" property */
  public static final int DEFAULT_MCAST_TTL = 32;
  /**
   * The minimum mcastTtl.
   * <p> Actual value of this constant is <code>0</code>.
   */
  public static final int MIN_MCAST_TTL = 0;
  /**
   * The maximum mcastTtl.
   * <p> Actual value of this constant is <code>255</code>.
   */
  public static final int MAX_MCAST_TTL = 255;
  
  
  public static final int MIN_DISTRIBUTED_SYSTEM_ID = -1;
  
  public static final int MAX_DISTRIBUTED_SYSTEM_ID = 255;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#bind-address">"bind-address"</a>
   * property
   */
  public String getBindAddress();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#bind-address">"bind-address"</a>
   * property
   */
  public void setBindAddress(String value);
  /**
   * Returns true if the value of the
   * attribute can currently be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isBindAddressModifiable();

  /** The name of the "bindAddress" property */
  public static final String BIND_ADDRESS_NAME = "bind-address";

  /** The default value of the "bindAddress" property.
   * Current value is an empty string <code>""</code>
   */
  public static final String DEFAULT_BIND_ADDRESS = "";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-bind-address">"server-bind-address"</a>
   * property
   */
  public String getServerBindAddress();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-bind-address">"server-bind-address"</a>
   * property
   */
  public void setServerBindAddress(String value);
  /**
   * Returns true if the value of the
   * attribute can currently be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isServerBindAddressModifiable();

  /** The name of the "serverBindAddress" property */
  public static final String SERVER_BIND_ADDRESS_NAME = "server-bind-address";

  /** The default value of the "serverBindAddress" property.
   * Current value is an empty string <code>""</code>
   */
  public static final String DEFAULT_SERVER_BIND_ADDRESS = "";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#locators">"locators"</a> property
   */
  public String getLocators();
  /**
   * Sets the system's locator list.
   * A locator list is optional and by default empty.
   * Its used to by the system to locator other system nodes
   * and to publish itself so it can be located by others.
   * @param value must be of the form <code>hostName[portNum]</code>.
   *  Multiple elements are allowed and must be seperated by a comma.
   * @throws IllegalArgumentException if the specified value is not acceptable.
   * @throws com.gemstone.gemfire.UnmodifiableException if this attribute can not be modified.
   * @throws com.gemstone.gemfire.GemFireIOException if the set failure is caused by an error
   *   when writing to the system's configuration file.
   */
  public void setLocators(String value);
  /**
   * Returns true if the value of the
   * attribute can currently be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isLocatorsModifiable();

  /** The name of the "locators" property */
  public static final String LOCATORS_NAME = "locators";

  /** The default value of the "locators" property */
  public static final String DEFAULT_LOCATORS = "";

  /**
   * Locator wait time - how long to wait for a locator to start before giving up &
   * throwing a GemFireConfigException
   */
  public static final String LOCATOR_WAIT_TIME_NAME = "locator-wait-time";
  public static final int DEFAULT_LOCATOR_WAIT_TIME = 0; 
  public int getLocatorWaitTime();
  public void setLocatorWaitTime(int seconds);
  public boolean isLocatorWaitTimeModifiable();
  
  
  /**
   * returns the value of the <a href="../DistribytedSystem.html#start-locator">"start-locator"
   * </a> property
   */
  public String getStartLocator();
  /**
   * Sets the start-locators property.  This is a string in the form
   * bindAddress[port] and, if set, tells the distributed system to start
   * a locator prior to connecting
   * @param value must be of the form <code>hostName[portNum]</code>
   */
  public void setStartLocator(String value);
  /**
   * returns true if the value of the attribute can currently be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isStartLocatorModifiable();
  /**
   * The name of the "start-locators" property
   */
  public static final String START_LOCATOR_NAME = "start-locator";
  /**
   * The default value of the "start-locators" property
   */
  public static final String DEFAULT_START_LOCATOR = "";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#deploy-working-dir">"deploy-working-dir"</a> property
   */
  public File getDeployWorkingDir();
  
  /**
   * Sets the system's deploy working directory.
   * @throws IllegalArgumentException if the specified value is not acceptable.
   * @throws com.gemstone.gemfire.UnmodifiableException if this attribute can not be modified.
   * @throws com.gemstone.gemfire.GemFireIOException if the set failure is caused by an error
   *   when writing to the system's configuration file.
   */
  public void setDeployWorkingDir(File value);
  
  /**
   * Returns true if the value of the deploy-working-dir attribute can 
   * currently be modified. Some attributes can not be modified while the system is running.
   */
  public boolean isDeployWorkingDirModifiable();
  
  /**
   * The name of the "deploy-working-dir" property.
   */
  public static final String DEPLOY_WORKING_DIR = "deploy-working-dir";
  
  /**
   * Default will be the current working directory as determined by 
   * <code>System.getProperty("user.dir")</code>.
   */
  public static final File DEFAULT_DEPLOY_WORKING_DIR = new File(".");
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#user-command-packages">"user-command-packages"</a> property
   */
  public String getUserCommandPackages();
  
  /**
   * Sets the system's user command path.
   * @throws IllegalArgumentException if the specified value is not acceptable.
   * @throws com.gemstone.gemfire.UnmodifiableException if this attribute can not be modified.
   * @throws com.gemstone.gemfire.GemFireIOException if the set failure is caused by an error
   *   when writing to the system's configuration file.
   */
  public void setUserCommandPackages(String value);
  
  /**
   * Returns true if the value of the user-command-packages attribute can 
   * currently be modified. Some attributes can not be modified while the system is running.
   */
  public boolean isUserCommandPackagesModifiable();
  
  /**
   * The name of the "user-command-packages" property.
   */
  public static final String USER_COMMAND_PACKAGES = "user-command-packages";
  
  /**
   * The default value of the "user-command-packages" property
   */
  public static final String DEFAULT_USER_COMMAND_PACKAGES = "";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#log-file">"log-file"</a> property
   *
   * @return <code>null</code> if logging information goes to standard
   *         out
   */
  public File getLogFile();
  /**
   * Sets the system's log file.
   * <p> Non-absolute log files are relative to the system directory.
   * <p> The system log file can not be changed while the system is running.
   * @throws IllegalArgumentException if the specified value is not acceptable.
   * @throws com.gemstone.gemfire.UnmodifiableException if this attribute can not be modified.
   * @throws com.gemstone.gemfire.GemFireIOException if the set failure is caused by an error
   *   when writing to the system's configuration file.
   */
  public void setLogFile(File value);
  /**
   * Returns true if the value of the logFile attribute can currently
   * be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isLogFileModifiable();
  /** The name of the "logFile" property */
  public static final String LOG_FILE_NAME = "log-file";

  /**
   * The default log file.
   * <p> Actual value of this constant is <code>""</code> which directs
   * log message to standard output.
   */
  public static final File DEFAULT_LOG_FILE = new File("");

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#log-level">"log-level"</a> property
   *
   * @see com.gemstone.gemfire.internal.logging.LogWriterImpl
   */
  public int getLogLevel();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#log-level">"log-level"</a> property
   *
   * @see com.gemstone.gemfire.internal.logging.LogWriterImpl
   */
  public void setLogLevel(int value);

  /**
   * Returns true if the value of the logLevel attribute can currently
   * be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isLogLevelModifiable();

  /** The name of the "logLevel" property */
  public static final String LOG_LEVEL_NAME = "log-level";

  /**
   * The default log level.
   * <p> Actual value of this constant is {@link InternalLogWriter#CONFIG_LEVEL}.
   */
  public static final int DEFAULT_LOG_LEVEL = InternalLogWriter.CONFIG_LEVEL;
  /**
   * The minimum log level.
   * <p> Actual value of this constant is {@link InternalLogWriter#ALL_LEVEL}.
   */
  public static final int MIN_LOG_LEVEL = InternalLogWriter.ALL_LEVEL;
  /**
   * The maximum log level.
   * <p> Actual value of this constant is {@link InternalLogWriter#NONE_LEVEL}.
   */
  public static final int MAX_LOG_LEVEL = InternalLogWriter.NONE_LEVEL;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#statistic-sampling-enabled">"statistic-sampling-enabled"</a>
   * property
   */
  public boolean getStatisticSamplingEnabled();
  /**
   * Sets StatisticSamplingEnabled
   */
  public void setStatisticSamplingEnabled(boolean newValue);
  /**
   * Returns true if the value of the StatisticSamplingEnabled attribute can currently
   * be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isStatisticSamplingEnabledModifiable();
  /** The name of the "statisticSamplingEnabled" property */
  public static final String STATISTIC_SAMPLING_ENABLED_NAME =
    "statistic-sampling-enabled";

  /** The default value of the "statisticSamplingEnabled" property */
  public static final boolean DEFAULT_STATISTIC_SAMPLING_ENABLED = true;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#statistic-sample-rate">"statistic-sample-rate"</a>
   * property
   */
  public int getStatisticSampleRate();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#statistic-sample-rate">"statistic-sample-rate"</a>
   * property
   */
  public void setStatisticSampleRate(int value);
  /**
   * Returns true if the value of the statisticSampleRate attribute can currently
   * be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isStatisticSampleRateModifiable();
  /**
   * The default statistic sample rate.
   * <p> Actual value of this constant is <code>1000</code> milliseconds.
   */
  public static final int DEFAULT_STATISTIC_SAMPLE_RATE = 1000;
  /**
   * The minimum statistic sample rate.
   * <p> Actual value of this constant is <code>100</code> milliseconds.
   */
  public static final int MIN_STATISTIC_SAMPLE_RATE = 100;
  /**
   * The maximum statistic sample rate.
   * <p> Actual value of this constant is <code>60000</code> milliseconds.
   */
  public static final int MAX_STATISTIC_SAMPLE_RATE = 60000;

  /** The name of the "statisticSampleRate" property */
  public static final String STATISTIC_SAMPLE_RATE_NAME =
    "statistic-sample-rate";

  /**
   * Returns the value of the <a href="../DistributedSystem.html#statistic-archive-file">"statistic-archive-file"</a> property.
   *
   * @return <code>null</code> if no file was specified
   */
  public File getStatisticArchiveFile();
  /**
   * Sets the value of the <a href="../DistributedSystem.html#statistic-archive-file">"statistic-archive-file"</a> property.
   */
  public void setStatisticArchiveFile(File value);
  /**
   * Returns true if the value of the statisticArchiveFile attribute can currently
   * be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isStatisticArchiveFileModifiable();
  /** The name of the "statisticArchiveFile" property */
  public static final String STATISTIC_ARCHIVE_FILE_NAME =
    "statistic-archive-file";

  /**
   * The default statistic archive file.
   * <p> Actual value of this constant is <code>""</code> which
   * causes no archive file to be created.
   */
  public static final File DEFAULT_STATISTIC_ARCHIVE_FILE = new File(""); // fix for bug 29786


  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cache-xml-file">"cache-xml-file"</a>
   * property
   */
  public File getCacheXmlFile();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cache-xml-file">"cache-xml-file"</a>
   * property
   */
  public void setCacheXmlFile(File value);
  /**
   * Returns true if the value of the
   * attribute can currently be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isCacheXmlFileModifiable();
  /** The name of the "cacheXmlFile" property */
  public static final String CACHE_XML_FILE_NAME = "cache-xml-file";

  /** The default value of the "cacheXmlFile" property */
  public static final File DEFAULT_CACHE_XML_FILE = new File("cache.xml");

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#ack-wait-threshold">"ack-wait-threshold"</a>
   * property
   */
  public int getAckWaitThreshold();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#ack-wait-threshold">"ack-wait-threshold"</a>
   * property
     * Setting this value too low will cause spurious alerts.
     */
  public void setAckWaitThreshold(int newThreshold);
  /**
   * Returns true if the value of the AckWaitThreshold attribute can currently
   * be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isAckWaitThresholdModifiable();
  /** The name of the "ackWaitThreshold" property */
  public static final String ACK_WAIT_THRESHOLD_NAME = "ack-wait-threshold";

  /**
   * The default AckWaitThreshold.
   * <p> Actual value of this constant is <code>15</code> seconds.
   */
  public static final int DEFAULT_ACK_WAIT_THRESHOLD = 15;
  /**
   * The minimum AckWaitThreshold.
   * <p> Actual value of this constant is <code>1</code> second.
   */
  public static final int MIN_ACK_WAIT_THRESHOLD = 1;
  /**
   * The maximum AckWaitThreshold.
   * <p> Actual value of this constant is <code>MAX_INT</code> seconds.
   */
  public static final int MAX_ACK_WAIT_THRESHOLD = Integer.MAX_VALUE;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#ack-severe-alert-threshold">"ack-severe-alert-threshold"</a>
   * property
   */
  public int getAckSevereAlertThreshold();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#ack-severe-alert-threshold">"ack-severe-alert-threshold"</a>
   * property
     * Setting this value too low will cause spurious forced disconnects.
     */
  public void setAckSevereAlertThreshold(int newThreshold);
  /**
   * Returns true if the value of the ackSevereAlertThreshold attribute can currently
   * be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isAckSevereAlertThresholdModifiable();
  /** The name of the "ackSevereAlertThreshold" property */
  public static final String ACK_SEVERE_ALERT_THRESHOLD_NAME = "ack-severe-alert-threshold";
  /**
   * The default ackSevereAlertThreshold.
   * <p> Actual value of this constant is <code>0</code> seconds, which
   * turns off shunning.
   */
  public static final int DEFAULT_ACK_SEVERE_ALERT_THRESHOLD = 0;
  /**
   * The minimum ackSevereAlertThreshold.
   * <p> Actual value of this constant is <code>0</code> second,
   * which turns off shunning.
   */
  public static final int MIN_ACK_SEVERE_ALERT_THRESHOLD = 0;
  /**
   * The maximum ackSevereAlertThreshold.
   * <p> Actual value of this constant is <code>MAX_INT</code> seconds.
   */
  public static final int MAX_ACK_SEVERE_ALERT_THRESHOLD = Integer.MAX_VALUE;


  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#archive-file-size-limit">"archive-file-size-limit"</a>
   * property
   */
  public int getArchiveFileSizeLimit();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#archive-file-size-limit">"archive-file-size-limit"</a>
   * property
   */
  public void setArchiveFileSizeLimit(int value);
  /**
   * Returns true if the value of the ArchiveFileSizeLimit attribute can currently
   * be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isArchiveFileSizeLimitModifiable();
  /**
   * The default statistic archive file size limit.
   * <p> Actual value of this constant is <code>0</code> megabytes.
   */
  public static final int DEFAULT_ARCHIVE_FILE_SIZE_LIMIT = 0;
  /**
   * The minimum statistic archive file size limit.
   * <p> Actual value of this constant is <code>0</code> megabytes.
   */
  public static final int MIN_ARCHIVE_FILE_SIZE_LIMIT = 0;
  /**
   * The maximum statistic archive file size limit.
   * <p> Actual value of this constant is <code>1000000</code> megabytes.
   */
  public static final int MAX_ARCHIVE_FILE_SIZE_LIMIT = 1000000;

  /** The name of the "ArchiveFileSizeLimit" property */
  public static final String ARCHIVE_FILE_SIZE_LIMIT_NAME =
    "archive-file-size-limit";
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#archive-disk-space-limit">"archive-disk-space-limit"</a>
   * property
   */
  public int getArchiveDiskSpaceLimit();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#archive-disk-space-limit">"archive-disk-space-limit"</a>
   * property
   */
  public void setArchiveDiskSpaceLimit(int value);
  /**
   * Returns true if the value of the ArchiveDiskSpaceLimit attribute can currently
   * be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isArchiveDiskSpaceLimitModifiable();
  /**
   * The default archive disk space limit.
   * <p> Actual value of this constant is <code>0</code> megabytes.
   */
  public static final int DEFAULT_ARCHIVE_DISK_SPACE_LIMIT = 0;
  /**
   * The minimum archive disk space limit.
   * <p> Actual value of this constant is <code>0</code> megabytes.
   */
  public static final int MIN_ARCHIVE_DISK_SPACE_LIMIT = 0;
  /**
   * The maximum archive disk space limit.
   * <p> Actual value of this constant is <code>1000000</code> megabytes.
   */
  public static final int MAX_ARCHIVE_DISK_SPACE_LIMIT = 1000000;

  /** The name of the "ArchiveDiskSpaceLimit" property */
  public static final String ARCHIVE_DISK_SPACE_LIMIT_NAME =
    "archive-disk-space-limit";
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#log-file-size-limit">"log-file-size-limit"</a>
   * property
   */
  public int getLogFileSizeLimit();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#log-file-size-limit">"log-file-size-limit"</a>
   * property
   */
  public void setLogFileSizeLimit(int value);
  /**
   * Returns true if the value of the LogFileSizeLimit attribute can currently
   * be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isLogFileSizeLimitModifiable();
  /**
   * The default log file size limit.
   * <p> Actual value of this constant is <code>0</code> megabytes.
   */
  public static final int DEFAULT_LOG_FILE_SIZE_LIMIT = 0;
  /**
   * The minimum log file size limit.
   * <p> Actual value of this constant is <code>0</code> megabytes.
   */
  public static final int MIN_LOG_FILE_SIZE_LIMIT = 0;
  /**
   * The maximum log file size limit.
   * <p> Actual value of this constant is <code>1000000</code> megabytes.
   */
  public static final int MAX_LOG_FILE_SIZE_LIMIT = 1000000;

  /** The name of the "LogFileSizeLimit" property */
  public static final String LOG_FILE_SIZE_LIMIT_NAME =
    "log-file-size-limit";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#log-disk-space-limit">"log-disk-space-limit"</a>
   * property
   */
  public int getLogDiskSpaceLimit();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#log-disk-space-limit">"log-disk-space-limit"</a>
   * property
   */
  public void setLogDiskSpaceLimit(int value);
  /**
   * Returns true if the value of the LogDiskSpaceLimit attribute can currently
   * be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isLogDiskSpaceLimitModifiable();
  /**
   * The default log disk space limit.
   * <p> Actual value of this constant is <code>0</code> megabytes.
   */
  public static final int DEFAULT_LOG_DISK_SPACE_LIMIT = 0;
  /**
   * The minimum log disk space limit.
   * <p> Actual value of this constant is <code>0</code> megabytes.
   */
  public static final int MIN_LOG_DISK_SPACE_LIMIT = 0;
  /**
   * The maximum log disk space limit.
   * <p> Actual value of this constant is <code>1000000</code> megabytes.
   */
  public static final int MAX_LOG_DISK_SPACE_LIMIT = 1000000;

  /** The name of the "LogDiskSpaceLimit" property */
  public static final String LOG_DISK_SPACE_LIMIT_NAME =
    "log-disk-space-limit";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#ssl-enabled">"ssl-enabled"</a>
   * property.
   * @deprecated as of 8.0 use {@link #getClusterSSLEnabled} instead.
   */
  public boolean getSSLEnabled();

  /**
   * The default ssl-enabled state.
   * <p> Actual value of this constant is <code>false</code>.
   * @deprecated as of 8.0 use {@link #DEFAULT_CLUSTER_SSL_ENABLED} instead.
   */
  public static final boolean DEFAULT_SSL_ENABLED = false;
  
  /** The name of the "SSLEnabled" property 
   * @deprecated as of 8.0 use {@link #CLUSTER_SSL_ENABLED_NAME} instead.
   */
  public static final String SSL_ENABLED_NAME =
    "ssl-enabled";

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#ssl-enabled">"ssl-enabled"</a>
   * property.
   * @deprecated as of 8.0 use {@link #setClusterSSLEnabled} instead.
   */
  public void setSSLEnabled( boolean enabled );

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#ssl-protocols">"ssl-protocols"</a>
   * property.
   * @deprecated as of 8.0 use {@link #getClusterSSLProtocols} instead.
   */
   public String getSSLProtocols( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#ssl-protocols">"ssl-protocols"</a>
   * property.
   * @deprecated as of 8.0 use {@link #setClusterSSLProtocols} instead.
   */
   public void setSSLProtocols( String protocols );

  /**
   * The default ssl-protocols value.
   * <p> Actual value of this constant is <code>any</code>.
   * @deprecated as of 8.0 use {@link #DEFAULT_CLUSTER_SSL_PROTOCOLS} instead.
   */
  public static final String DEFAULT_SSL_PROTOCOLS = "any";
  /** The name of the "SSLProtocols" property 
   * @deprecated as of 8.0 use {@link #CLUSTER_SSL_PROTOCOLS_NAME} instead.
   */
  public static final String SSL_PROTOCOLS_NAME =
    "ssl-protocols";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#ssl-ciphers">"ssl-ciphers"</a>
   * property.
   * @deprecated as of 8.0 use {@link #getClusterSSLCiphers} instead.
   */
   public String getSSLCiphers( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#ssl-ciphers">"ssl-ciphers"</a>
   * property.
   * @deprecated as of 8.0 use {@link #setClusterSSLCiphers} instead.
   */
   public void setSSLCiphers( String ciphers );

   /**
   * The default ssl-ciphers value.
   * <p> Actual value of this constant is <code>any</code>.
   * @deprecated as of 8.0 use {@link #DEFAULT_CLUSTER_SSL_CIPHERS} instead.
   */
  public static final String DEFAULT_SSL_CIPHERS = "any";
  /** The name of the "SSLCiphers" property 
   * @deprecated as of 8.0 use {@link #CLUSTER_SSL_CIPHERS_NAME} instead.
   */
  public static final String SSL_CIPHERS_NAME =
    "ssl-ciphers";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#ssl-require-authentication">"ssl-require-authentication"</a>
   * property.
   * @deprecated as of 8.0 use {@link #getClusterSSLRequireAuthentication} instead.
   */
   public boolean getSSLRequireAuthentication( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#ssl-require-authentication">"ssl-require-authentication"</a>
   * property.
   * @deprecated as of 8.0 use {@link #setClusterSSLRequireAuthentication} instead.
   */
   public void setSSLRequireAuthentication( boolean enabled );

   /**
   * The default ssl-require-authentication value.
   * <p> Actual value of this constant is <code>true</code>.
   * @deprecated as of 8.0 use {@link #DEFAULT_CLUSTER_SSL_REQUIRE_AUTHENTICATION} instead.
   */
  public static final boolean DEFAULT_SSL_REQUIRE_AUTHENTICATION = true;
  /** The name of the "SSLRequireAuthentication" property 
   * @deprecated as of 8.0 use {@link #CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME} instead.
   */
  public static final String SSL_REQUIRE_AUTHENTICATION_NAME =
    "ssl-require-authentication";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-enabled">"cluster-ssl-enabled"</a>
   * property.
   */
  public boolean getClusterSSLEnabled();

  /**
   * The default cluster-ssl-enabled state.
   * <p> Actual value of this constant is <code>false</code>.
   */
  public static final boolean DEFAULT_CLUSTER_SSL_ENABLED = false;
    /** The name of the "ClusterSSLEnabled" property */
  public static final String CLUSTER_SSL_ENABLED_NAME =
    "cluster-ssl-enabled";

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-enabled">"cluster-ssl-enabled"</a>
   * property.
   */
  public void setClusterSSLEnabled( boolean enabled );

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-protocols">"cluster-ssl-protocols"</a>
   * property.
   */
   public String getClusterSSLProtocols( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-protocols">"cluster-ssl-protocols"</a>
   * property.
   */
   public void setClusterSSLProtocols( String protocols );

  /**
   * The default cluster-ssl-protocols value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  public static final String DEFAULT_CLUSTER_SSL_PROTOCOLS = "any";
  /** The name of the "ClusterSSLProtocols" property */
  public static final String CLUSTER_SSL_PROTOCOLS_NAME =
    "cluster-ssl-protocols";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-ciphers">"cluster-ssl-ciphers"</a>
   * property.
   */
   public String getClusterSSLCiphers( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-ciphers">"cluster-ssl-ciphers"</a>
   * property.
   */
   public void setClusterSSLCiphers( String ciphers );

   /**
   * The default cluster-ssl-ciphers value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  public static final String DEFAULT_CLUSTER_SSL_CIPHERS = "any";
  /** The name of the "ClusterSSLCiphers" property */
  public static final String CLUSTER_SSL_CIPHERS_NAME =
    "cluster-ssl-ciphers";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-require-authentication">"cluster-ssl-require-authentication"</a>
   * property.
   */
   public boolean getClusterSSLRequireAuthentication( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-require-authentication">"cluster-ssl-require-authentication"</a>
   * property.
   */
   public void setClusterSSLRequireAuthentication( boolean enabled );

   /**
   * The default cluster-ssl-require-authentication value.
   * <p> Actual value of this constant is <code>true</code>.
   */
  public static final boolean DEFAULT_CLUSTER_SSL_REQUIRE_AUTHENTICATION = true;
  /** The name of the "ClusterSSLRequireAuthentication" property */
  public static final String CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME =
    "cluster-ssl-require-authentication";

  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-keystore">"cluster-ssl-keystore"</a>
   * property.
   */
  public String getClusterSSLKeyStore( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-keystore">"cluster-ssl-keystore"</a>
   * property.
   */
  public void setClusterSSLKeyStore( String keyStore);
  
  /**
   * The default cluster-ssl-keystore value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_CLUSTER_SSL_KEYSTORE = "";
  
  /** The name of the "ClusterSSLKeyStore" property */
  public static final String CLUSTER_SSL_KEYSTORE_NAME = "cluster-ssl-keystore";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-keystore-type">"cluster-ssl-keystore-type"</a>
   * property.
   */
  public String getClusterSSLKeyStoreType( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-keystore-type">"cluster-ssl-keystore-type"</a>
   * property.
   */
  public void setClusterSSLKeyStoreType( String keyStoreType);
  
  /**
   * The default cluster-ssl-keystore-type value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_CLUSTER_SSL_KEYSTORE_TYPE = "";
  
  /** The name of the "ClusterSSLKeyStoreType" property */
  public static final String CLUSTER_SSL_KEYSTORE_TYPE_NAME = "cluster-ssl-keystore-type";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-keystore-password">"cluster-ssl-keystore-password"</a>
   * property.
   */
  public String getClusterSSLKeyStorePassword( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-keystore-password">"cluster-ssl-keystore-password"</a>
   * property.
   */
  public void setClusterSSLKeyStorePassword( String keyStorePassword);
  
  /**
   * The default cluster-ssl-keystore-password value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_CLUSTER_SSL_KEYSTORE_PASSWORD = "";
  
  /** The name of the "ClusterSSLKeyStorePassword" property */
  public static final String CLUSTER_SSL_KEYSTORE_PASSWORD_NAME = "cluster-ssl-keystore-password";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-truststore">"cluster-ssl-truststore"</a>
   * property.
   */
  public String getClusterSSLTrustStore( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-truststore">"cluster-ssl-truststore"</a>
   * property.
   */
  public void setClusterSSLTrustStore( String trustStore);
  
  /**
   * The default cluster-ssl-truststore value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_CLUSTER_SSL_TRUSTSTORE = "";
  
  /** The name of the "ClusterSSLTrustStore" property */
  public static final String CLUSTER_SSL_TRUSTSTORE_NAME = "cluster-ssl-truststore";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-truststore-password">"cluster-ssl-truststore-password"</a>
   * property.
   */
  public String getClusterSSLTrustStorePassword( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-truststore-password">"cluster-ssl-truststore-password"</a>
   * property.
   */
  public void setClusterSSLTrustStorePassword( String trusStorePassword);
  /**
   * The default cluster-ssl-truststore-password value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_CLUSTER_SSL_TRUSTSTORE_PASSWORD = "";
  
  /** The name of the "ClusterSSLKeyStorePassword" property */
  public static final String CLUSTER_SSL_TRUSTSTORE_PASSWORD_NAME = "cluster-ssl-truststore-password";
  
  
  /** The name of an internal property that specifies a {@link
   * com.gemstone.gemfire.i18n.LogWriterI18n} instance to log to.
   *  Set this property with put(), not with setProperty()
   *
   * @since 4.0 */
  public static final String LOG_WRITER_NAME = "log-writer";

  /** The name of an internal property that specifies a 
   *  a DistributionConfigImpl that the locator is passing
   *  in to a ds connect.
   *  Set this property with put(), not with setProperty()
   *
   * @since 7.0 */
  public static final String DS_CONFIG_NAME = "ds-config";
  
  /**
   * The name of an internal property that specifies whether
   * the distributed system is reconnecting after a forced-
   * disconnect.
   * @since 8.1
   */
  public static final String DS_RECONNECTING_NAME = "ds-reconnecting";
  
  /**
   * The name of an internal property that specifies the
   * quorum checker for the system that was forcibly disconnected.
   * This should be used if the DS_RECONNECTING_NAME property
   * is used.
   */
  public static final String DS_QUORUM_CHECKER_NAME = "ds-quorum-checker";
  
  /**
   * The name of an internal property that specifies a {@link
   * com.gemstone.gemfire.LogWriter} instance to log security messages to. Set
   * this property with put(), not with setProperty()
   *
   * @since 5.5
   */
  public static final String SECURITY_LOG_WRITER_NAME = "security-log-writer";

  /** The name of an internal property that specifies a
   * FileOutputStream associated with the internal property
   * LOG_WRITER_NAME.  If this property is set, the
   * FileOutputStream will be closed when the distributed
   * system disconnects.  Set this property with put(), not
   * with setProperty()
   * @since 5.0
   */
  public static final String LOG_OUTPUTSTREAM_NAME = "log-output-stream";

  /**
   * The name of an internal property that specifies a FileOutputStream
   * associated with the internal property SECURITY_LOG_WRITER_NAME. If this
   * property is set, the FileOutputStream will be closed when the distributed
   * system disconnects. Set this property with put(), not with setProperty()
   *
   * @since 5.5
   */
  public static final String SECURITY_LOG_OUTPUTSTREAM_NAME = "security-log-output-stream";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#socket-lease-time">"socket-lease-time"</a>
   * property
   */
  public int getSocketLeaseTime();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#socket-lease-time">"socket-lease-time"</a>
   * property
   */
  public void setSocketLeaseTime(int value);
  /**
   * Returns true if the value of the
   * attribute can currently be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isSocketLeaseTimeModifiable();

  /** The name of the "socketLeaseTime" property */
  public static final String SOCKET_LEASE_TIME_NAME = "socket-lease-time";

  /** The default value of the "socketLeaseTime" property */
  public static final int DEFAULT_SOCKET_LEASE_TIME = 60000;
  /**
   * The minimum socketLeaseTime.
   * <p> Actual value of this constant is <code>0</code>.
   */
  public static final int MIN_SOCKET_LEASE_TIME = 0;
  /**
   * The maximum socketLeaseTime.
   * <p> Actual value of this constant is <code>600000</code>.
   */
  public static final int MAX_SOCKET_LEASE_TIME = 600000;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#socket-buffer-size">"socket-buffer-size"</a>
   * property
   */
  public int getSocketBufferSize();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#socket-buffer-size">"socket-buffer-size"</a>
   * property
   */
  public void setSocketBufferSize(int value);
  /**
   * Returns true if the value of the
   * attribute can currently be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isSocketBufferSizeModifiable();

  /** The name of the "socketBufferSize" property */
  public static final String SOCKET_BUFFER_SIZE_NAME = "socket-buffer-size";

  /** The default value of the "socketBufferSize" property */
  public static final int DEFAULT_SOCKET_BUFFER_SIZE = 32768;
  /**
   * The minimum socketBufferSize.
   * <p> Actual value of this constant is <code>1024</code>.
   */
  public static final int MIN_SOCKET_BUFFER_SIZE = 1024;
  /**
   * The maximum socketBufferSize.
   * <p> Actual value of this constant is <code>20000000</code>.
   */
  public static final int MAX_SOCKET_BUFFER_SIZE = Connection.MAX_MSG_SIZE;

  public static final boolean VALIDATE = Boolean.getBoolean("gemfire.validateMessageSize");
  public static final int VALIDATE_CEILING = Integer.getInteger("gemfire.validateMessageSizeCeiling",  8 * 1024 * 1024).intValue();

  /**
   * Get the value of the
   * <a href="../DistributedSystem.html#mcast-send-buffer-size">"mcast-send-buffer-size"</a>
   * property
   */
  public int getMcastSendBufferSize();

  /**
   * Set the value of the
   * <a href="../DistributedSystem.html#mcast-send-buffer-size">"mcast-send-buffer-size"</a>
   * property
   */
  public void setMcastSendBufferSize(int value);

  /**
   * Returns true if the corresponding property is currently modifiable.
   * Some attributes can't be modified while a DistributedSystem is running.
   */
  public boolean isMcastSendBufferSizeModifiable();

  /**
   * The name of the corresponding property
   */
  public static final String MCAST_SEND_BUFFER_SIZE_NAME = "mcast-send-buffer-size";

  /**
   * The default value of the corresponding property
   */
  public static final int DEFAULT_MCAST_SEND_BUFFER_SIZE = 65535;


  /**
   * The minimum size of the buffer, in bytes.
   * <p> Actual value of this constant is <code>2048</code>.
   */
  public static final int MIN_MCAST_SEND_BUFFER_SIZE = 2048;

  /**
   * Get the value of the
   * <a href="../DistributedSystem.html#mcast-recv-buffer-size">"mcast-recv-buffer-size"</a>
   * property
   */
  public int getMcastRecvBufferSize();

  /**
   * Set the value of the
   * <a href="../DistributedSystem.html#mcast-recv-buffer-size">"mcast-recv-buffer-size"</a>
   * property
   */
  public void setMcastRecvBufferSize(int value);

  /**
   * Returns true if the corresponding property is currently modifiable.
   * Some attributes can't be modified while a DistributedSystem is running.
   */
  public boolean isMcastRecvBufferSizeModifiable();

  /**
   * The name of the corresponding property
   */
  public static final String MCAST_RECV_BUFFER_SIZE_NAME = "mcast-recv-buffer-size";

  /**
   * The default value of the corresponding property
   */
  public static final int DEFAULT_MCAST_RECV_BUFFER_SIZE = 1048576;

  /**
   * The minimum size of the buffer, in bytes.
   * <p> Actual value of this constant is <code>2048</code>.
   */
  public static final int MIN_MCAST_RECV_BUFFER_SIZE = 2048;




  /**
   * Get the value of the
   * <a href="../DistributedSystem.html#mcast-flow-control">"mcast-flow-control"</a>
   * property.
   */
  public FlowControlParams getMcastFlowControl();

  /**
   * Set the value of the
   * <a href="../DistributedSystem.html#mcast-flow-control">"mcast-flow-control"</a>
   * property
   */
  public void setMcastFlowControl(FlowControlParams values);

  /**
   * Returns true if the corresponding property is currently modifiable.
   * Some attributes can't be modified while a DistributedSystem is running.
   */
  public boolean isMcastFlowControlModifiable();

  /**
   * The name of the corresponding property
   */
  public static final String MCAST_FLOW_CONTROL_NAME = "mcast-flow-control";

  /**
   * The default value of the corresponding property
   */
  public static final FlowControlParams DEFAULT_MCAST_FLOW_CONTROL
    = new FlowControlParams(1048576, (float)0.25, 5000);

  /**
   * The minimum byteAllowance for the mcast-flow-control setting of
   * <code>100000</code>.
   */
  public static final int MIN_FC_BYTE_ALLOWANCE = 10000;

  /**
   * The minimum rechargeThreshold for the mcast-flow-control setting of
   * <code>0.1</code>
   */
  public static final float MIN_FC_RECHARGE_THRESHOLD = (float)0.1;

  /**
   * The maximum rechargeThreshold for the mcast-flow-control setting of
   * <code>0.5</code>
   */
  public static final float MAX_FC_RECHARGE_THRESHOLD = (float)0.5;

  /**
   * The minimum rechargeBlockMs for the mcast-flow-control setting of
   * <code>500</code>
   */
  public static final int MIN_FC_RECHARGE_BLOCK_MS = 500;

  /**
   * The maximum rechargeBlockMs for the mcast-flow-control setting of
   * <code>60000</code>
   */
  public static final int MAX_FC_RECHARGE_BLOCK_MS = 60000;

  /**
   * Get the value of the
   * <a href="../DistributedSystem.html#udp-fragment-size">"udp-fragment-size"</a>
   * property.
   */
  public int getUdpFragmentSize();

  /**
   * Set the value of the
   * <a href="../DistributedSystem.html#udp-fragment-size">"udp-fragment-size"</a>
   * property
   */
  public void setUdpFragmentSize(int value);

  /**
   * Returns true if the corresponding property is currently modifiable.
   * Some attributes can't be modified while a DistributedSystem is running.
   */
  public boolean isUdpFragmentSizeModifiable();

  /**
   * The name of the corresponding property
   */
  public static final String UDP_FRAGMENT_SIZE_NAME = "udp-fragment-size";

  /**
   * The default value of the corresponding property
   */
  public static final int DEFAULT_UDP_FRAGMENT_SIZE = 60000;

  /** The minimum allowed udp-fragment-size setting of 1000
  */
  public static final int MIN_UDP_FRAGMENT_SIZE = 1000;

  /** The maximum allowed udp-fragment-size setting of 60000
  */
  public static final int MAX_UDP_FRAGMENT_SIZE = 60000;






  /**
   * Get the value of the
   * <a href="../DistributedSystem.html#udp-send-buffer-size">"udp-send-buffer-size"</a>
   * property
   */
  public int getUdpSendBufferSize();

  /**
   * Set the value of the
   * <a href="../DistributedSystem.html#udp-send-buffer-size">"udp-send-buffer-size"</a>
   * property
   */
  public void setUdpSendBufferSize(int value);

  /**
   * Returns true if the corresponding property is currently modifiable.
   * Some attributes can't be modified while a DistributedSystem is running.
   */
  public boolean isUdpSendBufferSizeModifiable();

  /**
   * The name of the corresponding property
   */
  public static final String UDP_SEND_BUFFER_SIZE_NAME = "udp-send-buffer-size";

  /**
   * The default value of the corresponding property
   */
  public static final int DEFAULT_UDP_SEND_BUFFER_SIZE = 65535;

  /**
   * The minimum size of the buffer, in bytes.
   * <p> Actual value of this constant is <code>2048</code>.
   */
  public static final int MIN_UDP_SEND_BUFFER_SIZE = 2048;



  /**
   * Get the value of the
   * <a href="../DistributedSystem.html#udp-recv-buffer-size">"udp-recv-buffer-size"</a>
   * property
   */
  public int getUdpRecvBufferSize();

  /**
   * Set the value of the
   * <a href="../DistributedSystem.html#udp-recv-buffer-size">"udp-recv-buffer-size"</a>
   * property
   */
  public void setUdpRecvBufferSize(int value);

  /**
   * Returns true if the corresponding property is currently modifiable.
   * Some attributes can't be modified while a DistributedSystem is running.
   */
  public boolean isUdpRecvBufferSizeModifiable();

  /**
   * The name of the corresponding property
   */
  public static final String UDP_RECV_BUFFER_SIZE_NAME = "udp-recv-buffer-size";

  /**
   * The default value of the unicast receive buffer size property
   */
  public static final int DEFAULT_UDP_RECV_BUFFER_SIZE = 1048576;

  /**
   * The default size of the unicast receive buffer property if tcp/ip sockets are
   * enabled and multicast is disabled
   */
  public static final int DEFAULT_UDP_RECV_BUFFER_SIZE_REDUCED = 65535;

  /**
   * The minimum size of the buffer, in bytes.
   * <p> Actual value of this constant is <code>2048</code>.
   */
  public static final int MIN_UDP_RECV_BUFFER_SIZE = 2048;


  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#disable-tcp">"disable-tcp"</a>
   * property
   */
  public boolean getDisableTcp();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#disable-tcp">"disable-tcp"</a>
   * property.
   */
  public void setDisableTcp(boolean newValue);
  /**
   * Returns true if the value of the DISABLE_TCP attribute can currently
   * be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isDisableTcpModifiable();
  /** The name of the corresponding property */
  public static final String DISABLE_TCP_NAME = "disable-tcp";

  /** The default value of the corresponding property */
  public static final boolean DEFAULT_DISABLE_TCP = false;


  /**
   * Turns on timing statistics for the distributed system
   */
  public void setEnableTimeStatistics(boolean newValue);

  /**
   * Returns the value of <a
   * href="../DistributedSystem.html#enable-time-statistics">enable-time-statistics</a>
   * property
   */
  public boolean getEnableTimeStatistics();

  /** the name of the corresponding property */
  public static final String ENABLE_TIME_STATISTICS_NAME = "enable-time-statistics";

  /** The default value of the corresponding property */
  public static final boolean DEFAULT_ENABLE_TIME_STATISTICS = false;


  /**
   * Sets the value for 
   <a href="../DistributedSystem.html#use-cluster-configuration">use-shared-configuration</a>
   */
  
  public void setUseSharedConfiguration(boolean newValue);

  /**
   * Returns the value of <a
   * href="../DistributedSystem.html#use-cluster-configuration">use-cluster-configuration</a>
   * property
   */
  public boolean getUseSharedConfiguration();

  /** the name of the corresponding property */
  public static final String USE_CLUSTER_CONFIGURATION_NAME = "use-cluster-configuration";

  /** The default value of the corresponding property */
  public static final boolean DEFAULT_USE_CLUSTER_CONFIGURATION = true;
  
  /**
   * Sets the value for 
   <a href="../DistributedSystem.html#enable-cluster-configuration">enable-cluster-configuration</a>
   */
  
  public void setEnableClusterConfiguration(boolean newValue);

  /**
   * Returns the value of <a
   * href="../DistributedSystem.html#enable-cluster-configuration">enable-cluster-configuration</a>
   * property
   */
  public boolean getEnableClusterConfiguration();

  /** the name of the corresponding property */
  public static final String ENABLE_CLUSTER_CONFIGURATION_NAME = "enable-cluster-configuration";
  /** The default value of the corresponding property */
  public static final boolean DEFAULT_ENABLE_CLUSTER_CONFIGURATION = true;
  
  public static final String LOAD_CLUSTER_CONFIG_FROM_DIR_NAME = "load-cluster-configuration-from-dir";
  public static final boolean DEFAULT_LOAD_CLUSTER_CONFIG_FROM_DIR = false;
  
  /**
   * Returns the value of 
   * <a href="../DistributedSystem.html#cluster-configuration-dir">cluster-configuration-dir</a>
   * property
   */
  public boolean getLoadClusterConfigFromDir();
  
  /**
   * Sets the value of 
   * <a href="../DistributedSystem.html#cluster-configuration-dir">cluster-configuration-dir</a>
   * property
   */
  public void setLoadClusterConfigFromDir(boolean newValue);
  
  public static final String CLUSTER_CONFIGURATION_DIR = "cluster-configuration-dir";
  public static final String DEFAULT_CLUSTER_CONFIGURATION_DIR = System.getProperty("user.dir");
  
  public String getClusterConfigDir(); 
  public void setClusterConfigDir(final String clusterConfigDir);
  
  /** Turns on network partition detection */
  public void setEnableNetworkPartitionDetection(boolean newValue);
  /**
   * Returns the value of the enable-network-partition-detection property
   */
  public boolean getEnableNetworkPartitionDetection();
  /** the name of the corresponding property */
  public static final String ENABLE_NETWORK_PARTITION_DETECTION_NAME =
    "enable-network-partition-detection";
  public static final boolean DEFAULT_ENABLE_NETWORK_PARTITION_DETECTION = false;

  /**
   * Get the value of the
   * <a href="../DistributedSystem.html#member-timeout">"member-timeout"</a>
   * property
   */
  public int getMemberTimeout();

  /**
   * Set the value of the
   * <a href="../DistributedSystem.html#member-timeout">"member-timeout"</a>
   * property
   */
  public void setMemberTimeout(int value);

  /**
   * Returns true if the corresponding property is currently modifiable.
   * Some attributes can't be modified while a DistributedSystem is running.
   */
  public boolean isMemberTimeoutModifiable();

  /**
   * The name of the corresponding property
   */
  public static final String MEMBER_TIMEOUT_NAME = "member-timeout";

  /**
   * The default value of the corresponding property
   */
  public static final int DEFAULT_MEMBER_TIMEOUT = 5000;

  /** The minimum member-timeout setting of 1000 milliseconds */
  public static final int MIN_MEMBER_TIMEOUT = 10;

  /**The maximum member-timeout setting of 600000 millieseconds */
  public static final int MAX_MEMBER_TIMEOUT = 600000;


  public static final String MEMBERSHIP_PORT_RANGE_NAME = "membership-port-range";
  
  public static final int[] DEFAULT_MEMBERSHIP_PORT_RANGE = new int[]{1024,65535};
  
  public int[] getMembershipPortRange();
  
  public void setMembershipPortRange(int[] range);
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#conserve-sockets">"conserve-sockets"</a>
   * property
   */
  public boolean getConserveSockets();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#conserve-sockets">"conserve-sockets"</a>
   * property.
   */
  public void setConserveSockets(boolean newValue);
  /**
   * Returns true if the value of the ConserveSockets attribute can currently
   * be modified.
   * Some attributes can not be modified while the system is running.
   */
  public boolean isConserveSocketsModifiable();
  /** The name of the "conserveSockets" property */
  public static final String CONSERVE_SOCKETS_NAME = "conserve-sockets";

  /** The default value of the "conserveSockets" property */
  public static final boolean DEFAULT_CONSERVE_SOCKETS = true;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#roles">"roles"</a>
   * property
   */
  public String getRoles();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#roles">"roles"</a>
   * property.
   */
  public void setRoles(String roles);
  /** The name of the "roles" property */
  public static final String ROLES_NAME = "roles";
  /** The default value of the "roles" property */
  public static final String DEFAULT_ROLES = "";


  /**
   * The name of the "max wait time for reconnect" property */
  public static final String MAX_WAIT_TIME_FOR_RECONNECT_NAME = "max-wait-time-reconnect";

  /**
   * Default value for MAX_WAIT_TIME_FOR_RECONNECT, 60,000 milliseconds.
   */
  public static final int DEFAULT_MAX_WAIT_TIME_FOR_RECONNECT = 60000;

  /**
   * Sets the max wait timeout, in milliseconds, for reconnect.
   * */
  public void setMaxWaitTimeForReconnect( int timeOut);

  /**
   * Returns the max wait timeout, in milliseconds, for reconnect.
   * */
  public int getMaxWaitTimeForReconnect();

  /**
   * The name of the "max number of tries for reconnect" property.
   * */
  public static final String MAX_NUM_RECONNECT_TRIES = "max-num-reconnect-tries";

  /**
   * Default value for MAX_NUM_RECONNECT_TRIES.
   */
  public static final int DEFAULT_MAX_NUM_RECONNECT_TRIES = 3;

  /**
   * Sets the max number of tries for reconnect.
   * */
  public void setMaxNumReconnectTries(int tries);

  /**
   * Returns the value for max number of tries for reconnect.
   * */
  public int getMaxNumReconnectTries();

  // ------------------- Asynchronous Messaging Properties -------------------

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#async-distribution-timeout">
   * "async-distribution-timeout"</a> property.
   */
  public int getAsyncDistributionTimeout();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#async-distribution-timeout">
   * "async-distribution-timeout"</a> property.
   */
  public void setAsyncDistributionTimeout(int newValue);
  /**
   * Returns true if the value of the asyncDistributionTimeout attribute can
   * currently be modified. Some attributes can not be modified while the
   * system is running.
   */
  public boolean isAsyncDistributionTimeoutModifiable();

  /** The name of the "asyncDistributionTimeout" property */
  public static final String ASYNC_DISTRIBUTION_TIMEOUT_NAME = "async-distribution-timeout";
  /** The default value of "asyncDistributionTimeout" is <code>0</code>. */
  public static final int DEFAULT_ASYNC_DISTRIBUTION_TIMEOUT = 0;
  /** The minimum value of "asyncDistributionTimeout" is <code>0</code>. */
  public static final int MIN_ASYNC_DISTRIBUTION_TIMEOUT = 0;
  /** The maximum value of "asyncDistributionTimeout" is <code>60000</code>. */
  public static final int MAX_ASYNC_DISTRIBUTION_TIMEOUT = 60000;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#async-queue-timeout">
   * "async-queue-timeout"</a> property.
   */
  public int getAsyncQueueTimeout();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#async-queue-timeout">
   * "async-queue-timeout"</a> property.
   */
  public void setAsyncQueueTimeout(int newValue);
  /**
   * Returns true if the value of the asyncQueueTimeout attribute can
   * currently be modified. Some attributes can not be modified while the
   * system is running.
   */
  public boolean isAsyncQueueTimeoutModifiable();

  /** The name of the "asyncQueueTimeout" property */
  public static final String ASYNC_QUEUE_TIMEOUT_NAME = "async-queue-timeout";
  /** The default value of "asyncQueueTimeout" is <code>60000</code>. */
  public static final int DEFAULT_ASYNC_QUEUE_TIMEOUT = 60000;
  /** The minimum value of "asyncQueueTimeout" is <code>0</code>. */
  public static final int MIN_ASYNC_QUEUE_TIMEOUT = 0;
  /** The maximum value of "asyncQueueTimeout" is <code>86400000</code>. */
  public static final int MAX_ASYNC_QUEUE_TIMEOUT = 86400000;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#async-max-queue-size">
   * "async-max-queue-size"</a> property.
   */
  public int getAsyncMaxQueueSize();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#async-max-queue-size">
   * "async-max-queue-size"</a> property.
   */
  public void setAsyncMaxQueueSize(int newValue);
  /**
   * Returns true if the value of the asyncMaxQueueSize attribute can
   * currently be modified. Some attributes can not be modified while the
   * system is running.
   */
  public boolean isAsyncMaxQueueSizeModifiable();

  /** The name of the "asyncMaxQueueSize" property */
  public static final String ASYNC_MAX_QUEUE_SIZE_NAME = "async-max-queue-size";
  /** The default value of "asyncMaxQueueSize" is <code>8</code>. */
  public static final int DEFAULT_ASYNC_MAX_QUEUE_SIZE = 8;
  /** The minimum value of "asyncMaxQueueSize" is <code>0</code>. */
  public static final int MIN_ASYNC_MAX_QUEUE_SIZE = 0;
  /** The maximum value of "asyncMaxQueueSize" is <code>1024</code>. */
  public static final int MAX_ASYNC_MAX_QUEUE_SIZE = 1024;

  /** @since 5.7 */
  public static final String CLIENT_CONFLATION_PROP_NAME = "conflate-events";
  /** @since 5.7 */
  public static final String CLIENT_CONFLATION_PROP_VALUE_DEFAULT = "server";
  /** @since 5.7 */
  public static final String CLIENT_CONFLATION_PROP_VALUE_ON = "true";
  /** @since 5.7 */
  public static final String CLIENT_CONFLATION_PROP_VALUE_OFF = "false";
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#conflate-events">"conflate-events"</a>
   * property.
   * @since 5.7
   */
  public String getClientConflation();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#conflate-events">"conflate-events"</a>
   * property.
   * @since 5.7
   */
  public void setClientConflation(String clientConflation);
  /** @since 5.7 */
  public boolean isClientConflationModifiable();
  // -------------------------------------------------------------------------
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#durable-client-id">"durable-client-id"</a>
   * property.
   */
  public String getDurableClientId();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#durable-client-id">"durable-client-id"</a>
   * property.
   */
  public void setDurableClientId(String durableClientId);

  /**
   * Returns true if the value of the durableClientId attribute can currently
   * be modified. Some attributes can not be modified while the system is
   * running.
   */
  public boolean isDurableClientIdModifiable();

  /** The name of the "durableClientId" property */
  public static final String DURABLE_CLIENT_ID_NAME = "durable-client-id";

  /**
   * The default durable client id.
   * <p> Actual value of this constant is <code>""</code>.
   */
  public static final String DEFAULT_DURABLE_CLIENT_ID = "";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#durable-client-timeout">"durable-client-timeout"</a>
   * property.
   */
  public int getDurableClientTimeout();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#durable-client-timeout">"durable-client-timeout"</a>
   * property.
   */
  public void setDurableClientTimeout(int durableClientTimeout);

  /**
   * Returns true if the value of the durableClientTimeout attribute can
   * currently be modified. Some attributes can not be modified while the
   * system is running.
   */
  public boolean isDurableClientTimeoutModifiable();

  /** The name of the "durableClientTimeout" property */
  public static final String DURABLE_CLIENT_TIMEOUT_NAME = "durable-client-timeout";

  /**
   * The default durable client timeout in seconds.
   * <p> Actual value of this constant is <code>"300"</code>.
   */
  public static final int DEFAULT_DURABLE_CLIENT_TIMEOUT = 300;

  /**
   * Returns user module name for client authentication initializer in <a
   * href="../DistributedSystem.html#security-client-auth-init">"security-client-auth-init"</a>
   */
  public String getSecurityClientAuthInit();

  /**
   * Sets the user module name in <a
   * href="../DistributedSystem.html#security-client-auth-init">"security-client-auth-init"</a>
   * property.
   */
  public void setSecurityClientAuthInit(String attValue);

  /**
   * Returns true if the value of the authentication initializer method name can
   * currently be modified. Some attributes can not be modified while the
   * system is running.
   */
  public boolean isSecurityClientAuthInitModifiable();

  /** The name of user defined method name for "security-client-auth-init" property*/
  public static final String SECURITY_CLIENT_AUTH_INIT_NAME = "security-client-auth-init";

  /**
   * The default client authentication initializer method name.
   * <p> Actual value of this is in format <code>"jar file:module name"</code>.
   */
  public static final String DEFAULT_SECURITY_CLIENT_AUTH_INIT = "";

  /**
   * Returns user module name authenticating client credentials in <a
   * href="../DistributedSystem.html#security-client-authenticator">"security-client-authenticator"</a>
   */
  public String getSecurityClientAuthenticator();

  /**
   * Sets the user defined method name in <a
   * href="../DistributedSystem.html#security-client-authenticator">"security-client-authenticator"</a>
   * property.
   */
  public void setSecurityClientAuthenticator(String attValue);

  /**
   * Returns true if the value of the authenticating method name can
   * currently be modified. Some attributes can not be modified while the
   * system is running.
   */
  public boolean isSecurityClientAuthenticatorModifiable();

  /** The name of factory method for "security-client-authenticator" property */
  public static final String SECURITY_CLIENT_AUTHENTICATOR_NAME = "security-client-authenticator";

  /**
   * The default client authentication method name.
   * <p> Actual value of this is fully qualified <code>"method name"</code>.
   */
  public static final String DEFAULT_SECURITY_CLIENT_AUTHENTICATOR = "";

  /**
   * Returns name of algorithm to use for Diffie-Hellman key exchange <a
   * href="../DistributedSystem.html#security-client-dhalgo">"security-client-dhalgo"</a>
   */
  public String getSecurityClientDHAlgo();

  /**
   * Set the name of algorithm to use for Diffie-Hellman key exchange <a
   * href="../DistributedSystem.html#security-client-dhalgo">"security-client-dhalgo"</a>
   * property.
   */
  public void setSecurityClientDHAlgo(String attValue);

  /**
   * Returns true if the value of the Diffie-Hellman algorithm can currently be
   * modified. Some attributes can not be modified while the system is running.
   */
  public boolean isSecurityClientDHAlgoModifiable();

  /**
   * The name of the Diffie-Hellman symmetric algorithm "security-client-dhalgo"
   * property.
   */
  public static final String SECURITY_CLIENT_DHALGO_NAME = "security-client-dhalgo";

  /**
   * The default Diffie-Hellman symmetric algorithm name.
   * <p>
   * Actual value of this is one of the available symmetric algorithm names in
   * JDK like "DES", "DESede", "AES", "Blowfish".
   */
  public static final String DEFAULT_SECURITY_CLIENT_DHALGO = "";

  /**
   * Returns user defined method name for peer authentication initializer in <a
   * href="../DistributedSystem.html#security-peer-auth-init">"security-peer-auth-init"</a>
   */
  public String getSecurityPeerAuthInit();

  /**
   * Sets the user module name in <a
   * href="../DistributedSystem.html#security-peer-auth-init">"security-peer-auth-init"</a>
   * property.
   */
  public void setSecurityPeerAuthInit(String attValue);

  /**
   * Returns true if the value of the AuthInit method name can
   * currently be modified. Some attributes can not be modified while the
   * system is running.
   */
  public boolean isSecurityPeerAuthInitModifiable();

  /** The name of user module for "security-peer-auth-init" property*/
  public static final String SECURITY_PEER_AUTH_INIT_NAME = "security-peer-auth-init";

  /**
   * The default client authenticaiton method name.
   * <p> Actual value of this is fully qualified <code>"method name"</code>.
   */
  public static final String DEFAULT_SECURITY_PEER_AUTH_INIT = "";

  /**
   * Returns user defined method name authenticating peer's credentials in <a
   * href="../DistributedSystem.html#security-peer-authenticator">"security-peer-authenticator"</a>
   */
  public String getSecurityPeerAuthenticator();

  /**
   * Sets the user module name in <a
   * href="../DistributedSystem.html#security-peer-authenticator">"security-peer-authenticator"</a>
   * property.
   */
  public void setSecurityPeerAuthenticator(String attValue);

  /**
   * Returns true if the value of the security module name can
   * currently be modified. Some attributes can not be modified while the
   * system is running.
   */
  public boolean isSecurityPeerAuthenticatorModifiable();

  /** The name of user defined method for "security-peer-authenticator" property*/
  public static final String SECURITY_PEER_AUTHENTICATOR_NAME = "security-peer-authenticator";

  /**
   * The default client authenticaiton method.
   * <p> Actual value of this is fully qualified <code>"method name"</code>.
   */
  public static final String DEFAULT_SECURITY_PEER_AUTHENTICATOR = "";

  /**
   * Returns user module name authorizing client credentials in <a
   * href="../DistributedSystem.html#security-client-accessor">"security-client-accessor"</a>
   */
  public String getSecurityClientAccessor();

  /**
   * Sets the user defined method name in <a
   * href="../DistributedSystem.html#security-client-accessor">"security-client-accessor"</a>
   * property.
   */
  public void setSecurityClientAccessor(String attValue);

  /** The name of the factory method for "security-client-accessor" property */
  public static final String SECURITY_CLIENT_ACCESSOR_NAME = "security-client-accessor";

  /**
   * The default client authorization module factory method name.
   * <p> Actual value of this is fully qualified <code>"method name"</code>.
   */
  public static final String DEFAULT_SECURITY_CLIENT_ACCESSOR = "";

  /**
   * Returns user module name authorizing client credentials in <a
   * href="../DistributedSystem.html#security-client-accessor-pp">"security-client-accessor-pp"</a>
   */
  public String getSecurityClientAccessorPP();

  /**
   * Sets the user defined method name in <a
   * href="../DistributedSystem.html#security-client-accessor-pp">"security-client-accessor-pp"</a>
   * property.
   */
  public void setSecurityClientAccessorPP(String attValue);

  /** The name of the factory method for "security-client-accessor-pp" property */
  public static final String SECURITY_CLIENT_ACCESSOR_PP_NAME = "security-client-accessor-pp";

  /**
   * The default client post-operation authorization module factory method name.
   * <p> Actual value of this is fully qualified <code>"method name"</code>.
   */
  public static final String DEFAULT_SECURITY_CLIENT_ACCESSOR_PP = "";

  /**
   * Get the current log-level for security logging.
   *
   * @return the current security log-level
   */
  public int getSecurityLogLevel();

  /**
   * Set the log-level for security logging.
   *
   * @param level
   *                the new security log-level
   */
  public void setSecurityLogLevel(int level);

  /**
   * Returns true if the value of the logLevel attribute can currently be
   * modified. Some attributes can not be modified while the system is running.
   */
  public boolean isSecurityLogLevelModifiable();

  /**
   * The name of "security-log-level" property that sets the log-level for
   * security logger obtained using
   * {@link DistributedSystem#getSecurityLogWriter()}
   */
  public static final String SECURITY_LOG_LEVEL_NAME = "security-log-level";

  /**
   * Returns the value of the "security-log-file" property
   *
   * @return <code>null</code> if logging information goes to standard out
   */
  public File getSecurityLogFile();

  /**
   * Sets the system's security log file containing security related messages.
   * <p>
   * Non-absolute log files are relative to the system directory.
   * <p>
   * The security log file can not be changed while the system is running.
   *
   * @throws IllegalArgumentException
   *                 if the specified value is not acceptable.
   * @throws com.gemstone.gemfire.UnmodifiableException
   *                 if this attribute can not be modified.
   * @throws com.gemstone.gemfire.GemFireIOException
   *                 if the set failure is caused by an error when writing to
   *                 the system's configuration file.
   */
  public void setSecurityLogFile(File value);

  /**
   * Returns true if the value of the <code>security-log-file</code> attribute
   * can currently be modified. Some attributes can not be modified while the
   * system is running.
   */
  public boolean isSecurityLogFileModifiable();

  /**
   * The name of the "security-log-file" property. This property is the path of
   * the file where security related messages are logged.
   */
  public static final String SECURITY_LOG_FILE_NAME = "security-log-file";

  /**
   * The default security log file.
   * <p> *
   * <p>
   * Actual value of this constant is <code>""</code> which directs security
   * log messages to the same place as the system log file.
   */
  public static final File DEFAULT_SECURITY_LOG_FILE = new File("");

  /**
   * Get timeout for peer membership check when security is enabled.
   *
   * @return Timeout in milliseconds.
   */
  public int getSecurityPeerMembershipTimeout();

  /**
   * Set timeout for peer membership check when security is enabled. The timeout must be less
   * than peer handshake timeout.
   * @param attValue
   */
  public void setSecurityPeerMembershipTimeout(int attValue);

  /**
   * Returns true if the value of the peer membership timeout attribute can currently be modified.
   * Some attributes can not be modified while the system is running.
   * @return true if timeout is modifiable.
   */
  public boolean isSecurityPeerMembershipTimeoutModifiable();

  /** The name of the peer membership check timeout property */
  public static final String SECURITY_PEER_VERIFYMEMBER_TIMEOUT_NAME = "security-peer-verifymember-timeout";

  /**
   * The default peer membership check timeout is 1 second.
   */
  public static final int DEFAULT_SECURITY_PEER_VERIFYMEMBER_TIMEOUT = 1000;

  /**
   * Max membership timeout must be less than max peer handshake timeout. Currently this is set to
   * default handshake timeout of 60 seconds.
   */
  public static final int MAX_SECURITY_PEER_VERIFYMEMBER_TIMEOUT = 60000;

  /**
   * Returns all properties starting with <a
   * href="../DistributedSystem.html#security-">"security-"</a>.
   */
  public Properties getSecurityProps();

  /**
   * Returns the value of security property <a
   * href="../DistributedSystem.html#security-">"security-"</a>
   * for an exact attribute name match.
   */
  public String getSecurity(String attName);

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#security-">"security-*"</a>
   * property.
   */
  public void setSecurity(String attName, String attValue);

  /**
   * Returns true if the value of the security attributes can
   * currently be modified. Some attributes can not be modified while the
   * system is running.
   */
  public boolean isSecurityModifiable();

  /** For the "security-" prefixed properties */
  public static final String SECURITY_PREFIX_NAME = "security-";

  /** The prefix used for Gemfire properties set through java system properties */
  public static final String GEMFIRE_PREFIX = "gemfire.";

  /** For the "custom-" prefixed properties */
  public static final String USERDEFINED_PREFIX_NAME = "custom-";

  /** For ssl keystore and trust store properties */
  public static final String SSL_SYSTEM_PROPS_NAME = "javax.net.ssl";
  
  public static final String KEY_STORE_TYPE_NAME = ".keyStoreType";
  public static final String KEY_STORE_NAME = ".keyStore";
  public static final String KEY_STORE_PASSWORD_NAME = ".keyStorePassword";
  public static final String TRUST_STORE_NAME = ".trustStore";
  public static final String TRUST_STORE_PASSWORD_NAME = ".trustStorePassword";


  /** Suffix for ssl keystore and trust store properties for JMX*/
  public static final String JMX_SSL_PROPS_SUFFIX = "-jmx";

  /** For security properties starting with sysprop in gfsecurity.properties file */
  public static final String SYS_PROP_NAME = "sysprop-";
   /**
    * The property decides whether to remove unresponsive client from the server.
    */
   public static final String REMOVE_UNRESPONSIVE_CLIENT_PROP_NAME = "remove-unresponsive-client";

   /**
    * The default value of remove unresponsive client is false.
    */
   public static final boolean DEFAULT_REMOVE_UNRESPONSIVE_CLIENT = false;
   /**
    * Returns the value of the <a
    * href="../DistributedSystem.html#remove-unresponsive-client">"remove-unresponsive-client"</a>
    * property.
    * @since 6.0
    */
   public boolean getRemoveUnresponsiveClient();
   /**
    * Sets the value of the <a
    * href="../DistributedSystem.html#remove-unresponsive-client">"remove-unresponsive-client"</a>
    * property.
    * @since 6.0
    */
   public void setRemoveUnresponsiveClient(boolean value);
   /** @since 6.0 */
   public boolean isRemoveUnresponsiveClientModifiable();

   /** @since 6.3 */
   public static final String DELTA_PROPAGATION_PROP_NAME = "delta-propagation";

   public static final boolean DEFAULT_DELTA_PROPAGATION = true;
   /**
    * Returns the value of the <a
    * href="../DistributedSystem.html#delta-propagation">"delta-propagation"</a>
    * property.
    * @since 6.3
    */
   public boolean getDeltaPropagation();

   /**
    * Sets the value of the <a
    * href="../DistributedSystem.html#delta-propagation">"delta-propagation"</a>
    * property.
    * @since 6.3
    */
   public void setDeltaPropagation(boolean value);

   /** @since 6.3 */
   public boolean isDeltaPropagationModifiable();
   
   /**
    * @since 6.6
    */
   public static final String DISTRIBUTED_SYSTEM_ID_NAME = "distributed-system-id";
   public static final int DEFAULT_DISTRIBUTED_SYSTEM_ID = -1;

   public static final String REDUNDANCY_ZONE_NAME = "redundancy-zone";
   public static final String DEFAULT_REDUNDANCY_ZONE = null;
   
   /**
    * @since 6.6
    */
   public void setDistributedSystemId(int distributedSystemId);

   public void setRedundancyZone(String redundancyZone);
   
   /**
    * @since 6.6
    */
   public int getDistributedSystemId();
   
   public String getRedundancyZone();
   
   /**
    * @since 6.6.2
    */
   public void setSSLProperty(String attName, String attValue);
   
   /**
    * @since 6.6.2
    */
   public Properties getSSLProperties();
   
   public Properties getClusterSSLProperties();

   /**
    * @since 8.0
    */
   public Properties getJmxSSLProperties();
   /**
    * @since 6.6
    */
   public static final String ENFORCE_UNIQUE_HOST_NAME = "enforce-unique-host";
   /** Using the system property to set the default here to retain backwards compatibility
    * with customers that are already using this system property.
    */
   public static boolean DEFAULT_ENFORCE_UNIQUE_HOST = Boolean.getBoolean("gemfire.EnforceUniqueHostStorageAllocation");
   
   public void setEnforceUniqueHost(boolean enforceUniqueHost);
   
   public boolean getEnforceUniqueHost();

   public Properties getUserDefinedProps();


   /**
    * Returns the value of the <a
    * href="../DistributedSystem.html#name">"groups"</a> property
    * <p> The default value is: {@link #DEFAULT_GROUPS}.
    * @return the value of the property
    * @since 7.0
    */
   public String getGroups();
   /**
    * Sets the groups gemfire property.
    * <p> The groups can not be changed while the system is running.
    * @throws IllegalArgumentException if the specified value is not acceptable.
    * @throws com.gemstone.gemfire.UnmodifiableException if this attribute can not be modified.
    * @throws com.gemstone.gemfire.GemFireIOException if the set failure is caused by an error
    *   when writing to the system's configuration file.
    * @since 7.0
    */
   public void setGroups(String value);
   /**
    * Returns true if the value of the <code>groups</code> attribute can currently
    * be modified.
    * Some attributes can not be modified while the system is running.
    * @since 7.0
    */
   public boolean isGroupsModifiable();
   /** The name of the "groups" property 
    * @since 7.0
    */
   public static final String GROUPS_NAME = "groups";
   /**
    * The default groups.
    * <p> Actual value of this constant is <code>""</code>.
    * @since 7.0
    */
   public static final String DEFAULT_GROUPS = "";

   /** Any cleanup required before closing the distributed system */
  public void close();
  
  public void setRemoteLocators(String locators);
  public String getRemoteLocators();
  /** The name of the "remote-locators" property */
  public static final String REMOTE_LOCATORS_NAME = "remote-locators";
  /** The default value of the "remote-locators" property */
  public static final String DEFAULT_REMOTE_LOCATORS = "";

  public boolean getJmxManager();
  public void setJmxManager(boolean value);
  public boolean isJmxManagerModifiable();
  public static String JMX_MANAGER_NAME = "jmx-manager";
  public static boolean DEFAULT_JMX_MANAGER = false;

  
  public boolean getJmxManagerStart();
  public void setJmxManagerStart(boolean value);
  public boolean isJmxManagerStartModifiable();
  public static String JMX_MANAGER_START_NAME = "jmx-manager-start";
  public static boolean DEFAULT_JMX_MANAGER_START = false;
  
  public int getJmxManagerPort();
  public void setJmxManagerPort(int value);
  public boolean isJmxManagerPortModifiable();
  public static String JMX_MANAGER_PORT_NAME = "jmx-manager-port";
  public static int DEFAULT_JMX_MANAGER_PORT = 1099;
  
  /** @deprecated as of 8.0 use {@link #getJmxManagerSSLEnabled} instead.*/
  public boolean getJmxManagerSSL();
  /** @deprecated as of 8.0 use {@link #setJmxManagerSSLEnabled} instead.*/
  public void setJmxManagerSSL(boolean value);
  /** @deprecated as of 8.0 */
  public boolean isJmxManagerSSLModifiable();
  /** @deprecated as of 8.0 use {@link #JMX_MANAGER_SSL_ENABLED_NAME} instead.*/
  public static String JMX_MANAGER_SSL_NAME = "jmx-manager-ssl";
  /** @deprecated as of 8.0 use {@link #DEFAULT_JMX_MANAGER_SSL_ENABLED} instead.*/
  public static boolean DEFAULT_JMX_MANAGER_SSL = false;
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-enabled">"jmx-manager-ssl-enabled"</a>
   * property.
   */
  public boolean getJmxManagerSSLEnabled();

  /**
   * The default jmx-manager-ssl-enabled state.
   * <p> Actual value of this constant is <code>false</code>.
   */
  public static final boolean DEFAULT_JMX_MANAGER_SSL_ENABLED = false;
    /** The name of the "CacheJmxManagerSSLEnabled" property */
  public static final String JMX_MANAGER_SSL_ENABLED_NAME =
    "jmx-manager-ssl-enabled";

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-enabled">"jmx-manager-ssl-enabled"</a>
   * property.
   */
  public void setJmxManagerSSLEnabled( boolean enabled );

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-protocols">"jmx-manager-ssl-protocols"</a>
   * property.
   */
   public String getJmxManagerSSLProtocols( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-protocols">"jmx-manager-ssl-protocols"</a>
   * property.
   */
   public void setJmxManagerSSLProtocols( String protocols );

  /**
   * The default jmx-manager-ssl-protocols value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  public static final String DEFAULT_JMX_MANAGER_SSL_PROTOCOLS = "any";
  /** The name of the "CacheJmxManagerSSLProtocols" property */
  public static final String JMX_MANAGER_SSL_PROTOCOLS_NAME =
    "jmx-manager-ssl-protocols";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-ciphers">"jmx-manager-ssl-ciphers"</a>
   * property.
   */
   public String getJmxManagerSSLCiphers( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-ciphers">"jmx-manager-ssl-ciphers"</a>
   * property.
   */
   public void setJmxManagerSSLCiphers( String ciphers );

   /**
   * The default jmx-manager-ssl-ciphers value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  public static final String DEFAULT_JMX_MANAGER_SSL_CIPHERS = "any";
  /** The name of the "CacheJmxManagerSSLCiphers" property */
  public static final String JMX_MANAGER_SSL_CIPHERS_NAME =
    "jmx-manager-ssl-ciphers";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-require-authentication">"jmx-manager-ssl-require-authentication"</a>
   * property.
   */
   public boolean getJmxManagerSSLRequireAuthentication( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-require-authentication">"jmx-manager-ssl-require-authentication"</a>
   * property.
   */
   public void setJmxManagerSSLRequireAuthentication( boolean enabled );

   /**
   * The default jmx-manager-ssl-require-authentication value.
   * <p> Actual value of this constant is <code>true</code>.
   */
  public static final boolean DEFAULT_JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION = true;
  /** The name of the "CacheJmxManagerSSLRequireAuthentication" property */
  public static final String JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION_NAME =
    "jmx-manager-ssl-require-authentication";

  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-keystore">"jmx-manager-ssl-keystore"</a>
   * property.
   */
  public String getJmxManagerSSLKeyStore( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-keystore">"jmx-manager-ssl-keystore"</a>
   * property.
   */
  public void setJmxManagerSSLKeyStore( String keyStore);
  
  /**
   * The default jmx-manager-ssl-keystore value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_JMX_MANAGER_SSL_KEYSTORE = "";
  
  /** The name of the "CacheJmxManagerSSLKeyStore" property */
  public static final String JMX_MANAGER_SSL_KEYSTORE_NAME = "jmx-manager-ssl-keystore";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-keystore-type">"jmx-manager-ssl-keystore-type"</a>
   * property.
   */
  public String getJmxManagerSSLKeyStoreType( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-keystore-type">"jmx-manager-ssl-keystore-type"</a>
   * property.
   */
  public void setJmxManagerSSLKeyStoreType( String keyStoreType);
  
  /**
   * The default jmx-manager-ssl-keystore-type value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_JMX_MANAGER_SSL_KEYSTORE_TYPE = "";
  
  /** The name of the "CacheJmxManagerSSLKeyStoreType" property */
  public static final String JMX_MANAGER_SSL_KEYSTORE_TYPE_NAME = "jmx-manager-ssl-keystore-type";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-keystore-password">"jmx-manager-ssl-keystore-password"</a>
   * property.
   */
  public String getJmxManagerSSLKeyStorePassword( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-keystore-password">"jmx-manager-ssl-keystore-password"</a>
   * property.
   */
  public void setJmxManagerSSLKeyStorePassword( String keyStorePassword);
  
  /**
   * The default jmx-manager-ssl-keystore-password value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_JMX_MANAGER_SSL_KEYSTORE_PASSWORD = "";
  
  /** The name of the "CacheJmxManagerSSLKeyStorePassword" property */
  public static final String JMX_MANAGER_SSL_KEYSTORE_PASSWORD_NAME = "jmx-manager-ssl-keystore-password";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-truststore">"jmx-manager-ssl-truststore"</a>
   * property.
   */
  public String getJmxManagerSSLTrustStore( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-truststore">"jmx-manager-ssl-truststore"</a>
   * property.
   */
  public void setJmxManagerSSLTrustStore( String trustStore);
  
  /**
   * The default jmx-manager-ssl-truststore value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_JMX_MANAGER_SSL_TRUSTSTORE = "";
  
  /** The name of the "CacheJmxManagerSSLTrustStore" property */
  public static final String JMX_MANAGER_SSL_TRUSTSTORE_NAME = "jmx-manager-ssl-truststore";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-truststore-password">"jmx-manager-ssl-truststore-password"</a>
   * property.
   */
  public String getJmxManagerSSLTrustStorePassword( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-truststore-password">"jmx-manager-ssl-truststore-password"</a>
   * property.
   */
  public void setJmxManagerSSLTrustStorePassword( String trusStorePassword);
  /**
   * The default jmx-manager-ssl-truststore-password value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD = "";
  
  /** The name of the "JmxManagerSSLKeyStorePassword" property */
  public static final String JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD_NAME = "jmx-manager-ssl-truststore-password";
  

  public String getJmxManagerBindAddress();
  public void setJmxManagerBindAddress(String value);
  public boolean isJmxManagerBindAddressModifiable();
  public static String JMX_MANAGER_BIND_ADDRESS_NAME = "jmx-manager-bind-address";
  public static String DEFAULT_JMX_MANAGER_BIND_ADDRESS = "";
  
  public String getJmxManagerHostnameForClients();
  public void setJmxManagerHostnameForClients(String value);
  public boolean isJmxManagerHostnameForClientsModifiable();
  public static String JMX_MANAGER_HOSTNAME_FOR_CLIENTS_NAME = "jmx-manager-hostname-for-clients";
  public static String DEFAULT_JMX_MANAGER_HOSTNAME_FOR_CLIENTS = "";
  
  public String getJmxManagerPasswordFile();
  public void setJmxManagerPasswordFile(String value);
  public boolean isJmxManagerPasswordFileModifiable();
  public static String JMX_MANAGER_PASSWORD_FILE_NAME = "jmx-manager-password-file";
  public static String DEFAULT_JMX_MANAGER_PASSWORD_FILE = "";
  
  public String getJmxManagerAccessFile();
  public void setJmxManagerAccessFile(String value);
  public boolean isJmxManagerAccessFileModifiable();
  public static String JMX_MANAGER_ACCESS_FILE_NAME = "jmx-manager-access-file";
  public static String DEFAULT_JMX_MANAGER_ACCESS_FILE = "";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-http-port">"jmx-manager-http-port"</a> property
   * @deprecated as of 8.0 use {@link #getHttpServicePort()} instead.
   */
  public int getJmxManagerHttpPort();
  
  /**
   * Set the jmx-manager-http-port for jmx-manager.
   * @param value the port number for jmx-manager HTTP service
   * @deprecated as of 8.0 use {@link #setHttpServicePort(int)} instead.               
   */
  public void setJmxManagerHttpPort(int value);
  
  /**
   * Returns true if the value of the
   * attribute can currently be modified.
   * Some attributes can not be modified while the system is running.
   * @deprecated as of 8.0 use {@link #isHttpServicePortModifiable()} instead.
   */
  public boolean isJmxManagerHttpPortModifiable();
  
  /**
   * The name of the "jmx-manager-http-port" property.
   * @deprecated as of 8.0 use {@link #HTTP_SERVICE_PORT_NAME} instead.
   */
  public static String JMX_MANAGER_HTTP_PORT_NAME = "jmx-manager-http-port";
  
  /**
   * The default value of the "jmx-manager-http-port" property.
   * Current value is a <code>7070</code>
   * @deprecated as of 8.0 use {@link #DEFAULT_HTTP_SERVICE_PORT} instead.
   */
  public static int DEFAULT_JMX_MANAGER_HTTP_PORT = 7070;

  public int getJmxManagerUpdateRate();
  public void setJmxManagerUpdateRate(int value);
  public boolean isJmxManagerUpdateRateModifiable();
  public static final int DEFAULT_JMX_MANAGER_UPDATE_RATE = 2000;
  public static final int MIN_JMX_MANAGER_UPDATE_RATE = 1000;
  public static final int MAX_JMX_MANAGER_UPDATE_RATE = 60000*5;
  public static final String JMX_MANAGER_UPDATE_RATE_NAME =
    "jmx-manager-update-rate";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#memcached-port">"memcached-port"</a> property
   * @return the port on which GemFireMemcachedServer should be started
   * @since 7.0
   */
  public int getMemcachedPort();
  public void setMemcachedPort(int value);
  public boolean isMemcachedPortModifiable();
  public static String MEMCACHED_PORT_NAME = "memcached-port";
  public static int DEFAULT_MEMCACHED_PORT = 0;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#memcached-protocol">"memcached-protocol"</a> property
   * @return the protocol for GemFireMemcachedServer
   * @since 7.0
   */
  public String getMemcachedProtocol();
  public void setMemcachedProtocol(String protocol);
  public boolean isMemcachedProtocolModifiable();
  public static String MEMCACHED_PROTOCOL_NAME = "memcached-protocol";
  public static String DEFAULT_MEMCACHED_PROTOCOL = GemFireMemcachedServer.Protocol.ASCII.name();
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#memcached-bind-address">"memcached-bind-address"</a> property
   * @return the bind address for GemFireMemcachedServer
   * @since 7.0
   */
  public String getMemcachedBindAddress();
  public void setMemcachedBindAddress(String bindAddress);
  public boolean isMemcachedBindAddressModifiable();
  public static String MEMCACHED_BIND_ADDRESS_NAME = "memcached-bind-address";
  public static String DEFAULT_MEMCACHED_BIND_ADDRESS = "";

  //Added for the HTTP service
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-port">"http-service-port"</a> property
   * @return the HTTP service port
   * @since 8.0
   */
  public int getHttpServicePort();
  
  /**
   * Set the http-service-port for HTTP service.
   * @param value the port number for HTTP service
   * @since 8.0               
   */
  public void setHttpServicePort(int value);
  
  /**
   * Returns true if the value of the
   * attribute can currently be modified.
   * Some attributes can not be modified while the system is running.
   * @since 8.0
   */
  public boolean isHttpServicePortModifiable();
  
  /**
   * The name of the "http-service-port" property
   * @since 8.0
   */
  public static String HTTP_SERVICE_PORT_NAME = "http-service-port";
  
  /**
   * The default value of the "http-service-port" property.
   * Current value is a <code>7070</code>
   * @since 8.0 
   */
  public static int DEFAULT_HTTP_SERVICE_PORT = 7070;
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-bind-address">"http-service-bind-address"</a> property
   * @return the bind-address for HTTP service
   * @since 8.0
   */
  public String getHttpServiceBindAddress();
  
  /**
   * Set the http-service-bind-address for HTTP service.
   * @param value the bind-address for HTTP service
   * @since 8.0               
   */
  public void setHttpServiceBindAddress(String value);
  
  /**
   * Returns true if the value of the
   * attribute can currently be modified.
   * Some attributes can not be modified while the system is running.
   * @since 8.0
   */
  public boolean isHttpServiceBindAddressModifiable();
  
  /** 
   * The name of the "http-service-bind-address" property
   * @since 8.0
   */
  public static String HTTP_SERVICE_BIND_ADDRESS_NAME = "http-service-bind-address";
  
  /**
   * The default value of the "http-service-bind-address" property.
   * Current value is an empty string <code>""</code>
   * @since 8.0 
   */
  public static String DEFAULT_HTTP_SERVICE_BIND_ADDRESS = "";
  
  
  //Added for HTTP Service SSL
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-enabled">"http-service-ssl-enabled"</a>
   * property.
   */
  public boolean getHttpServiceSSLEnabled();  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-enabled">"http-service-ssl-enabled"</a>
   * property.
   */
  public void setHttpServiceSSLEnabled(boolean httpServiceSSLEnabled);
  /**
   * The default http-service-ssl-enabled state.
   * <p> Actual value of this constant is <code>false</code>.
   */
  public static final boolean DEFAULT_HTTP_SERVICE_SSL_ENABLED = false;
  
  /** The name of the "HttpServiceSSLEnabled" property */
  public static final String HTTP_SERVICE_SSL_ENABLED_NAME = "http-service-ssl-enabled";  
  
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-require-authentication">"http-service-ssl-require-authentication"</a>
   * property.
   */
  public boolean getHttpServiceSSLRequireAuthentication();  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-require-authentication">"http-service-ssl-require-authentication"</a>
   * property.
   */
  public void setHttpServiceSSLRequireAuthentication(boolean httpServiceSSLRequireAuthentication);
  /**
  * The default http-service-ssl-require-authentication value.
  * <p> Actual value of this constant is <code>true</code>.
  */
  public static final boolean DEFAULT_HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION = false;
  
  /** The name of the "HttpServiceSSLRequireAuthentication" property */
  public static final String HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION_NAME = "http-service-ssl-require-authentication"; 
  
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-protocols">"http-service-ssl-protocols"</a>
   * property.
   */
  public String getHttpServiceSSLProtocols();  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-protocols">"http-service-ssl-protocols"</a>
   * property.
   */
  public void setHttpServiceSSLProtocols(String protocols);
  /**
   * The default http-service-ssl-protocols value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  public static final String DEFAULT_HTTP_SERVICE_SSL_PROTOCOLS = "any";
  
  /** The name of the "HttpServiceSSLProtocols" property */
  public static final String HTTP_SERVICE_SSL_PROTOCOLS_NAME = "http-service-ssl-protocols"; 
  
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-ciphers">"http-service-ssl-ciphers"</a>
   * property.
   */
  public String getHttpServiceSSLCiphers();  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-ciphers">"http-service-ssl-ciphers"</a>
   * property.
   */
  public void setHttpServiceSSLCiphers(String ciphers);
  /**
   * The default http-service-ssl-ciphers value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  public static final String DEFAULT_HTTP_SERVICE_SSL_CIPHERS = "any";
  
  /** The name of the "HttpServiceSSLCiphers" property */
  public static final String HTTP_SERVICE_SSL_CIPHERS_NAME = "http-service-ssl-ciphers";
  
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-keystore">"http-service-ssl-keystore"</a>
   * property.
   */
  public String getHttpServiceSSLKeyStore( );  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-keystore">"http-service-ssl-keystore"</a>
   * property.
   */
  public void setHttpServiceSSLKeyStore(String keyStore);
  /**
   * The default http-service-ssl-keystore value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_HTTP_SERVICE_SSL_KEYSTORE = "";
  
  /** The name of the "HttpServiceSSLKeyStore" property */
  public static final String HTTP_SERVICE_SSL_KEYSTORE_NAME = "http-service-ssl-keystore";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-keystore-password">"http-service-ssl-keystore-password"</a>
   * property.
   */
  public String getHttpServiceSSLKeyStorePassword( );  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-keystore-password">"http-service-ssl-keystore-password"</a>
   * property.
   */
  public void setHttpServiceSSLKeyStorePassword(String keyStorePassword);
  /**
   * The default http-service-ssl-keystore-password value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_HTTP_SERVICE_SSL_KEYSTORE_PASSWORD = "";
  
  /** The name of the "HttpServiceSSLKeyStorePassword" property */
  public static final String HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME = "http-service-ssl-keystore-password";
  
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-keystore-type">"http-service-ssl-keystore-type"</a>
   * property.
   */
  public String getHttpServiceSSLKeyStoreType( );  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-keystore-type">"http-service-ssl-keystore-type"</a>
   * property.
   */
  public void setHttpServiceSSLKeyStoreType(String keyStoreType);
  /**
   * The default gateway-ssl-truststore-password value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_HTTP_SERVICE_SSL_KEYSTORE_TYPE = "";
  
  /** The name of the "HttpServiceKeyStoreType" property */
  public static final String HTTP_SERVICE_SSL_KEYSTORE_TYPE_NAME = "http-service-ssl-keystore-type";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-truststore">"http-service-ssl-truststore"</a>
   * property.
   */
  public String getHttpServiceSSLTrustStore( );  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-truststore">"http-service-ssl-truststore"</a>
   * property.
   */
  public void setHttpServiceSSLTrustStore(String trustStore);
  /**
   * The default http-service-ssl-truststore value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_HTTP_SERVICE_SSL_TRUSTSTORE = "";
  
  /** The name of the "HttpServiceTrustStore" property */
  public static final String HTTP_SERVICE_SSL_TRUSTSTORE_NAME = "http-service-ssl-truststore";
  
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-truststore-password">"http-service-ssl-truststore-password"</a>
   * property.
   */
  public String getHttpServiceSSLTrustStorePassword( );  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-truststore-password">"http-service-ssl-truststore-password"</a>
   * property.
   */
  public void setHttpServiceSSLTrustStorePassword(String trustStorePassword);
  /**
   * The default http-service-ssl-truststore-password value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD = "";
  
  /** The name of the "HttpServiceTrustStorePassword" property */
  public static final String HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD_NAME = "http-service-ssl-truststore-password";
  
  
  public Properties getHttpServiceSSLProperties();
  
  //Added for API REST
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#start-dev-rest-api">"start-dev-rest-api"</a> property
   * @return the value of the property
   * @since 8.0
   */
  public boolean getStartDevRestApi();
  
  /**
   * Set the start-dev-rest-api for HTTP service.
   * @param value for the property
   * @since 8.0               
   */
  public void setStartDevRestApi(boolean value);
  
  /**
   * Returns true if the value of the
   * attribute can currently be modified.
   * Some attributes can not be modified while the system is running.
   * @since 8.0
   */
  public boolean isStartDevRestApiModifiable();
  
  /** 
   * The name of the "start-dev-rest-api" property
   * @since 8.0
   */
  public static String START_DEV_REST_API_NAME = "start-dev-rest-api";
  
  /**
   * The default value of the "start-dev-rest-api" property.
   * Current value is <code>"false"</code>
   * @since 8.0 
   */
  public static boolean DEFAULT_START_DEV_REST_API = false;
  
  /**
   * The name of the "default-auto-reconnect" property
   * @since 8.0
   */
  public static final String DISABLE_AUTO_RECONNECT_NAME = "disable-auto-reconnect";

  /** The default value of the corresponding property */
  public static final boolean DEFAULT_DISABLE_AUTO_RECONNECT = false;
  
  /**
   * Gets the value of <a href="../DistributedSystem.html#disable-auto-reconnect">"disable-auto-reconnect"</a>
   */
  public boolean getDisableAutoReconnect();

  /**
   * Sets the value of <a href="../DistributedSystem.html#disable-auto-reconnect">"disable-auto-reconnect"</a>
   * @param value the new setting
   */
  public void setDisableAutoReconnect(boolean value);
  
  
  public Properties getServerSSLProperties();
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-enabled">"server-ssl-enabled"</a>
   * property.
   */
  public boolean getServerSSLEnabled();

  /**
   * The default server-ssl-enabled state.
   * <p> Actual value of this constant is <code>false</code>.
   */
  public static final boolean DEFAULT_SERVER_SSL_ENABLED = false;
    /** The name of the "ServerSSLEnabled" property */
  public static final String SERVER_SSL_ENABLED_NAME =
    "server-ssl-enabled";

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-enabled">"server-ssl-enabled"</a>
   * property.
   */
  public void setServerSSLEnabled( boolean enabled );

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-protocols">"server-ssl-protocols"</a>
   * property.
   */
   public String getServerSSLProtocols( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-protocols">"server-ssl-protocols"</a>
   * property.
   */
   public void setServerSSLProtocols( String protocols );

  /**
   * The default server-ssl-protocols value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  public static final String DEFAULT_SERVER_SSL_PROTOCOLS = "any";
  /** The name of the "ServerSSLProtocols" property */
  public static final String SERVER_SSL_PROTOCOLS_NAME =
    "server-ssl-protocols";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-ciphers">"server-ssl-ciphers"</a>
   * property.
   */
   public String getServerSSLCiphers( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-ciphers">"server-ssl-ciphers"</a>
   * property.
   */
   public void setServerSSLCiphers( String ciphers );

   /**
   * The default server-ssl-ciphers value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  public static final String DEFAULT_SERVER_SSL_CIPHERS = "any";
  /** The name of the "ServerSSLCiphers" property */
  public static final String SERVER_SSL_CIPHERS_NAME =
    "server-ssl-ciphers";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-require-authentication">"server-ssl-require-authentication"</a>
   * property.
   */
   public boolean getServerSSLRequireAuthentication( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-require-authentication">"server-ssl-require-authentication"</a>
   * property.
   */
   public void setServerSSLRequireAuthentication( boolean enabled );

   /**
   * The default server-ssl-require-authentication value.
   * <p> Actual value of this constant is <code>true</code>.
   */
  public static final boolean DEFAULT_SERVER_SSL_REQUIRE_AUTHENTICATION = true;
  /** The name of the "ServerSSLRequireAuthentication" property */
  public static final String SERVER_SSL_REQUIRE_AUTHENTICATION_NAME =
    "server-ssl-require-authentication";

  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-keystore">"server-ssl-keystore"</a>
   * property.
   */
  public String getServerSSLKeyStore( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-keystore">"server-ssl-keystore"</a>
   * property.
   */
  public void setServerSSLKeyStore( String keyStore);
  
  /**
   * The default server-ssl-keystore value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_SERVER_SSL_KEYSTORE = "";
  
  /** The name of the "ServerSSLKeyStore" property */
  public static final String SERVER_SSL_KEYSTORE_NAME = "server-ssl-keystore";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-keystore-type">"server-ssl-keystore-type"</a>
   * property.
   */
  public String getServerSSLKeyStoreType( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-keystore-type">"server-ssl-keystore-type"</a>
   * property.
   */
  public void setServerSSLKeyStoreType( String keyStoreType);
  
  /**
   * The default server-ssl-keystore-type value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_SERVER_SSL_KEYSTORE_TYPE = "";
  
  /** The name of the "ServerSSLKeyStoreType" property */
  public static final String SERVER_SSL_KEYSTORE_TYPE_NAME = "server-ssl-keystore-type";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-keystore-password">"server-ssl-keystore-password"</a>
   * property.
   */
  public String getServerSSLKeyStorePassword( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-keystore-password">"server-ssl-keystore-password"</a>
   * property.
   */
  public void setServerSSLKeyStorePassword( String keyStorePassword);
  
  /**
   * The default server-ssl-keystore-password value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_SERVER_SSL_KEYSTORE_PASSWORD = "";
  
  /** The name of the "ServerSSLKeyStorePassword" property */
  public static final String SERVER_SSL_KEYSTORE_PASSWORD_NAME = "server-ssl-keystore-password";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-truststore">"server-ssl-truststore"</a>
   * property.
   */
  public String getServerSSLTrustStore( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-truststore">"server-ssl-truststore"</a>
   * property.
   */
  public void setServerSSLTrustStore( String trustStore);
  
  /**
   * The default server-ssl-truststore value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_SERVER_SSL_TRUSTSTORE = "";
  
  /** The name of the "ServerSSLTrustStore" property */
  public static final String SERVER_SSL_TRUSTSTORE_NAME = "server-ssl-truststore";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-truststore-password">"server-ssl-truststore-password"</a>
   * property.
   */
  public String getServerSSLTrustStorePassword( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-truststore-password">"server-ssl-truststore-password"</a>
   * property.
   */
  public void setServerSSLTrustStorePassword( String trusStorePassword);
  /**
   * The default server-ssl-truststore-password value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_SERVER_SSL_TRUSTSTORE_PASSWORD = "";
  
  /** The name of the "ServerSSLKeyStorePassword" property */
  public static final String SERVER_SSL_TRUSTSTORE_PASSWORD_NAME = "server-ssl-truststore-password";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-enabled">"cluster-ssl-enabled"</a>
   * property.
   */
  public boolean getGatewaySSLEnabled();

  /**
   * The default gateway-ssl-enabled state.
   * <p> Actual value of this constant is <code>false</code>.
   */
  public static final boolean DEFAULT_GATEWAY_SSL_ENABLED = false;
    /** The name of the "GatewaySSLEnabled" property */
  public static final String GATEWAY_SSL_ENABLED_NAME =
    "gateway-ssl-enabled";

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-enabled">"gateway-ssl-enabled"</a>
   * property.
   */
  public void setGatewaySSLEnabled( boolean enabled );

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-protocols">"gateway-ssl-protocols"</a>
   * property.
   */
   public String getGatewaySSLProtocols( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-protocols">"gateway-ssl-protocols"</a>
   * property.
   */
   public void setGatewaySSLProtocols( String protocols );

  /**
   * The default gateway-ssl-protocols value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  public static final String DEFAULT_GATEWAY_SSL_PROTOCOLS = "any";
  /** The name of the "GatewaySSLProtocols" property */
  public static final String GATEWAY_SSL_PROTOCOLS_NAME =
    "gateway-ssl-protocols";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-ciphers">"gateway-ssl-ciphers"</a>
   * property.
   */
   public String getGatewaySSLCiphers( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-ciphers">"gateway-ssl-ciphers"</a>
   * property.
   */
   public void setGatewaySSLCiphers( String ciphers );

   /**
   * The default gateway-ssl-ciphers value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  public static final String DEFAULT_GATEWAY_SSL_CIPHERS = "any";
  /** The name of the "GatewaySSLCiphers" property */
  public static final String GATEWAY_SSL_CIPHERS_NAME =
    "gateway-ssl-ciphers";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-require-authentication">"gateway-ssl-require-authentication"</a>
   * property.
   */
   public boolean getGatewaySSLRequireAuthentication( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-require-authentication">"gateway-ssl-require-authentication"</a>
   * property.
   */
   public void setGatewaySSLRequireAuthentication( boolean enabled );

   /**
   * The default gateway-ssl-require-authentication value.
   * <p> Actual value of this constant is <code>true</code>.
   */
  public static final boolean DEFAULT_GATEWAY_SSL_REQUIRE_AUTHENTICATION = true;
  /** The name of the "GatewaySSLRequireAuthentication" property */
  public static final String GATEWAY_SSL_REQUIRE_AUTHENTICATION_NAME =
    "gateway-ssl-require-authentication";

  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-keystore">"gateway-ssl-keystore"</a>
   * property.
   */
  public String getGatewaySSLKeyStore( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-keystore">"gateway-ssl-keystore"</a>
   * property.
   */
  public void setGatewaySSLKeyStore( String keyStore);
  
  /**
   * The default gateway-ssl-keystore value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_GATEWAY_SSL_KEYSTORE = "";
  
  /** The name of the "GatewaySSLKeyStore" property */
  public static final String GATEWAY_SSL_KEYSTORE_NAME = "gateway-ssl-keystore";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-keystore-type">"gateway-ssl-keystore-type"</a>
   * property.
   */
  public String getGatewaySSLKeyStoreType( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-keystore-type">"gateway-ssl-keystore-type"</a>
   * property.
   */
  public void setGatewaySSLKeyStoreType( String keyStoreType);
  
  /**
   * The default gateway-ssl-keystore-type value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_GATEWAY_SSL_KEYSTORE_TYPE = "";
  
  /** The name of the "GatewaySSLKeyStoreType" property */
  public static final String GATEWAY_SSL_KEYSTORE_TYPE_NAME = "gateway-ssl-keystore-type";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-keystore-password">"gateway-ssl-keystore-password"</a>
   * property.
   */
  public String getGatewaySSLKeyStorePassword( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-keystore-password">"gateway-ssl-keystore-password"</a>
   * property.
   */
  public void setGatewaySSLKeyStorePassword( String keyStorePassword);
  
  /**
   * The default gateway-ssl-keystore-password value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_GATEWAY_SSL_KEYSTORE_PASSWORD = "";
  
  /** The name of the "GatewaySSLKeyStorePassword" property */
  public static final String GATEWAY_SSL_KEYSTORE_PASSWORD_NAME = "gateway-ssl-keystore-password";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-truststore">"gateway-ssl-truststore"</a>
   * property.
   */
  public String getGatewaySSLTrustStore( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-truststore">"gateway-ssl-truststore"</a>
   * property.
   */
  public void setGatewaySSLTrustStore( String trustStore);
  
  /**
   * The default gateway-ssl-truststore value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_GATEWAY_SSL_TRUSTSTORE = "";
  
  /** The name of the "GatewaySSLTrustStore" property */
  public static final String GATEWAY_SSL_TRUSTSTORE_NAME = "gateway-ssl-truststore";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-truststore-password">"gateway-ssl-truststore-password"</a>
   * property.
   */
  public String getGatewaySSLTrustStorePassword( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-truststore-password">"gateway-ssl-truststore-password"</a>
   * property.
   */
  public void setGatewaySSLTrustStorePassword( String trusStorePassword);
  /**
   * The default gateway-ssl-truststore-password value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_GATEWAY_SSL_TRUSTSTORE_PASSWORD = "";
  
  /** The name of the "GatewaySSLKeyStorePassword" property */
  public static final String GATEWAY_SSL_TRUSTSTORE_PASSWORD_NAME = "gateway-ssl-truststore-password";
  
  
  public Properties getGatewaySSLProperties();

  public ConfigSource getConfigSource(String attName);
}

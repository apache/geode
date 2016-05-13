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

package com.gemstone.gemfire.distributed.internal;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 *
 * @since 2.1
 */
public interface
DistributionConfig extends Config, LogConfig {

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
  @ConfigAttributeGetter(name=NAME_NAME)
  public String getName();

  /**
   * Sets the member's name.
   * <p> The name can not be changed while the system is running.
   * @throws IllegalArgumentException if the specified value is not acceptable.
   * @throws com.gemstone.gemfire.UnmodifiableException if this attribute can not be modified.
   * @throws com.gemstone.gemfire.GemFireIOException if the set failure is caused by an error
   *   when writing to the system's configuration file.
   */
  @ConfigAttributeSetter(name=NAME_NAME)
  public void setName(String value);

  /** The name of the "name" property */
  @ConfigAttribute(type=String.class)
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
  @ConfigAttributeGetter(name=MCAST_PORT_NAME)
  public int getMcastPort();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#mcast-port">"mcast-port"</a>
   * property
   */
  @ConfigAttributeSetter(name=MCAST_PORT_NAME)
  public void setMcastPort(int value);


  /** The default value of the "mcastPort" property */
  public static final int DEFAULT_MCAST_PORT = 0;
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

  /** The name of the "mcastPort" property */
  @ConfigAttribute(type=Integer.class, min=MIN_MCAST_PORT, max=MAX_MCAST_PORT)
  public static final String MCAST_PORT_NAME = "mcast-port";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#tcp-port">"tcp-port"</a>
   * property
   */
  @ConfigAttributeGetter(name=TCP_PORT_NAME)
  public int getTcpPort();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#tcp-port">"tcp-port"</a>
   * property
   */
  @ConfigAttributeSetter(name=TCP_PORT_NAME)
  public void setTcpPort(int value);

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
  /** The name of the "tcpPort" property */
  @ConfigAttribute(type=Integer.class, min=MIN_TCP_PORT, max=MAX_TCP_PORT)
  public static final String TCP_PORT_NAME = "tcp-port";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#mcast-address">"mcast-address"</a>
   * property
   */
  @ConfigAttributeGetter(name=MCAST_ADDRESS_NAME)
  public InetAddress getMcastAddress();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#mcast-address">"mcast-address"</a>
   * property
   */
  @ConfigAttributeSetter(name=MCAST_ADDRESS_NAME)
  public void setMcastAddress(InetAddress value);


  /** The name of the "mcastAddress" property */
  @ConfigAttribute(type=InetAddress.class)
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
  @ConfigAttributeGetter(name=MCAST_TTL_NAME)
  public int getMcastTtl();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#mcast-ttl">"mcast-ttl"</a>
   * property
   */
  @ConfigAttributeSetter(name=MCAST_TTL_NAME)
  public void setMcastTtl(int value);

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

  /** The name of the "mcastTtl" property */
  @ConfigAttribute(type=Integer.class, min=MIN_MCAST_TTL, max=MAX_MCAST_TTL)
  public static final String MCAST_TTL_NAME = "mcast-ttl";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#bind-address">"bind-address"</a>
   * property
   */
  @ConfigAttributeGetter(name=BIND_ADDRESS_NAME)
  public String getBindAddress();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#bind-address">"bind-address"</a>
   * property
   */
  @ConfigAttributeSetter(name=BIND_ADDRESS_NAME)
  public void setBindAddress(String value);

  /** The name of the "bindAddress" property */
  @ConfigAttribute(type=String.class)
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
  @ConfigAttributeGetter(name=SERVER_BIND_ADDRESS_NAME)
  public String getServerBindAddress();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-bind-address">"server-bind-address"</a>
   * property
   */
  @ConfigAttributeSetter(name=SERVER_BIND_ADDRESS_NAME)
  public void setServerBindAddress(String value);

  /** The name of the "serverBindAddress" property */
  @ConfigAttribute(type=String.class)
  public static final String SERVER_BIND_ADDRESS_NAME = "server-bind-address";

  /** The default value of the "serverBindAddress" property.
   * Current value is an empty string <code>""</code>
   */
  public static final String DEFAULT_SERVER_BIND_ADDRESS = "";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#locators">"locators"</a> property
   */
  @ConfigAttributeGetter(name=LOCATORS_NAME)
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
  @ConfigAttributeSetter(name=LOCATORS_NAME)
  public void setLocators(String value);

  /** The name of the "locators" property */
  @ConfigAttribute(type=String.class)
  public static final String LOCATORS_NAME = "locators";

  /** The default value of the "locators" property */
  public static final String DEFAULT_LOCATORS = "";

  /**
   * Locator wait time - how long to wait for a locator to start before giving up &
   * throwing a GemFireConfigException
   */
  @ConfigAttribute(type=Integer.class)
  public static final String LOCATOR_WAIT_TIME_NAME = "locator-wait-time";
  public static final int DEFAULT_LOCATOR_WAIT_TIME = 0;
  @ConfigAttributeGetter(name=LOCATOR_WAIT_TIME_NAME)
  public int getLocatorWaitTime();
  @ConfigAttributeSetter(name=LOCATOR_WAIT_TIME_NAME)
  public void setLocatorWaitTime(int seconds);
  
  
  /**
   * returns the value of the <a href="../DistribytedSystem.html#start-locator">"start-locator"
   * </a> property
   */
  @ConfigAttributeGetter(name=START_LOCATOR_NAME)
  public String getStartLocator();
  /**
   * Sets the start-locator property.  This is a string in the form
   * bindAddress[port] and, if set, tells the distributed system to start
   * a locator prior to connecting
   * @param value must be of the form <code>hostName[portNum]</code>
   */
  @ConfigAttributeSetter(name=START_LOCATOR_NAME)
  public void setStartLocator(String value);

  /**
   * The name of the "start-locator" property
   */
  @ConfigAttribute(type=String.class)
  public static final String START_LOCATOR_NAME = "start-locator";
  /**
   * The default value of the "start-locator" property
   */
  public static final String DEFAULT_START_LOCATOR = "";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#deploy-working-dir">"deploy-working-dir"</a> property
   */
  @ConfigAttributeGetter(name=DEPLOY_WORKING_DIR)
  public File getDeployWorkingDir();
  
  /**
   * Sets the system's deploy working directory.
   * @throws IllegalArgumentException if the specified value is not acceptable.
   * @throws com.gemstone.gemfire.UnmodifiableException if this attribute can not be modified.
   * @throws com.gemstone.gemfire.GemFireIOException if the set failure is caused by an error
   *   when writing to the system's configuration file.
   */
  @ConfigAttributeSetter(name=DEPLOY_WORKING_DIR)
  public void setDeployWorkingDir(File value);
  
  /**
   * The name of the "deploy-working-dir" property.
   */
  @ConfigAttribute(type=File.class)
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
  @ConfigAttributeGetter(name=USER_COMMAND_PACKAGES)
  public String getUserCommandPackages();
  
  /**
   * Sets the system's user command path.
   * @throws IllegalArgumentException if the specified value is not acceptable.
   * @throws com.gemstone.gemfire.UnmodifiableException if this attribute can not be modified.
   * @throws com.gemstone.gemfire.GemFireIOException if the set failure is caused by an error
   *   when writing to the system's configuration file.
   */
  @ConfigAttributeSetter(name=USER_COMMAND_PACKAGES)
  public void setUserCommandPackages(String value);
  
  /**
   * The name of the "user-command-packages" property.
   */
  @ConfigAttribute(type=String.class)
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
  @ConfigAttributeGetter(name=LOG_FILE_NAME)
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
  @ConfigAttributeSetter(name=LOG_FILE_NAME)
  public void setLogFile(File value);

  /** The name of the "logFile" property */
  @ConfigAttribute(type=File.class)
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
  @ConfigAttributeGetter(name=LOG_LEVEL_NAME)
  public int getLogLevel();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#log-level">"log-level"</a> property
   *
   * @see com.gemstone.gemfire.internal.logging.LogWriterImpl
   */
  @ConfigAttributeSetter(name=LOG_LEVEL_NAME)
  public void setLogLevel(int value);

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

  /** The name of the "logLevel" property */
  // type is String because the config file contains "config", "debug", "fine" etc, not a code, but the setter/getter accepts int
  @ConfigAttribute(type=String.class)
  public static final String LOG_LEVEL_NAME = "log-level";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#statistic-sampling-enabled">"statistic-sampling-enabled"</a>
   * property
   */
  @ConfigAttributeGetter(name=STATISTIC_SAMPLING_ENABLED_NAME)
  public boolean getStatisticSamplingEnabled();
  /**
   * Sets StatisticSamplingEnabled
   */
  @ConfigAttributeSetter(name=STATISTIC_SAMPLING_ENABLED_NAME)
  public void setStatisticSamplingEnabled(boolean newValue);

  /** The name of the "statisticSamplingEnabled" property */
  @ConfigAttribute(type=Boolean.class)
  public static final String STATISTIC_SAMPLING_ENABLED_NAME =
    "statistic-sampling-enabled";

  /** The default value of the "statisticSamplingEnabled" property */
  public static final boolean DEFAULT_STATISTIC_SAMPLING_ENABLED = true;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#statistic-sample-rate">"statistic-sample-rate"</a>
   * property
   */
  @ConfigAttributeGetter(name=STATISTIC_SAMPLE_RATE_NAME)
  public int getStatisticSampleRate();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#statistic-sample-rate">"statistic-sample-rate"</a>
   * property
   */
  @ConfigAttributeSetter(name=STATISTIC_SAMPLE_RATE_NAME)
  public void setStatisticSampleRate(int value);

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
  @ConfigAttribute(type=Integer.class, min=MIN_STATISTIC_SAMPLE_RATE, max=MAX_STATISTIC_SAMPLE_RATE)
  public static final String STATISTIC_SAMPLE_RATE_NAME =
    "statistic-sample-rate";

  /**
   * Returns the value of the <a href="../DistributedSystem.html#statistic-archive-file">"statistic-archive-file"</a> property.
   *
   * @return <code>null</code> if no file was specified
   */
  @ConfigAttributeGetter(name=STATISTIC_ARCHIVE_FILE_NAME)
  public File getStatisticArchiveFile();
  /**
   * Sets the value of the <a href="../DistributedSystem.html#statistic-archive-file">"statistic-archive-file"</a> property.
   */
  @ConfigAttributeSetter(name=STATISTIC_ARCHIVE_FILE_NAME)
  public void setStatisticArchiveFile(File value);

  /** The name of the "statisticArchiveFile" property */
  @ConfigAttribute(type=File.class)
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
  @ConfigAttributeGetter(name=CACHE_XML_FILE_NAME)
  public File getCacheXmlFile();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cache-xml-file">"cache-xml-file"</a>
   * property
   */
  @ConfigAttributeSetter(name=CACHE_XML_FILE_NAME)
  public void setCacheXmlFile(File value);

  /** The name of the "cacheXmlFile" property */
  @ConfigAttribute(type=File.class)
  public static final String CACHE_XML_FILE_NAME = "cache-xml-file";

  /** The default value of the "cacheXmlFile" property */
  public static final File DEFAULT_CACHE_XML_FILE = new File("cache.xml");

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#ack-wait-threshold">"ack-wait-threshold"</a>
   * property
   */
  @ConfigAttributeGetter(name=ACK_WAIT_THRESHOLD_NAME)
  public int getAckWaitThreshold();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#ack-wait-threshold">"ack-wait-threshold"</a>
   * property
     * Setting this value too low will cause spurious alerts.
     */
  @ConfigAttributeSetter(name=ACK_WAIT_THRESHOLD_NAME)
  public void setAckWaitThreshold(int newThreshold);

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
  /** The name of the "ackWaitThreshold" property */
  @ConfigAttribute(type=Integer.class, min=MIN_ACK_WAIT_THRESHOLD)
  public static final String ACK_WAIT_THRESHOLD_NAME = "ack-wait-threshold";


  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#ack-severe-alert-threshold">"ack-severe-alert-threshold"</a>
   * property
   */
  @ConfigAttributeGetter(name=ACK_SEVERE_ALERT_THRESHOLD_NAME)
  public int getAckSevereAlertThreshold();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#ack-severe-alert-threshold">"ack-severe-alert-threshold"</a>
   * property
     * Setting this value too low will cause spurious forced disconnects.
     */
  @ConfigAttributeSetter(name=ACK_SEVERE_ALERT_THRESHOLD_NAME)
  public void setAckSevereAlertThreshold(int newThreshold);

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
  /** The name of the "ackSevereAlertThreshold" property */
  @ConfigAttribute(type=Integer.class, min=MIN_ACK_SEVERE_ALERT_THRESHOLD)
  public static final String ACK_SEVERE_ALERT_THRESHOLD_NAME = "ack-severe-alert-threshold";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#archive-file-size-limit">"archive-file-size-limit"</a>
   * property
   */
  @ConfigAttributeGetter(name=ARCHIVE_FILE_SIZE_LIMIT_NAME)
  public int getArchiveFileSizeLimit();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#archive-file-size-limit">"archive-file-size-limit"</a>
   * property
   */
  @ConfigAttributeSetter(name=ARCHIVE_FILE_SIZE_LIMIT_NAME)
  public void setArchiveFileSizeLimit(int value);

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
  @ConfigAttribute(type=Integer.class, min=MIN_ARCHIVE_FILE_SIZE_LIMIT, max=MAX_ARCHIVE_FILE_SIZE_LIMIT)
  public static final String ARCHIVE_FILE_SIZE_LIMIT_NAME =
    "archive-file-size-limit";
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#archive-disk-space-limit">"archive-disk-space-limit"</a>
   * property
   */
  @ConfigAttributeGetter(name=ARCHIVE_DISK_SPACE_LIMIT_NAME)
  public int getArchiveDiskSpaceLimit();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#archive-disk-space-limit">"archive-disk-space-limit"</a>
   * property
   */
  @ConfigAttributeSetter(name=ARCHIVE_DISK_SPACE_LIMIT_NAME)
  public void setArchiveDiskSpaceLimit(int value);

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
  @ConfigAttribute(type=Integer.class, min=MIN_ARCHIVE_DISK_SPACE_LIMIT, max=MAX_ARCHIVE_DISK_SPACE_LIMIT)
  public static final String ARCHIVE_DISK_SPACE_LIMIT_NAME =
    "archive-disk-space-limit";
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#log-file-size-limit">"log-file-size-limit"</a>
   * property
   */
  @ConfigAttributeGetter(name=LOG_FILE_SIZE_LIMIT_NAME)
  public int getLogFileSizeLimit();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#log-file-size-limit">"log-file-size-limit"</a>
   * property
   */
  @ConfigAttributeSetter(name=LOG_FILE_SIZE_LIMIT_NAME)
  public void setLogFileSizeLimit(int value);

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
  @ConfigAttribute(type=Integer.class, min=MIN_LOG_FILE_SIZE_LIMIT, max=MAX_LOG_FILE_SIZE_LIMIT)
  public static final String LOG_FILE_SIZE_LIMIT_NAME =
    "log-file-size-limit";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#log-disk-space-limit">"log-disk-space-limit"</a>
   * property
   */
  @ConfigAttributeGetter(name=LOG_DISK_SPACE_LIMIT_NAME)
  public int getLogDiskSpaceLimit();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#log-disk-space-limit">"log-disk-space-limit"</a>
   * property
   */
  @ConfigAttributeSetter(name=LOG_DISK_SPACE_LIMIT_NAME)
  public void setLogDiskSpaceLimit(int value);

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
  @ConfigAttribute(type=Integer.class, min=MIN_LOG_DISK_SPACE_LIMIT, max=MAX_LOG_DISK_SPACE_LIMIT)
  public static final String LOG_DISK_SPACE_LIMIT_NAME =
    "log-disk-space-limit";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#ssl-enabled">"ssl-enabled"</a>
   * property.
   * @deprecated as of 8.0 use {@link #getClusterSSLEnabled} instead.
   */
  @ConfigAttributeGetter(name=SSL_ENABLED_NAME)
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
  @ConfigAttribute(type=Boolean.class)
  public static final String SSL_ENABLED_NAME =
    "ssl-enabled";

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#ssl-enabled">"ssl-enabled"</a>
   * property.
   * @deprecated as of 8.0 use {@link #setClusterSSLEnabled} instead.
   */
  @ConfigAttributeSetter(name=SSL_ENABLED_NAME)
  public void setSSLEnabled( boolean enabled );

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#ssl-protocols">"ssl-protocols"</a>
   * property.
   * @deprecated as of 8.0 use {@link #getClusterSSLProtocols} instead.
   */
  @ConfigAttributeGetter(name=SSL_PROTOCOLS_NAME)
   public String getSSLProtocols( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#ssl-protocols">"ssl-protocols"</a>
   * property.
   * @deprecated as of 8.0 use {@link #setClusterSSLProtocols} instead.
   */
  @ConfigAttributeSetter(name=SSL_PROTOCOLS_NAME)
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
  @ConfigAttribute(type=String.class)
  public static final String SSL_PROTOCOLS_NAME =
    "ssl-protocols";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#ssl-ciphers">"ssl-ciphers"</a>
   * property.
   * @deprecated as of 8.0 use {@link #getClusterSSLCiphers} instead.
   */
  @ConfigAttributeGetter(name=SSL_CIPHERS_NAME)
   public String getSSLCiphers( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#ssl-ciphers">"ssl-ciphers"</a>
   * property.
   * @deprecated as of 8.0 use {@link #setClusterSSLCiphers} instead.
   */
  @ConfigAttributeSetter(name=SSL_CIPHERS_NAME)
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
  @ConfigAttribute(type=String.class)
  public static final String SSL_CIPHERS_NAME =
    "ssl-ciphers";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#ssl-require-authentication">"ssl-require-authentication"</a>
   * property.
   * @deprecated as of 8.0 use {@link #getClusterSSLRequireAuthentication} instead.
   */
  @ConfigAttributeGetter(name=SSL_REQUIRE_AUTHENTICATION_NAME)
   public boolean getSSLRequireAuthentication( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#ssl-require-authentication">"ssl-require-authentication"</a>
   * property.
   * @deprecated as of 8.0 use {@link #setClusterSSLRequireAuthentication} instead.
   */
  @ConfigAttributeSetter(name=SSL_REQUIRE_AUTHENTICATION_NAME)
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
  @ConfigAttribute(type=Boolean.class)
  public static final String SSL_REQUIRE_AUTHENTICATION_NAME =
    "ssl-require-authentication";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-enabled">"cluster-ssl-enabled"</a>
   * property.
   */
  @ConfigAttributeGetter(name=CLUSTER_SSL_ENABLED_NAME)
  public boolean getClusterSSLEnabled();

  /**
   * The default cluster-ssl-enabled state.
   * <p> Actual value of this constant is <code>false</code>.
   */
  public static final boolean DEFAULT_CLUSTER_SSL_ENABLED = false;
  /** The name of the "ClusterSSLEnabled" property */
  @ConfigAttribute(type=Boolean.class)
  public static final String CLUSTER_SSL_ENABLED_NAME =
    "cluster-ssl-enabled";

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-enabled">"cluster-ssl-enabled"</a>
   * property.
   */
  @ConfigAttributeSetter(name=CLUSTER_SSL_ENABLED_NAME)
  public void setClusterSSLEnabled( boolean enabled );

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-protocols">"cluster-ssl-protocols"</a>
   * property.
   */
  @ConfigAttributeGetter(name=CLUSTER_SSL_PROTOCOLS_NAME)
   public String getClusterSSLProtocols( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-protocols">"cluster-ssl-protocols"</a>
   * property.
   */
  @ConfigAttributeSetter(name=CLUSTER_SSL_PROTOCOLS_NAME)
   public void setClusterSSLProtocols( String protocols );

  /**
   * The default cluster-ssl-protocols value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  public static final String DEFAULT_CLUSTER_SSL_PROTOCOLS = "any";
  /** The name of the "ClusterSSLProtocols" property */
  @ConfigAttribute(type=String.class)
  public static final String CLUSTER_SSL_PROTOCOLS_NAME =
    "cluster-ssl-protocols";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-ciphers">"cluster-ssl-ciphers"</a>
   * property.
   */
  @ConfigAttributeGetter(name=CLUSTER_SSL_CIPHERS_NAME)
   public String getClusterSSLCiphers( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-ciphers">"cluster-ssl-ciphers"</a>
   * property.
   */
  @ConfigAttributeSetter(name=CLUSTER_SSL_CIPHERS_NAME)
   public void setClusterSSLCiphers( String ciphers );

   /**
   * The default cluster-ssl-ciphers value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  public static final String DEFAULT_CLUSTER_SSL_CIPHERS = "any";
  /** The name of the "ClusterSSLCiphers" property */
  @ConfigAttribute(type=String.class)
  public static final String CLUSTER_SSL_CIPHERS_NAME =
    "cluster-ssl-ciphers";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-require-authentication">"cluster-ssl-require-authentication"</a>
   * property.
   */
  @ConfigAttributeGetter(name=CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME)
   public boolean getClusterSSLRequireAuthentication( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-require-authentication">"cluster-ssl-require-authentication"</a>
   * property.
   */
  @ConfigAttributeSetter(name=CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME)
   public void setClusterSSLRequireAuthentication( boolean enabled );

   /**
   * The default cluster-ssl-require-authentication value.
   * <p> Actual value of this constant is <code>true</code>.
   */
  public static final boolean DEFAULT_CLUSTER_SSL_REQUIRE_AUTHENTICATION = true;
  /** The name of the "ClusterSSLRequireAuthentication" property */
  @ConfigAttribute(type=Boolean.class)
  public static final String CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME =
    "cluster-ssl-require-authentication";

  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-keystore">"cluster-ssl-keystore"</a>
   * property.
   */
  @ConfigAttributeGetter(name=CLUSTER_SSL_KEYSTORE_NAME)
  public String getClusterSSLKeyStore( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-keystore">"cluster-ssl-keystore"</a>
   * property.
   */
  @ConfigAttributeSetter(name=CLUSTER_SSL_KEYSTORE_NAME)
  public void setClusterSSLKeyStore( String keyStore);
  
  /**
   * The default cluster-ssl-keystore value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_CLUSTER_SSL_KEYSTORE = "";
  
  /** The name of the "ClusterSSLKeyStore" property */
  @ConfigAttribute(type=String.class)
  public static final String CLUSTER_SSL_KEYSTORE_NAME = "cluster-ssl-keystore";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-keystore-type">"cluster-ssl-keystore-type"</a>
   * property.
   */
  @ConfigAttributeGetter(name=CLUSTER_SSL_KEYSTORE_TYPE_NAME)
  public String getClusterSSLKeyStoreType( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-keystore-type">"cluster-ssl-keystore-type"</a>
   * property.
   */
  @ConfigAttributeSetter(name=CLUSTER_SSL_KEYSTORE_TYPE_NAME)
  public void setClusterSSLKeyStoreType( String keyStoreType);
  
  /**
   * The default cluster-ssl-keystore-type value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_CLUSTER_SSL_KEYSTORE_TYPE = "";
  
  /** The name of the "ClusterSSLKeyStoreType" property */
  @ConfigAttribute(type=String.class)
  public static final String CLUSTER_SSL_KEYSTORE_TYPE_NAME = "cluster-ssl-keystore-type";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-keystore-password">"cluster-ssl-keystore-password"</a>
   * property.
   */
  @ConfigAttributeGetter(name=CLUSTER_SSL_KEYSTORE_PASSWORD_NAME)
  public String getClusterSSLKeyStorePassword( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-keystore-password">"cluster-ssl-keystore-password"</a>
   * property.
   */
  @ConfigAttributeSetter(name=CLUSTER_SSL_KEYSTORE_PASSWORD_NAME)
  public void setClusterSSLKeyStorePassword( String keyStorePassword);
  
  /**
   * The default cluster-ssl-keystore-password value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_CLUSTER_SSL_KEYSTORE_PASSWORD = "";
  
  /** The name of the "ClusterSSLKeyStorePassword" property */
  @ConfigAttribute(type=String.class)
  public static final String CLUSTER_SSL_KEYSTORE_PASSWORD_NAME = "cluster-ssl-keystore-password";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-truststore">"cluster-ssl-truststore"</a>
   * property.
   */
  @ConfigAttributeGetter(name=CLUSTER_SSL_TRUSTSTORE_NAME)
  public String getClusterSSLTrustStore( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-truststore">"cluster-ssl-truststore"</a>
   * property.
   */
  @ConfigAttributeSetter(name=CLUSTER_SSL_TRUSTSTORE_NAME)
  public void setClusterSSLTrustStore( String trustStore);
  
  /**
   * The default cluster-ssl-truststore value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_CLUSTER_SSL_TRUSTSTORE = "";
  
  /** The name of the "ClusterSSLTrustStore" property */
  @ConfigAttribute(type=String.class)
  public static final String CLUSTER_SSL_TRUSTSTORE_NAME = "cluster-ssl-truststore";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-truststore-password">"cluster-ssl-truststore-password"</a>
   * property.
   */
  @ConfigAttributeGetter(name=CLUSTER_SSL_TRUSTSTORE_PASSWORD_NAME)
  public String getClusterSSLTrustStorePassword( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-truststore-password">"cluster-ssl-truststore-password"</a>
   * property.
   */
  @ConfigAttributeSetter(name=CLUSTER_SSL_TRUSTSTORE_PASSWORD_NAME)
  public void setClusterSSLTrustStorePassword( String trusStorePassword);
  /**
   * The default cluster-ssl-truststore-password value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_CLUSTER_SSL_TRUSTSTORE_PASSWORD = "";
  
  /** The name of the "ClusterSSLKeyStorePassword" property */
  @ConfigAttribute(type=String.class)
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
  @ConfigAttributeGetter(name=SOCKET_LEASE_TIME_NAME)
  public int getSocketLeaseTime();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#socket-lease-time">"socket-lease-time"</a>
   * property
   */
  @ConfigAttributeSetter(name=SOCKET_LEASE_TIME_NAME)
  public void setSocketLeaseTime(int value);



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

  /** The name of the "socketLeaseTime" property */
  @ConfigAttribute(type=Integer.class, min=MIN_SOCKET_LEASE_TIME, max=MAX_SOCKET_LEASE_TIME)
  public static final String SOCKET_LEASE_TIME_NAME = "socket-lease-time";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#socket-buffer-size">"socket-buffer-size"</a>
   * property
   */
  @ConfigAttributeGetter(name=SOCKET_BUFFER_SIZE_NAME)
  public int getSocketBufferSize();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#socket-buffer-size">"socket-buffer-size"</a>
   * property
   */
  @ConfigAttributeSetter(name=SOCKET_BUFFER_SIZE_NAME)
  public void setSocketBufferSize(int value);


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

  /** The name of the "socketBufferSize" property */
  @ConfigAttribute(type=Integer.class, min=MIN_SOCKET_BUFFER_SIZE, max=MAX_SOCKET_BUFFER_SIZE)
  public static final String SOCKET_BUFFER_SIZE_NAME = "socket-buffer-size";


  /**
   * Get the value of the
   * <a href="../DistributedSystem.html#mcast-send-buffer-size">"mcast-send-buffer-size"</a>
   * property
   */
  @ConfigAttributeGetter(name=MCAST_SEND_BUFFER_SIZE_NAME)
  public int getMcastSendBufferSize();

  /**
   * Set the value of the
   * <a href="../DistributedSystem.html#mcast-send-buffer-size">"mcast-send-buffer-size"</a>
   * property
   */
  @ConfigAttributeSetter(name=MCAST_SEND_BUFFER_SIZE_NAME)
  public void setMcastSendBufferSize(int value);


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
   * The name of the corresponding property
   */
  @ConfigAttribute(type=Integer.class, min=MIN_MCAST_SEND_BUFFER_SIZE)
  public static final String MCAST_SEND_BUFFER_SIZE_NAME = "mcast-send-buffer-size";

  /**
   * Get the value of the
   * <a href="../DistributedSystem.html#mcast-recv-buffer-size">"mcast-recv-buffer-size"</a>
   * property
   */
  @ConfigAttributeGetter(name=MCAST_RECV_BUFFER_SIZE_NAME)
  public int getMcastRecvBufferSize();

  /**
   * Set the value of the
   * <a href="../DistributedSystem.html#mcast-recv-buffer-size">"mcast-recv-buffer-size"</a>
   * property
   */
  @ConfigAttributeSetter(name=MCAST_RECV_BUFFER_SIZE_NAME)
  public void setMcastRecvBufferSize(int value);


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
   * The name of the corresponding property
   */
  @ConfigAttribute(type=Integer.class, min=MIN_MCAST_RECV_BUFFER_SIZE)
  public static final String MCAST_RECV_BUFFER_SIZE_NAME = "mcast-recv-buffer-size";


  /**
   * Get the value of the
   * <a href="../DistributedSystem.html#mcast-flow-control">"mcast-flow-control"</a>
   * property.
   */
  @ConfigAttributeGetter(name=MCAST_FLOW_CONTROL_NAME)
  public FlowControlParams getMcastFlowControl();

  /**
   * Set the value of the
   * <a href="../DistributedSystem.html#mcast-flow-control">"mcast-flow-control"</a>
   * property
   */
  @ConfigAttributeSetter(name=MCAST_FLOW_CONTROL_NAME)
  public void setMcastFlowControl(FlowControlParams values);

  /**
   * The name of the corresponding property
   */
  @ConfigAttribute(type=FlowControlParams.class)
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
  @ConfigAttributeGetter(name=UDP_FRAGMENT_SIZE_NAME)
  public int getUdpFragmentSize();

  /**
   * Set the value of the
   * <a href="../DistributedSystem.html#udp-fragment-size">"udp-fragment-size"</a>
   * property
   */
  @ConfigAttributeSetter(name=UDP_FRAGMENT_SIZE_NAME)
  public void setUdpFragmentSize(int value);

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
   * The name of the corresponding property
   */
  @ConfigAttribute(type=Integer.class, min=MIN_UDP_FRAGMENT_SIZE, max=MAX_UDP_FRAGMENT_SIZE)
  public static final String UDP_FRAGMENT_SIZE_NAME = "udp-fragment-size";




  /**
   * Get the value of the
   * <a href="../DistributedSystem.html#udp-send-buffer-size">"udp-send-buffer-size"</a>
   * property
   */
  @ConfigAttributeGetter(name=UDP_SEND_BUFFER_SIZE_NAME)
  public int getUdpSendBufferSize();

  /**
   * Set the value of the
   * <a href="../DistributedSystem.html#udp-send-buffer-size">"udp-send-buffer-size"</a>
   * property
   */
  @ConfigAttributeSetter(name=UDP_SEND_BUFFER_SIZE_NAME)
  public void setUdpSendBufferSize(int value);

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
   * The name of the corresponding property
   */
  @ConfigAttribute(type=Integer.class, min=MIN_UDP_SEND_BUFFER_SIZE)
  public static final String UDP_SEND_BUFFER_SIZE_NAME = "udp-send-buffer-size";

  /**
   * Get the value of the
   * <a href="../DistributedSystem.html#udp-recv-buffer-size">"udp-recv-buffer-size"</a>
   * property
   */
  @ConfigAttributeGetter(name=UDP_RECV_BUFFER_SIZE_NAME)
  public int getUdpRecvBufferSize();

  /**
   * Set the value of the
   * <a href="../DistributedSystem.html#udp-recv-buffer-size">"udp-recv-buffer-size"</a>
   * property
   */
  @ConfigAttributeSetter(name=UDP_RECV_BUFFER_SIZE_NAME)
  public void setUdpRecvBufferSize(int value);

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
   * The name of the corresponding property
   */
  @ConfigAttribute(type=Integer.class, min=MIN_UDP_RECV_BUFFER_SIZE)
  public static final String UDP_RECV_BUFFER_SIZE_NAME = "udp-recv-buffer-size";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#disable-tcp">"disable-tcp"</a>
   * property
   */
  @ConfigAttributeGetter(name=DISABLE_TCP_NAME)
  public boolean getDisableTcp();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#disable-tcp">"disable-tcp"</a>
   * property.
   */
  @ConfigAttributeSetter(name=DISABLE_TCP_NAME)
  public void setDisableTcp(boolean newValue);

  /** The name of the corresponding property */
  @ConfigAttribute(type=Boolean.class)
  public static final String DISABLE_TCP_NAME = "disable-tcp";

  /** The default value of the corresponding property */
  public static final boolean DEFAULT_DISABLE_TCP = false;


  /**
   * Turns on timing statistics for the distributed system
   */
  @ConfigAttributeSetter(name=ENABLE_TIME_STATISTICS_NAME)
  public void setEnableTimeStatistics(boolean newValue);

  /**
   * Returns the value of <a
   * href="../DistributedSystem.html#enable-time-statistics">enable-time-statistics</a>
   * property
   */
  @ConfigAttributeGetter(name=ENABLE_TIME_STATISTICS_NAME)
  public boolean getEnableTimeStatistics();

  /** the name of the corresponding property */
  @ConfigAttribute(type=Boolean.class)
  public static final String ENABLE_TIME_STATISTICS_NAME = "enable-time-statistics";

  /** The default value of the corresponding property */
  public static final boolean DEFAULT_ENABLE_TIME_STATISTICS = false;


  /**
   * Sets the value for 
   <a href="../DistributedSystem.html#use-cluster-configuration">use-shared-configuration</a>
   */
  @ConfigAttributeSetter(name=USE_CLUSTER_CONFIGURATION_NAME)
  public void setUseSharedConfiguration(boolean newValue);

  /**
   * Returns the value of <a
   * href="../DistributedSystem.html#use-cluster-configuration">use-cluster-configuration</a>
   * property
   */
  @ConfigAttributeGetter(name=USE_CLUSTER_CONFIGURATION_NAME)
  public boolean getUseSharedConfiguration();

  /** the name of the corresponding property */
  @ConfigAttribute(type=Boolean.class)
  public static final String USE_CLUSTER_CONFIGURATION_NAME = "use-cluster-configuration";

  /** The default value of the corresponding property */
  public static final boolean DEFAULT_USE_CLUSTER_CONFIGURATION = true;
  
  /**
   * Sets the value for 
   <a href="../DistributedSystem.html#enable-cluster-configuration">enable-cluster-configuration</a>
   */
  @ConfigAttributeSetter(name=ENABLE_CLUSTER_CONFIGURATION_NAME)
  public void setEnableClusterConfiguration(boolean newValue);

  /**
   * Returns the value of <a
   * href="../DistributedSystem.html#enable-cluster-configuration">enable-cluster-configuration</a>
   * property
   */
  @ConfigAttributeGetter(name=ENABLE_CLUSTER_CONFIGURATION_NAME)
  public boolean getEnableClusterConfiguration();

  /** the name of the corresponding property */
  @ConfigAttribute(type=Boolean.class)
  public static final String ENABLE_CLUSTER_CONFIGURATION_NAME = "enable-cluster-configuration";
  /** The default value of the corresponding property */
  public static final boolean DEFAULT_ENABLE_CLUSTER_CONFIGURATION = true;

  @ConfigAttribute(type=Boolean.class)
  public static final String LOAD_CLUSTER_CONFIG_FROM_DIR_NAME = "load-cluster-configuration-from-dir";
  public static final boolean DEFAULT_LOAD_CLUSTER_CONFIG_FROM_DIR = false;
  
  /**
   * Returns the value of 
   * <a href="../DistributedSystem.html#cluster-configuration-dir">cluster-configuration-dir</a>
   * property
   */
  @ConfigAttributeGetter(name=LOAD_CLUSTER_CONFIG_FROM_DIR_NAME)
  public boolean getLoadClusterConfigFromDir();
  
  /**
   * Sets the value of 
   * <a href="../DistributedSystem.html#cluster-configuration-dir">cluster-configuration-dir</a>
   * property
   */
  @ConfigAttributeSetter(name=LOAD_CLUSTER_CONFIG_FROM_DIR_NAME)
  public void setLoadClusterConfigFromDir(boolean newValue);

  @ConfigAttribute(type=String.class)
  public static final String CLUSTER_CONFIGURATION_DIR = "cluster-configuration-dir";
  public static final String DEFAULT_CLUSTER_CONFIGURATION_DIR = System.getProperty("user.dir");

  @ConfigAttributeGetter(name=CLUSTER_CONFIGURATION_DIR)
  public String getClusterConfigDir();
  @ConfigAttributeSetter(name=CLUSTER_CONFIGURATION_DIR)
  public void setClusterConfigDir(final String clusterConfigDir);
  
  /** Turns on network partition detection */
  @ConfigAttributeSetter(name=ENABLE_NETWORK_PARTITION_DETECTION_NAME)
  public void setEnableNetworkPartitionDetection(boolean newValue);
  /**
   * Returns the value of the enable-network-partition-detection property
   */
  @ConfigAttributeGetter(name=ENABLE_NETWORK_PARTITION_DETECTION_NAME)
  public boolean getEnableNetworkPartitionDetection();
  /** the name of the corresponding property */
  @ConfigAttribute(type=Boolean.class)
  public static final String ENABLE_NETWORK_PARTITION_DETECTION_NAME =
    "enable-network-partition-detection";
  public static final boolean DEFAULT_ENABLE_NETWORK_PARTITION_DETECTION = false;

  /**
   * Get the value of the
   * <a href="../DistributedSystem.html#member-timeout">"member-timeout"</a>
   * property
   */
  @ConfigAttributeGetter(name=MEMBER_TIMEOUT_NAME)
  public int getMemberTimeout();

  /**
   * Set the value of the
   * <a href="../DistributedSystem.html#member-timeout">"member-timeout"</a>
   * property
   */
  @ConfigAttributeSetter(name=MEMBER_TIMEOUT_NAME)
  public void setMemberTimeout(int value);

  /**
   * The default value of the corresponding property
   */
  public static final int DEFAULT_MEMBER_TIMEOUT = 5000;

  /** The minimum member-timeout setting of 1000 milliseconds */
  public static final int MIN_MEMBER_TIMEOUT = 10;

  /**The maximum member-timeout setting of 600000 millieseconds */
  public static final int MAX_MEMBER_TIMEOUT = 600000;
  /**
   * The name of the corresponding property
   */
  @ConfigAttribute(type=Integer.class, min=MIN_MEMBER_TIMEOUT, max=MAX_MEMBER_TIMEOUT)
  public static final String MEMBER_TIMEOUT_NAME = "member-timeout";

  @ConfigAttribute(type=int[].class)
  public static final String MEMBERSHIP_PORT_RANGE_NAME = "membership-port-range";
  
  public static final int[] DEFAULT_MEMBERSHIP_PORT_RANGE = new int[]{1024,65535};

  @ConfigAttributeGetter(name=MEMBERSHIP_PORT_RANGE_NAME)
  public int[] getMembershipPortRange();

  @ConfigAttributeSetter(name=MEMBERSHIP_PORT_RANGE_NAME)
  public void setMembershipPortRange(int[] range);
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#conserve-sockets">"conserve-sockets"</a>
   * property
   */
  @ConfigAttributeGetter(name=CONSERVE_SOCKETS_NAME)
  public boolean getConserveSockets();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#conserve-sockets">"conserve-sockets"</a>
   * property.
   */
  @ConfigAttributeSetter(name=CONSERVE_SOCKETS_NAME)
  public void setConserveSockets(boolean newValue);

  /** The name of the "conserveSockets" property */
  @ConfigAttribute(type=Boolean.class)
  public static final String CONSERVE_SOCKETS_NAME = "conserve-sockets";

  /** The default value of the "conserveSockets" property */
  public static final boolean DEFAULT_CONSERVE_SOCKETS = true;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#roles">"roles"</a>
   * property
   */
  @ConfigAttributeGetter(name=ROLES_NAME)
  public String getRoles();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#roles">"roles"</a>
   * property.
   */
  @ConfigAttributeSetter(name=ROLES_NAME)
  public void setRoles(String roles);
  /** The name of the "roles" property */
  @ConfigAttribute(type=String.class)
  public static final String ROLES_NAME = "roles";
  /** The default value of the "roles" property */
  public static final String DEFAULT_ROLES = "";


  /**
   * The name of the "max wait time for reconnect" property */
  @ConfigAttribute(type=Integer.class)
  public static final String MAX_WAIT_TIME_FOR_RECONNECT_NAME = "max-wait-time-reconnect";

  /**
   * Default value for MAX_WAIT_TIME_FOR_RECONNECT, 60,000 milliseconds.
   */
  public static final int DEFAULT_MAX_WAIT_TIME_FOR_RECONNECT = 60000;

  /**
   * Sets the max wait timeout, in milliseconds, for reconnect.
   * */
  @ConfigAttributeSetter(name=MAX_WAIT_TIME_FOR_RECONNECT_NAME)
  public void setMaxWaitTimeForReconnect( int timeOut);

  /**
   * Returns the max wait timeout, in milliseconds, for reconnect.
   * */
  @ConfigAttributeGetter(name=MAX_WAIT_TIME_FOR_RECONNECT_NAME)
  public int getMaxWaitTimeForReconnect();

  /**
   * The name of the "max number of tries for reconnect" property.
   * */
  @ConfigAttribute(type=Integer.class)
  public static final String MAX_NUM_RECONNECT_TRIES = "max-num-reconnect-tries";

  /**
   * Default value for MAX_NUM_RECONNECT_TRIES.
   */
  public static final int DEFAULT_MAX_NUM_RECONNECT_TRIES = 3;

  /**
   * Sets the max number of tries for reconnect.
   * */
  @ConfigAttributeSetter(name=MAX_NUM_RECONNECT_TRIES)
  public void setMaxNumReconnectTries(int tries);

  /**
   * Returns the value for max number of tries for reconnect.
   * */
  @ConfigAttributeGetter(name=MAX_NUM_RECONNECT_TRIES)
  public int getMaxNumReconnectTries();

  // ------------------- Asynchronous Messaging Properties -------------------

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#async-distribution-timeout">
   * "async-distribution-timeout"</a> property.
   */
  @ConfigAttributeGetter(name=ASYNC_DISTRIBUTION_TIMEOUT_NAME)
  public int getAsyncDistributionTimeout();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#async-distribution-timeout">
   * "async-distribution-timeout"</a> property.
   */
  @ConfigAttributeSetter(name=ASYNC_DISTRIBUTION_TIMEOUT_NAME)
  public void setAsyncDistributionTimeout(int newValue);

  /** The default value of "asyncDistributionTimeout" is <code>0</code>. */
  public static final int DEFAULT_ASYNC_DISTRIBUTION_TIMEOUT = 0;
  /** The minimum value of "asyncDistributionTimeout" is <code>0</code>. */
  public static final int MIN_ASYNC_DISTRIBUTION_TIMEOUT = 0;
  /** The maximum value of "asyncDistributionTimeout" is <code>60000</code>. */
  public static final int MAX_ASYNC_DISTRIBUTION_TIMEOUT = 60000;

  /** The name of the "asyncDistributionTimeout" property */
  @ConfigAttribute(type=Integer.class, min=MIN_ASYNC_DISTRIBUTION_TIMEOUT, max=MAX_ASYNC_DISTRIBUTION_TIMEOUT)
  public static final String ASYNC_DISTRIBUTION_TIMEOUT_NAME = "async-distribution-timeout";
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#async-queue-timeout">
   * "async-queue-timeout"</a> property.
   */
  @ConfigAttributeGetter(name=ASYNC_QUEUE_TIMEOUT_NAME)
  public int getAsyncQueueTimeout();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#async-queue-timeout">
   * "async-queue-timeout"</a> property.
   */
  @ConfigAttributeSetter(name=ASYNC_QUEUE_TIMEOUT_NAME)
  public void setAsyncQueueTimeout(int newValue);

  /** The default value of "asyncQueueTimeout" is <code>60000</code>. */
  public static final int DEFAULT_ASYNC_QUEUE_TIMEOUT = 60000;
  /** The minimum value of "asyncQueueTimeout" is <code>0</code>. */
  public static final int MIN_ASYNC_QUEUE_TIMEOUT = 0;
  /** The maximum value of "asyncQueueTimeout" is <code>86400000</code>. */
  public static final int MAX_ASYNC_QUEUE_TIMEOUT = 86400000;
  /** The name of the "asyncQueueTimeout" property */
  @ConfigAttribute(type=Integer.class, min=MIN_ASYNC_QUEUE_TIMEOUT, max=MAX_ASYNC_QUEUE_TIMEOUT)
  public static final String ASYNC_QUEUE_TIMEOUT_NAME = "async-queue-timeout";
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#async-max-queue-size">
   * "async-max-queue-size"</a> property.
   */
  @ConfigAttributeGetter(name=ASYNC_MAX_QUEUE_SIZE_NAME)
  public int getAsyncMaxQueueSize();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#async-max-queue-size">
   * "async-max-queue-size"</a> property.
   */
  @ConfigAttributeSetter(name=ASYNC_MAX_QUEUE_SIZE_NAME)
  public void setAsyncMaxQueueSize(int newValue);

  /** The default value of "asyncMaxQueueSize" is <code>8</code>. */
  public static final int DEFAULT_ASYNC_MAX_QUEUE_SIZE = 8;
  /** The minimum value of "asyncMaxQueueSize" is <code>0</code>. */
  public static final int MIN_ASYNC_MAX_QUEUE_SIZE = 0;
  /** The maximum value of "asyncMaxQueueSize" is <code>1024</code>. */
  public static final int MAX_ASYNC_MAX_QUEUE_SIZE = 1024;

  /** The name of the "asyncMaxQueueSize" property */
  @ConfigAttribute(type=Integer.class, min=MIN_ASYNC_MAX_QUEUE_SIZE, max=MAX_ASYNC_MAX_QUEUE_SIZE)
  public static final String ASYNC_MAX_QUEUE_SIZE_NAME = "async-max-queue-size";
  /** @since 5.7 */
  @ConfigAttribute(type=String.class)
  public static final String CLIENT_CONFLATION_PROP_NAME = "conflate-events";
  /** @since 5.7 */
  public static final String CLIENT_CONFLATION_PROP_VALUE_DEFAULT = "server";
  /** @since 5.7 */
  public static final String CLIENT_CONFLATION_PROP_VALUE_ON = "true";
  /** @since 5.7 */
  public static final String CLIENT_CONFLATION_PROP_VALUE_OFF = "false";
  
     
  /** @since 9.0 */
  @ConfigAttribute(type=Boolean.class)
  public static final String DISTRIBUTED_TRANSACTIONS_NAME = "distributed-transactions";
  public static final boolean DEFAULT_DISTRIBUTED_TRANSACTIONS = false;

  @ConfigAttributeGetter(name=DISTRIBUTED_TRANSACTIONS_NAME)
  public boolean getDistributedTransactions();

  @ConfigAttributeSetter(name=DISTRIBUTED_TRANSACTIONS_NAME)
  public void setDistributedTransactions(boolean value);
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#conflate-events">"conflate-events"</a>
   * property.
   * @since 5.7
   */
  @ConfigAttributeGetter(name=CLIENT_CONFLATION_PROP_NAME)
  public String getClientConflation();
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#conflate-events">"conflate-events"</a>
   * property.
   * @since 5.7
   */
  @ConfigAttributeSetter(name=CLIENT_CONFLATION_PROP_NAME)
  public void setClientConflation(String clientConflation);
  // -------------------------------------------------------------------------
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#durable-client-id">"durable-client-id"</a>
   * property.
   */
  @ConfigAttributeGetter(name=DURABLE_CLIENT_ID_NAME)
  public String getDurableClientId();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#durable-client-id">"durable-client-id"</a>
   * property.
   */
  @ConfigAttributeSetter(name=DURABLE_CLIENT_ID_NAME)
  public void setDurableClientId(String durableClientId);

  /** The name of the "durableClientId" property */
  @ConfigAttribute(type=String.class)
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
  @ConfigAttributeGetter(name=DURABLE_CLIENT_TIMEOUT_NAME)
  public int getDurableClientTimeout();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#durable-client-timeout">"durable-client-timeout"</a>
   * property.
   */
  @ConfigAttributeSetter(name=DURABLE_CLIENT_TIMEOUT_NAME)
  public void setDurableClientTimeout(int durableClientTimeout);

  /** The name of the "durableClientTimeout" property */
  @ConfigAttribute(type=Integer.class)
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
  @ConfigAttributeGetter(name=SECURITY_CLIENT_AUTH_INIT_NAME)
  public String getSecurityClientAuthInit();

  /**
   * Sets the user module name in <a
   * href="../DistributedSystem.html#security-client-auth-init">"security-client-auth-init"</a>
   * property.
   */
  @ConfigAttributeSetter(name=SECURITY_CLIENT_AUTH_INIT_NAME)
  public void setSecurityClientAuthInit(String attValue);

  /** The name of user defined method name for "security-client-auth-init" property*/
  @ConfigAttribute(type=String.class)
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
  @ConfigAttributeGetter(name=SECURITY_CLIENT_AUTHENTICATOR_NAME)
  public String getSecurityClientAuthenticator();

  /**
   * Sets the user defined method name in <a
   * href="../DistributedSystem.html#security-client-authenticator">"security-client-authenticator"</a>
   * property.
   */
  @ConfigAttributeSetter(name=SECURITY_CLIENT_AUTHENTICATOR_NAME)
  public void setSecurityClientAuthenticator(String attValue);

  /** The name of factory method for "security-client-authenticator" property */
  @ConfigAttribute(type=String.class)
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
  @ConfigAttributeGetter(name=SECURITY_CLIENT_DHALGO_NAME)
  public String getSecurityClientDHAlgo();

  /**
   * Set the name of algorithm to use for Diffie-Hellman key exchange <a
   * href="../DistributedSystem.html#security-client-dhalgo">"security-client-dhalgo"</a>
   * property.
   */
  @ConfigAttributeSetter(name=SECURITY_CLIENT_DHALGO_NAME)
  public void setSecurityClientDHAlgo(String attValue);

  /**
   * The name of the Diffie-Hellman symmetric algorithm "security-client-dhalgo"
   * property.
   */
  @ConfigAttribute(type=String.class)
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
  @ConfigAttributeGetter(name=SECURITY_PEER_AUTH_INIT_NAME)
  public String getSecurityPeerAuthInit();

  /**
   * Sets the user module name in <a
   * href="../DistributedSystem.html#security-peer-auth-init">"security-peer-auth-init"</a>
   * property.
   */
  @ConfigAttributeSetter(name=SECURITY_PEER_AUTH_INIT_NAME)
  public void setSecurityPeerAuthInit(String attValue);

  /** The name of user module for "security-peer-auth-init" property*/
  @ConfigAttribute(type=String.class)
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
  @ConfigAttributeGetter(name=SECURITY_PEER_AUTHENTICATOR_NAME)
  public String getSecurityPeerAuthenticator();

  /**
   * Sets the user module name in <a
   * href="../DistributedSystem.html#security-peer-authenticator">"security-peer-authenticator"</a>
   * property.
   */
  @ConfigAttributeSetter(name=SECURITY_PEER_AUTHENTICATOR_NAME)
  public void setSecurityPeerAuthenticator(String attValue);

  /** The name of user defined method for "security-peer-authenticator" property*/
  @ConfigAttribute(type=String.class)
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
  @ConfigAttributeGetter(name=SECURITY_CLIENT_ACCESSOR_NAME)
  public String getSecurityClientAccessor();

  /**
   * Sets the user defined method name in <a
   * href="../DistributedSystem.html#security-client-accessor">"security-client-accessor"</a>
   * property.
   */
  @ConfigAttributeSetter(name=SECURITY_CLIENT_ACCESSOR_NAME)
  public void setSecurityClientAccessor(String attValue);

  /** The name of the factory method for "security-client-accessor" property */
  @ConfigAttribute(type=String.class)
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
  @ConfigAttributeGetter(name=SECURITY_CLIENT_ACCESSOR_PP_NAME)
  public String getSecurityClientAccessorPP();

  /**
   * Sets the user defined method name in <a
   * href="../DistributedSystem.html#security-client-accessor-pp">"security-client-accessor-pp"</a>
   * property.
   */
  @ConfigAttributeSetter(name=SECURITY_CLIENT_ACCESSOR_PP_NAME)
  public void setSecurityClientAccessorPP(String attValue);

  /** The name of the factory method for "security-client-accessor-pp" property */
  @ConfigAttribute(type=String.class)
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
  @ConfigAttributeGetter(name=SECURITY_LOG_LEVEL_NAME)
  public int getSecurityLogLevel();

  /**
   * Set the log-level for security logging.
   *
   * @param level
   *                the new security log-level
   */
  @ConfigAttributeSetter(name=SECURITY_LOG_LEVEL_NAME)
  public void setSecurityLogLevel(int level);

  /**
   * The name of "security-log-level" property that sets the log-level for
   * security logger obtained using
   * {@link DistributedSystem#getSecurityLogWriter()}
   */
  // type is String because the config file "config", "debug", "fine" etc, but the setter getter accepts int
  @ConfigAttribute(type=String.class)
  public static final String SECURITY_LOG_LEVEL_NAME = "security-log-level";

  /**
   * Returns the value of the "security-log-file" property
   *
   * @return <code>null</code> if logging information goes to standard out
   */
  @ConfigAttributeGetter(name=SECURITY_LOG_FILE_NAME)
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
  @ConfigAttributeSetter(name=SECURITY_LOG_FILE_NAME)
  public void setSecurityLogFile(File value);

  /**
   * The name of the "security-log-file" property. This property is the path of
   * the file where security related messages are logged.
   */
  @ConfigAttribute(type=File.class)
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
  @ConfigAttributeGetter(name=SECURITY_PEER_VERIFYMEMBER_TIMEOUT_NAME)
  public int getSecurityPeerMembershipTimeout();

  /**
   * Set timeout for peer membership check when security is enabled. The timeout must be less
   * than peer handshake timeout.
   * @param attValue
   */
  @ConfigAttributeSetter(name=SECURITY_PEER_VERIFYMEMBER_TIMEOUT_NAME)
  public void setSecurityPeerMembershipTimeout(int attValue);


  /**
   * The default peer membership check timeout is 1 second.
   */
  public static final int DEFAULT_SECURITY_PEER_VERIFYMEMBER_TIMEOUT = 1000;

  /**
   * Max membership timeout must be less than max peer handshake timeout. Currently this is set to
   * default handshake timeout of 60 seconds.
   */
  public static final int MAX_SECURITY_PEER_VERIFYMEMBER_TIMEOUT = 60000;

  /** The name of the peer membership check timeout property */
  @ConfigAttribute(type=Integer.class, min=0, max=MAX_SECURITY_PEER_VERIFYMEMBER_TIMEOUT)
  public static final String SECURITY_PEER_VERIFYMEMBER_TIMEOUT_NAME = "security-peer-verifymember-timeout";
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
   @ConfigAttribute(type=Boolean.class)
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
   @ConfigAttributeGetter(name=REMOVE_UNRESPONSIVE_CLIENT_PROP_NAME)
   public boolean getRemoveUnresponsiveClient();
   /**
    * Sets the value of the <a
    * href="../DistributedSystem.html#remove-unresponsive-client">"remove-unresponsive-client"</a>
    * property.
    * @since 6.0
    */
   @ConfigAttributeSetter(name=REMOVE_UNRESPONSIVE_CLIENT_PROP_NAME)
   public void setRemoveUnresponsiveClient(boolean value);

   /** @since 6.3 */
   @ConfigAttribute(type=Boolean.class)
   public static final String DELTA_PROPAGATION_PROP_NAME = "delta-propagation";

   public static final boolean DEFAULT_DELTA_PROPAGATION = true;
   /**
    * Returns the value of the <a
    * href="../DistributedSystem.html#delta-propagation">"delta-propagation"</a>
    * property.
    * @since 6.3
    */
   @ConfigAttributeGetter(name=DELTA_PROPAGATION_PROP_NAME)
   public boolean getDeltaPropagation();

   /**
    * Sets the value of the <a
    * href="../DistributedSystem.html#delta-propagation">"delta-propagation"</a>
    * property.
    * @since 6.3
    */
   @ConfigAttributeSetter(name=DELTA_PROPAGATION_PROP_NAME)
   public void setDeltaPropagation(boolean value);

  public static final int MIN_DISTRIBUTED_SYSTEM_ID = -1;
  public static final int MAX_DISTRIBUTED_SYSTEM_ID = 255;
   /**
    * @since 6.6
    */
   @ConfigAttribute(type=Integer.class)
   public static final String DISTRIBUTED_SYSTEM_ID_NAME = "distributed-system-id";
   public static final int DEFAULT_DISTRIBUTED_SYSTEM_ID = -1;

  @ConfigAttribute(type=String.class)
   public static final String REDUNDANCY_ZONE_NAME = "redundancy-zone";
   public static final String DEFAULT_REDUNDANCY_ZONE = "";
   
   /**
    * @since 6.6
    */
   @ConfigAttributeSetter(name=DISTRIBUTED_SYSTEM_ID_NAME)
   public void setDistributedSystemId(int distributedSystemId);

  @ConfigAttributeSetter(name=REDUNDANCY_ZONE_NAME)
   public void setRedundancyZone(String redundancyZone);
   
   /**
    * @since 6.6
    */
   @ConfigAttributeGetter(name=DISTRIBUTED_SYSTEM_ID_NAME)
   public int getDistributedSystemId();

  @ConfigAttributeGetter(name=REDUNDANCY_ZONE_NAME)
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
   @ConfigAttribute(type=Boolean.class)
   public static final String ENFORCE_UNIQUE_HOST_NAME = "enforce-unique-host";
   /** Using the system property to set the default here to retain backwards compatibility
    * with customers that are already using this system property.
    */
   public static boolean DEFAULT_ENFORCE_UNIQUE_HOST = Boolean.getBoolean("gemfire.EnforceUniqueHostStorageAllocation");

  @ConfigAttributeSetter(name=ENFORCE_UNIQUE_HOST_NAME)
   public void setEnforceUniqueHost(boolean enforceUniqueHost);

  @ConfigAttributeGetter(name=ENFORCE_UNIQUE_HOST_NAME)
   public boolean getEnforceUniqueHost();

   public Properties getUserDefinedProps();


   /**
    * Returns the value of the <a
    * href="../DistributedSystem.html#name">"groups"</a> property
    * <p> The default value is: {@link #DEFAULT_GROUPS}.
    * @return the value of the property
    * @since 7.0
    */
   @ConfigAttributeGetter(name=GROUPS_NAME)
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
   @ConfigAttributeSetter(name=GROUPS_NAME)
   public void setGroups(String value);

   /** The name of the "groups" property 
    * @since 7.0
    */
   @ConfigAttribute(type=String.class)
   public static final String GROUPS_NAME = "groups";
   /**
    * The default groups.
    * <p> Actual value of this constant is <code>""</code>.
    * @since 7.0
    */
   public static final String DEFAULT_GROUPS = "";

   /** Any cleanup required before closing the distributed system */
  public void close();

  @ConfigAttributeSetter(name=REMOTE_LOCATORS_NAME)
  public void setRemoteLocators(String locators);
  @ConfigAttributeGetter(name=REMOTE_LOCATORS_NAME)
  public String getRemoteLocators();
  /** The name of the "remote-locators" property */
  @ConfigAttribute(type=String.class)
  public static final String REMOTE_LOCATORS_NAME = "remote-locators";
  /** The default value of the "remote-locators" property */
  public static final String DEFAULT_REMOTE_LOCATORS = "";

  @ConfigAttributeGetter(name=JMX_MANAGER_NAME)
  public boolean getJmxManager();
  @ConfigAttributeSetter(name=JMX_MANAGER_NAME)
  public void setJmxManager(boolean value);

  @ConfigAttribute(type=Boolean.class)
  public static String JMX_MANAGER_NAME = "jmx-manager";
  public static boolean DEFAULT_JMX_MANAGER = false;

  @ConfigAttributeGetter(name=JMX_MANAGER_START_NAME)
  public boolean getJmxManagerStart();
  @ConfigAttributeSetter(name=JMX_MANAGER_START_NAME)
  public void setJmxManagerStart(boolean value);

  @ConfigAttribute(type=Boolean.class)
  public static String JMX_MANAGER_START_NAME = "jmx-manager-start";
  public static boolean DEFAULT_JMX_MANAGER_START = false;

  @ConfigAttributeGetter(name=JMX_MANAGER_PORT_NAME)
  public int getJmxManagerPort();
  @ConfigAttributeSetter(name=JMX_MANAGER_PORT_NAME)
  public void setJmxManagerPort(int value);

  @ConfigAttribute(type=Integer.class, min=0, max=65535)
  public static String JMX_MANAGER_PORT_NAME = "jmx-manager-port";
  public static int DEFAULT_JMX_MANAGER_PORT = 1099;
  
  /** @deprecated as of 8.0 use {@link #getJmxManagerSSLEnabled} instead.*/
  @ConfigAttributeGetter(name=JMX_MANAGER_SSL_NAME)
  public boolean getJmxManagerSSL();
  /** @deprecated as of 8.0 use {@link #setJmxManagerSSLEnabled} instead.*/
  @ConfigAttributeSetter(name=JMX_MANAGER_SSL_NAME)
  public void setJmxManagerSSL(boolean value);

  /** @deprecated as of 8.0 use {@link #JMX_MANAGER_SSL_ENABLED_NAME} instead.*/
  @ConfigAttribute(type=Boolean.class)
  public static String JMX_MANAGER_SSL_NAME = "jmx-manager-ssl";
  /** @deprecated as of 8.0 use {@link #DEFAULT_JMX_MANAGER_SSL_ENABLED} instead.*/
  public static boolean DEFAULT_JMX_MANAGER_SSL = false;
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-enabled">"jmx-manager-ssl-enabled"</a>
   * property.
   */
  @ConfigAttributeGetter(name=JMX_MANAGER_SSL_ENABLED_NAME)
  public boolean getJmxManagerSSLEnabled();

  /**
   * The default jmx-manager-ssl-enabled state.
   * <p> Actual value of this constant is <code>false</code>.
   */
  public static final boolean DEFAULT_JMX_MANAGER_SSL_ENABLED = false;

  /** The name of the "CacheJmxManagerSSLEnabled" property */
  @ConfigAttribute(type=Boolean.class)
  public static final String JMX_MANAGER_SSL_ENABLED_NAME =
    "jmx-manager-ssl-enabled";

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-enabled">"jmx-manager-ssl-enabled"</a>
   * property.
   */
  @ConfigAttributeSetter(name=JMX_MANAGER_SSL_ENABLED_NAME)
  public void setJmxManagerSSLEnabled( boolean enabled );


  /**
   * Returns the value of the <a 
   * href="../DistributedSystem.html#off-heap-memory-size">"off-heap-memory-size"</a> 
   * property.
   * @since 9.0
   */
  @ConfigAttributeGetter(name=OFF_HEAP_MEMORY_SIZE_NAME)
  public String getOffHeapMemorySize();
  /**
   * Sets the value of the <a 
   * href="../DistributedSystem.html#off-heap-memory-size">"off-heap-memory-size"</a> 
   * property.
   * @since 9.0
   */
  @ConfigAttributeSetter(name=OFF_HEAP_MEMORY_SIZE_NAME)
  public void setOffHeapMemorySize(String value);
  /** 
   * The name of the "off-heap-memory-size" property 
   * @since 9.0
   */
  @ConfigAttribute(type=String.class)
  public static final String OFF_HEAP_MEMORY_SIZE_NAME = "off-heap-memory-size";
  /** 
   * The default <a 
   * href="../DistributedSystem.html#off-heap-memory-size">"off-heap-memory-size"</a>
   * value of <code>""</code>. 
   * @since 9.0
   */
  public static final String DEFAULT_OFF_HEAP_MEMORY_SIZE = "";


  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-protocols">"jmx-manager-ssl-protocols"</a>
   * property.
   */
  @ConfigAttributeGetter(name=JMX_MANAGER_SSL_PROTOCOLS_NAME)
  public String getJmxManagerSSLProtocols( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-protocols">"jmx-manager-ssl-protocols"</a>
   * property.
   */
  @ConfigAttributeSetter(name=JMX_MANAGER_SSL_PROTOCOLS_NAME)
   public void setJmxManagerSSLProtocols( String protocols );

  /**
   * The default jmx-manager-ssl-protocols value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  public static final String DEFAULT_JMX_MANAGER_SSL_PROTOCOLS = "any";
  /** The name of the "CacheJmxManagerSSLProtocols" property */
  @ConfigAttribute(type=String.class)
  public static final String JMX_MANAGER_SSL_PROTOCOLS_NAME =
    "jmx-manager-ssl-protocols";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-ciphers">"jmx-manager-ssl-ciphers"</a>
   * property.
   */
  @ConfigAttributeGetter(name=JMX_MANAGER_SSL_CIPHERS_NAME)
   public String getJmxManagerSSLCiphers( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-ciphers">"jmx-manager-ssl-ciphers"</a>
   * property.
   */
  @ConfigAttributeSetter(name=JMX_MANAGER_SSL_CIPHERS_NAME)
   public void setJmxManagerSSLCiphers( String ciphers );

   /**
   * The default jmx-manager-ssl-ciphers value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  public static final String DEFAULT_JMX_MANAGER_SSL_CIPHERS = "any";
  /** The name of the "CacheJmxManagerSSLCiphers" property */
  @ConfigAttribute(type=String.class)
  public static final String JMX_MANAGER_SSL_CIPHERS_NAME =
    "jmx-manager-ssl-ciphers";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-require-authentication">"jmx-manager-ssl-require-authentication"</a>
   * property.
   */
  @ConfigAttributeGetter(name=JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION_NAME)
   public boolean getJmxManagerSSLRequireAuthentication( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-require-authentication">"jmx-manager-ssl-require-authentication"</a>
   * property.
   */
  @ConfigAttributeSetter(name=JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION_NAME)
   public void setJmxManagerSSLRequireAuthentication( boolean enabled );

   /**
   * The default jmx-manager-ssl-require-authentication value.
   * <p> Actual value of this constant is <code>true</code>.
   */
  public static final boolean DEFAULT_JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION = true;
  /** The name of the "CacheJmxManagerSSLRequireAuthentication" property */
  @ConfigAttribute(type=Boolean.class)
  public static final String JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION_NAME =
    "jmx-manager-ssl-require-authentication";

  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-keystore">"jmx-manager-ssl-keystore"</a>
   * property.
   */
  @ConfigAttributeGetter(name=JMX_MANAGER_SSL_KEYSTORE_NAME)
  public String getJmxManagerSSLKeyStore( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-keystore">"jmx-manager-ssl-keystore"</a>
   * property.
   */
  @ConfigAttributeSetter(name=JMX_MANAGER_SSL_KEYSTORE_NAME)
  public void setJmxManagerSSLKeyStore( String keyStore);
  
  /**
   * The default jmx-manager-ssl-keystore value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_JMX_MANAGER_SSL_KEYSTORE = "";
  
  /** The name of the "CacheJmxManagerSSLKeyStore" property */
  @ConfigAttribute(type=String.class)
  public static final String JMX_MANAGER_SSL_KEYSTORE_NAME = "jmx-manager-ssl-keystore";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-keystore-type">"jmx-manager-ssl-keystore-type"</a>
   * property.
   */
  @ConfigAttributeGetter(name=JMX_MANAGER_SSL_KEYSTORE_TYPE_NAME)
  public String getJmxManagerSSLKeyStoreType( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-keystore-type">"jmx-manager-ssl-keystore-type"</a>
   * property.
   */
  @ConfigAttributeSetter(name=JMX_MANAGER_SSL_KEYSTORE_TYPE_NAME)
  public void setJmxManagerSSLKeyStoreType( String keyStoreType);
  
  /**
   * The default jmx-manager-ssl-keystore-type value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_JMX_MANAGER_SSL_KEYSTORE_TYPE = "";
  
  /** The name of the "CacheJmxManagerSSLKeyStoreType" property */
  @ConfigAttribute(type=String.class)
  public static final String JMX_MANAGER_SSL_KEYSTORE_TYPE_NAME = "jmx-manager-ssl-keystore-type";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-keystore-password">"jmx-manager-ssl-keystore-password"</a>
   * property.
   */
  @ConfigAttributeGetter(name=JMX_MANAGER_SSL_KEYSTORE_PASSWORD_NAME)
  public String getJmxManagerSSLKeyStorePassword( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-keystore-password">"jmx-manager-ssl-keystore-password"</a>
   * property.
   */
  @ConfigAttributeSetter(name=JMX_MANAGER_SSL_KEYSTORE_PASSWORD_NAME)
  public void setJmxManagerSSLKeyStorePassword( String keyStorePassword);
  
  /**
   * The default jmx-manager-ssl-keystore-password value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_JMX_MANAGER_SSL_KEYSTORE_PASSWORD = "";
  
  /** The name of the "CacheJmxManagerSSLKeyStorePassword" property */
  @ConfigAttribute(type=String.class)
  public static final String JMX_MANAGER_SSL_KEYSTORE_PASSWORD_NAME = "jmx-manager-ssl-keystore-password";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-truststore">"jmx-manager-ssl-truststore"</a>
   * property.
   */
  @ConfigAttributeGetter(name=JMX_MANAGER_SSL_TRUSTSTORE_NAME)
  public String getJmxManagerSSLTrustStore( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-truststore">"jmx-manager-ssl-truststore"</a>
   * property.
   */
  @ConfigAttributeSetter(name=JMX_MANAGER_SSL_TRUSTSTORE_NAME)
  public void setJmxManagerSSLTrustStore( String trustStore);
  
  /**
   * The default jmx-manager-ssl-truststore value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_JMX_MANAGER_SSL_TRUSTSTORE = "";
  
  /** The name of the "CacheJmxManagerSSLTrustStore" property */
  @ConfigAttribute(type=String.class)
  public static final String JMX_MANAGER_SSL_TRUSTSTORE_NAME = "jmx-manager-ssl-truststore";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-truststore-password">"jmx-manager-ssl-truststore-password"</a>
   * property.
   */
  @ConfigAttributeGetter(name=JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD_NAME)
  public String getJmxManagerSSLTrustStorePassword( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-truststore-password">"jmx-manager-ssl-truststore-password"</a>
   * property.
   */
  @ConfigAttributeSetter(name=JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD_NAME)
  public void setJmxManagerSSLTrustStorePassword( String trusStorePassword);
  /**
   * The default jmx-manager-ssl-truststore-password value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD = "";
  
  /** The name of the "JmxManagerSSLKeyStorePassword" property */
  @ConfigAttribute(type=String.class)
  public static final String JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD_NAME = "jmx-manager-ssl-truststore-password";


  @ConfigAttributeGetter(name=JMX_MANAGER_BIND_ADDRESS_NAME)
  public String getJmxManagerBindAddress();
  @ConfigAttributeSetter(name=JMX_MANAGER_BIND_ADDRESS_NAME)
  public void setJmxManagerBindAddress(String value);

  @ConfigAttribute(type=String.class)
  public static String JMX_MANAGER_BIND_ADDRESS_NAME = "jmx-manager-bind-address";
  public static String DEFAULT_JMX_MANAGER_BIND_ADDRESS = "";

  @ConfigAttributeGetter(name=JMX_MANAGER_HOSTNAME_FOR_CLIENTS_NAME)
  public String getJmxManagerHostnameForClients();
  @ConfigAttributeSetter(name=JMX_MANAGER_HOSTNAME_FOR_CLIENTS_NAME)
  public void setJmxManagerHostnameForClients(String value);

  @ConfigAttribute(type=String.class)
  public static String JMX_MANAGER_HOSTNAME_FOR_CLIENTS_NAME = "jmx-manager-hostname-for-clients";
  public static String DEFAULT_JMX_MANAGER_HOSTNAME_FOR_CLIENTS = "";

  @ConfigAttributeGetter(name=JMX_MANAGER_PASSWORD_FILE_NAME)
  public String getJmxManagerPasswordFile();
  @ConfigAttributeSetter(name=JMX_MANAGER_PASSWORD_FILE_NAME)
  public void setJmxManagerPasswordFile(String value);

  @ConfigAttribute(type=String.class)
  public static String JMX_MANAGER_PASSWORD_FILE_NAME = "jmx-manager-password-file";
  public static String DEFAULT_JMX_MANAGER_PASSWORD_FILE = "";

  @ConfigAttributeGetter(name=JMX_MANAGER_ACCESS_FILE_NAME)
  public String getJmxManagerAccessFile();
  @ConfigAttributeSetter(name=JMX_MANAGER_ACCESS_FILE_NAME)
  public void setJmxManagerAccessFile(String value);

  @ConfigAttribute(type=String.class)
  public static String JMX_MANAGER_ACCESS_FILE_NAME = "jmx-manager-access-file";
  public static String DEFAULT_JMX_MANAGER_ACCESS_FILE = "";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-http-port">"jmx-manager-http-port"</a> property
   * @deprecated as of 8.0 use {@link #getHttpServicePort()} instead.
   */
  @ConfigAttributeGetter(name=JMX_MANAGER_HTTP_PORT_NAME)
  public int getJmxManagerHttpPort();
  
  /**
   * Set the jmx-manager-http-port for jmx-manager.
   * @param value the port number for jmx-manager HTTP service
   * @deprecated as of 8.0 use {@link #setHttpServicePort(int)} instead.               
   */
  @ConfigAttributeSetter(name=JMX_MANAGER_HTTP_PORT_NAME)
  public void setJmxManagerHttpPort(int value);

  /**
   * The name of the "jmx-manager-http-port" property.
   * @deprecated as of 8.0 use {@link #HTTP_SERVICE_PORT_NAME} instead.
   */
  @ConfigAttribute(type=Integer.class, min=0, max=65535)
  public static String JMX_MANAGER_HTTP_PORT_NAME = "jmx-manager-http-port";
  
  /**
   * The default value of the "jmx-manager-http-port" property.
   * Current value is a <code>7070</code>
   * @deprecated as of 8.0 use {@link #DEFAULT_HTTP_SERVICE_PORT} instead.
   */
  public static int DEFAULT_JMX_MANAGER_HTTP_PORT = 7070;

  @ConfigAttributeGetter(name=JMX_MANAGER_UPDATE_RATE_NAME)
  public int getJmxManagerUpdateRate();
  @ConfigAttributeSetter(name=JMX_MANAGER_UPDATE_RATE_NAME)
  public void setJmxManagerUpdateRate(int value);

  public static final int DEFAULT_JMX_MANAGER_UPDATE_RATE = 2000;
  public static final int MIN_JMX_MANAGER_UPDATE_RATE = 1000;
  public static final int MAX_JMX_MANAGER_UPDATE_RATE = 60000*5;
  @ConfigAttribute(type=Integer.class, min=MIN_JMX_MANAGER_UPDATE_RATE, max=MAX_JMX_MANAGER_UPDATE_RATE)
  public static final String JMX_MANAGER_UPDATE_RATE_NAME =
    "jmx-manager-update-rate";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#memcached-port">"memcached-port"</a> property
   * @return the port on which GemFireMemcachedServer should be started
   * @since 7.0
   */
  @ConfigAttributeGetter(name=MEMCACHED_PORT_NAME)
  public int getMemcachedPort();
  @ConfigAttributeSetter(name=MEMCACHED_PORT_NAME)
  public void setMemcachedPort(int value);

  @ConfigAttribute(type=Integer.class, min=0, max=65535)
  public static String MEMCACHED_PORT_NAME = "memcached-port";
  public static int DEFAULT_MEMCACHED_PORT = 0;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#memcached-protocol">"memcached-protocol"</a> property
   * @return the protocol for GemFireMemcachedServer
   * @since 7.0
   */
  @ConfigAttributeGetter(name=MEMCACHED_PROTOCOL_NAME)
  public String getMemcachedProtocol();
  @ConfigAttributeSetter(name=MEMCACHED_PROTOCOL_NAME)
  public void setMemcachedProtocol(String protocol);

  @ConfigAttribute(type=String.class)
  public static String MEMCACHED_PROTOCOL_NAME = "memcached-protocol";
  public static String DEFAULT_MEMCACHED_PROTOCOL = GemFireMemcachedServer.Protocol.ASCII.name();
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#memcached-bind-address">"memcached-bind-address"</a> property
   * @return the bind address for GemFireMemcachedServer
   * @since 7.0
   */
  @ConfigAttributeGetter(name=MEMCACHED_BIND_ADDRESS_NAME)
  public String getMemcachedBindAddress();
  @ConfigAttributeSetter(name=MEMCACHED_BIND_ADDRESS_NAME)
  public void setMemcachedBindAddress(String bindAddress);

  @ConfigAttribute(type=String.class)
  public static String MEMCACHED_BIND_ADDRESS_NAME = "memcached-bind-address";
  public static String DEFAULT_MEMCACHED_BIND_ADDRESS = "";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#redis-port">"redis-port"</a> property
   * @return the port on which GemFireRedisServer should be started
   * @since 8.0
   */
  @ConfigAttributeGetter(name=REDIS_PORT_NAME)
  public int getRedisPort();
  @ConfigAttributeSetter(name=REDIS_PORT_NAME)
  public void setRedisPort(int value);

  @ConfigAttribute(type=Integer.class, min=0, max=65535)
  public static String REDIS_PORT_NAME = "redis-port";
  public static int DEFAULT_REDIS_PORT = 0;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#redis-bind-address">"redis-bind-address"</a> property
   * @return the bind address for GemFireRedisServer
   * @since 8.0
   */
  @ConfigAttributeGetter(name=REDIS_BIND_ADDRESS_NAME)
  public String getRedisBindAddress();
  @ConfigAttributeSetter(name=REDIS_BIND_ADDRESS_NAME)
  public void setRedisBindAddress(String bindAddress);

  @ConfigAttribute(type=String.class)
  public static String REDIS_BIND_ADDRESS_NAME = "redis-bind-address";
  public static String DEFAULT_REDIS_BIND_ADDRESS = "";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#redis-password">"redis-password"</a> property
   * @return the authentication password for GemFireRedisServer
   * @since 8.0
   */
  @ConfigAttributeGetter(name=REDIS_PASSWORD_NAME)
  public String getRedisPassword();
  @ConfigAttributeSetter(name=REDIS_PASSWORD_NAME)
  public void setRedisPassword(String password);

  @ConfigAttribute(type=String.class)
  public static String REDIS_PASSWORD_NAME = "redis-password";
  public static String DEFAULT_REDIS_PASSWORD = "";


  //Added for the HTTP service
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-port">"http-service-port"</a> property
   * @return the HTTP service port
   * @since 8.0
   */
  @ConfigAttributeGetter(name=HTTP_SERVICE_PORT_NAME)
  public int getHttpServicePort();
  
  /**
   * Set the http-service-port for HTTP service.
   * @param value the port number for HTTP service
   * @since 8.0               
   */
  @ConfigAttributeSetter(name=HTTP_SERVICE_PORT_NAME)
  public void setHttpServicePort(int value);
  
  /**
   * The name of the "http-service-port" property
   * @since 8.0
   */
  @ConfigAttribute(type=Integer.class, min=0, max=65535)
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
  @ConfigAttributeGetter(name=HTTP_SERVICE_BIND_ADDRESS_NAME)
  public String getHttpServiceBindAddress();
  
  /**
   * Set the http-service-bind-address for HTTP service.
   * @param value the bind-address for HTTP service
   * @since 8.0               
   */
  @ConfigAttributeSetter(name=HTTP_SERVICE_BIND_ADDRESS_NAME)
  public void setHttpServiceBindAddress(String value);
  
  /** 
   * The name of the "http-service-bind-address" property
   * @since 8.0
   */
  @ConfigAttribute(type=String.class)
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
  @ConfigAttributeGetter(name=HTTP_SERVICE_SSL_ENABLED_NAME)
  public boolean getHttpServiceSSLEnabled();  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-enabled">"http-service-ssl-enabled"</a>
   * property.
   */
  @ConfigAttributeSetter(name=HTTP_SERVICE_SSL_ENABLED_NAME)
  public void setHttpServiceSSLEnabled(boolean httpServiceSSLEnabled);
  /**
   * The default http-service-ssl-enabled state.
   * <p> Actual value of this constant is <code>false</code>.
   */
  public static final boolean DEFAULT_HTTP_SERVICE_SSL_ENABLED = false;
  
  /** The name of the "HttpServiceSSLEnabled" property */
  @ConfigAttribute(type=Boolean.class)
  public static final String HTTP_SERVICE_SSL_ENABLED_NAME = "http-service-ssl-enabled";  
  
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-require-authentication">"http-service-ssl-require-authentication"</a>
   * property.
   */
  @ConfigAttributeGetter(name=HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION_NAME)
  public boolean getHttpServiceSSLRequireAuthentication();  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-require-authentication">"http-service-ssl-require-authentication"</a>
   * property.
   */
  @ConfigAttributeSetter(name=HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION_NAME)
  public void setHttpServiceSSLRequireAuthentication(boolean httpServiceSSLRequireAuthentication);
  /**
  * The default http-service-ssl-require-authentication value.
  * <p> Actual value of this constant is <code>true</code>.
  */
  public static final boolean DEFAULT_HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION = false;
  
  /** The name of the "HttpServiceSSLRequireAuthentication" property */
  @ConfigAttribute(type=Boolean.class)
  public static final String HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION_NAME = "http-service-ssl-require-authentication"; 
  
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-protocols">"http-service-ssl-protocols"</a>
   * property.
   */
  @ConfigAttributeGetter(name=HTTP_SERVICE_SSL_PROTOCOLS_NAME)
  public String getHttpServiceSSLProtocols();  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-protocols">"http-service-ssl-protocols"</a>
   * property.
   */
  @ConfigAttributeSetter(name=HTTP_SERVICE_SSL_PROTOCOLS_NAME)
  public void setHttpServiceSSLProtocols(String protocols);
  /**
   * The default http-service-ssl-protocols value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  public static final String DEFAULT_HTTP_SERVICE_SSL_PROTOCOLS = "any";
  
  /** The name of the "HttpServiceSSLProtocols" property */
  @ConfigAttribute(type=String.class)
  public static final String HTTP_SERVICE_SSL_PROTOCOLS_NAME = "http-service-ssl-protocols"; 
  
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-ciphers">"http-service-ssl-ciphers"</a>
   * property.
   */
  @ConfigAttributeGetter(name=HTTP_SERVICE_SSL_CIPHERS_NAME)
  public String getHttpServiceSSLCiphers();  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-ciphers">"http-service-ssl-ciphers"</a>
   * property.
   */
  @ConfigAttributeSetter(name=HTTP_SERVICE_SSL_CIPHERS_NAME)
  public void setHttpServiceSSLCiphers(String ciphers);
  /**
   * The default http-service-ssl-ciphers value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  public static final String DEFAULT_HTTP_SERVICE_SSL_CIPHERS = "any";
  
  /** The name of the "HttpServiceSSLCiphers" property */
  @ConfigAttribute(type=String.class)
  public static final String HTTP_SERVICE_SSL_CIPHERS_NAME = "http-service-ssl-ciphers";
  
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-keystore">"http-service-ssl-keystore"</a>
   * property.
   */
  @ConfigAttributeGetter(name=HTTP_SERVICE_SSL_KEYSTORE_NAME)
  public String getHttpServiceSSLKeyStore( );  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-keystore">"http-service-ssl-keystore"</a>
   * property.
   */
  @ConfigAttributeSetter(name=HTTP_SERVICE_SSL_KEYSTORE_NAME)
  public void setHttpServiceSSLKeyStore(String keyStore);
  /**
   * The default http-service-ssl-keystore value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_HTTP_SERVICE_SSL_KEYSTORE = "";
  
  /** The name of the "HttpServiceSSLKeyStore" property */
  @ConfigAttribute(type=String.class)
  public static final String HTTP_SERVICE_SSL_KEYSTORE_NAME = "http-service-ssl-keystore";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-keystore-password">"http-service-ssl-keystore-password"</a>
   * property.
   */
  @ConfigAttributeGetter(name=HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME)
  public String getHttpServiceSSLKeyStorePassword( );  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-keystore-password">"http-service-ssl-keystore-password"</a>
   * property.
   */
  @ConfigAttributeSetter(name=HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME)
  public void setHttpServiceSSLKeyStorePassword(String keyStorePassword);
  /**
   * The default http-service-ssl-keystore-password value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_HTTP_SERVICE_SSL_KEYSTORE_PASSWORD = "";
  
  /** The name of the "HttpServiceSSLKeyStorePassword" property */
  @ConfigAttribute(type=String.class)
  public static final String HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME = "http-service-ssl-keystore-password";
  
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-keystore-type">"http-service-ssl-keystore-type"</a>
   * property.
   */
  @ConfigAttributeGetter(name=HTTP_SERVICE_SSL_KEYSTORE_TYPE_NAME)
  public String getHttpServiceSSLKeyStoreType( );  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-keystore-type">"http-service-ssl-keystore-type"</a>
   * property.
   */
  @ConfigAttributeSetter(name=HTTP_SERVICE_SSL_KEYSTORE_TYPE_NAME)
  public void setHttpServiceSSLKeyStoreType(String keyStoreType);
  /**
   * The default gateway-ssl-truststore-password value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_HTTP_SERVICE_SSL_KEYSTORE_TYPE = "";
  
  /** The name of the "HttpServiceKeyStoreType" property */
  @ConfigAttribute(type=String.class)
  public static final String HTTP_SERVICE_SSL_KEYSTORE_TYPE_NAME = "http-service-ssl-keystore-type";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-truststore">"http-service-ssl-truststore"</a>
   * property.
   */
  @ConfigAttributeGetter(name=HTTP_SERVICE_SSL_TRUSTSTORE_NAME)
  public String getHttpServiceSSLTrustStore( );  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-truststore">"http-service-ssl-truststore"</a>
   * property.
   */
  @ConfigAttributeSetter(name=HTTP_SERVICE_SSL_TRUSTSTORE_NAME)
  public void setHttpServiceSSLTrustStore(String trustStore);
  /**
   * The default http-service-ssl-truststore value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_HTTP_SERVICE_SSL_TRUSTSTORE = "";
  
  /** The name of the "HttpServiceTrustStore" property */
  @ConfigAttribute(type=String.class)
  public static final String HTTP_SERVICE_SSL_TRUSTSTORE_NAME = "http-service-ssl-truststore";
  
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-truststore-password">"http-service-ssl-truststore-password"</a>
   * property.
   */
  @ConfigAttributeGetter(name=HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD_NAME)
  public String getHttpServiceSSLTrustStorePassword( );  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-truststore-password">"http-service-ssl-truststore-password"</a>
   * property.
   */
  @ConfigAttributeSetter(name=HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD_NAME)
  public void setHttpServiceSSLTrustStorePassword(String trustStorePassword);
  /**
   * The default http-service-ssl-truststore-password value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD = "";
  
  /** The name of the "HttpServiceTrustStorePassword" property */
  @ConfigAttribute(type=String.class)
  public static final String HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD_NAME = "http-service-ssl-truststore-password";
  
  
  public Properties getHttpServiceSSLProperties();
  
  //Added for API REST
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#start-dev-rest-api">"start-dev-rest-api"</a> property
   * @return the value of the property
   * @since 8.0
   */
  @ConfigAttributeGetter(name=START_DEV_REST_API_NAME)
  public boolean getStartDevRestApi();
  
  /**
   * Set the start-dev-rest-api for HTTP service.
   * @param value for the property
   * @since 8.0               
   */
  @ConfigAttributeSetter(name=START_DEV_REST_API_NAME)
  public void setStartDevRestApi(boolean value);
  
  /** 
   * The name of the "start-dev-rest-api" property
   * @since 8.0
   */
  @ConfigAttribute(type=Boolean.class)
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
  @ConfigAttribute(type=Boolean.class)
  public static final String DISABLE_AUTO_RECONNECT_NAME = "disable-auto-reconnect";

  /** The default value of the corresponding property */
  public static final boolean DEFAULT_DISABLE_AUTO_RECONNECT = false;
  
  /**
   * Gets the value of <a href="../DistributedSystem.html#disable-auto-reconnect">"disable-auto-reconnect"</a>
   */
  @ConfigAttributeGetter(name=DISABLE_AUTO_RECONNECT_NAME)
  public boolean getDisableAutoReconnect();

  /**
   * Sets the value of <a href="../DistributedSystem.html#disable-auto-reconnect">"disable-auto-reconnect"</a>
   * @param value the new setting
   */
  @ConfigAttributeSetter(name=DISABLE_AUTO_RECONNECT_NAME)
  public void setDisableAutoReconnect(boolean value);
  
  
  public Properties getServerSSLProperties();
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-enabled">"server-ssl-enabled"</a>
   * property.
   */
  @ConfigAttributeGetter(name=SERVER_SSL_ENABLED_NAME)
  public boolean getServerSSLEnabled();

  /**
   * The default server-ssl-enabled state.
   * <p> Actual value of this constant is <code>false</code>.
   */
  public static final boolean DEFAULT_SERVER_SSL_ENABLED = false;
  /** The name of the "ServerSSLEnabled" property */
  @ConfigAttribute(type=Boolean.class)
  public static final String SERVER_SSL_ENABLED_NAME =
    "server-ssl-enabled";

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-enabled">"server-ssl-enabled"</a>
   * property.
   */
  @ConfigAttributeSetter(name=SERVER_SSL_ENABLED_NAME)
  public void setServerSSLEnabled( boolean enabled );

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-protocols">"server-ssl-protocols"</a>
   * property.
   */
  @ConfigAttributeGetter(name=SERVER_SSL_PROTOCOLS_NAME)
   public String getServerSSLProtocols( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-protocols">"server-ssl-protocols"</a>
   * property.
   */
  @ConfigAttributeSetter(name=SERVER_SSL_PROTOCOLS_NAME)
   public void setServerSSLProtocols( String protocols );

  /**
   * The default server-ssl-protocols value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  public static final String DEFAULT_SERVER_SSL_PROTOCOLS = "any";
  /** The name of the "ServerSSLProtocols" property */
  @ConfigAttribute(type=String.class)
  public static final String SERVER_SSL_PROTOCOLS_NAME =
    "server-ssl-protocols";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-ciphers">"server-ssl-ciphers"</a>
   * property.
   */
  @ConfigAttributeGetter(name=SERVER_SSL_CIPHERS_NAME)
   public String getServerSSLCiphers( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-ciphers">"server-ssl-ciphers"</a>
   * property.
   */
  @ConfigAttributeSetter(name=SERVER_SSL_CIPHERS_NAME)
   public void setServerSSLCiphers( String ciphers );

   /**
   * The default server-ssl-ciphers value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  public static final String DEFAULT_SERVER_SSL_CIPHERS = "any";
  /** The name of the "ServerSSLCiphers" property */
  @ConfigAttribute(type=String.class)
  public static final String SERVER_SSL_CIPHERS_NAME =
    "server-ssl-ciphers";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-require-authentication">"server-ssl-require-authentication"</a>
   * property.
   */
  @ConfigAttributeGetter(name=SERVER_SSL_REQUIRE_AUTHENTICATION_NAME)
   public boolean getServerSSLRequireAuthentication( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-require-authentication">"server-ssl-require-authentication"</a>
   * property.
   */
  @ConfigAttributeSetter(name=SERVER_SSL_REQUIRE_AUTHENTICATION_NAME)
   public void setServerSSLRequireAuthentication( boolean enabled );

   /**
   * The default server-ssl-require-authentication value.
   * <p> Actual value of this constant is <code>true</code>.
   */
  public static final boolean DEFAULT_SERVER_SSL_REQUIRE_AUTHENTICATION = true;
  /** The name of the "ServerSSLRequireAuthentication" property */
  @ConfigAttribute(type=Boolean.class)
  public static final String SERVER_SSL_REQUIRE_AUTHENTICATION_NAME =
    "server-ssl-require-authentication";

  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-keystore">"server-ssl-keystore"</a>
   * property.
   */
  @ConfigAttributeGetter(name=SERVER_SSL_KEYSTORE_NAME)
  public String getServerSSLKeyStore( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-keystore">"server-ssl-keystore"</a>
   * property.
   */
  @ConfigAttributeSetter(name=SERVER_SSL_KEYSTORE_NAME)
  public void setServerSSLKeyStore( String keyStore);
  
  /**
   * The default server-ssl-keystore value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_SERVER_SSL_KEYSTORE = "";
  
  /** The name of the "ServerSSLKeyStore" property */
  @ConfigAttribute(type=String.class)
  public static final String SERVER_SSL_KEYSTORE_NAME = "server-ssl-keystore";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-keystore-type">"server-ssl-keystore-type"</a>
   * property.
   */
  @ConfigAttributeGetter(name=SERVER_SSL_KEYSTORE_TYPE_NAME)
  public String getServerSSLKeyStoreType( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-keystore-type">"server-ssl-keystore-type"</a>
   * property.
   */
  @ConfigAttributeSetter(name=SERVER_SSL_KEYSTORE_TYPE_NAME)
  public void setServerSSLKeyStoreType( String keyStoreType);
  
  /**
   * The default server-ssl-keystore-type value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_SERVER_SSL_KEYSTORE_TYPE = "";
  
  /** The name of the "ServerSSLKeyStoreType" property */
  @ConfigAttribute(type=String.class)
  public static final String SERVER_SSL_KEYSTORE_TYPE_NAME = "server-ssl-keystore-type";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-keystore-password">"server-ssl-keystore-password"</a>
   * property.
   */
  @ConfigAttributeGetter(name=SERVER_SSL_KEYSTORE_PASSWORD_NAME)
  public String getServerSSLKeyStorePassword( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-keystore-password">"server-ssl-keystore-password"</a>
   * property.
   */
  @ConfigAttributeSetter(name=SERVER_SSL_KEYSTORE_PASSWORD_NAME)
  public void setServerSSLKeyStorePassword( String keyStorePassword);
  
  /**
   * The default server-ssl-keystore-password value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_SERVER_SSL_KEYSTORE_PASSWORD = "";
  
  /** The name of the "ServerSSLKeyStorePassword" property */
  @ConfigAttribute(type=String.class)
  public static final String SERVER_SSL_KEYSTORE_PASSWORD_NAME = "server-ssl-keystore-password";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-truststore">"server-ssl-truststore"</a>
   * property.
   */
  @ConfigAttributeGetter(name=SERVER_SSL_TRUSTSTORE_NAME)
  public String getServerSSLTrustStore( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-truststore">"server-ssl-truststore"</a>
   * property.
   */
  @ConfigAttributeSetter(name=SERVER_SSL_TRUSTSTORE_NAME)
  public void setServerSSLTrustStore( String trustStore);
  
  /**
   * The default server-ssl-truststore value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_SERVER_SSL_TRUSTSTORE = "";
  
  /** The name of the "ServerSSLTrustStore" property */
  @ConfigAttribute(type=String.class)
  public static final String SERVER_SSL_TRUSTSTORE_NAME = "server-ssl-truststore";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-truststore-password">"server-ssl-truststore-password"</a>
   * property.
   */
  @ConfigAttributeGetter(name=SERVER_SSL_TRUSTSTORE_PASSWORD_NAME)
  public String getServerSSLTrustStorePassword( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-truststore-password">"server-ssl-truststore-password"</a>
   * property.
   */
  @ConfigAttributeSetter(name=SERVER_SSL_TRUSTSTORE_PASSWORD_NAME)
  public void setServerSSLTrustStorePassword( String trusStorePassword);
  /**
   * The default server-ssl-truststore-password value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_SERVER_SSL_TRUSTSTORE_PASSWORD = "";
  
  /** The name of the "ServerSSLKeyStorePassword" property */
  @ConfigAttribute(type=String.class)
  public static final String SERVER_SSL_TRUSTSTORE_PASSWORD_NAME = "server-ssl-truststore-password";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-enabled">"cluster-ssl-enabled"</a>
   * property.
   */
  @ConfigAttributeGetter(name=GATEWAY_SSL_ENABLED_NAME)
  public boolean getGatewaySSLEnabled();

  /**
   * The default gateway-ssl-enabled state.
   * <p> Actual value of this constant is <code>false</code>.
   */
  public static final boolean DEFAULT_GATEWAY_SSL_ENABLED = false;
  /** The name of the "GatewaySSLEnabled" property */
  @ConfigAttribute(type=Boolean.class)
  public static final String GATEWAY_SSL_ENABLED_NAME =
    "gateway-ssl-enabled";

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-enabled">"gateway-ssl-enabled"</a>
   * property.
   */
  @ConfigAttributeSetter(name=GATEWAY_SSL_ENABLED_NAME)
  public void setGatewaySSLEnabled( boolean enabled );

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-protocols">"gateway-ssl-protocols"</a>
   * property.
   */
  @ConfigAttributeGetter(name=GATEWAY_SSL_PROTOCOLS_NAME)
   public String getGatewaySSLProtocols( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-protocols">"gateway-ssl-protocols"</a>
   * property.
   */
  @ConfigAttributeSetter(name=GATEWAY_SSL_PROTOCOLS_NAME)
   public void setGatewaySSLProtocols( String protocols );

  /**
   * The default gateway-ssl-protocols value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  public static final String DEFAULT_GATEWAY_SSL_PROTOCOLS = "any";
  /** The name of the "GatewaySSLProtocols" property */
  @ConfigAttribute(type=String.class)
  public static final String GATEWAY_SSL_PROTOCOLS_NAME =
    "gateway-ssl-protocols";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-ciphers">"gateway-ssl-ciphers"</a>
   * property.
   */
  @ConfigAttributeGetter(name=GATEWAY_SSL_CIPHERS_NAME)
   public String getGatewaySSLCiphers( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-ciphers">"gateway-ssl-ciphers"</a>
   * property.
   */
  @ConfigAttributeSetter(name=GATEWAY_SSL_CIPHERS_NAME)
   public void setGatewaySSLCiphers( String ciphers );

   /**
   * The default gateway-ssl-ciphers value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  public static final String DEFAULT_GATEWAY_SSL_CIPHERS = "any";
  /** The name of the "GatewaySSLCiphers" property */
  @ConfigAttribute(type=String.class)
  public static final String GATEWAY_SSL_CIPHERS_NAME =
    "gateway-ssl-ciphers";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-require-authentication">"gateway-ssl-require-authentication"</a>
   * property.
   */
  @ConfigAttributeGetter(name=GATEWAY_SSL_REQUIRE_AUTHENTICATION_NAME)
   public boolean getGatewaySSLRequireAuthentication( );

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-require-authentication">"gateway-ssl-require-authentication"</a>
   * property.
   */
  @ConfigAttributeSetter(name=GATEWAY_SSL_REQUIRE_AUTHENTICATION_NAME)
   public void setGatewaySSLRequireAuthentication( boolean enabled );

   /**
   * The default gateway-ssl-require-authentication value.
   * <p> Actual value of this constant is <code>true</code>.
   */
  public static final boolean DEFAULT_GATEWAY_SSL_REQUIRE_AUTHENTICATION = true;
  /** The name of the "GatewaySSLRequireAuthentication" property */
  @ConfigAttribute(type=Boolean.class)
  public static final String GATEWAY_SSL_REQUIRE_AUTHENTICATION_NAME =
    "gateway-ssl-require-authentication";

  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-keystore">"gateway-ssl-keystore"</a>
   * property.
   */
  @ConfigAttributeGetter(name=GATEWAY_SSL_KEYSTORE_NAME)
  public String getGatewaySSLKeyStore( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-keystore">"gateway-ssl-keystore"</a>
   * property.
   */
  @ConfigAttributeSetter(name=GATEWAY_SSL_KEYSTORE_NAME)
  public void setGatewaySSLKeyStore( String keyStore);
  
  /**
   * The default gateway-ssl-keystore value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_GATEWAY_SSL_KEYSTORE = "";
  
  /** The name of the "GatewaySSLKeyStore" property */
  @ConfigAttribute(type=String.class)
  public static final String GATEWAY_SSL_KEYSTORE_NAME = "gateway-ssl-keystore";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-keystore-type">"gateway-ssl-keystore-type"</a>
   * property.
   */
  @ConfigAttributeGetter(name=GATEWAY_SSL_KEYSTORE_TYPE_NAME)
  public String getGatewaySSLKeyStoreType( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-keystore-type">"gateway-ssl-keystore-type"</a>
   * property.
   */
  @ConfigAttributeSetter(name=GATEWAY_SSL_KEYSTORE_TYPE_NAME)
  public void setGatewaySSLKeyStoreType( String keyStoreType);
  
  /**
   * The default gateway-ssl-keystore-type value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_GATEWAY_SSL_KEYSTORE_TYPE = "";
  
  /** The name of the "GatewaySSLKeyStoreType" property */
  @ConfigAttribute(type=String.class)
  public static final String GATEWAY_SSL_KEYSTORE_TYPE_NAME = "gateway-ssl-keystore-type";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-keystore-password">"gateway-ssl-keystore-password"</a>
   * property.
   */
  @ConfigAttributeGetter(name=GATEWAY_SSL_KEYSTORE_PASSWORD_NAME)
  public String getGatewaySSLKeyStorePassword( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-keystore-password">"gateway-ssl-keystore-password"</a>
   * property.
   */
  @ConfigAttributeSetter(name=GATEWAY_SSL_KEYSTORE_PASSWORD_NAME)
  public void setGatewaySSLKeyStorePassword( String keyStorePassword);
  
  /**
   * The default gateway-ssl-keystore-password value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_GATEWAY_SSL_KEYSTORE_PASSWORD = "";
  
  /** The name of the "GatewaySSLKeyStorePassword" property */
  @ConfigAttribute(type=String.class)
  public static final String GATEWAY_SSL_KEYSTORE_PASSWORD_NAME = "gateway-ssl-keystore-password";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-truststore">"gateway-ssl-truststore"</a>
   * property.
   */
  @ConfigAttributeGetter(name=GATEWAY_SSL_TRUSTSTORE_NAME)
  public String getGatewaySSLTrustStore( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-truststore">"gateway-ssl-truststore"</a>
   * property.
   */
  @ConfigAttributeSetter(name=GATEWAY_SSL_TRUSTSTORE_NAME)
  public void setGatewaySSLTrustStore( String trustStore);
  
  /**
   * The default gateway-ssl-truststore value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_GATEWAY_SSL_TRUSTSTORE = "";
  
  /** The name of the "GatewaySSLTrustStore" property */
  @ConfigAttribute(type=String.class)
  public static final String GATEWAY_SSL_TRUSTSTORE_NAME = "gateway-ssl-truststore";
  
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-truststore-password">"gateway-ssl-truststore-password"</a>
   * property.
   */
  @ConfigAttributeGetter(name=GATEWAY_SSL_TRUSTSTORE_PASSWORD_NAME)
  public String getGatewaySSLTrustStorePassword( );
  
  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-truststore-password">"gateway-ssl-truststore-password"</a>
   * property.
   */
  @ConfigAttributeSetter(name=GATEWAY_SSL_TRUSTSTORE_PASSWORD_NAME)
  public void setGatewaySSLTrustStorePassword( String trusStorePassword);
  /**
   * The default gateway-ssl-truststore-password value.
   * <p> Actual value of this constant is "".
   */
  public static final String DEFAULT_GATEWAY_SSL_TRUSTSTORE_PASSWORD = "";
  
  /** The name of the "GatewaySSLKeyStorePassword" property */
  @ConfigAttribute(type=String.class)
  public static final String GATEWAY_SSL_TRUSTSTORE_PASSWORD_NAME = "gateway-ssl-truststore-password";
  
  
  public Properties getGatewaySSLProperties();

  public ConfigSource getConfigSource(String attName);

 
  /**
   * The name of the "lock-memory" property.  Used to cause pages to be locked
   * into memory, thereby preventing them from being swapped to disk.
   * @since 9.0
   */
  @ConfigAttribute(type=Boolean.class)
  public static String LOCK_MEMORY_NAME = "lock-memory";
  public static final boolean DEFAULT_LOCK_MEMORY = false;
  /**
   * Gets the value of <a href="../DistributedSystem.html#lock-memory">"lock-memory"</a>
   * @since 9.0
   */
  @ConfigAttributeGetter(name=LOCK_MEMORY_NAME)
  public boolean getLockMemory();
  /**
   * Set the value of <a href="../DistributedSystem.html#lock-memory">"lock-memory"</a>
   * @param value the new setting
   * @since 9.0
   */
  @ConfigAttributeSetter(name=LOCK_MEMORY_NAME)
  public void setLockMemory(boolean value);

  @ConfigAttribute(type=String.class)
  public String SECURITY_SHIRO_INIT_NAME ="security-shiro-init";

  @ConfigAttributeSetter(name= SECURITY_SHIRO_INIT_NAME)
  public void setShiroInit(String value);
  @ConfigAttributeGetter(name= SECURITY_SHIRO_INIT_NAME)
  public String getShiroInit();


  //*************** Initializers to gather all the annotations in this class ************************

  static final Map<String, ConfigAttribute> attributes = new HashMap<String, ConfigAttribute>();
  static final Map<String, Method> setters = new HashMap<String, Method>();
  static final Map<String, Method> getters = new HashMap<String, Method>();
  static final String[] dcValidAttributeNames = init();
  static String[] init(){
    List<String> atts = new ArrayList<String>();
    for(Field field:DistributionConfig.class.getDeclaredFields()) {
      if (field.isAnnotationPresent(ConfigAttribute.class)) {
        try {
          atts.add((String) field.get(null));
          attributes.put((String) field.get(null), field.getAnnotation(ConfigAttribute.class));
        } catch (IllegalAccessException e) {
          e.printStackTrace();
        }
      }
    }

    for(Method method:DistributionConfig.class.getDeclaredMethods()){
      if(method.isAnnotationPresent(ConfigAttributeGetter.class)){
        ConfigAttributeGetter getter = method.getAnnotation(ConfigAttributeGetter.class);
        getters.put(getter.name(), method);
      }
      else if(method.isAnnotationPresent(ConfigAttributeSetter.class)){
        ConfigAttributeSetter setter = method.getAnnotation(ConfigAttributeSetter.class);
        setters.put(setter.name(), method);
      }
    }
    Collections.sort(atts);
    return (String[])atts.toArray(new String[atts.size()]);
  }

}

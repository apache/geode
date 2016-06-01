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

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.DistributedSystemConfigProperties;
import com.gemstone.gemfire.internal.Config;
import com.gemstone.gemfire.internal.ConfigSource;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LogConfig;
import com.gemstone.gemfire.internal.tcp.Connection;
import com.gemstone.gemfire.memcached.GemFireMemcachedServer;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.*;

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
 * @since GemFire 2.1
 */
public interface DistributionConfig extends Config, LogConfig, DistributedSystemConfigProperties {

  ////////////////////  Instance Methods  ////////////////////

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#name">"name"</a> property
   * Gets the member's name.
   * A name is optional and by default empty.
   * If set it must be unique in the ds.
   * When set its used by tools to help identify the member.
   * <p> The default value is: {@link #DEFAULT_NAME}.
   *
   * @return the system's name.
   */
  @ConfigAttributeGetter(name = NAME_NAME)
  String getName();

  /**
   * Sets the member's name.
   * <p> The name can not be changed while the system is running.
   *
   * @throws IllegalArgumentException                   if the specified value is not acceptable.
   * @throws com.gemstone.gemfire.UnmodifiableException if this attribute can not be modified.
   * @throws com.gemstone.gemfire.GemFireIOException    if the set failure is caused by an error
   *                                                    when writing to the system's configuration file.
   */
  @ConfigAttributeSetter(name = NAME_NAME)
  void setName(String value);

  /**
   * The "name" property, representing the system's name
   */
  @ConfigAttribute(type = String.class)
  String NAME_NAME = NAME;

  /**
   * The default system name.
   * <p> Actual value of this constant is <code>""</code>.
   */
  String DEFAULT_NAME = "";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#mcast-port">"mcast-port"</a>
   * property
   */
  @ConfigAttributeGetter(name = MCAST_PORT_NAME)
  int getMcastPort();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#mcast-port">"mcast-port"</a>
   * property
   */
  @ConfigAttributeSetter(name = MCAST_PORT_NAME)
  void setMcastPort(int value);

  /**
   * The default value of the "mcast-port" property
   */
  int DEFAULT_MCAST_PORT = 0;

  /**
   * The minimum mcastPort.
   * <p> Actual value of this constant is <code>0</code>.
   */
  int MIN_MCAST_PORT = 0;

  /**
   * The maximum mcastPort.
   * <p> Actual value of this constant is <code>65535</code>.
   */
  int MAX_MCAST_PORT = 65535;

  /**
   * The name of the "mcast-port" property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_MCAST_PORT, max = MAX_MCAST_PORT)
  String MCAST_PORT_NAME = MCAST_PORT;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#tcp-port">"tcp-port"</a>
   * property
   */
  @ConfigAttributeGetter(name = TCP_PORT)
  int getTcpPort();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#tcp-port">"tcp-port"</a>
   * property
   */
  @ConfigAttributeSetter(name = TCP_PORT)
  void setTcpPort(int value);

  /**
   * The default value of the "tcpPort" property
   */
  int DEFAULT_TCP_PORT = 0;

  /**
   * The minimum tcpPort.
   * <p> Actual value of this constant is <code>0</code>.
   */
  int MIN_TCP_PORT = 0;

  /**
   * The maximum tcpPort.
   * <p> Actual value of this constant is <code>65535</code>.
   */
  int MAX_TCP_PORT = 65535;

  /**
   * The name of the "tcpPort" property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_TCP_PORT, max = MAX_TCP_PORT)
  String TCP_PORT_NAME = TCP_PORT;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#mcast-address">"mcast-address"</a>
   * property
   */
  @ConfigAttributeGetter(name = MCAST_ADDRESS)
  InetAddress getMcastAddress();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#mcast-address">"mcast-address"</a>
   * property
   */
  @ConfigAttributeSetter(name = MCAST_ADDRESS)
  void setMcastAddress(InetAddress value);

  /**
   * The name of the "mcastAddress" property
   */
  @ConfigAttribute(type = InetAddress.class)
  String MCAST_ADDRESS_NAME = MCAST_ADDRESS;

  /**
   * The default value of the "mcastAddress" property.
   * Current value is <code>239.192.81.1</code>
   */
  InetAddress DEFAULT_MCAST_ADDRESS = AbstractDistributionConfig._getDefaultMcastAddress();

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#mcast-ttl">"mcast-ttl"</a>
   * property
   */
  @ConfigAttributeGetter(name = MCAST_TTL)
  int getMcastTtl();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#mcast-ttl">"mcast-ttl"</a>
   * property
   */
  @ConfigAttributeSetter(name = MCAST_TTL)
  void setMcastTtl(int value);

  /**
   * The default value of the "mcastTtl" property
   */
  int DEFAULT_MCAST_TTL = 32;

  /**
   * The minimum mcastTtl.
   * <p> Actual value of this constant is <code>0</code>.
   */
  int MIN_MCAST_TTL = 0;

  /**
   * The maximum mcastTtl.
   * <p> Actual value of this constant is <code>255</code>.
   */
  int MAX_MCAST_TTL = 255;

  /**
   * The name of the "mcastTtl" property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_MCAST_TTL, max = MAX_MCAST_TTL)
  String MCAST_TTL_NAME = MCAST_TTL;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#bind-address">"bind-address"</a>
   * property
   */
  @ConfigAttributeGetter(name = BIND_ADDRESS)
  String getBindAddress();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#bind-address">"bind-address"</a>
   * property
   */
  @ConfigAttributeSetter(name = BIND_ADDRESS)
  void setBindAddress(String value);

  /**
   * The name of the "bindAddress" property
   */
  @ConfigAttribute(type = String.class)
  String BIND_ADDRESS_NAME = BIND_ADDRESS;

  /**
   * The default value of the "bindAddress" property.
   * Current value is an empty string <code>""</code>
   */
  String DEFAULT_BIND_ADDRESS = "";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-bind-address">"server-bind-address"</a>
   * property
   */
  @ConfigAttributeGetter(name = SERVER_BIND_ADDRESS)
  String getServerBindAddress();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-bind-address">"server-bind-address"</a>
   * property
   */
  @ConfigAttributeSetter(name = SERVER_BIND_ADDRESS)
  void setServerBindAddress(String value);

  /**
   * The name of the "serverBindAddress" property
   */
  @ConfigAttribute(type = String.class)
  String SERVER_BIND_ADDRESS_NAME = SERVER_BIND_ADDRESS;

  /**
   * The default value of the "serverBindAddress" property.
   * Current value is an empty string <code>""</code>
   */
  String DEFAULT_SERVER_BIND_ADDRESS = "";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#locators">"locators"</a> property
   */
  @ConfigAttributeGetter(name = LOCATORS)
  String getLocators();

  /**
   * Sets the system's locator list.
   * A locator list is optional and by default empty.
   * Its used to by the system to locator other system nodes
   * and to publish itself so it can be located by others.
   *
   * @param value must be of the form <code>hostName[portNum]</code>.
   *              Multiple elements are allowed and must be seperated by a comma.
   * @throws IllegalArgumentException                   if the specified value is not acceptable.
   * @throws com.gemstone.gemfire.UnmodifiableException if this attribute can not be modified.
   * @throws com.gemstone.gemfire.GemFireIOException    if the set failure is caused by an error
   *                                                    when writing to the system's configuration file.
   */
  @ConfigAttributeSetter(name = LOCATORS)
  void setLocators(String value);

  /**
   * The name of the "locators" property
   */
  @ConfigAttribute(type = String.class)
  String LOCATORS_NAME = LOCATORS;

  /**
   * The default value of the "locators" property
   */
  String DEFAULT_LOCATORS = "";

  /**
   * Locator wait time - how long to wait for a locator to start before giving up &
   * throwing a GemFireConfigException
   */
  @ConfigAttribute(type = Integer.class)
  String LOCATOR_WAIT_TIME_NAME = LOCATOR_WAIT_TIME;

  int DEFAULT_LOCATOR_WAIT_TIME = 0;

  @ConfigAttributeGetter(name = LOCATOR_WAIT_TIME)
  int getLocatorWaitTime();

  @ConfigAttributeSetter(name = LOCATOR_WAIT_TIME)
  void setLocatorWaitTime(int seconds);

  /**
   * returns the value of the <a href="../DistribytedSystem.html#start-locator">"start-locator"
   * </a> property
   */
  @ConfigAttributeGetter(name = START_LOCATOR)
  String getStartLocator();

  /**
   * Sets the start-locator property.  This is a string in the form
   * bindAddress[port] and, if set, tells the distributed system to start
   * a locator prior to connecting
   *
   * @param value must be of the form <code>hostName[portNum]</code>
   */
  @ConfigAttributeSetter(name = START_LOCATOR)
  void setStartLocator(String value);

  /**
   * The name of the "start-locator" property
   */
  @ConfigAttribute(type = String.class)
  String START_LOCATOR_NAME = START_LOCATOR;
  /**
   * The default value of the "start-locator" property
   */
  String DEFAULT_START_LOCATOR = "";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#deploy-working-dir">"deploy-working-dir"</a> property
   */
  @ConfigAttributeGetter(name = DEPLOY_WORKING_DIR)
  File getDeployWorkingDir();

  /**
   * Sets the system's deploy working directory.
   *
   * @throws IllegalArgumentException                   if the specified value is not acceptable.
   * @throws com.gemstone.gemfire.UnmodifiableException if this attribute can not be modified.
   * @throws com.gemstone.gemfire.GemFireIOException    if the set failure is caused by an error
   *                                                    when writing to the system's configuration file.
   */
  @ConfigAttributeSetter(name = DEPLOY_WORKING_DIR)
  void setDeployWorkingDir(File value);

  /**
   * The name of the "deploy-working-dir" property.
   */
  @ConfigAttribute(type = File.class)
  String DEPLOY_WORKING_DIR_NAME = DEPLOY_WORKING_DIR;

  /**
   * Default will be the current working directory as determined by
   * <code>System.getProperty("user.dir")</code>.
   */
  File DEFAULT_DEPLOY_WORKING_DIR = new File(".");

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#user-command-packages">"user-command-packages"</a> property
   */
  @ConfigAttributeGetter(name = USER_COMMAND_PACKAGES)
  String getUserCommandPackages();

  /**
   * Sets the system's user command path.
   *
   * @throws IllegalArgumentException                   if the specified value is not acceptable.
   * @throws com.gemstone.gemfire.UnmodifiableException if this attribute can not be modified.
   * @throws com.gemstone.gemfire.GemFireIOException    if the set failure is caused by an error
   *                                                    when writing to the system's configuration file.
   */
  @ConfigAttributeSetter(name = USER_COMMAND_PACKAGES)
  void setUserCommandPackages(String value);

  /**
   * The name of the "user-command-packages" property.
   */
  @ConfigAttribute(type = String.class)
  String USER_COMMAND_PACKAGES_NAME = USER_COMMAND_PACKAGES;

  /**
   * The default value of the "user-command-packages" property
   */
  String DEFAULT_USER_COMMAND_PACKAGES = "";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#log-file">"log-file"</a> property
   *
   * @return <code>null</code> if logging information goes to standard
   * out
   */
  @ConfigAttributeGetter(name = LOG_FILE)
  File getLogFile();

  /**
   * Sets the system's log file.
   * <p> Non-absolute log files are relative to the system directory.
   * <p> The system log file can not be changed while the system is running.
   *
   * @throws IllegalArgumentException                   if the specified value is not acceptable.
   * @throws com.gemstone.gemfire.UnmodifiableException if this attribute can not be modified.
   * @throws com.gemstone.gemfire.GemFireIOException    if the set failure is caused by an error
   *                                                    when writing to the system's configuration file.
   */

  @ConfigAttributeSetter(name = LOG_FILE)
  void setLogFile(File value);

  /**
   * The name of the "logFile" property
   */
  @ConfigAttribute(type = File.class)
  String LOG_FILE_NAME = LOG_FILE;

  /**
   * The default log file.
   * <p> Actual value of this constant is <code>""</code> which directs
   * log message to standard output.
   */
  File DEFAULT_LOG_FILE = new File("");

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#log-level">"log-level"</a> property
   *
   * @see com.gemstone.gemfire.internal.logging.LogWriterImpl
   */
  @ConfigAttributeGetter(name = LOG_LEVEL)
  int getLogLevel();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#log-level">"log-level"</a> property
   *
   * @see com.gemstone.gemfire.internal.logging.LogWriterImpl
   */
  @ConfigAttributeSetter(name = LOG_LEVEL)
  void setLogLevel(int value);

  /**
   * The default log level.
   * <p> Actual value of this constant is {@link InternalLogWriter#CONFIG_LEVEL}.
   */
  int DEFAULT_LOG_LEVEL = InternalLogWriter.CONFIG_LEVEL;
  /**
   * The minimum log level.
   * <p> Actual value of this constant is {@link InternalLogWriter#ALL_LEVEL}.
   */
  int MIN_LOG_LEVEL = InternalLogWriter.ALL_LEVEL;
  /**
   * The maximum log level.
   * <p> Actual value of this constant is {@link InternalLogWriter#NONE_LEVEL}.
   */
  int MAX_LOG_LEVEL = InternalLogWriter.NONE_LEVEL;

  /**
   * The name of the "logLevel" property
   */
  // type is String because the config file contains "config", "debug", "fine" etc, not a code, but the setter/getter accepts int
  @ConfigAttribute(type = String.class)
  String LOG_LEVEL_NAME = LOG_LEVEL;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#statistic-sampling-enabled">"statistic-sampling-enabled"</a>
   * property
   */
  @ConfigAttributeGetter(name = STATISTIC_SAMPLING_ENABLED)
  boolean getStatisticSamplingEnabled();

  /**
   * Sets StatisticSamplingEnabled
   */
  @ConfigAttributeSetter(name = STATISTIC_SAMPLING_ENABLED)
  void setStatisticSamplingEnabled(boolean newValue);

  /**
   * The name of the "statisticSamplingEnabled" property
   */
  @ConfigAttribute(type = Boolean.class)
  String STATISTIC_SAMPLING_ENABLED_NAME = STATISTIC_SAMPLING_ENABLED;

  /**
   * The default value of the "statisticSamplingEnabled" property
   */
  boolean DEFAULT_STATISTIC_SAMPLING_ENABLED = true;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#statistic-sample-rate">"statistic-sample-rate"</a>
   * property
   */
  @ConfigAttributeGetter(name = STATISTIC_SAMPLE_RATE)
  int getStatisticSampleRate();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#statistic-sample-rate">"statistic-sample-rate"</a>
   * property
   */
  @ConfigAttributeSetter(name = STATISTIC_SAMPLE_RATE)
  void setStatisticSampleRate(int value);

  /**
   * The default statistic sample rate.
   * <p> Actual value of this constant is <code>1000</code> milliseconds.
   */
  int DEFAULT_STATISTIC_SAMPLE_RATE = 1000;
  /**
   * The minimum statistic sample rate.
   * <p> Actual value of this constant is <code>100</code> milliseconds.
   */
  int MIN_STATISTIC_SAMPLE_RATE = 100;
  /**
   * The maximum statistic sample rate.
   * <p> Actual value of this constant is <code>60000</code> milliseconds.
   */
  int MAX_STATISTIC_SAMPLE_RATE = 60000;

  /**
   * The name of the "statisticSampleRate" property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_STATISTIC_SAMPLE_RATE, max = MAX_STATISTIC_SAMPLE_RATE)
  String STATISTIC_SAMPLE_RATE_NAME = STATISTIC_SAMPLE_RATE;

  /**
   * Returns the value of the <a href="../DistributedSystem.html#statistic-archive-file">"statistic-archive-file"</a> property.
   *
   * @return <code>null</code> if no file was specified
   */
  @ConfigAttributeGetter(name = STATISTIC_ARCHIVE_FILE)
  File getStatisticArchiveFile();

  /**
   * Sets the value of the <a href="../DistributedSystem.html#statistic-archive-file">"statistic-archive-file"</a> property.
   */
  @ConfigAttributeSetter(name = STATISTIC_ARCHIVE_FILE)
  void setStatisticArchiveFile(File value);

  /**
   * The name of the "statisticArchiveFile" property
   */
  @ConfigAttribute(type = File.class)
  String STATISTIC_ARCHIVE_FILE_NAME = STATISTIC_ARCHIVE_FILE;

  /**
   * The default statistic archive file.
   * <p> Actual value of this constant is <code>""</code> which
   * causes no archive file to be created.
   */
  File DEFAULT_STATISTIC_ARCHIVE_FILE = new File(""); // fix for bug 29786

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cache-xml-file">"cache-xml-file"</a>
   * property
   */
  @ConfigAttributeGetter(name = CACHE_XML_FILE)
  File getCacheXmlFile();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cache-xml-file">"cache-xml-file"</a>
   * property
   */
  @ConfigAttributeSetter(name = CACHE_XML_FILE)
  void setCacheXmlFile(File value);

  /**
   * The name of the "cacheXmlFile" property
   */
  @ConfigAttribute(type = File.class)
  String CACHE_XML_FILE_NAME = CACHE_XML_FILE;

  /**
   * The default value of the "cacheXmlFile" property
   */
  File DEFAULT_CACHE_XML_FILE = new File("cache.xml");

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#ack-wait-threshold">"ack-wait-threshold"</a>
   * property
   */
  @ConfigAttributeGetter(name = ACK_WAIT_THRESHOLD)
  int getAckWaitThreshold();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#ack-wait-threshold">"ack-wait-threshold"</a>
   * property
   * Setting this value too low will cause spurious alerts.
   */
  @ConfigAttributeSetter(name = ACK_WAIT_THRESHOLD)
  void setAckWaitThreshold(int newThreshold);

  /**
   * The default AckWaitThreshold.
   * <p> Actual value of this constant is <code>15</code> seconds.
   */
  int DEFAULT_ACK_WAIT_THRESHOLD = 15;
  /**
   * The minimum AckWaitThreshold.
   * <p> Actual value of this constant is <code>1</code> second.
   */
  int MIN_ACK_WAIT_THRESHOLD = 1;
  /**
   * The maximum AckWaitThreshold.
   * <p> Actual value of this constant is <code>MAX_INT</code> seconds.
   */
  int MAX_ACK_WAIT_THRESHOLD = Integer.MAX_VALUE;
  /**
   * The name of the "ackWaitThreshold" property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_ACK_WAIT_THRESHOLD)
  String ACK_WAIT_THRESHOLD_NAME = ACK_WAIT_THRESHOLD;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#ack-severe-alert-threshold">"ack-severe-alert-threshold"</a>
   * property
   */
  @ConfigAttributeGetter(name = ACK_SEVERE_ALERT_THRESHOLD)
  int getAckSevereAlertThreshold();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#ack-severe-alert-threshold">"ack-severe-alert-threshold"</a>
   * property
   * Setting this value too low will cause spurious forced disconnects.
   */
  @ConfigAttributeSetter(name = ACK_SEVERE_ALERT_THRESHOLD)
  void setAckSevereAlertThreshold(int newThreshold);

  /**
   * The default ackSevereAlertThreshold.
   * <p> Actual value of this constant is <code>0</code> seconds, which
   * turns off shunning.
   */
  int DEFAULT_ACK_SEVERE_ALERT_THRESHOLD = 0;
  /**
   * The minimum ackSevereAlertThreshold.
   * <p> Actual value of this constant is <code>0</code> second,
   * which turns off shunning.
   */
  int MIN_ACK_SEVERE_ALERT_THRESHOLD = 0;
  /**
   * The maximum ackSevereAlertThreshold.
   * <p> Actual value of this constant is <code>MAX_INT</code> seconds.
   */
  int MAX_ACK_SEVERE_ALERT_THRESHOLD = Integer.MAX_VALUE;
  /**
   * The name of the "ackSevereAlertThreshold" property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_ACK_SEVERE_ALERT_THRESHOLD)
  String ACK_SEVERE_ALERT_THRESHOLD_NAME = ACK_SEVERE_ALERT_THRESHOLD;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#archive-file-size-limit">"archive-file-size-limit"</a>
   * property
   */
  @ConfigAttributeGetter(name = ARCHIVE_FILE_SIZE_LIMIT)
  int getArchiveFileSizeLimit();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#archive-file-size-limit">"archive-file-size-limit"</a>
   * property
   */
  @ConfigAttributeSetter(name = ARCHIVE_FILE_SIZE_LIMIT)
  void setArchiveFileSizeLimit(int value);

  /**
   * The default statistic archive file size limit.
   * <p> Actual value of this constant is <code>0</code> megabytes.
   */
  int DEFAULT_ARCHIVE_FILE_SIZE_LIMIT = 0;
  /**
   * The minimum statistic archive file size limit.
   * <p> Actual value of this constant is <code>0</code> megabytes.
   */
  int MIN_ARCHIVE_FILE_SIZE_LIMIT = 0;
  /**
   * The maximum statistic archive file size limit.
   * <p> Actual value of this constant is <code>1000000</code> megabytes.
   */
  int MAX_ARCHIVE_FILE_SIZE_LIMIT = 1000000;

  /**
   * The name of the "ArchiveFileSizeLimit" property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_ARCHIVE_FILE_SIZE_LIMIT, max = MAX_ARCHIVE_FILE_SIZE_LIMIT)
  String ARCHIVE_FILE_SIZE_LIMIT_NAME = ARCHIVE_FILE_SIZE_LIMIT;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#archive-disk-space-limit">"archive-disk-space-limit"</a>
   * property
   */
  @ConfigAttributeGetter(name = ARCHIVE_DISK_SPACE_LIMIT)
  int getArchiveDiskSpaceLimit();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#archive-disk-space-limit">"archive-disk-space-limit"</a>
   * property
   */
  @ConfigAttributeSetter(name = ARCHIVE_DISK_SPACE_LIMIT)
  void setArchiveDiskSpaceLimit(int value);

  /**
   * The default archive disk space limit.
   * <p> Actual value of this constant is <code>0</code> megabytes.
   */
  int DEFAULT_ARCHIVE_DISK_SPACE_LIMIT = 0;
  /**
   * The minimum archive disk space limit.
   * <p> Actual value of this constant is <code>0</code> megabytes.
   */
  int MIN_ARCHIVE_DISK_SPACE_LIMIT = 0;
  /**
   * The maximum archive disk space limit.
   * <p> Actual value of this constant is <code>1000000</code> megabytes.
   */
  int MAX_ARCHIVE_DISK_SPACE_LIMIT = 1000000;

  /**
   * The name of the "ArchiveDiskSpaceLimit" property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_ARCHIVE_DISK_SPACE_LIMIT, max = MAX_ARCHIVE_DISK_SPACE_LIMIT)
  String ARCHIVE_DISK_SPACE_LIMIT_NAME = ARCHIVE_DISK_SPACE_LIMIT;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#log-file-size-limit">"log-file-size-limit"</a>
   * property
   */
  @ConfigAttributeGetter(name = LOG_FILE_SIZE_LIMIT)
  int getLogFileSizeLimit();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#log-file-size-limit">"log-file-size-limit"</a>
   * property
   */
  @ConfigAttributeSetter(name = LOG_FILE_SIZE_LIMIT)
  void setLogFileSizeLimit(int value);

  /**
   * The default log file size limit.
   * <p> Actual value of this constant is <code>0</code> megabytes.
   */
  int DEFAULT_LOG_FILE_SIZE_LIMIT = 0;
  /**
   * The minimum log file size limit.
   * <p> Actual value of this constant is <code>0</code> megabytes.
   */
  int MIN_LOG_FILE_SIZE_LIMIT = 0;
  /**
   * The maximum log file size limit.
   * <p> Actual value of this constant is <code>1000000</code> megabytes.
   */
  int MAX_LOG_FILE_SIZE_LIMIT = 1000000;

  /**
   * The name of the "LogFileSizeLimit" property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_LOG_FILE_SIZE_LIMIT, max = MAX_LOG_FILE_SIZE_LIMIT)
  String LOG_FILE_SIZE_LIMIT_NAME = LOG_FILE_SIZE_LIMIT;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#log-disk-space-limit">"log-disk-space-limit"</a>
   * property
   */
  @ConfigAttributeGetter(name = LOG_DISK_SPACE_LIMIT)
  int getLogDiskSpaceLimit();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#log-disk-space-limit">"log-disk-space-limit"</a>
   * property
   */
  @ConfigAttributeSetter(name = LOG_DISK_SPACE_LIMIT)
  void setLogDiskSpaceLimit(int value);

  /**
   * The default log disk space limit.
   * <p> Actual value of this constant is <code>0</code> megabytes.
   */
  int DEFAULT_LOG_DISK_SPACE_LIMIT = 0;
  /**
   * The minimum log disk space limit.
   * <p> Actual value of this constant is <code>0</code> megabytes.
   */
  int MIN_LOG_DISK_SPACE_LIMIT = 0;
  /**
   * The maximum log disk space limit.
   * <p> Actual value of this constant is <code>1000000</code> megabytes.
   */
  int MAX_LOG_DISK_SPACE_LIMIT = 1000000;

  /**
   * The name of the "LogDiskSpaceLimit" property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_LOG_DISK_SPACE_LIMIT, max = MAX_LOG_DISK_SPACE_LIMIT)
  String LOG_DISK_SPACE_LIMIT_NAME = LOG_DISK_SPACE_LIMIT;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#ssl-enabled">"ssl-enabled"</a>
   * property.
   *
   * @deprecated as of 8.0 use {@link #getClusterSSLEnabled} instead.
   */
  @ConfigAttributeGetter(name = SSL_ENABLED)
  boolean getSSLEnabled();

  /**
   * The default ssl-enabled state.
   * <p> Actual value of this constant is <code>false</code>.
   *
   * @deprecated as of 8.0 use {@link #DEFAULT_CLUSTER_SSL_ENABLED} instead.
   */
  boolean DEFAULT_SSL_ENABLED = false;

  /**
   * The name of the "SSLEnabled" property
   *
   * @deprecated as of 8.0 use {@link #CLUSTER_SSL_ENABLED} instead.
   */
  @ConfigAttribute(type = Boolean.class)
  String SSL_ENABLED_NAME = SSL_ENABLED;

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#ssl-enabled">"ssl-enabled"</a>
   * property.
   *
   * @deprecated as of 8.0 use {@link #setClusterSSLEnabled} instead.
   */
  @ConfigAttributeSetter(name = SSL_ENABLED)
  void setSSLEnabled(boolean enabled);

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#ssl-protocols">"ssl-protocols"</a>
   * property.
   *
   * @deprecated as of 8.0 use {@link #getClusterSSLProtocols} instead.
   */
  @ConfigAttributeGetter(name = SSL_PROTOCOLS)
  String getSSLProtocols();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#ssl-protocols">"ssl-protocols"</a>
   * property.
   *
   * @deprecated as of 8.0 use {@link #setClusterSSLProtocols} instead.
   */
  @ConfigAttributeSetter(name = SSL_PROTOCOLS)
  void setSSLProtocols(String protocols);

  /**
   * The default ssl-protocols value.
   * <p> Actual value of this constant is <code>any</code>.
   *
   * @deprecated as of 8.0 use {@link #DEFAULT_CLUSTER_SSL_PROTOCOLS} instead.
   */
  String DEFAULT_SSL_PROTOCOLS = "any";
  /**
   * The name of the "SSLProtocols" property
   *
   * @deprecated as of 8.0 use {@link #CLUSTER_SSL_PROTOCOLS} instead.
   */
  @ConfigAttribute(type = String.class)
  String SSL_PROTOCOLS_NAME = SSL_PROTOCOLS;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#ssl-ciphers">"ssl-ciphers"</a>
   * property.
   *
   * @deprecated as of 8.0 use {@link #getClusterSSLCiphers} instead.
   */
  @ConfigAttributeGetter(name = SSL_CIPHERS)
  String getSSLCiphers();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#ssl-ciphers">"ssl-ciphers"</a>
   * property.
   *
   * @deprecated as of 8.0 use {@link #setClusterSSLCiphers} instead.
   */
  @ConfigAttributeSetter(name = SSL_CIPHERS)
  void setSSLCiphers(String ciphers);

  /**
   * The default ssl-ciphers value.
   * <p> Actual value of this constant is <code>any</code>.
   *
   * @deprecated as of 8.0 use {@link #DEFAULT_CLUSTER_SSL_CIPHERS} instead.
   */
  String DEFAULT_SSL_CIPHERS = "any";
  /**
   * The name of the "SSLCiphers" property
   *
   * @deprecated as of 8.0 use {@link #CLUSTER_SSL_CIPHERS} instead.
   */
  @ConfigAttribute(type = String.class)
  String SSL_CIPHERS_NAME = SSL_CIPHERS;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#ssl-require-authentication">"ssl-require-authentication"</a>
   * property.
   *
   * @deprecated as of 8.0 use {@link #getClusterSSLRequireAuthentication} instead.
   */
  @ConfigAttributeGetter(name = SSL_REQUIRE_AUTHENTICATION)
  boolean getSSLRequireAuthentication();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#ssl-require-authentication">"ssl-require-authentication"</a>
   * property.
   *
   * @deprecated as of 8.0 use {@link #setClusterSSLRequireAuthentication} instead.
   */
  @ConfigAttributeSetter(name = SSL_REQUIRE_AUTHENTICATION)
  void setSSLRequireAuthentication(boolean enabled);

  /**
   * The default ssl-require-authentication value.
   * <p> Actual value of this constant is <code>true</code>.
   *
   * @deprecated as of 8.0 use {@link #DEFAULT_CLUSTER_SSL_REQUIRE_AUTHENTICATION} instead.
   */
  boolean DEFAULT_SSL_REQUIRE_AUTHENTICATION = true;
  /**
   * The name of the "SSLRequireAuthentication" property
   *
   * @deprecated as of 8.0 use {@link #CLUSTER_SSL_REQUIRE_AUTHENTICATION} instead.
   */
  @ConfigAttribute(type = Boolean.class)
  String SSL_REQUIRE_AUTHENTICATION_NAME = SSL_REQUIRE_AUTHENTICATION;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-enabled">"cluster-ssl-enabled"</a>
   * property.
   */
  @ConfigAttributeGetter(name = CLUSTER_SSL_ENABLED)
  boolean getClusterSSLEnabled();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-enabled">"cluster-ssl-enabled"</a>
   * property.
   */
  @ConfigAttributeSetter(name = CLUSTER_SSL_ENABLED)
  void setClusterSSLEnabled(boolean enabled);

  /**
   * The default cluster-ssl-enabled state.
   * <p> Actual value of this constant is <code>false</code>.
   */
  boolean DEFAULT_CLUSTER_SSL_ENABLED = false;
  /**
   * The name of the "ClusterSSLEnabled" property
   */
  @ConfigAttribute(type = Boolean.class)
  String CLUSTER_SSL_ENABLED_NAME = CLUSTER_SSL_ENABLED;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-protocols">"cluster-ssl-protocols"</a>
   * property.
   */
  @ConfigAttributeGetter(name = CLUSTER_SSL_PROTOCOLS)
  String getClusterSSLProtocols();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-protocols">"cluster-ssl-protocols"</a>
   * property.
   */
  @ConfigAttributeSetter(name = CLUSTER_SSL_PROTOCOLS)
  void setClusterSSLProtocols(String protocols);

  /**
   * The default cluster-ssl-protocols value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  String DEFAULT_CLUSTER_SSL_PROTOCOLS = "any";
  /**
   * The name of the "ClusterSSLProtocols" property
   */
  @ConfigAttribute(type = String.class)
  String CLUSTER_SSL_PROTOCOLS_NAME = CLUSTER_SSL_PROTOCOLS;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-ciphers">"cluster-ssl-ciphers"</a>
   * property.
   */
  @ConfigAttributeGetter(name = CLUSTER_SSL_CIPHERS)
  String getClusterSSLCiphers();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-ciphers">"cluster-ssl-ciphers"</a>
   * property.
   */
  @ConfigAttributeSetter(name = CLUSTER_SSL_CIPHERS)
  void setClusterSSLCiphers(String ciphers);

  /**
   * The default cluster-ssl-ciphers value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  String DEFAULT_CLUSTER_SSL_CIPHERS = "any";
  /**
   * The name of the "ClusterSSLCiphers" property
   */
  @ConfigAttribute(type = String.class)
  String CLUSTER_SSL_CIPHERS_NAME = CLUSTER_SSL_CIPHERS;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-require-authentication">"cluster-ssl-require-authentication"</a>
   * property.
   */
  @ConfigAttributeGetter(name = CLUSTER_SSL_REQUIRE_AUTHENTICATION)
  boolean getClusterSSLRequireAuthentication();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-require-authentication">"cluster-ssl-require-authentication"</a>
   * property.
   */
  @ConfigAttributeSetter(name = CLUSTER_SSL_REQUIRE_AUTHENTICATION)
  void setClusterSSLRequireAuthentication(boolean enabled);

  /**
   * The default cluster-ssl-require-authentication value.
   * <p> Actual value of this constant is <code>true</code>.
   */
  boolean DEFAULT_CLUSTER_SSL_REQUIRE_AUTHENTICATION = true;
  /**
   * The name of the "ClusterSSLRequireAuthentication" property
   */
  @ConfigAttribute(type = Boolean.class)
  String CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME = CLUSTER_SSL_REQUIRE_AUTHENTICATION;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-keystore">"cluster-ssl-keystore"</a>
   * property.
   */
  @ConfigAttributeGetter(name = CLUSTER_SSL_KEYSTORE)
  String getClusterSSLKeyStore();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-keystore">"cluster-ssl-keystore"</a>
   * property.
   */
  @ConfigAttributeSetter(name = CLUSTER_SSL_KEYSTORE)
  void setClusterSSLKeyStore(String keyStore);

  /**
   * The default cluster-ssl-keystore value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_CLUSTER_SSL_KEYSTORE = "";

  /**
   * The name of the "ClusterSSLKeyStore" property
   */
  @ConfigAttribute(type = String.class)
  String CLUSTER_SSL_KEYSTORE_NAME = CLUSTER_SSL_KEYSTORE;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-keystore-type">"cluster-ssl-keystore-type"</a>
   * property.
   */
  @ConfigAttributeGetter(name = CLUSTER_SSL_KEYSTORE_TYPE)
  String getClusterSSLKeyStoreType();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-keystore-type">"cluster-ssl-keystore-type"</a>
   * property.
   */
  @ConfigAttributeSetter(name = CLUSTER_SSL_KEYSTORE_TYPE)
  void setClusterSSLKeyStoreType(String keyStoreType);

  /**
   * The default cluster-ssl-keystore-type value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_CLUSTER_SSL_KEYSTORE_TYPE = "";

  /**
   * The name of the "ClusterSSLKeyStoreType" property
   */
  @ConfigAttribute(type = String.class)
  String CLUSTER_SSL_KEYSTORE_TYPE_NAME = CLUSTER_SSL_KEYSTORE_TYPE;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-keystore-password">"cluster-ssl-keystore-password"</a>
   * property.
   */
  @ConfigAttributeGetter(name = CLUSTER_SSL_KEYSTORE_PASSWORD)
  String getClusterSSLKeyStorePassword();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-keystore-password">"cluster-ssl-keystore-password"</a>
   * property.
   */
  @ConfigAttributeSetter(name = CLUSTER_SSL_KEYSTORE_PASSWORD)
  void setClusterSSLKeyStorePassword(String keyStorePassword);

  /**
   * The default cluster-ssl-keystore-password value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_CLUSTER_SSL_KEYSTORE_PASSWORD = "";

  /**
   * The name of the "ClusterSSLKeyStorePassword" property
   */
  @ConfigAttribute(type = String.class)
  String CLUSTER_SSL_KEYSTORE_PASSWORD_NAME = CLUSTER_SSL_KEYSTORE_PASSWORD;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-truststore">"cluster-ssl-truststore"</a>
   * property.
   */
  @ConfigAttributeGetter(name = CLUSTER_SSL_TRUSTSTORE)
  String getClusterSSLTrustStore();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-truststore">"cluster-ssl-truststore"</a>
   * property.
   */
  @ConfigAttributeSetter(name = CLUSTER_SSL_TRUSTSTORE)
  void setClusterSSLTrustStore(String trustStore);

  /**
   * The default cluster-ssl-truststore value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_CLUSTER_SSL_TRUSTSTORE = "";

  /**
   * The name of the "ClusterSSLTrustStore" property
   */
  @ConfigAttribute(type = String.class)
  String CLUSTER_SSL_TRUSTSTORE_NAME = CLUSTER_SSL_TRUSTSTORE;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-truststore-password">"cluster-ssl-truststore-password"</a>
   * property.
   */
  @ConfigAttributeGetter(name = CLUSTER_SSL_TRUSTSTORE_PASSWORD)
  String getClusterSSLTrustStorePassword();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-truststore-password">"cluster-ssl-truststore-password"</a>
   * property.
   */
  @ConfigAttributeSetter(name = CLUSTER_SSL_TRUSTSTORE_PASSWORD)
  void setClusterSSLTrustStorePassword(String trusStorePassword);

  /**
   * The default cluster-ssl-truststore-password value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_CLUSTER_SSL_TRUSTSTORE_PASSWORD = "";

  /**
   * The name of the "ClusterSSLKeyStorePassword" property
   */
  @ConfigAttribute(type = String.class)
  String CLUSTER_SSL_TRUSTSTORE_PASSWORD_NAME = CLUSTER_SSL_TRUSTSTORE_PASSWORD;

  /**
   * The name of an internal property that specifies a {@link
   * com.gemstone.gemfire.i18n.LogWriterI18n} instance to log to.
   * Set this property with put(), not with setProperty()
   *
   * @since GemFire 4.0 */
  String LOG_WRITER_NAME = "log-writer";

  /**
   * The name of an internal property that specifies a
   * a DistributionConfigImpl that the locator is passing
   * in to a ds connect.
   * Set this property with put(), not with setProperty()
   *
   * @since GemFire 7.0 */
  String DS_CONFIG_NAME = "ds-config";

  /**
   * The name of an internal property that specifies whether
   * the distributed system is reconnecting after a forced-
   * disconnect.
   * @since GemFire 8.1
   */
  String DS_RECONNECTING_NAME = "ds-reconnecting";

  /**
   * The name of an internal property that specifies the
   * quorum checker for the system that was forcibly disconnected.
   * This should be used if the DS_RECONNECTING_NAME property
   * is used.
   */
  String DS_QUORUM_CHECKER_NAME = "ds-quorum-checker";

  /**
   * The name of an internal property that specifies a {@link
   * com.gemstone.gemfire.LogWriter} instance to log security messages to. Set
   * this property with put(), not with setProperty()
   *
   * @since GemFire 5.5
   */
  String SECURITY_LOG_WRITER_NAME = "security-log-writer";

  /**
   * The name of an internal property that specifies a
   * FileOutputStream associated with the internal property
   * LOG_WRITER_NAME.  If this property is set, the
   * FileOutputStream will be closed when the distributed
   * system disconnects.  Set this property with put(), not
   * with setProperty()
   * @since GemFire 5.0
   */
  String LOG_OUTPUTSTREAM_NAME = "log-output-stream";

  /**
   * The name of an internal property that specifies a FileOutputStream
   * associated with the internal property SECURITY_LOG_WRITER_NAME. If this
   * property is set, the FileOutputStream will be closed when the distributed
   * system disconnects. Set this property with put(), not with setProperty()
   *
   * @since GemFire 5.5
   */
  String SECURITY_LOG_OUTPUTSTREAM_NAME = "security-log-output-stream";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#socket-lease-time">"socket-lease-time"</a>
   * property
   */
  @ConfigAttributeGetter(name = SOCKET_LEASE_TIME)
  int getSocketLeaseTime();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#socket-lease-time">"socket-lease-time"</a>
   * property
   */
  @ConfigAttributeSetter(name = SOCKET_LEASE_TIME)
  void setSocketLeaseTime(int value);

  /**
   * The default value of the "socketLeaseTime" property
   */
  int DEFAULT_SOCKET_LEASE_TIME = 60000;
  /**
   * The minimum socketLeaseTime.
   * <p> Actual value of this constant is <code>0</code>.
   */
  int MIN_SOCKET_LEASE_TIME = 0;
  /**
   * The maximum socketLeaseTime.
   * <p> Actual value of this constant is <code>600000</code>.
   */
  int MAX_SOCKET_LEASE_TIME = 600000;

  /**
   * The name of the "socketLeaseTime" property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_SOCKET_LEASE_TIME, max = MAX_SOCKET_LEASE_TIME)
  String SOCKET_LEASE_TIME_NAME = SOCKET_LEASE_TIME;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#socket-buffer-size">"socket-buffer-size"</a>
   * property
   */
  @ConfigAttributeGetter(name = SOCKET_BUFFER_SIZE)
  int getSocketBufferSize();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#socket-buffer-size">"socket-buffer-size"</a>
   * property
   */
  @ConfigAttributeSetter(name = SOCKET_BUFFER_SIZE)
  void setSocketBufferSize(int value);

  /**
   * The default value of the "socketBufferSize" property
   */
  int DEFAULT_SOCKET_BUFFER_SIZE = 32768;
  /**
   * The minimum socketBufferSize.
   * <p> Actual value of this constant is <code>1024</code>.
   */
  int MIN_SOCKET_BUFFER_SIZE = 1024;
  /**
   * The maximum socketBufferSize.
   * <p> Actual value of this constant is <code>20000000</code>.
   */
  int MAX_SOCKET_BUFFER_SIZE = Connection.MAX_MSG_SIZE;

  boolean VALIDATE = Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "validateMessageSize");
  int VALIDATE_CEILING = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "validateMessageSizeCeiling", 8 * 1024 * 1024).intValue();

  /**
   * The name of the "socketBufferSize" property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_SOCKET_BUFFER_SIZE, max = MAX_SOCKET_BUFFER_SIZE)
  String SOCKET_BUFFER_SIZE_NAME = SOCKET_BUFFER_SIZE;

  /**
   * Get the value of the
   * <a href="../DistributedSystem.html#mcast-send-buffer-size">"mcast-send-buffer-size"</a>
   * property
   */
  @ConfigAttributeGetter(name = MCAST_SEND_BUFFER_SIZE)
  int getMcastSendBufferSize();

  /**
   * Set the value of the
   * <a href="../DistributedSystem.html#mcast-send-buffer-size">"mcast-send-buffer-size"</a>
   * property
   */
  @ConfigAttributeSetter(name = MCAST_SEND_BUFFER_SIZE)
  void setMcastSendBufferSize(int value);

  /**
   * The default value of the corresponding property
   */
  int DEFAULT_MCAST_SEND_BUFFER_SIZE = 65535;

  /**
   * The minimum size of the buffer, in bytes.
   * <p> Actual value of this constant is <code>2048</code>.
   */
  int MIN_MCAST_SEND_BUFFER_SIZE = 2048;

  /**
   * The name of the corresponding property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_MCAST_SEND_BUFFER_SIZE)
  String MCAST_SEND_BUFFER_SIZE_NAME = MCAST_SEND_BUFFER_SIZE;

  /**
   * Get the value of the
   * <a href="../DistributedSystem.html#mcast-recv-buffer-size">"mcast-recv-buffer-size"</a>
   * property
   */
  @ConfigAttributeGetter(name = MCAST_RECV_BUFFER_SIZE)
  int getMcastRecvBufferSize();

  /**
   * Set the value of the
   * <a href="../DistributedSystem.html#mcast-recv-buffer-size">"mcast-recv-buffer-size"</a>
   * property
   */
  @ConfigAttributeSetter(name = MCAST_RECV_BUFFER_SIZE)
  void setMcastRecvBufferSize(int value);

  /**
   * The default value of the corresponding property
   */
  int DEFAULT_MCAST_RECV_BUFFER_SIZE = 1048576;

  /**
   * The minimum size of the buffer, in bytes.
   * <p> Actual value of this constant is <code>2048</code>.
   */
  int MIN_MCAST_RECV_BUFFER_SIZE = 2048;

  /**
   * The name of the corresponding property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_MCAST_RECV_BUFFER_SIZE)
  String MCAST_RECV_BUFFER_SIZE_NAME = MCAST_RECV_BUFFER_SIZE;

  /**
   * Get the value of the
   * <a href="../DistributedSystem.html#mcast-flow-control">"mcast-flow-control"</a>
   * property.
   */
  @ConfigAttributeGetter(name = MCAST_FLOW_CONTROL)
  FlowControlParams getMcastFlowControl();

  /**
   * Set the value of the
   * <a href="../DistributedSystem.html#mcast-flow-control">"mcast-flow-control"</a>
   * property
   */
  @ConfigAttributeSetter(name = MCAST_FLOW_CONTROL)
  void setMcastFlowControl(FlowControlParams values);

  /**
   * The name of the corresponding property
   */
  @ConfigAttribute(type = FlowControlParams.class)
  String MCAST_FLOW_CONTROL_NAME = MCAST_FLOW_CONTROL;

  /**
   * The default value of the corresponding property
   */
  FlowControlParams DEFAULT_MCAST_FLOW_CONTROL
      = new FlowControlParams(1048576, (float) 0.25, 5000);

  /**
   * The minimum byteAllowance for the mcast-flow-control setting of
   * <code>100000</code>.
   */
  int MIN_FC_BYTE_ALLOWANCE = 10000;

  /**
   * The minimum rechargeThreshold for the mcast-flow-control setting of
   * <code>0.1</code>
   */
  float MIN_FC_RECHARGE_THRESHOLD = (float) 0.1;

  /**
   * The maximum rechargeThreshold for the mcast-flow-control setting of
   * <code>0.5</code>
   */
  float MAX_FC_RECHARGE_THRESHOLD = (float) 0.5;

  /**
   * The minimum rechargeBlockMs for the mcast-flow-control setting of
   * <code>500</code>
   */
  int MIN_FC_RECHARGE_BLOCK_MS = 500;

  /**
   * The maximum rechargeBlockMs for the mcast-flow-control setting of
   * <code>60000</code>
   */
  int MAX_FC_RECHARGE_BLOCK_MS = 60000;

  /**
   * Get the value of the
   * <a href="../DistributedSystem.html#udp-fragment-size">"udp-fragment-size"</a>
   * property.
   */
  @ConfigAttributeGetter(name = UDP_FRAGMENT_SIZE)
  int getUdpFragmentSize();

  /**
   * Set the value of the
   * <a href="../DistributedSystem.html#udp-fragment-size">"udp-fragment-size"</a>
   * property
   */
  @ConfigAttributeSetter(name = UDP_FRAGMENT_SIZE)
  void setUdpFragmentSize(int value);

  /**
   * The default value of the corresponding property
   */
  int DEFAULT_UDP_FRAGMENT_SIZE = 60000;

  /**
   * The minimum allowed udp-fragment-size setting of 1000
   */
  int MIN_UDP_FRAGMENT_SIZE = 1000;

  /**
   * The maximum allowed udp-fragment-size setting of 60000
   */
  int MAX_UDP_FRAGMENT_SIZE = 60000;

  /**
   * The name of the corresponding property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_UDP_FRAGMENT_SIZE, max = MAX_UDP_FRAGMENT_SIZE)
  String UDP_FRAGMENT_SIZE_NAME = UDP_FRAGMENT_SIZE;

  /**
   * Get the value of the
   * <a href="../DistributedSystem.html#udp-send-buffer-size">"udp-send-buffer-size"</a>
   * property
   */
  @ConfigAttributeGetter(name = UDP_SEND_BUFFER_SIZE)
  int getUdpSendBufferSize();

  /**
   * Set the value of the
   * <a href="../DistributedSystem.html#udp-send-buffer-size">"udp-send-buffer-size"</a>
   * property
   */
  @ConfigAttributeSetter(name = UDP_SEND_BUFFER_SIZE)
  void setUdpSendBufferSize(int value);

  /**
   * The default value of the corresponding property
   */
  int DEFAULT_UDP_SEND_BUFFER_SIZE = 65535;

  /**
   * The minimum size of the buffer, in bytes.
   * <p> Actual value of this constant is <code>2048</code>.
   */
  int MIN_UDP_SEND_BUFFER_SIZE = 2048;

  /**
   * The name of the corresponding property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_UDP_SEND_BUFFER_SIZE)
  String UDP_SEND_BUFFER_SIZE_NAME = UDP_SEND_BUFFER_SIZE;

  /**
   * Get the value of the
   * <a href="../DistributedSystem.html#udp-recv-buffer-size">"udp-recv-buffer-size"</a>
   * property
   */
  @ConfigAttributeGetter(name = UDP_RECV_BUFFER_SIZE)
  int getUdpRecvBufferSize();

  /**
   * Set the value of the
   * <a href="../DistributedSystem.html#udp-recv-buffer-size">"udp-recv-buffer-size"</a>
   * property
   */
  @ConfigAttributeSetter(name = UDP_RECV_BUFFER_SIZE)
  void setUdpRecvBufferSize(int value);

  /**
   * The default value of the unicast receive buffer size property
   */
  int DEFAULT_UDP_RECV_BUFFER_SIZE = 1048576;

  /**
   * The default size of the unicast receive buffer property if tcp/ip sockets are
   * enabled and multicast is disabled
   */
  int DEFAULT_UDP_RECV_BUFFER_SIZE_REDUCED = 65535;

  /**
   * The minimum size of the buffer, in bytes.
   * <p> Actual value of this constant is <code>2048</code>.
   */
  int MIN_UDP_RECV_BUFFER_SIZE = 2048;

  /**
   * The name of the corresponding property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_UDP_RECV_BUFFER_SIZE)
  String UDP_RECV_BUFFER_SIZE_NAME = UDP_RECV_BUFFER_SIZE;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#disable-tcp">"disable-tcp"</a>
   * property
   */
  @ConfigAttributeGetter(name = DISABLE_TCP)
  boolean getDisableTcp();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#disable-tcp">"disable-tcp"</a>
   * property.
   */
  @ConfigAttributeSetter(name = DISABLE_TCP)
  void setDisableTcp(boolean newValue);

  /**
   * The name of the corresponding property
   */
  @ConfigAttribute(type = Boolean.class)
  String DISABLE_TCP_NAME = DISABLE_TCP;

  /**
   * The default value of the corresponding property
   */
  boolean DEFAULT_DISABLE_TCP = false;

  /**
   * Turns on timing statistics for the distributed system
   */
  @ConfigAttributeSetter(name = ENABLE_TIME_STATISTICS)
  void setEnableTimeStatistics(boolean newValue);

  /**
   * Returns the value of <a
   * href="../DistributedSystem.html#enable-time-statistics">enable-time-statistics</a>
   * property
   */
  @ConfigAttributeGetter(name = ENABLE_TIME_STATISTICS)
  boolean getEnableTimeStatistics();

  /**
   * the name of the corresponding property
   */
  @ConfigAttribute(type = Boolean.class)
  String ENABLE_TIME_STATISTICS_NAME = ENABLE_TIME_STATISTICS;

  /**
   * The default value of the corresponding property
   */
  boolean DEFAULT_ENABLE_TIME_STATISTICS = false;

  /**
   * Sets the value for
   * <a href="../DistributedSystem.html#use-cluster-configuration">use-shared-configuration</a>
   */
  @ConfigAttributeSetter(name = USE_CLUSTER_CONFIGURATION)
  void setUseSharedConfiguration(boolean newValue);

  /**
   * Returns the value of <a
   * href="../DistributedSystem.html#use-cluster-configuration">use-cluster-configuration</a>
   * property
   */
  @ConfigAttributeGetter(name = USE_CLUSTER_CONFIGURATION)
  boolean getUseSharedConfiguration();

  /**
   * the name of the corresponding property
   */
  @ConfigAttribute(type = Boolean.class)
  String USE_CLUSTER_CONFIGURATION_NAME = USE_CLUSTER_CONFIGURATION;

  /**
   * The default value of the corresponding property
   */
  boolean DEFAULT_USE_CLUSTER_CONFIGURATION = true;

  /**
   * Sets the value for
   * <a href="../DistributedSystem.html#enable-cluster-configuration">enable-cluster-configuration</a>
   */
  @ConfigAttributeSetter(name = ENABLE_CLUSTER_CONFIGURATION)
  void setEnableClusterConfiguration(boolean newValue);

  /**
   * Returns the value of <a
   * href="../DistributedSystem.html#enable-cluster-configuration">enable-cluster-configuration</a>
   * property
   */
  @ConfigAttributeGetter(name = ENABLE_CLUSTER_CONFIGURATION)
  boolean getEnableClusterConfiguration();

  /**
   * the name of the corresponding property
   */
  @ConfigAttribute(type = Boolean.class)
  String ENABLE_CLUSTER_CONFIGURATION_NAME = ENABLE_CLUSTER_CONFIGURATION;

  /**
   * The default value of the corresponding property
   */
  boolean DEFAULT_ENABLE_CLUSTER_CONFIGURATION = true;

  @ConfigAttribute(type = Boolean.class)
  String LOAD_CLUSTER_CONFIG_FROM_DIR_NAME = LOAD_CLUSTER_CONFIGURATION_FROM_DIR;
  boolean DEFAULT_LOAD_CLUSTER_CONFIG_FROM_DIR = false;

  /**
   * Returns the value of
   * <a href="../DistributedSystem.html#cluster-configuration-dir">cluster-configuration-dir</a>
   * property
   */
  @ConfigAttributeGetter(name = LOAD_CLUSTER_CONFIGURATION_FROM_DIR)
  boolean getLoadClusterConfigFromDir();

  /**
   * Sets the value of
   * <a href="../DistributedSystem.html#cluster-configuration-dir">cluster-configuration-dir</a>
   * property
   */
  @ConfigAttributeSetter(name = LOAD_CLUSTER_CONFIGURATION_FROM_DIR)
  void setLoadClusterConfigFromDir(boolean newValue);

  @ConfigAttribute(type = String.class)
  String CLUSTER_CONFIGURATION_DIR_NAME = CLUSTER_CONFIGURATION_DIR;
  String DEFAULT_CLUSTER_CONFIGURATION_DIR = System.getProperty("user.dir");

  @ConfigAttributeGetter(name = CLUSTER_CONFIGURATION_DIR)
  String getClusterConfigDir();

  @ConfigAttributeSetter(name = CLUSTER_CONFIGURATION_DIR)
  void setClusterConfigDir(String clusterConfigDir);

  /**
   * Turns on network partition detection
   */
  @ConfigAttributeSetter(name = ENABLE_NETWORK_PARTITION_DETECTION)
  void setEnableNetworkPartitionDetection(boolean newValue);

  /**
   * Returns the value of the enable-network-partition-detection property
   */
  @ConfigAttributeGetter(name = ENABLE_NETWORK_PARTITION_DETECTION)
  boolean getEnableNetworkPartitionDetection();

  /**
   * the name of the corresponding property
   */
  @ConfigAttribute(type = Boolean.class)
  String ENABLE_NETWORK_PARTITION_DETECTION_NAME = ENABLE_NETWORK_PARTITION_DETECTION;
  boolean DEFAULT_ENABLE_NETWORK_PARTITION_DETECTION = false;

  /**
   * Get the value of the
   * <a href="../DistributedSystem.html#member-timeout">"member-timeout"</a>
   * property
   */
  @ConfigAttributeGetter(name = MEMBER_TIMEOUT)
  int getMemberTimeout();

  /**
   * Set the value of the
   * <a href="../DistributedSystem.html#member-timeout">"member-timeout"</a>
   * property
   */
  @ConfigAttributeSetter(name = MEMBER_TIMEOUT)
  void setMemberTimeout(int value);

  /**
   * The default value of the corresponding property
   */
  int DEFAULT_MEMBER_TIMEOUT = 5000;

  /**
   * The minimum member-timeout setting of 1000 milliseconds
   */
  int MIN_MEMBER_TIMEOUT = 10;

  /**
   * The maximum member-timeout setting of 600000 millieseconds
   */
  int MAX_MEMBER_TIMEOUT = 600000;
  /**
   * The name of the corresponding property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_MEMBER_TIMEOUT, max = MAX_MEMBER_TIMEOUT)
  String MEMBER_TIMEOUT_NAME = MEMBER_TIMEOUT;

  @ConfigAttribute(type = int[].class)
  String MEMBERSHIP_PORT_RANGE_NAME = MEMBERSHIP_PORT_RANGE;

  int[] DEFAULT_MEMBERSHIP_PORT_RANGE = new int[] { 1024, 65535 };

  @ConfigAttributeGetter(name = MEMBERSHIP_PORT_RANGE)
  int[] getMembershipPortRange();

  @ConfigAttributeSetter(name = MEMBERSHIP_PORT_RANGE)
  void setMembershipPortRange(int[] range);

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#conserve-sockets">"conserve-sockets"</a>
   * property
   */
  @ConfigAttributeGetter(name = CONSERVE_SOCKETS)
  boolean getConserveSockets();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#conserve-sockets">"conserve-sockets"</a>
   * property.
   */
  @ConfigAttributeSetter(name = CONSERVE_SOCKETS)
  void setConserveSockets(boolean newValue);

  /**
   * The name of the "conserveSockets" property
   */
  @ConfigAttribute(type = Boolean.class)
  String CONSERVE_SOCKETS_NAME = CONSERVE_SOCKETS;

  /**
   * The default value of the "conserveSockets" property
   */
  boolean DEFAULT_CONSERVE_SOCKETS = true;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#roles">"roles"</a>
   * property
   */
  @ConfigAttributeGetter(name = ROLES)
  String getRoles();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#roles">"roles"</a>
   * property.
   */
  @ConfigAttributeSetter(name = ROLES)
  void setRoles(String roles);

  /**
   * The name of the "roles" property
   */
  @ConfigAttribute(type = String.class)
  String ROLES_NAME = ROLES;

  /**
   * The default value of the "roles" property
   */
  String DEFAULT_ROLES = "";

  /**
   * The name of the "max wait time for reconnect" property
   */
  @ConfigAttribute(type = Integer.class)
  String MAX_WAIT_TIME_FOR_RECONNECT_NAME = MAX_WAIT_TIME_RECONNECT;

  /**
   * Default value for MAX_WAIT_TIME_FOR_RECONNECT, 60,000 milliseconds.
   */
  int DEFAULT_MAX_WAIT_TIME_FOR_RECONNECT = 60000;

  /**
   * Sets the max wait timeout, in milliseconds, for reconnect.
   */
  @ConfigAttributeSetter(name = MAX_WAIT_TIME_RECONNECT)
  void setMaxWaitTimeForReconnect(int timeOut);

  /**
   * Returns the max wait timeout, in milliseconds, for reconnect.
   */
  @ConfigAttributeGetter(name = MAX_WAIT_TIME_RECONNECT)
  int getMaxWaitTimeForReconnect();

  /**
   * The name of the "max number of tries for reconnect" property.
   */
  @ConfigAttribute(type = Integer.class)
  String MAX_NUM_RECONNECT_TRIES_NAME = MAX_NUM_RECONNECT_TRIES;

  /**
   * Default value for MAX_NUM_RECONNECT_TRIES_NAME.
   */
  int DEFAULT_MAX_NUM_RECONNECT_TRIES = 3;

  /**
   * Sets the max number of tries for reconnect.
   */
  @ConfigAttributeSetter(name = MAX_NUM_RECONNECT_TRIES)
  void setMaxNumReconnectTries(int tries);

  /**
   * Returns the value for max number of tries for reconnect.
   */
  @ConfigAttributeGetter(name = MAX_NUM_RECONNECT_TRIES)
  int getMaxNumReconnectTries();

  // ------------------- Asynchronous Messaging Properties -------------------

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#async-distribution-timeout">
   * "async-distribution-timeout"</a> property.
   */
  @ConfigAttributeGetter(name = ASYNC_DISTRIBUTION_TIMEOUT)
  int getAsyncDistributionTimeout();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#async-distribution-timeout">
   * "async-distribution-timeout"</a> property.
   */
  @ConfigAttributeSetter(name = ASYNC_DISTRIBUTION_TIMEOUT)
  void setAsyncDistributionTimeout(int newValue);

  /**
   * The default value of "asyncDistributionTimeout" is <code>0</code>.
   */
  int DEFAULT_ASYNC_DISTRIBUTION_TIMEOUT = 0;
  /**
   * The minimum value of "asyncDistributionTimeout" is <code>0</code>.
   */
  int MIN_ASYNC_DISTRIBUTION_TIMEOUT = 0;
  /**
   * The maximum value of "asyncDistributionTimeout" is <code>60000</code>.
   */
  int MAX_ASYNC_DISTRIBUTION_TIMEOUT = 60000;

  /**
   * The name of the "asyncDistributionTimeout" property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_ASYNC_DISTRIBUTION_TIMEOUT, max = MAX_ASYNC_DISTRIBUTION_TIMEOUT)
  String ASYNC_DISTRIBUTION_TIMEOUT_NAME = ASYNC_DISTRIBUTION_TIMEOUT;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#async-queue-timeout">
   * "async-queue-timeout"</a> property.
   */
  @ConfigAttributeGetter(name = ASYNC_QUEUE_TIMEOUT)
  int getAsyncQueueTimeout();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#async-queue-timeout">
   * "async-queue-timeout"</a> property.
   */
  @ConfigAttributeSetter(name = ASYNC_QUEUE_TIMEOUT)
  void setAsyncQueueTimeout(int newValue);

  /**
   * The default value of "asyncQueueTimeout" is <code>60000</code>.
   */
  int DEFAULT_ASYNC_QUEUE_TIMEOUT = 60000;
  /**
   * The minimum value of "asyncQueueTimeout" is <code>0</code>.
   */
  int MIN_ASYNC_QUEUE_TIMEOUT = 0;
  /**
   * The maximum value of "asyncQueueTimeout" is <code>86400000</code>.
   */
  int MAX_ASYNC_QUEUE_TIMEOUT = 86400000;
  /**
   * The name of the "asyncQueueTimeout" property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_ASYNC_QUEUE_TIMEOUT, max = MAX_ASYNC_QUEUE_TIMEOUT)
  String ASYNC_QUEUE_TIMEOUT_NAME = ASYNC_QUEUE_TIMEOUT;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#async-max-queue-size">
   * "async-max-queue-size"</a> property.
   */
  @ConfigAttributeGetter(name = ASYNC_MAX_QUEUE_SIZE)
  int getAsyncMaxQueueSize();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#async-max-queue-size">
   * "async-max-queue-size"</a> property.
   */
  @ConfigAttributeSetter(name = ASYNC_MAX_QUEUE_SIZE)
  void setAsyncMaxQueueSize(int newValue);

  /**
   * The default value of "asyncMaxQueueSize" is <code>8</code>.
   */
  int DEFAULT_ASYNC_MAX_QUEUE_SIZE = 8;
  /**
   * The minimum value of "asyncMaxQueueSize" is <code>0</code>.
   */
  int MIN_ASYNC_MAX_QUEUE_SIZE = 0;
  /**
   * The maximum value of "asyncMaxQueueSize" is <code>1024</code>.
   */
  int MAX_ASYNC_MAX_QUEUE_SIZE = 1024;

  /**
   * The name of the "asyncMaxQueueSize" property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_ASYNC_MAX_QUEUE_SIZE, max = MAX_ASYNC_MAX_QUEUE_SIZE)
  String ASYNC_MAX_QUEUE_SIZE_NAME = ASYNC_MAX_QUEUE_SIZE;
  /**
   * @since GemFire 5.7
   */
  @ConfigAttribute(type = String.class)
  String CLIENT_CONFLATION_PROP_NAME = CONFLATE_EVENTS;
  /**
   * @since GemFire 5.7
   */
  String CLIENT_CONFLATION_PROP_VALUE_DEFAULT = "server";
  /**
   * @since GemFire 5.7
   */
  String CLIENT_CONFLATION_PROP_VALUE_ON = "true";
  /**
   * @since GemFire 5.7
   */
  String CLIENT_CONFLATION_PROP_VALUE_OFF = "false";

  /**
   * @since Geode 1.0
   */
  @ConfigAttribute(type = Boolean.class)
  String DISTRIBUTED_TRANSACTIONS_NAME = DISTRIBUTED_TRANSACTIONS;
  boolean DEFAULT_DISTRIBUTED_TRANSACTIONS = false;

  @ConfigAttributeGetter(name = DISTRIBUTED_TRANSACTIONS)
  boolean getDistributedTransactions();

  @ConfigAttributeSetter(name = DISTRIBUTED_TRANSACTIONS)
  void setDistributedTransactions(boolean value);

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#conflate-events">"conflate-events"</a>
   * property.
   * @since GemFire 5.7
   */
  @ConfigAttributeGetter(name = CONFLATE_EVENTS)
  String getClientConflation();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#conflate-events">"conflate-events"</a>
   * property.
   * @since GemFire 5.7
   */
  @ConfigAttributeSetter(name = CONFLATE_EVENTS)
  void setClientConflation(String clientConflation);
  // -------------------------------------------------------------------------

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#durable-client-id">"durable-client-id"</a>
   * property.
   */
  @ConfigAttributeGetter(name = DURABLE_CLIENT_ID)
  String getDurableClientId();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#durable-client-id">"durable-client-id"</a>
   * property.
   */
  @ConfigAttributeSetter(name = DURABLE_CLIENT_ID)
  void setDurableClientId(String durableClientId);

  /**
   * The name of the "durableClientId" property
   */
  @ConfigAttribute(type = String.class)
  String DURABLE_CLIENT_ID_NAME = DURABLE_CLIENT_ID;

  /**
   * The default durable client id.
   * <p> Actual value of this constant is <code>""</code>.
   */
  String DEFAULT_DURABLE_CLIENT_ID = "";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#durable-client-timeout">"durable-client-timeout"</a>
   * property.
   */
  @ConfigAttributeGetter(name = DURABLE_CLIENT_TIMEOUT)
  int getDurableClientTimeout();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#durable-client-timeout">"durable-client-timeout"</a>
   * property.
   */
  @ConfigAttributeSetter(name = DURABLE_CLIENT_TIMEOUT)
  void setDurableClientTimeout(int durableClientTimeout);

  /**
   * The name of the "durableClientTimeout" property
   */
  @ConfigAttribute(type = Integer.class)
  String DURABLE_CLIENT_TIMEOUT_NAME = DURABLE_CLIENT_TIMEOUT;

  /**
   * The default durable client timeout in seconds.
   * <p> Actual value of this constant is <code>"300"</code>.
   */
  int DEFAULT_DURABLE_CLIENT_TIMEOUT = 300;

  /**
   * Returns user module name for client authentication initializer in <a
   * href="../DistributedSystem.html#security-client-auth-init">"security-client-auth-init"</a>
   */
  @ConfigAttributeGetter(name = SECURITY_CLIENT_AUTH_INIT)
  String getSecurityClientAuthInit();

  /**
   * Sets the user module name in <a
   * href="../DistributedSystem.html#security-client-auth-init">"security-client-auth-init"</a>
   * property.
   */
  @ConfigAttributeSetter(name = SECURITY_CLIENT_AUTH_INIT)
  void setSecurityClientAuthInit(String attValue);

  /**
   * The name of user defined method name for "security-client-auth-init" property
   */
  @ConfigAttribute(type = String.class)
  String SECURITY_CLIENT_AUTH_INIT_NAME = SECURITY_CLIENT_AUTH_INIT;

  /**
   * The default client authentication initializer method name.
   * <p> Actual value of this is in format <code>"jar file:module name"</code>.
   */
  String DEFAULT_SECURITY_CLIENT_AUTH_INIT = "";

  /**
   * Returns user module name authenticating client credentials in <a
   * href="../DistributedSystem.html#security-client-authenticator">"security-client-authenticator"</a>
   */
  @ConfigAttributeGetter(name = SECURITY_CLIENT_AUTHENTICATOR)
  String getSecurityClientAuthenticator();

  /**
   * Sets the user defined method name in <a
   * href="../DistributedSystem.html#security-client-authenticator">"security-client-authenticator"</a>
   * property.
   */
  @ConfigAttributeSetter(name = SECURITY_CLIENT_AUTHENTICATOR)
  void setSecurityClientAuthenticator(String attValue);

  /**
   * The name of factory method for "security-client-authenticator" property
   */
  @ConfigAttribute(type = String.class)
  String SECURITY_CLIENT_AUTHENTICATOR_NAME = SECURITY_CLIENT_AUTHENTICATOR;

  /**
   * The default client authentication method name.
   * <p> Actual value of this is fully qualified <code>"method name"</code>.
   */
  String DEFAULT_SECURITY_CLIENT_AUTHENTICATOR = "";

  /**
   * Returns name of algorithm to use for Diffie-Hellman key exchange <a
   * href="../DistributedSystem.html#security-client-dhalgo">"security-client-dhalgo"</a>
   */
  @ConfigAttributeGetter(name = SECURITY_CLIENT_DHALGO)
  String getSecurityClientDHAlgo();

  /**
   * Set the name of algorithm to use for Diffie-Hellman key exchange <a
   * href="../DistributedSystem.html#security-client-dhalgo">"security-client-dhalgo"</a>
   * property.
   */
  @ConfigAttributeSetter(name = SECURITY_CLIENT_DHALGO)
  void setSecurityClientDHAlgo(String attValue);

  /**
   * The name of the Diffie-Hellman symmetric algorithm "security-client-dhalgo"
   * property.
   */
  @ConfigAttribute(type = String.class)
  String SECURITY_CLIENT_DHALGO_NAME = SECURITY_CLIENT_DHALGO;

  /**
   * The default Diffie-Hellman symmetric algorithm name.
   * <p>
   * Actual value of this is one of the available symmetric algorithm names in
   * JDK like "DES", "DESede", "AES", "Blowfish".
   */
  String DEFAULT_SECURITY_CLIENT_DHALGO = "";

  /**
   * Returns user defined method name for peer authentication initializer in <a
   * href="../DistributedSystem.html#security-peer-auth-init">"security-peer-auth-init"</a>
   */
  @ConfigAttributeGetter(name = SECURITY_PEER_AUTH_INIT)
  String getSecurityPeerAuthInit();

  /**
   * Sets the user module name in <a
   * href="../DistributedSystem.html#security-peer-auth-init">"security-peer-auth-init"</a>
   * property.
   */
  @ConfigAttributeSetter(name = SECURITY_PEER_AUTH_INIT)
  void setSecurityPeerAuthInit(String attValue);

  /**
   * The name of user module for "security-peer-auth-init" property
   */
  @ConfigAttribute(type = String.class)
  String SECURITY_PEER_AUTH_INIT_NAME = SECURITY_PEER_AUTH_INIT;

  /**
   * The default client authenticaiton method name.
   * <p> Actual value of this is fully qualified <code>"method name"</code>.
   */
  String DEFAULT_SECURITY_PEER_AUTH_INIT = "";

  /**
   * Returns user defined method name authenticating peer's credentials in <a
   * href="../DistributedSystem.html#security-peer-authenticator">"security-peer-authenticator"</a>
   */
  @ConfigAttributeGetter(name = SECURITY_PEER_AUTHENTICATOR)
  String getSecurityPeerAuthenticator();

  /**
   * Sets the user module name in <a
   * href="../DistributedSystem.html#security-peer-authenticator">"security-peer-authenticator"</a>
   * property.
   */
  @ConfigAttributeSetter(name = SECURITY_PEER_AUTHENTICATOR)
  void setSecurityPeerAuthenticator(String attValue);

  /**
   * The name of user defined method for "security-peer-authenticator" property
   */
  @ConfigAttribute(type = String.class)
  String SECURITY_PEER_AUTHENTICATOR_NAME = SECURITY_PEER_AUTHENTICATOR;

  /**
   * The default client authenticaiton method.
   * <p> Actual value of this is fully qualified <code>"method name"</code>.
   */
  String DEFAULT_SECURITY_PEER_AUTHENTICATOR = "";

  /**
   * Returns user module name authorizing client credentials in <a
   * href="../DistributedSystem.html#security-client-accessor">"security-client-accessor"</a>
   */
  @ConfigAttributeGetter(name = SECURITY_CLIENT_ACCESSOR)
  String getSecurityClientAccessor();

  /**
   * Sets the user defined method name in <a
   * href="../DistributedSystem.html#security-client-accessor">"security-client-accessor"</a>
   * property.
   */
  @ConfigAttributeSetter(name = SECURITY_CLIENT_ACCESSOR)
  void setSecurityClientAccessor(String attValue);

  /**
   * The name of the factory method for "security-client-accessor" property
   */
  @ConfigAttribute(type = String.class)
  String SECURITY_CLIENT_ACCESSOR_NAME = SECURITY_CLIENT_ACCESSOR;

  /**
   * The default client authorization module factory method name.
   * <p> Actual value of this is fully qualified <code>"method name"</code>.
   */
  String DEFAULT_SECURITY_CLIENT_ACCESSOR = "";

  /**
   * Returns user module name authorizing client credentials in <a
   * href="../DistributedSystem.html#security-client-accessor-pp">"security-client-accessor-pp"</a>
   */
  @ConfigAttributeGetter(name = SECURITY_CLIENT_ACCESSOR_PP)
  String getSecurityClientAccessorPP();

  /**
   * Sets the user defined method name in <a
   * href="../DistributedSystem.html#security-client-accessor-pp">"security-client-accessor-pp"</a>
   * property.
   */
  @ConfigAttributeSetter(name = SECURITY_CLIENT_ACCESSOR_PP)
  void setSecurityClientAccessorPP(String attValue);

  /**
   * The name of the factory method for "security-client-accessor-pp" property
   */
  @ConfigAttribute(type = String.class)
  String SECURITY_CLIENT_ACCESSOR_PP_NAME = SECURITY_CLIENT_ACCESSOR_PP;

  /**
   * The default client post-operation authorization module factory method name.
   * <p> Actual value of this is fully qualified <code>"method name"</code>.
   */
  String DEFAULT_SECURITY_CLIENT_ACCESSOR_PP = "";

  /**
   * Get the current log-level for security logging.
   *
   * @return the current security log-level
   */
  @ConfigAttributeGetter(name = SECURITY_LOG_LEVEL)
  int getSecurityLogLevel();

  /**
   * Set the log-level for security logging.
   *
   * @param level the new security log-level
   */
  @ConfigAttributeSetter(name = SECURITY_LOG_LEVEL)
  void setSecurityLogLevel(int level);

  /**
   * The name of "security-log-level" property that sets the log-level for
   * security logger obtained using
   * {@link DistributedSystem#getSecurityLogWriter()}
   */
  // type is String because the config file "config", "debug", "fine" etc, but the setter getter accepts int
  @ConfigAttribute(type = String.class)
  String SECURITY_LOG_LEVEL_NAME = SECURITY_LOG_LEVEL;

  /**
   * Returns the value of the "security-log-file" property
   *
   * @return <code>null</code> if logging information goes to standard out
   */
  @ConfigAttributeGetter(name = SECURITY_LOG_FILE)
  File getSecurityLogFile();

  /**
   * Sets the system's security log file containing security related messages.
   * <p>
   * Non-absolute log files are relative to the system directory.
   * <p>
   * The security log file can not be changed while the system is running.
   *
   * @throws IllegalArgumentException                   if the specified value is not acceptable.
   * @throws com.gemstone.gemfire.UnmodifiableException if this attribute can not be modified.
   * @throws com.gemstone.gemfire.GemFireIOException    if the set failure is caused by an error when writing to
   *                                                    the system's configuration file.
   */
  @ConfigAttributeSetter(name = SECURITY_LOG_FILE)
  void setSecurityLogFile(File value);

  /**
   * The name of the "security-log-file" property. This property is the path of
   * the file where security related messages are logged.
   */
  @ConfigAttribute(type = File.class)
  String SECURITY_LOG_FILE_NAME = SECURITY_LOG_FILE;

  /**
   * The default security log file.
   * <p> *
   * <p>
   * Actual value of this constant is <code>""</code> which directs security
   * log messages to the same place as the system log file.
   */
  File DEFAULT_SECURITY_LOG_FILE = new File("");

  /**
   * Get timeout for peer membership check when security is enabled.
   *
   * @return Timeout in milliseconds.
   */
  @ConfigAttributeGetter(name = SECURITY_PEER_VERIFY_MEMBER_TIMEOUT)
  int getSecurityPeerMembershipTimeout();

  /**
   * Set timeout for peer membership check when security is enabled. The timeout must be less
   * than peer handshake timeout.
   *
   * @param attValue
   */
  @ConfigAttributeSetter(name = SECURITY_PEER_VERIFY_MEMBER_TIMEOUT)
  void setSecurityPeerMembershipTimeout(int attValue);

  /**
   * The default peer membership check timeout is 1 second.
   */
  int DEFAULT_SECURITY_PEER_VERIFYMEMBER_TIMEOUT = 1000;

  /**
   * Max membership timeout must be less than max peer handshake timeout. Currently this is set to
   * default handshake timeout of 60 seconds.
   */
  int MAX_SECURITY_PEER_VERIFYMEMBER_TIMEOUT = 60000;

  /**
   * The name of the peer membership check timeout property
   */
  @ConfigAttribute(type = Integer.class, min = 0, max = MAX_SECURITY_PEER_VERIFYMEMBER_TIMEOUT)
  String SECURITY_PEER_VERIFYMEMBER_TIMEOUT_NAME = SECURITY_PEER_VERIFY_MEMBER_TIMEOUT;

  /**
   * Returns all properties starting with <a
   * href="../DistributedSystem.html#security-">"security-"</a>.
   */
  Properties getSecurityProps();

  /**
   * Returns the value of security property <a
   * href="../DistributedSystem.html#security-">"security-"</a>
   * for an exact attribute name match.
   */
  String getSecurity(String attName);

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#security-">"security-*"</a>
   * property.
   */
  void setSecurity(String attName, String attValue);

  /**
   * For the "security-" prefixed properties
   */
  String SECURITY_PREFIX_NAME = SECURITY_PREFIX;

  /**
   * The prefix used for Gemfire properties set through java system properties
   */
  String GEMFIRE_PREFIX = "gemfire.";

  /**
   * For the "custom-" prefixed properties
   */
  String USERDEFINED_PREFIX_NAME = "custom-";

  /**
   * For ssl keystore and trust store properties
   */
  String SSL_SYSTEM_PROPS_NAME = "javax.net.ssl";

  String KEY_STORE_TYPE_NAME = ".keyStoreType";
  String KEY_STORE_NAME = ".keyStore";
  String KEY_STORE_PASSWORD_NAME = ".keyStorePassword";
  String TRUST_STORE_NAME = ".trustStore";
  String TRUST_STORE_PASSWORD_NAME = ".trustStorePassword";

  /**
   * Suffix for ssl keystore and trust store properties for JMX
   */
  String JMX_SSL_PROPS_SUFFIX = "-jmx";

  /**
   * For security properties starting with sysprop in gfsecurity.properties file
   */
  String SYS_PROP_NAME = "sysprop-";
  /**
   * The property decides whether to remove unresponsive client from the server.
   */
  @ConfigAttribute(type = Boolean.class)
  String REMOVE_UNRESPONSIVE_CLIENT_PROP_NAME = REMOVE_UNRESPONSIVE_CLIENT;

  /**
   * The default value of remove unresponsive client is false.
   */
  boolean DEFAULT_REMOVE_UNRESPONSIVE_CLIENT = false;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#remove-unresponsive-client">"remove-unresponsive-client"</a>
   * property.
   *
   * @since GemFire 6.0
   */
  @ConfigAttributeGetter(name = REMOVE_UNRESPONSIVE_CLIENT)
  boolean getRemoveUnresponsiveClient();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#remove-unresponsive-client">"remove-unresponsive-client"</a>
   * property.
   *
   * @since GemFire 6.0
   */
  @ConfigAttributeSetter(name = REMOVE_UNRESPONSIVE_CLIENT)
  void setRemoveUnresponsiveClient(boolean value);

  /**
   * @since GemFire 6.3
   */
  @ConfigAttribute(type = Boolean.class)
  String DELTA_PROPAGATION_PROP_NAME = DELTA_PROPAGATION;

  boolean DEFAULT_DELTA_PROPAGATION = true;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#delta-propagation">"delta-propagation"</a>
   * property.
   * @since GemFire 6.3
   */
  @ConfigAttributeGetter(name = DELTA_PROPAGATION)
  boolean getDeltaPropagation();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#delta-propagation">"delta-propagation"</a>
   * property.
   *
   * @since GemFire 6.3
   */
  @ConfigAttributeSetter(name = DELTA_PROPAGATION)
  void setDeltaPropagation(boolean value);

  int MIN_DISTRIBUTED_SYSTEM_ID = -1;
  int MAX_DISTRIBUTED_SYSTEM_ID = 255;
  /**
   * @since GemFire 6.6
   */
  @ConfigAttribute(type = Integer.class)
  String DISTRIBUTED_SYSTEM_ID_NAME = DISTRIBUTED_SYSTEM_ID;
  int DEFAULT_DISTRIBUTED_SYSTEM_ID = -1;

  /**
   * @since GemFire 6.6
   */
  @ConfigAttributeSetter(name = DISTRIBUTED_SYSTEM_ID)
  void setDistributedSystemId(int distributedSystemId);
  /**
   * @since GemFire 6.6
   */
  @ConfigAttributeGetter(name = DISTRIBUTED_SYSTEM_ID)
  int getDistributedSystemId();

  /**
   * @since GemFire 6.6
   */
  @ConfigAttribute(type = String.class)
  String REDUNDANCY_ZONE_NAME = REDUNDANCY_ZONE;
  String DEFAULT_REDUNDANCY_ZONE = "";

  /**
   * @since GemFire 6.6
   */
  @ConfigAttributeSetter(name = REDUNDANCY_ZONE)
  void setRedundancyZone(String redundancyZone);
  /**
   * @since GemFire 6.6
   */
  @ConfigAttributeGetter(name = REDUNDANCY_ZONE)
  String getRedundancyZone();

  /**
   * @since GemFire 6.6.2
   */
  void setSSLProperty(String attName, String attValue);

  /**
   * @since GemFire 6.6.2
   */
  Properties getSSLProperties();

  Properties getClusterSSLProperties();

  /**
   * @since GemFire 8.0
   */
  Properties getJmxSSLProperties();

  /**
   * @since GemFire 6.6
   */
  @ConfigAttribute(type = Boolean.class)
  String ENFORCE_UNIQUE_HOST_NAME = ENFORCE_UNIQUE_HOST;
  /**
   * Using the system property to set the default here to retain backwards compatibility
   * with customers that are already using this system property.
   */
  boolean DEFAULT_ENFORCE_UNIQUE_HOST = Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "EnforceUniqueHostStorageAllocation");

  @ConfigAttributeSetter(name = ENFORCE_UNIQUE_HOST)
  void setEnforceUniqueHost(boolean enforceUniqueHost);

  @ConfigAttributeGetter(name = ENFORCE_UNIQUE_HOST)
  boolean getEnforceUniqueHost();

  Properties getUserDefinedProps();

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#name">"groups"</a> property
   * <p> The default value is: {@link #DEFAULT_GROUPS}.
   *
   * @return the value of the property
   * @since GemFire 7.0
   */
  @ConfigAttributeGetter(name = GROUPS)
  String getGroups();

  /**
   * Sets the groups gemfire property.
   * <p> The groups can not be changed while the system is running.
   *
   * @throws IllegalArgumentException                   if the specified value is not acceptable.
   * @throws com.gemstone.gemfire.UnmodifiableException if this attribute can not be modified.
   * @throws com.gemstone.gemfire.GemFireIOException    if the set failure is caused by an error
   *                                                    when writing to the system's configuration file.
   * @since GemFire 7.0
   */
  @ConfigAttributeSetter(name = GROUPS)
  void setGroups(String value);

  /**
   * The name of the "groups" property
   *
   * @since GemFire 7.0
   */
  @ConfigAttribute(type = String.class)
  String GROUPS_NAME = GROUPS;
  /**
   * The default groups.
   * <p> Actual value of this constant is <code>""</code>.
   *
   * @since GemFire 7.0
   */
  String DEFAULT_GROUPS = "";

  /**
   * Any cleanup required before closing the distributed system
   */
  void close();

  @ConfigAttributeSetter(name = REMOTE_LOCATORS)
  void setRemoteLocators(String locators);

  @ConfigAttributeGetter(name = REMOTE_LOCATORS)
  String getRemoteLocators();

  /**
   * The name of the "remote-locators" property
   */
  @ConfigAttribute(type = String.class)
  String REMOTE_LOCATORS_NAME = REMOTE_LOCATORS;
  /**
   * The default value of the "remote-locators" property
   */
  String DEFAULT_REMOTE_LOCATORS = "";

  @ConfigAttributeGetter(name = JMX_MANAGER)
  boolean getJmxManager();

  @ConfigAttributeSetter(name = JMX_MANAGER)
  void setJmxManager(boolean value);

  @ConfigAttribute(type = Boolean.class)
  String JMX_MANAGER_NAME = JMX_MANAGER;
  boolean DEFAULT_JMX_MANAGER = false;

  @ConfigAttributeGetter(name = JMX_MANAGER_START)
  boolean getJmxManagerStart();

  @ConfigAttributeSetter(name = JMX_MANAGER_START)
  void setJmxManagerStart(boolean value);

  @ConfigAttribute(type = Boolean.class)
  String JMX_MANAGER_START_NAME = JMX_MANAGER_START;
  boolean DEFAULT_JMX_MANAGER_START = false;

  @ConfigAttributeGetter(name = JMX_MANAGER_PORT)
  int getJmxManagerPort();

  @ConfigAttributeSetter(name = JMX_MANAGER_PORT)
  void setJmxManagerPort(int value);

  @ConfigAttribute(type = Integer.class, min = 0, max = 65535)
  String JMX_MANAGER_PORT_NAME = JMX_MANAGER_PORT;

  int DEFAULT_JMX_MANAGER_PORT = 1099;

  /**
   * @deprecated as of 8.0 use {@link #getJmxManagerSSLEnabled} instead.
   */
  @ConfigAttributeGetter(name = JMX_MANAGER_SSL)
  boolean getJmxManagerSSL();

  /**
   * @deprecated as of 8.0 use {@link #setJmxManagerSSLEnabled} instead.
   */
  @ConfigAttributeSetter(name = JMX_MANAGER_SSL)
  void setJmxManagerSSL(boolean value);

  /**
   * @deprecated as of 8.0 use {@link #JMX_MANAGER_SSL_ENABLED_NAME} instead.
   */
  @ConfigAttribute(type = Boolean.class)
  String JMX_MANAGER_SSL_NAME = JMX_MANAGER_SSL;

  /**
   * @deprecated as of 8.0 use {@link #DEFAULT_JMX_MANAGER_SSL_ENABLED} instead.
   */
  boolean DEFAULT_JMX_MANAGER_SSL = false;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-enabled">"jmx-manager-ssl-enabled"</a>
   * property.
   */
  @ConfigAttributeGetter(name = JMX_MANAGER_SSL_ENABLED)
  boolean getJmxManagerSSLEnabled();

  /**
   * The default jmx-manager-ssl-enabled state.
   * <p> Actual value of this constant is <code>false</code>.
   */
  boolean DEFAULT_JMX_MANAGER_SSL_ENABLED = false;

  /**
   * The name of the "CacheJmxManagerSSLEnabled" property
   */
  @ConfigAttribute(type = Boolean.class)
  String JMX_MANAGER_SSL_ENABLED_NAME = JMX_MANAGER_SSL_ENABLED;

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-enabled">"jmx-manager-ssl-enabled"</a>
   * property.
   */
  @ConfigAttributeSetter(name = JMX_MANAGER_SSL_ENABLED)
  void setJmxManagerSSLEnabled(boolean enabled);

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#off-heap-memory-size">"off-heap-memory-size"</a>
   * property.
   * @since Geode 1.0
   */
  @ConfigAttributeGetter(name = OFF_HEAP_MEMORY_SIZE)
  String getOffHeapMemorySize();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#off-heap-memory-size">"off-heap-memory-size"</a>
   * property.
   * @since Geode 1.0
   */
  @ConfigAttributeSetter(name = OFF_HEAP_MEMORY_SIZE)
  void setOffHeapMemorySize(String value);
  /**
   * The name of the "off-heap-memory-size" property
   * @since Geode 1.0
   */
  @ConfigAttribute(type = String.class)
  String OFF_HEAP_MEMORY_SIZE_NAME = OFF_HEAP_MEMORY_SIZE;
  /**
   * The default <a
   * href="../DistributedSystem.html#off-heap-memory-size">"off-heap-memory-size"</a>
   * value of <code>""</code>.
   * @since Geode 1.0
   */
  String DEFAULT_OFF_HEAP_MEMORY_SIZE = "";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-protocols">"jmx-manager-ssl-protocols"</a>
   * property.
   */
  @ConfigAttributeGetter(name = JMX_MANAGER_SSL_PROTOCOLS)
  String getJmxManagerSSLProtocols();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-protocols">"jmx-manager-ssl-protocols"</a>
   * property.
   */
  @ConfigAttributeSetter(name = JMX_MANAGER_SSL_PROTOCOLS)
  void setJmxManagerSSLProtocols(String protocols);

  /**
   * The default jmx-manager-ssl-protocols value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  String DEFAULT_JMX_MANAGER_SSL_PROTOCOLS = "any";
  /**
   * The name of the "CacheJmxManagerSSLProtocols" property
   */
  @ConfigAttribute(type = String.class)
  String JMX_MANAGER_SSL_PROTOCOLS_NAME = JMX_MANAGER_SSL_PROTOCOLS;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-ciphers">"jmx-manager-ssl-ciphers"</a>
   * property.
   */
  @ConfigAttributeGetter(name = JMX_MANAGER_SSL_CIPHERS)
  String getJmxManagerSSLCiphers();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-ciphers">"jmx-manager-ssl-ciphers"</a>
   * property.
   */
  @ConfigAttributeSetter(name = JMX_MANAGER_SSL_CIPHERS)
  void setJmxManagerSSLCiphers(String ciphers);

  /**
   * The default jmx-manager-ssl-ciphers value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  String DEFAULT_JMX_MANAGER_SSL_CIPHERS = "any";
  /**
   * The name of the "CacheJmxManagerSSLCiphers" property
   */
  @ConfigAttribute(type = String.class)
  String JMX_MANAGER_SSL_CIPHERS_NAME = JMX_MANAGER_SSL_CIPHERS;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-require-authentication">"jmx-manager-ssl-require-authentication"</a>
   * property.
   */
  @ConfigAttributeGetter(name = JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION)
  boolean getJmxManagerSSLRequireAuthentication();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-require-authentication">"jmx-manager-ssl-require-authentication"</a>
   * property.
   */
  @ConfigAttributeSetter(name = JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION)
  void setJmxManagerSSLRequireAuthentication(boolean enabled);

  /**
   * The default jmx-manager-ssl-require-authentication value.
   * <p> Actual value of this constant is <code>true</code>.
   */
  boolean DEFAULT_JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION = true;
  /**
   * The name of the "CacheJmxManagerSSLRequireAuthentication" property
   */
  @ConfigAttribute(type = Boolean.class)
  String JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION_NAME = JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-keystore">"jmx-manager-ssl-keystore"</a>
   * property.
   */
  @ConfigAttributeGetter(name = JMX_MANAGER_SSL_KEYSTORE)
  String getJmxManagerSSLKeyStore();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-keystore">"jmx-manager-ssl-keystore"</a>
   * property.
   */
  @ConfigAttributeSetter(name = JMX_MANAGER_SSL_KEYSTORE)
  void setJmxManagerSSLKeyStore(String keyStore);

  /**
   * The default jmx-manager-ssl-keystore value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_JMX_MANAGER_SSL_KEYSTORE = "";

  /**
   * The name of the "CacheJmxManagerSSLKeyStore" property
   */
  @ConfigAttribute(type = String.class)
  String JMX_MANAGER_SSL_KEYSTORE_NAME = JMX_MANAGER_SSL_KEYSTORE;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-keystore-type">"jmx-manager-ssl-keystore-type"</a>
   * property.
   */
  @ConfigAttributeGetter(name = JMX_MANAGER_SSL_KEYSTORE_TYPE)
  String getJmxManagerSSLKeyStoreType();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-keystore-type">"jmx-manager-ssl-keystore-type"</a>
   * property.
   */
  @ConfigAttributeSetter(name = JMX_MANAGER_SSL_KEYSTORE_TYPE)
  void setJmxManagerSSLKeyStoreType(String keyStoreType);

  /**
   * The default jmx-manager-ssl-keystore-type value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_JMX_MANAGER_SSL_KEYSTORE_TYPE = "";

  /**
   * The name of the "CacheJmxManagerSSLKeyStoreType" property
   */
  @ConfigAttribute(type = String.class)
  String JMX_MANAGER_SSL_KEYSTORE_TYPE_NAME = JMX_MANAGER_SSL_KEYSTORE_TYPE;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-keystore-password">"jmx-manager-ssl-keystore-password"</a>
   * property.
   */
  @ConfigAttributeGetter(name = JMX_MANAGER_SSL_KEYSTORE_PASSWORD)
  String getJmxManagerSSLKeyStorePassword();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-keystore-password">"jmx-manager-ssl-keystore-password"</a>
   * property.
   */
  @ConfigAttributeSetter(name = JMX_MANAGER_SSL_KEYSTORE_PASSWORD)
  void setJmxManagerSSLKeyStorePassword(String keyStorePassword);

  /**
   * The default jmx-manager-ssl-keystore-password value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_JMX_MANAGER_SSL_KEYSTORE_PASSWORD = "";

  /**
   * The name of the "CacheJmxManagerSSLKeyStorePassword" property
   */
  @ConfigAttribute(type = String.class)
  String JMX_MANAGER_SSL_KEYSTORE_PASSWORD_NAME = JMX_MANAGER_SSL_KEYSTORE_PASSWORD;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-truststore">"jmx-manager-ssl-truststore"</a>
   * property.
   */
  @ConfigAttributeGetter(name = JMX_MANAGER_SSL_TRUSTSTORE)
  String getJmxManagerSSLTrustStore();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-truststore">"jmx-manager-ssl-truststore"</a>
   * property.
   */
  @ConfigAttributeSetter(name = JMX_MANAGER_SSL_TRUSTSTORE)
  void setJmxManagerSSLTrustStore(String trustStore);

  /**
   * The default jmx-manager-ssl-truststore value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_JMX_MANAGER_SSL_TRUSTSTORE = "";

  /**
   * The name of the "CacheJmxManagerSSLTrustStore" property
   */
  @ConfigAttribute(type = String.class)
  String JMX_MANAGER_SSL_TRUSTSTORE_NAME = JMX_MANAGER_SSL_TRUSTSTORE;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-truststore-password">"jmx-manager-ssl-truststore-password"</a>
   * property.
   */
  @ConfigAttributeGetter(name = JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD)
  String getJmxManagerSSLTrustStorePassword();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#jmx-manager-ssl-truststore-password">"jmx-manager-ssl-truststore-password"</a>
   * property.
   */
  @ConfigAttributeSetter(name = JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD)
  void setJmxManagerSSLTrustStorePassword(String trusStorePassword);

  /**
   * The default jmx-manager-ssl-truststore-password value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD = "";

  /**
   * The name of the "JmxManagerSSLKeyStorePassword" property
   */
  @ConfigAttribute(type = String.class)
  String JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD_NAME = JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD;

  @ConfigAttributeGetter(name = JMX_MANAGER_BIND_ADDRESS)
  String getJmxManagerBindAddress();

  @ConfigAttributeSetter(name = JMX_MANAGER_BIND_ADDRESS)
  void setJmxManagerBindAddress(String value);

  @ConfigAttribute(type = String.class)
  String JMX_MANAGER_BIND_ADDRESS_NAME = JMX_MANAGER_BIND_ADDRESS;
  String DEFAULT_JMX_MANAGER_BIND_ADDRESS = "";

  @ConfigAttributeGetter(name = JMX_MANAGER_HOSTNAME_FOR_CLIENTS)
  String getJmxManagerHostnameForClients();

  @ConfigAttributeSetter(name = JMX_MANAGER_HOSTNAME_FOR_CLIENTS)
  void setJmxManagerHostnameForClients(String value);

  @ConfigAttribute(type = String.class)
  String JMX_MANAGER_HOSTNAME_FOR_CLIENTS_NAME = JMX_MANAGER_HOSTNAME_FOR_CLIENTS;
  String DEFAULT_JMX_MANAGER_HOSTNAME_FOR_CLIENTS = "";

  @ConfigAttributeGetter(name = JMX_MANAGER_PASSWORD_FILE)
  String getJmxManagerPasswordFile();

  @ConfigAttributeSetter(name = JMX_MANAGER_PASSWORD_FILE)
  void setJmxManagerPasswordFile(String value);

  @ConfigAttribute(type = String.class)
  String JMX_MANAGER_PASSWORD_FILE_NAME = JMX_MANAGER_PASSWORD_FILE;
  String DEFAULT_JMX_MANAGER_PASSWORD_FILE = "";

  @ConfigAttributeGetter(name = JMX_MANAGER_ACCESS_FILE)
  String getJmxManagerAccessFile();

  @ConfigAttributeSetter(name = JMX_MANAGER_ACCESS_FILE)
  void setJmxManagerAccessFile(String value);

  @ConfigAttribute(type = String.class)
  String JMX_MANAGER_ACCESS_FILE_NAME = JMX_MANAGER_ACCESS_FILE;
  String DEFAULT_JMX_MANAGER_ACCESS_FILE = "";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#jmx-manager-http-port">"jmx-manager-http-port"</a> property
   *
   * @deprecated as of 8.0 use {@link #getHttpServicePort()} instead.
   */
  @ConfigAttributeGetter(name = JMX_MANAGER_HTTP_PORT)
  int getJmxManagerHttpPort();

  /**
   * Set the jmx-manager-http-port for jmx-manager.
   *
   * @param value the port number for jmx-manager HTTP service
   * @deprecated as of 8.0 use {@link #setHttpServicePort(int)} instead.
   */
  @ConfigAttributeSetter(name = JMX_MANAGER_HTTP_PORT)
  void setJmxManagerHttpPort(int value);

  /**
   * The name of the "jmx-manager-http-port" property.
   *
   * @deprecated as of 8.0 use {@link #HTTP_SERVICE_PORT} instead.
   */
  @ConfigAttribute(type = Integer.class, min = 0, max = 65535)
  String JMX_MANAGER_HTTP_PORT_NAME = JMX_MANAGER_HTTP_PORT;

  /**
   * The default value of the "jmx-manager-http-port" property.
   * Current value is a <code>7070</code>
   *
   * @deprecated as of 8.0 use {@link #DEFAULT_HTTP_SERVICE_PORT} instead.
   */
  int DEFAULT_JMX_MANAGER_HTTP_PORT = 7070;

  @ConfigAttributeGetter(name = JMX_MANAGER_UPDATE_RATE)
  int getJmxManagerUpdateRate();

  @ConfigAttributeSetter(name = JMX_MANAGER_UPDATE_RATE)
  void setJmxManagerUpdateRate(int value);

  int DEFAULT_JMX_MANAGER_UPDATE_RATE = 2000;
  int MIN_JMX_MANAGER_UPDATE_RATE = 1000;
  int MAX_JMX_MANAGER_UPDATE_RATE = 60000 * 5;
  @ConfigAttribute(type = Integer.class, min = MIN_JMX_MANAGER_UPDATE_RATE, max = MAX_JMX_MANAGER_UPDATE_RATE)
  String JMX_MANAGER_UPDATE_RATE_NAME = JMX_MANAGER_UPDATE_RATE;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#memcached-port">"memcached-port"</a> property
   *
   * @return the port on which GemFireMemcachedServer should be started
   * @since GemFire 7.0
   */
  @ConfigAttributeGetter(name = MEMCACHED_PORT)
  int getMemcachedPort();

  @ConfigAttributeSetter(name = MEMCACHED_PORT)
  void setMemcachedPort(int value);

  @ConfigAttribute(type = Integer.class, min = 0, max = 65535)
  String MEMCACHED_PORT_NAME = MEMCACHED_PORT;
  int DEFAULT_MEMCACHED_PORT = 0;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#memcached-protocol">"memcached-protocol"</a> property
   *
   * @return the protocol for GemFireMemcachedServer
   * @since GemFire 7.0
   */
  @ConfigAttributeGetter(name = MEMCACHED_PROTOCOL)
  String getMemcachedProtocol();

  @ConfigAttributeSetter(name = MEMCACHED_PROTOCOL)
  void setMemcachedProtocol(String protocol);

  @ConfigAttribute(type = String.class)
  String MEMCACHED_PROTOCOL_NAME = MEMCACHED_PROTOCOL;
  String DEFAULT_MEMCACHED_PROTOCOL = GemFireMemcachedServer.Protocol.ASCII.name();

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#memcached-bind-address">"memcached-bind-address"</a> property
   *
   * @return the bind address for GemFireMemcachedServer
   * @since GemFire 7.0
   */
  @ConfigAttributeGetter(name = MEMCACHED_BIND_ADDRESS)
  String getMemcachedBindAddress();

  @ConfigAttributeSetter(name = MEMCACHED_BIND_ADDRESS)
  void setMemcachedBindAddress(String bindAddress);

  @ConfigAttribute(type = String.class)
  String MEMCACHED_BIND_ADDRESS_NAME = MEMCACHED_BIND_ADDRESS;
  String DEFAULT_MEMCACHED_BIND_ADDRESS = "";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#redis-port">"redis-port"</a> property
   *
   * @return the port on which GemFireRedisServer should be started
   * @since GemFire 8.0
   */
  @ConfigAttributeGetter(name = REDIS_PORT)
  int getRedisPort();

  @ConfigAttributeSetter(name = REDIS_PORT)
  void setRedisPort(int value);

  @ConfigAttribute(type = Integer.class, min = 0, max = 65535)
  String REDIS_PORT_NAME = REDIS_PORT;
  int DEFAULT_REDIS_PORT = 0;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#redis-bind-address">"redis-bind-address"</a> property
   *
   * @return the bind address for GemFireRedisServer
   * @since GemFire 8.0
   */
  @ConfigAttributeGetter(name = REDIS_BIND_ADDRESS)
  String getRedisBindAddress();

  @ConfigAttributeSetter(name = REDIS_BIND_ADDRESS)
  void setRedisBindAddress(String bindAddress);

  @ConfigAttribute(type = String.class)
  String REDIS_BIND_ADDRESS_NAME = REDIS_BIND_ADDRESS;
  String DEFAULT_REDIS_BIND_ADDRESS = "";

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#redis-password">"redis-password"</a> property
   *
   * @return the authentication password for GemFireRedisServer
   * @since GemFire 8.0
   */
  @ConfigAttributeGetter(name = REDIS_PASSWORD)
  String getRedisPassword();

  @ConfigAttributeSetter(name = REDIS_PASSWORD)
  void setRedisPassword(String password);

  @ConfigAttribute(type = String.class)
  String REDIS_PASSWORD_NAME = REDIS_PASSWORD;
  String DEFAULT_REDIS_PASSWORD = "";

  //Added for the HTTP service

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-port">"http-service-port"</a> property
   *
   * @return the HTTP service port
   * @since GemFire 8.0
   */
  @ConfigAttributeGetter(name = HTTP_SERVICE_PORT)
  int getHttpServicePort();

  /**
   * Set the http-service-port for HTTP service.
   *
   * @param value the port number for HTTP service
   * @since GemFire 8.0
   */
  @ConfigAttributeSetter(name = HTTP_SERVICE_PORT)
  void setHttpServicePort(int value);

  /**
   * The name of the "http-service-port" property
   * @since GemFire 8.0
   */
  @ConfigAttribute(type = Integer.class, min = 0, max = 65535)
  String HTTP_SERVICE_PORT_NAME = HTTP_SERVICE_PORT;

  /**
   * The default value of the "http-service-port" property.
   * Current value is a <code>7070</code>
   * @since GemFire 8.0
   */
  int DEFAULT_HTTP_SERVICE_PORT = 7070;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-bind-address">"http-service-bind-address"</a> property
   *
   * @return the bind-address for HTTP service
   * @since GemFire 8.0
   */
  @ConfigAttributeGetter(name = HTTP_SERVICE_BIND_ADDRESS)
  String getHttpServiceBindAddress();

  /**
   * Set the http-service-bind-address for HTTP service.
   *
   * @param value the bind-address for HTTP service
   * @since GemFire 8.0
   */
  @ConfigAttributeSetter(name = HTTP_SERVICE_BIND_ADDRESS)
  void setHttpServiceBindAddress(String value);

  /**
   * The name of the "http-service-bind-address" property
   * @since GemFire 8.0
   */
  @ConfigAttribute(type = String.class)
  String HTTP_SERVICE_BIND_ADDRESS_NAME = HTTP_SERVICE_BIND_ADDRESS;

  /**
   * The default value of the "http-service-bind-address" property.
   * Current value is an empty string <code>""</code>
   * @since GemFire 8.0
   */
  String DEFAULT_HTTP_SERVICE_BIND_ADDRESS = "";

  //Added for HTTP Service SSL

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-enabled">"http-service-ssl-enabled"</a>
   * property.
   */
  @ConfigAttributeGetter(name = HTTP_SERVICE_SSL_ENABLED)
  boolean getHttpServiceSSLEnabled();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-enabled">"http-service-ssl-enabled"</a>
   * property.
   */
  @ConfigAttributeSetter(name = HTTP_SERVICE_SSL_ENABLED)
  void setHttpServiceSSLEnabled(boolean httpServiceSSLEnabled);

  /**
   * The default http-service-ssl-enabled state.
   * <p> Actual value of this constant is <code>false</code>.
   */
  boolean DEFAULT_HTTP_SERVICE_SSL_ENABLED = false;

  /**
   * The name of the "HttpServiceSSLEnabled" property
   */
  @ConfigAttribute(type = Boolean.class)
  String HTTP_SERVICE_SSL_ENABLED_NAME = HTTP_SERVICE_SSL_ENABLED;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-require-authentication">"http-service-ssl-require-authentication"</a>
   * property.
   */
  @ConfigAttributeGetter(name = HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION)
  boolean getHttpServiceSSLRequireAuthentication();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-require-authentication">"http-service-ssl-require-authentication"</a>
   * property.
   */
  @ConfigAttributeSetter(name = HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION)
  void setHttpServiceSSLRequireAuthentication(boolean httpServiceSSLRequireAuthentication);

  /**
   * The default http-service-ssl-require-authentication value.
   * <p> Actual value of this constant is <code>true</code>.
   */
  boolean DEFAULT_HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION = false;

  /**
   * The name of the "HttpServiceSSLRequireAuthentication" property
   */
  @ConfigAttribute(type = Boolean.class)
  String HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION_NAME = HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-protocols">"http-service-ssl-protocols"</a>
   * property.
   */
  @ConfigAttributeGetter(name = HTTP_SERVICE_SSL_PROTOCOLS)
  String getHttpServiceSSLProtocols();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-protocols">"http-service-ssl-protocols"</a>
   * property.
   */
  @ConfigAttributeSetter(name = HTTP_SERVICE_SSL_PROTOCOLS)
  void setHttpServiceSSLProtocols(String protocols);

  /**
   * The default http-service-ssl-protocols value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  String DEFAULT_HTTP_SERVICE_SSL_PROTOCOLS = "any";

  /**
   * The name of the "HttpServiceSSLProtocols" property
   */
  @ConfigAttribute(type = String.class)
  String HTTP_SERVICE_SSL_PROTOCOLS_NAME = HTTP_SERVICE_SSL_PROTOCOLS;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-ciphers">"http-service-ssl-ciphers"</a>
   * property.
   */
  @ConfigAttributeGetter(name = HTTP_SERVICE_SSL_CIPHERS)
  String getHttpServiceSSLCiphers();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-ciphers">"http-service-ssl-ciphers"</a>
   * property.
   */
  @ConfigAttributeSetter(name = HTTP_SERVICE_SSL_CIPHERS)
  void setHttpServiceSSLCiphers(String ciphers);

  /**
   * The default http-service-ssl-ciphers value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  String DEFAULT_HTTP_SERVICE_SSL_CIPHERS = "any";

  /**
   * The name of the "HttpServiceSSLCiphers" property
   */
  @ConfigAttribute(type = String.class)
  String HTTP_SERVICE_SSL_CIPHERS_NAME = HTTP_SERVICE_SSL_CIPHERS;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-keystore">"http-service-ssl-keystore"</a>
   * property.
   */
  @ConfigAttributeGetter(name = HTTP_SERVICE_SSL_KEYSTORE)
  String getHttpServiceSSLKeyStore();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-keystore">"http-service-ssl-keystore"</a>
   * property.
   */
  @ConfigAttributeSetter(name = HTTP_SERVICE_SSL_KEYSTORE)
  void setHttpServiceSSLKeyStore(String keyStore);

  /**
   * The default http-service-ssl-keystore value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_HTTP_SERVICE_SSL_KEYSTORE = "";

  /**
   * The name of the "HttpServiceSSLKeyStore" property
   */
  @ConfigAttribute(type = String.class)
  String HTTP_SERVICE_SSL_KEYSTORE_NAME = HTTP_SERVICE_SSL_KEYSTORE;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-keystore-password">"http-service-ssl-keystore-password"</a>
   * property.
   */
  @ConfigAttributeGetter(name = HTTP_SERVICE_SSL_KEYSTORE_PASSWORD)
  String getHttpServiceSSLKeyStorePassword();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-keystore-password">"http-service-ssl-keystore-password"</a>
   * property.
   */
  @ConfigAttributeSetter(name = HTTP_SERVICE_SSL_KEYSTORE_PASSWORD)
  void setHttpServiceSSLKeyStorePassword(String keyStorePassword);

  /**
   * The default http-service-ssl-keystore-password value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_HTTP_SERVICE_SSL_KEYSTORE_PASSWORD = "";

  /**
   * The name of the "HttpServiceSSLKeyStorePassword" property
   */
  @ConfigAttribute(type = String.class)
  String HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME = HTTP_SERVICE_SSL_KEYSTORE_PASSWORD;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-keystore-type">"http-service-ssl-keystore-type"</a>
   * property.
   */
  @ConfigAttributeGetter(name = HTTP_SERVICE_SSL_KEYSTORE_TYPE)
  String getHttpServiceSSLKeyStoreType();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-keystore-type">"http-service-ssl-keystore-type"</a>
   * property.
   */
  @ConfigAttributeSetter(name = HTTP_SERVICE_SSL_KEYSTORE_TYPE)
  void setHttpServiceSSLKeyStoreType(String keyStoreType);

  /**
   * The default gateway-ssl-truststore-password value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_HTTP_SERVICE_SSL_KEYSTORE_TYPE = "";

  /**
   * The name of the "HttpServiceKeyStoreType" property
   */
  @ConfigAttribute(type = String.class)
  String HTTP_SERVICE_SSL_KEYSTORE_TYPE_NAME = HTTP_SERVICE_SSL_KEYSTORE_TYPE;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-truststore">"http-service-ssl-truststore"</a>
   * property.
   */
  @ConfigAttributeGetter(name = HTTP_SERVICE_SSL_TRUSTSTORE)
  String getHttpServiceSSLTrustStore();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-truststore">"http-service-ssl-truststore"</a>
   * property.
   */
  @ConfigAttributeSetter(name = HTTP_SERVICE_SSL_TRUSTSTORE)
  void setHttpServiceSSLTrustStore(String trustStore);

  /**
   * The default http-service-ssl-truststore value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_HTTP_SERVICE_SSL_TRUSTSTORE = "";

  /**
   * The name of the "HttpServiceTrustStore" property
   */
  @ConfigAttribute(type = String.class)
  String HTTP_SERVICE_SSL_TRUSTSTORE_NAME = HTTP_SERVICE_SSL_TRUSTSTORE;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-truststore-password">"http-service-ssl-truststore-password"</a>
   * property.
   */
  @ConfigAttributeGetter(name = HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD)
  String getHttpServiceSSLTrustStorePassword();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#http-service-ssl-truststore-password">"http-service-ssl-truststore-password"</a>
   * property.
   */
  @ConfigAttributeSetter(name = HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD)
  void setHttpServiceSSLTrustStorePassword(String trustStorePassword);

  /**
   * The default http-service-ssl-truststore-password value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD = "";

  /**
   * The name of the "HttpServiceTrustStorePassword" property
   */
  @ConfigAttribute(type = String.class)
  String HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD_NAME = HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD;

  Properties getHttpServiceSSLProperties();

  //Added for API REST

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#start-dev-rest-api">"start-dev-rest-api"</a> property
   *
   * @return the value of the property
   * @since GemFire 8.0
   */
  @ConfigAttributeGetter(name = START_DEV_REST_API)
  boolean getStartDevRestApi();

  /**
   * Set the start-dev-rest-api for HTTP service.
   *
   * @param value for the property
   * @since GemFire 8.0
   */
  @ConfigAttributeSetter(name = START_DEV_REST_API)
  void setStartDevRestApi(boolean value);

  /**
   * The name of the "start-dev-rest-api" property
   * @since GemFire 8.0
   */
  @ConfigAttribute(type = Boolean.class)
  String START_DEV_REST_API_NAME = START_DEV_REST_API;

  /**
   * The default value of the "start-dev-rest-api" property.
   * Current value is <code>"false"</code>
   * @since GemFire 8.0
   */
  boolean DEFAULT_START_DEV_REST_API = false;

  /**
   * The name of the "default-auto-reconnect" property
   * @since GemFire 8.0
   */
  @ConfigAttribute(type = Boolean.class)
  String DISABLE_AUTO_RECONNECT_NAME = DISABLE_AUTO_RECONNECT;

  /**
   * The default value of the corresponding property
   */
  boolean DEFAULT_DISABLE_AUTO_RECONNECT = false;

  /**
   * Gets the value of <a href="../DistributedSystem.html#disable-auto-reconnect">"disable-auto-reconnect"</a>
   */
  @ConfigAttributeGetter(name = DISABLE_AUTO_RECONNECT)
  boolean getDisableAutoReconnect();

  /**
   * Sets the value of <a href="../DistributedSystem.html#disable-auto-reconnect">"disable-auto-reconnect"</a>
   *
   * @param value the new setting
   */
  @ConfigAttributeSetter(name = DISABLE_AUTO_RECONNECT)
  void setDisableAutoReconnect(boolean value);

  Properties getServerSSLProperties();

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-enabled">"server-ssl-enabled"</a>
   * property.
   */
  @ConfigAttributeGetter(name = SERVER_SSL_ENABLED)
  boolean getServerSSLEnabled();

  /**
   * The default server-ssl-enabled state.
   * <p> Actual value of this constant is <code>false</code>.
   */
  boolean DEFAULT_SERVER_SSL_ENABLED = false;
  /**
   * The name of the "ServerSSLEnabled" property
   */
  @ConfigAttribute(type = Boolean.class)
  String SERVER_SSL_ENABLED_NAME = SERVER_SSL_ENABLED;

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-enabled">"server-ssl-enabled"</a>
   * property.
   */
  @ConfigAttributeSetter(name = SERVER_SSL_ENABLED)
  void setServerSSLEnabled(boolean enabled);

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-protocols">"server-ssl-protocols"</a>
   * property.
   */
  @ConfigAttributeGetter(name = SERVER_SSL_PROTOCOLS)
  String getServerSSLProtocols();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-protocols">"server-ssl-protocols"</a>
   * property.
   */
  @ConfigAttributeSetter(name = SERVER_SSL_PROTOCOLS)
  void setServerSSLProtocols(String protocols);

  /**
   * The default server-ssl-protocols value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  String DEFAULT_SERVER_SSL_PROTOCOLS = "any";
  /**
   * The name of the "ServerSSLProtocols" property
   */
  @ConfigAttribute(type = String.class)
  String SERVER_SSL_PROTOCOLS_NAME = SERVER_SSL_PROTOCOLS;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-ciphers">"server-ssl-ciphers"</a>
   * property.
   */
  @ConfigAttributeGetter(name = SERVER_SSL_CIPHERS)
  String getServerSSLCiphers();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-ciphers">"server-ssl-ciphers"</a>
   * property.
   */
  @ConfigAttributeSetter(name = SERVER_SSL_CIPHERS)
  void setServerSSLCiphers(String ciphers);

  /**
   * The default server-ssl-ciphers value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  String DEFAULT_SERVER_SSL_CIPHERS = "any";
  /**
   * The name of the "ServerSSLCiphers" property
   */
  @ConfigAttribute(type = String.class)
  String SERVER_SSL_CIPHERS_NAME = SERVER_SSL_CIPHERS;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-require-authentication">"server-ssl-require-authentication"</a>
   * property.
   */
  @ConfigAttributeGetter(name = SERVER_SSL_REQUIRE_AUTHENTICATION)
  boolean getServerSSLRequireAuthentication();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-require-authentication">"server-ssl-require-authentication"</a>
   * property.
   */
  @ConfigAttributeSetter(name = SERVER_SSL_REQUIRE_AUTHENTICATION)
  void setServerSSLRequireAuthentication(boolean enabled);

  /**
   * The default server-ssl-require-authentication value.
   * <p> Actual value of this constant is <code>true</code>.
   */
  boolean DEFAULT_SERVER_SSL_REQUIRE_AUTHENTICATION = true;
  /**
   * The name of the "ServerSSLRequireAuthentication" property
   */
  @ConfigAttribute(type = Boolean.class)
  String SERVER_SSL_REQUIRE_AUTHENTICATION_NAME = SERVER_SSL_REQUIRE_AUTHENTICATION;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-keystore">"server-ssl-keystore"</a>
   * property.
   */
  @ConfigAttributeGetter(name = SERVER_SSL_KEYSTORE)
  String getServerSSLKeyStore();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-keystore">"server-ssl-keystore"</a>
   * property.
   */
  @ConfigAttributeSetter(name = SERVER_SSL_KEYSTORE)
  void setServerSSLKeyStore(String keyStore);

  /**
   * The default server-ssl-keystore value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_SERVER_SSL_KEYSTORE = "";

  /**
   * The name of the "ServerSSLKeyStore" property
   */
  @ConfigAttribute(type = String.class)
  String SERVER_SSL_KEYSTORE_NAME = SERVER_SSL_KEYSTORE;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-keystore-type">"server-ssl-keystore-type"</a>
   * property.
   */
  @ConfigAttributeGetter(name = SERVER_SSL_KEYSTORE_TYPE)
  String getServerSSLKeyStoreType();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-keystore-type">"server-ssl-keystore-type"</a>
   * property.
   */
  @ConfigAttributeSetter(name = SERVER_SSL_KEYSTORE_TYPE)
  void setServerSSLKeyStoreType(String keyStoreType);

  /**
   * The default server-ssl-keystore-type value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_SERVER_SSL_KEYSTORE_TYPE = "";

  /**
   * The name of the "ServerSSLKeyStoreType" property
   */
  @ConfigAttribute(type = String.class)
  String SERVER_SSL_KEYSTORE_TYPE_NAME = SERVER_SSL_KEYSTORE_TYPE;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-keystore-password">"server-ssl-keystore-password"</a>
   * property.
   */
  @ConfigAttributeGetter(name = SERVER_SSL_KEYSTORE_PASSWORD)
  String getServerSSLKeyStorePassword();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-keystore-password">"server-ssl-keystore-password"</a>
   * property.
   */
  @ConfigAttributeSetter(name = SERVER_SSL_KEYSTORE_PASSWORD)
  void setServerSSLKeyStorePassword(String keyStorePassword);

  /**
   * The default server-ssl-keystore-password value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_SERVER_SSL_KEYSTORE_PASSWORD = "";

  /**
   * The name of the "ServerSSLKeyStorePassword" property
   */
  @ConfigAttribute(type = String.class)
  String SERVER_SSL_KEYSTORE_PASSWORD_NAME = SERVER_SSL_KEYSTORE_PASSWORD;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-truststore">"server-ssl-truststore"</a>
   * property.
   */
  @ConfigAttributeGetter(name = SERVER_SSL_TRUSTSTORE)
  String getServerSSLTrustStore();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-truststore">"server-ssl-truststore"</a>
   * property.
   */
  @ConfigAttributeSetter(name = SERVER_SSL_TRUSTSTORE)
  void setServerSSLTrustStore(String trustStore);

  /**
   * The default server-ssl-truststore value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_SERVER_SSL_TRUSTSTORE = "";

  /**
   * The name of the "ServerSSLTrustStore" property
   */
  @ConfigAttribute(type = String.class)
  String SERVER_SSL_TRUSTSTORE_NAME = SERVER_SSL_TRUSTSTORE;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#server-ssl-truststore-password">"server-ssl-truststore-password"</a>
   * property.
   */
  @ConfigAttributeGetter(name = SERVER_SSL_TRUSTSTORE_PASSWORD)
  String getServerSSLTrustStorePassword();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#server-ssl-truststore-password">"server-ssl-truststore-password"</a>
   * property.
   */
  @ConfigAttributeSetter(name = SERVER_SSL_TRUSTSTORE_PASSWORD)
  void setServerSSLTrustStorePassword(String trusStorePassword);

  /**
   * The default server-ssl-truststore-password value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_SERVER_SSL_TRUSTSTORE_PASSWORD = "";

  /**
   * The name of the "ServerSSLKeyStorePassword" property
   */
  @ConfigAttribute(type = String.class)
  String SERVER_SSL_TRUSTSTORE_PASSWORD_NAME = SERVER_SSL_TRUSTSTORE_PASSWORD;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#cluster-ssl-enabled">"cluster-ssl-enabled"</a>
   * property.
   */
  @ConfigAttributeGetter(name = GATEWAY_SSL_ENABLED)
  boolean getGatewaySSLEnabled();

  /**
   * The default gateway-ssl-enabled state.
   * <p> Actual value of this constant is <code>false</code>.
   */
  boolean DEFAULT_GATEWAY_SSL_ENABLED = false;
  /**
   * The name of the "GatewaySSLEnabled" property
   */
  @ConfigAttribute(type = Boolean.class)
  String GATEWAY_SSL_ENABLED_NAME = GATEWAY_SSL_ENABLED;

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-enabled">"gateway-ssl-enabled"</a>
   * property.
   */
  @ConfigAttributeSetter(name = GATEWAY_SSL_ENABLED)
  void setGatewaySSLEnabled(boolean enabled);

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-protocols">"gateway-ssl-protocols"</a>
   * property.
   */
  @ConfigAttributeGetter(name = GATEWAY_SSL_PROTOCOLS)
  String getGatewaySSLProtocols();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-protocols">"gateway-ssl-protocols"</a>
   * property.
   */
  @ConfigAttributeSetter(name = GATEWAY_SSL_PROTOCOLS)
  void setGatewaySSLProtocols(String protocols);

  /**
   * The default gateway-ssl-protocols value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  String DEFAULT_GATEWAY_SSL_PROTOCOLS = "any";
  /**
   * The name of the "GatewaySSLProtocols" property
   */
  @ConfigAttribute(type = String.class)
  String GATEWAY_SSL_PROTOCOLS_NAME = GATEWAY_SSL_PROTOCOLS;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-ciphers">"gateway-ssl-ciphers"</a>
   * property.
   */
  @ConfigAttributeGetter(name = GATEWAY_SSL_CIPHERS)
  String getGatewaySSLCiphers();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-ciphers">"gateway-ssl-ciphers"</a>
   * property.
   */
  @ConfigAttributeSetter(name = GATEWAY_SSL_CIPHERS)
  void setGatewaySSLCiphers(String ciphers);

  /**
   * The default gateway-ssl-ciphers value.
   * <p> Actual value of this constant is <code>any</code>.
   */
  String DEFAULT_GATEWAY_SSL_CIPHERS = "any";
  /**
   * The name of the "GatewaySSLCiphers" property
   */
  @ConfigAttribute(type = String.class)
  String GATEWAY_SSL_CIPHERS_NAME = GATEWAY_SSL_CIPHERS;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-require-authentication">"gateway-ssl-require-authentication"</a>
   * property.
   */
  @ConfigAttributeGetter(name = GATEWAY_SSL_REQUIRE_AUTHENTICATION)
  boolean getGatewaySSLRequireAuthentication();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-require-authentication">"gateway-ssl-require-authentication"</a>
   * property.
   */
  @ConfigAttributeSetter(name = GATEWAY_SSL_REQUIRE_AUTHENTICATION)
  void setGatewaySSLRequireAuthentication(boolean enabled);

  /**
   * The default gateway-ssl-require-authentication value.
   * <p> Actual value of this constant is <code>true</code>.
   */
  boolean DEFAULT_GATEWAY_SSL_REQUIRE_AUTHENTICATION = true;
  /**
   * The name of the "GatewaySSLRequireAuthentication" property
   */
  @ConfigAttribute(type = Boolean.class)
  String GATEWAY_SSL_REQUIRE_AUTHENTICATION_NAME = GATEWAY_SSL_REQUIRE_AUTHENTICATION;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-keystore">"gateway-ssl-keystore"</a>
   * property.
   */
  @ConfigAttributeGetter(name = GATEWAY_SSL_KEYSTORE)
  String getGatewaySSLKeyStore();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-keystore">"gateway-ssl-keystore"</a>
   * property.
   */
  @ConfigAttributeSetter(name = GATEWAY_SSL_KEYSTORE)
  void setGatewaySSLKeyStore(String keyStore);

  /**
   * The default gateway-ssl-keystore value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_GATEWAY_SSL_KEYSTORE = "";

  /**
   * The name of the "GatewaySSLKeyStore" property
   */
  @ConfigAttribute(type = String.class)
  String GATEWAY_SSL_KEYSTORE_NAME = GATEWAY_SSL_KEYSTORE;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-keystore-type">"gateway-ssl-keystore-type"</a>
   * property.
   */
  @ConfigAttributeGetter(name = GATEWAY_SSL_KEYSTORE_TYPE)
  String getGatewaySSLKeyStoreType();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-keystore-type">"gateway-ssl-keystore-type"</a>
   * property.
   */
  @ConfigAttributeSetter(name = GATEWAY_SSL_KEYSTORE_TYPE)
  void setGatewaySSLKeyStoreType(String keyStoreType);

  /**
   * The default gateway-ssl-keystore-type value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_GATEWAY_SSL_KEYSTORE_TYPE = "";

  /**
   * The name of the "GatewaySSLKeyStoreType" property
   */
  @ConfigAttribute(type = String.class)
  String GATEWAY_SSL_KEYSTORE_TYPE_NAME = GATEWAY_SSL_KEYSTORE_TYPE;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-keystore-password">"gateway-ssl-keystore-password"</a>
   * property.
   */
  @ConfigAttributeGetter(name = GATEWAY_SSL_KEYSTORE_PASSWORD)
  String getGatewaySSLKeyStorePassword();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-keystore-password">"gateway-ssl-keystore-password"</a>
   * property.
   */
  @ConfigAttributeSetter(name = GATEWAY_SSL_KEYSTORE_PASSWORD)
  void setGatewaySSLKeyStorePassword(String keyStorePassword);

  /**
   * The default gateway-ssl-keystore-password value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_GATEWAY_SSL_KEYSTORE_PASSWORD = "";

  /**
   * The name of the "GatewaySSLKeyStorePassword" property
   */
  @ConfigAttribute(type = String.class)
  String GATEWAY_SSL_KEYSTORE_PASSWORD_NAME = GATEWAY_SSL_KEYSTORE_PASSWORD;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-truststore">"gateway-ssl-truststore"</a>
   * property.
   */
  @ConfigAttributeGetter(name = GATEWAY_SSL_TRUSTSTORE)
  String getGatewaySSLTrustStore();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-truststore">"gateway-ssl-truststore"</a>
   * property.
   */
  @ConfigAttributeSetter(name = GATEWAY_SSL_TRUSTSTORE)
  void setGatewaySSLTrustStore(String trustStore);

  /**
   * The default gateway-ssl-truststore value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_GATEWAY_SSL_TRUSTSTORE = "";

  /**
   * The name of the "GatewaySSLTrustStore" property
   */
  @ConfigAttribute(type = String.class)
  String GATEWAY_SSL_TRUSTSTORE_NAME = GATEWAY_SSL_TRUSTSTORE;

  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-truststore-password">"gateway-ssl-truststore-password"</a>
   * property.
   */
  @ConfigAttributeGetter(name = GATEWAY_SSL_TRUSTSTORE_PASSWORD)
  String getGatewaySSLTrustStorePassword();

  /**
   * Sets the value of the <a
   * href="../DistributedSystem.html#gateway-ssl-truststore-password">"gateway-ssl-truststore-password"</a>
   * property.
   */
  @ConfigAttributeSetter(name = GATEWAY_SSL_TRUSTSTORE_PASSWORD)
  void setGatewaySSLTrustStorePassword(String trusStorePassword);

  /**
   * The default gateway-ssl-truststore-password value.
   * <p> Actual value of this constant is "".
   */
  String DEFAULT_GATEWAY_SSL_TRUSTSTORE_PASSWORD = "";

  /**
   * The name of the "GatewaySSLKeyStorePassword" property
   */
  @ConfigAttribute(type = String.class)
  String GATEWAY_SSL_TRUSTSTORE_PASSWORD_NAME = GATEWAY_SSL_TRUSTSTORE_PASSWORD;

  Properties getGatewaySSLProperties();

  ConfigSource getConfigSource(String attName);

  /**
   * The name of the "lock-memory" property.  Used to cause pages to be locked
   * into memory, thereby preventing them from being swapped to disk.
   * @since Geode 1.0
   */
  @ConfigAttribute(type = Boolean.class)
  String LOCK_MEMORY_NAME = LOCK_MEMORY;
  boolean DEFAULT_LOCK_MEMORY = false;

  /**
   * Gets the value of <a href="../DistributedSystem.html#lock-memory">"lock-memory"</a>
   * @since Geode 1.0
   */
  @ConfigAttributeGetter(name = LOCK_MEMORY)
  boolean getLockMemory();

  /**
   * Set the value of <a href="../DistributedSystem.html#lock-memory">"lock-memory"</a>
   *
   * @param value the new setting
   * @since Geode 1.0
   */
  @ConfigAttributeSetter(name = LOCK_MEMORY)
  void setLockMemory(boolean value);

  @ConfigAttribute(type = String.class)
  String SECURITY_SHIRO_INIT_NAME = SECURITY_SHIRO_INIT;

  @ConfigAttributeSetter(name = SECURITY_SHIRO_INIT)
  void setShiroInit(String value);

  @ConfigAttributeGetter(name = SECURITY_SHIRO_INIT)
  String getShiroInit();

  //*************** Initializers to gather all the annotations in this class ************************

  Map<String, ConfigAttribute> attributes = new HashMap<>();
  Map<String, Method> setters = new HashMap<>();
  Map<String, Method> getters = new HashMap<>();
  String[] dcValidAttributeNames = init();

  static String[] init() {
    List<String> atts = new ArrayList<>();
    for (Field field : DistributionConfig.class.getDeclaredFields()) {
      if (field.isAnnotationPresent(ConfigAttribute.class)) {
        try {
          atts.add((String) field.get(null));
          attributes.put((String) field.get(null), field.getAnnotation(ConfigAttribute.class));
        } catch (IllegalAccessException e) {
          e.printStackTrace();
        }
      }
    }

    for (Method method : DistributionConfig.class.getDeclaredMethods()) {
      if (method.isAnnotationPresent(ConfigAttributeGetter.class)) {
        ConfigAttributeGetter getter = method.getAnnotation(ConfigAttributeGetter.class);
        getters.put(getter.name(), method);
      } else if (method.isAnnotationPresent(ConfigAttributeSetter.class)) {
        ConfigAttributeSetter setter = method.getAnnotation(ConfigAttributeSetter.class);
        setters.put(setter.name(), method);
      }
    }
    Collections.sort(atts);
    return atts.toArray(new String[atts.size()]);
  }

}

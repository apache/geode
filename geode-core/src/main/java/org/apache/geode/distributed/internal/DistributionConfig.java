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

package org.apache.geode.distributed.internal;

import static org.apache.geode.distributed.ConfigurationProperties.*;

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

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.Config;
import org.apache.geode.internal.ConfigSource;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogConfig;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.internal.tcp.Connection;
import org.apache.geode.memcached.GemFireMemcachedServer;

/**
 * Provides accessor (and in some cases mutator) methods for the various GemFire distribution
 * configuration properties. The interface also provides constants for the names of properties and
 * their default values.
 * <p>
 * <p>
 * <p>
 * Descriptions of these properties can be found {@link ConfigurationProperties}.
 *
 * @see org.apache.geode.internal.Config
 * @since GemFire 2.1
 */
public interface DistributionConfig extends Config, LogConfig {

  //////////////////// Instance Methods ////////////////////
  /**
   * The static String definition of the prefix used to defined ssl-* properties
   */
  String SSL_PREFIX = "ssl-";

  /**
   * The prefix used for Gemfire properties set through java system properties
   */
  String GEMFIRE_PREFIX = "gemfire.";

  /**
   * Returns the value of the {@link ConfigurationProperties#NAME} property Gets the member's name.
   * A name is optional and by default empty. If set it must be unique in the ds. When set its used
   * by tools to help identify the member.
   * <p>
   * The default value is: {@link #DEFAULT_NAME}.
   *
   * @return the system's name.
   */
  @ConfigAttributeGetter(name = NAME)
  String getName();

  /**
   * Sets the member's name.
   * <p>
   * The name can not be changed while the system is running.
   *
   * @throws IllegalArgumentException if the specified value is not acceptable.
   * @throws org.apache.geode.UnmodifiableException if this attribute can not be modified.
   * @throws org.apache.geode.GemFireIOException if the set failure is caused by an error when
   *         writing to the system's configuration file.
   */
  @ConfigAttributeSetter(name = NAME)
  void setName(String value);

  /**
   * The "name" property, representing the system's name
   */
  @ConfigAttribute(type = String.class)
  String NAME_NAME = NAME;

  /**
   * The default system name.
   * <p>
   * Actual value of this constant is <code>""</code>.
   */
  String DEFAULT_NAME = "";

  /**
   * Returns the value of the {@link ConfigurationProperties#MCAST_PORT}</a> property
   */
  @ConfigAttributeGetter(name = MCAST_PORT_NAME)
  int getMcastPort();

  /**
   * Sets the value of the {@link ConfigurationProperties#MCAST_PORT} property
   */
  @ConfigAttributeSetter(name = MCAST_PORT_NAME)
  void setMcastPort(int value);

  /**
   * The default value of the {@link ConfigurationProperties#MCAST_PORT} property
   */
  int DEFAULT_MCAST_PORT = 0;

  /**
   * The minimum {@link ConfigurationProperties#MCAST_PORT}.
   * <p>
   * Actual value of this constant is <code>0</code>.
   */
  int MIN_MCAST_PORT = 0;

  /**
   * The maximum {@link ConfigurationProperties#MCAST_PORT}.
   * <p>
   * Actual value of this constant is <code>65535</code>.
   */
  int MAX_MCAST_PORT = 65535;

  /**
   * The name of the {@link ConfigurationProperties#MCAST_PORT} property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_MCAST_PORT, max = MAX_MCAST_PORT)
  String MCAST_PORT_NAME = MCAST_PORT;

  /**
   * Returns the value of the {@link ConfigurationProperties#TCP_PORT} property
   */
  @ConfigAttributeGetter(name = TCP_PORT)
  int getTcpPort();

  /**
   * Sets the value of the {@link ConfigurationProperties#TCP_PORT} property
   */
  @ConfigAttributeSetter(name = TCP_PORT)
  void setTcpPort(int value);

  /**
   * The default value of the {@link ConfigurationProperties#TCP_PORT} property
   */
  int DEFAULT_TCP_PORT = 0;

  /**
   * The minimum {@link ConfigurationProperties#TCP_PORT}.
   * <p>
   * Actual value of this constant is <code>0</code>.
   */
  int MIN_TCP_PORT = 0;

  /**
   * The maximum {@link ConfigurationProperties#TCP_PORT}.
   * <p>
   * Actual value of this constant is <code>65535</code>.
   */
  int MAX_TCP_PORT = 65535;

  /**
   * The name of the {@link ConfigurationProperties#TCP_PORT} property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_TCP_PORT, max = MAX_TCP_PORT)
  String TCP_PORT_NAME = TCP_PORT;

  /**
   * Returns the value of the {@link ConfigurationProperties#MCAST_ADDRESS} property
   */
  @ConfigAttributeGetter(name = MCAST_ADDRESS)
  InetAddress getMcastAddress();

  /**
   * Sets the value of the {@link ConfigurationProperties#MCAST_ADDRESS} property
   */
  @ConfigAttributeSetter(name = MCAST_ADDRESS)
  void setMcastAddress(InetAddress value);

  /**
   * The name of the {@link ConfigurationProperties#MCAST_ADDRESS} property
   */
  @ConfigAttribute(type = InetAddress.class)
  String MCAST_ADDRESS_NAME = MCAST_ADDRESS;

  /**
   * The default value of the {@link ConfigurationProperties#MCAST_ADDRESS} property. Current value
   * is <code>239.192.81.1</code>
   */
  InetAddress DEFAULT_MCAST_ADDRESS = AbstractDistributionConfig._getDefaultMcastAddress();

  /**
   * Returns the value of the {@link ConfigurationProperties#MCAST_TTL} property
   */
  @ConfigAttributeGetter(name = MCAST_TTL)
  int getMcastTtl();

  /**
   * Sets the value of the {@link ConfigurationProperties#MCAST_TTL} property
   */
  @ConfigAttributeSetter(name = MCAST_TTL)
  void setMcastTtl(int value);

  /**
   * The default value of the {@link ConfigurationProperties#MCAST_TTL} property
   */
  int DEFAULT_MCAST_TTL = 32;

  /**
   * The minimum {@link ConfigurationProperties#MCAST_TTL}.
   * <p>
   * Actual value of this constant is <code>0</code>.
   */
  int MIN_MCAST_TTL = 0;

  /**
   * The maximum {@link ConfigurationProperties#MCAST_TTL}.
   * <p>
   * Actual value of this constant is <code>255</code>.
   */
  int MAX_MCAST_TTL = 255;

  /**
   * The name of the {@link ConfigurationProperties#MCAST_TTL} property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_MCAST_TTL, max = MAX_MCAST_TTL)
  String MCAST_TTL_NAME = MCAST_TTL;

  /**
   * Returns the value of the {@link ConfigurationProperties#BIND_ADDRESS} property
   */
  @ConfigAttributeGetter(name = BIND_ADDRESS)
  String getBindAddress();

  /**
   * Sets the value of the {@link ConfigurationProperties#BIND_ADDRESS} property
   */
  @ConfigAttributeSetter(name = BIND_ADDRESS)
  void setBindAddress(String value);

  /**
   * The name of the {@link ConfigurationProperties#BIND_ADDRESS} property
   */
  @ConfigAttribute(type = String.class)
  String BIND_ADDRESS_NAME = BIND_ADDRESS;

  /**
   * The default value of the {@link ConfigurationProperties#BIND_ADDRESS} property. Current value
   * is an empty string <code>""</code>
   */
  String DEFAULT_BIND_ADDRESS = "";

  /**
   * Returns the value of the {@link ConfigurationProperties#SERVER_BIND_ADDRESS} property
   */
  @ConfigAttributeGetter(name = SERVER_BIND_ADDRESS)
  String getServerBindAddress();

  /**
   * Sets the value of the {@link ConfigurationProperties#SERVER_BIND_ADDRESS} property
   */
  @ConfigAttributeSetter(name = SERVER_BIND_ADDRESS)
  void setServerBindAddress(String value);

  /**
   * The name of the {@link ConfigurationProperties#SERVER_BIND_ADDRESS} property
   */
  @ConfigAttribute(type = String.class)
  String SERVER_BIND_ADDRESS_NAME = SERVER_BIND_ADDRESS;

  /**
   * The default value of the {@link ConfigurationProperties#SERVER_BIND_ADDRESS} property. Current
   * value is an empty string <code>""</code>
   */
  String DEFAULT_SERVER_BIND_ADDRESS = "";

  /**
   * Returns the value of the {@link ConfigurationProperties#LOCATORS} property
   */
  @ConfigAttributeGetter(name = LOCATORS)
  String getLocators();

  /**
   * Sets the system's locator list. A locator list is optional and by default empty. Its used to by
   * the system to locator other system nodes and to publish itself so it can be located by others.
   *
   * @param value must be of the form <code>hostName[portNum]</code>. Multiple elements are allowed
   *        and must be separated by a comma.
   *
   * @throws IllegalArgumentException if the specified value is not acceptable.
   * @throws org.apache.geode.UnmodifiableException if this attribute can not be modified.
   * @throws org.apache.geode.GemFireIOException if the set failure is caused by an error when
   *         writing to the system's configuration file.
   */
  @ConfigAttributeSetter(name = LOCATORS)
  void setLocators(String value);

  /**
   * The name of the {@link ConfigurationProperties#LOCATORS} property
   */
  @ConfigAttribute(type = String.class)
  String LOCATORS_NAME = LOCATORS;

  /**
   * The default value of the {@link ConfigurationProperties#LOCATORS} property
   */
  String DEFAULT_LOCATORS = "";

  /**
   * Locator wait time - how long to wait for a locator to start before giving up & throwing a
   * GemFireConfigException
   */
  @ConfigAttribute(type = Integer.class)
  String LOCATOR_WAIT_TIME_NAME = LOCATOR_WAIT_TIME;

  int DEFAULT_LOCATOR_WAIT_TIME = 0;

  @ConfigAttributeGetter(name = LOCATOR_WAIT_TIME)
  int getLocatorWaitTime();

  @ConfigAttributeSetter(name = LOCATOR_WAIT_TIME)
  void setLocatorWaitTime(int seconds);

  /**
   * returns the value of the {@link ConfigurationProperties#START_LOCATOR} property
   */
  @ConfigAttributeGetter(name = START_LOCATOR)
  String getStartLocator();

  /**
   * Sets the {@link ConfigurationProperties#START_LOCATOR} property. This is a string in the form
   * bindAddress[port] and, if set, tells the distributed system to start a locator prior to
   * connecting
   *
   * @param value must be of the form <code>hostName[portNum]</code>
   */
  @ConfigAttributeSetter(name = START_LOCATOR)
  void setStartLocator(String value);

  /**
   * The name of the {@link ConfigurationProperties#START_LOCATOR} property
   */
  @ConfigAttribute(type = String.class)
  String START_LOCATOR_NAME = START_LOCATOR;
  /**
   * The default value of the {@link ConfigurationProperties#START_LOCATOR} property
   */
  String DEFAULT_START_LOCATOR = "";

  /**
   * Returns the value of the {@link ConfigurationProperties#DEPLOY_WORKING_DIR} property
   */
  @ConfigAttributeGetter(name = DEPLOY_WORKING_DIR)
  File getDeployWorkingDir();

  /**
   * Sets the system's deploy working directory.
   *
   * @throws IllegalArgumentException if the specified value is not acceptable.
   * @throws org.apache.geode.UnmodifiableException if this attribute can not be modified.
   * @throws org.apache.geode.GemFireIOException if the set failure is caused by an error when
   *         writing to the system's configuration file.
   */
  @ConfigAttributeSetter(name = DEPLOY_WORKING_DIR)
  void setDeployWorkingDir(File value);

  /**
   * The name of the {@link ConfigurationProperties#DEPLOY_WORKING_DIR} property.
   */
  @ConfigAttribute(type = File.class)
  String DEPLOY_WORKING_DIR_NAME = DEPLOY_WORKING_DIR;

  /**
   * Default will be the current working directory as determined by
   * <code>System.getProperty("user.dir")</code>.
   */
  File DEFAULT_DEPLOY_WORKING_DIR = new File(System.getProperty("user.dir"));

  /**
   * Returns the value of the {@link ConfigurationProperties#USER_COMMAND_PACKAGES} property
   */
  @ConfigAttributeGetter(name = USER_COMMAND_PACKAGES)
  String getUserCommandPackages();

  /**
   * Sets the system's user command path.
   *
   * @throws IllegalArgumentException if the specified value is not acceptable.
   * @throws org.apache.geode.UnmodifiableException if this attribute can not be modified.
   * @throws org.apache.geode.GemFireIOException if the set failure is caused by an error when
   *         writing to the system's configuration file.
   */
  @ConfigAttributeSetter(name = USER_COMMAND_PACKAGES)
  void setUserCommandPackages(String value);

  /**
   * The name of the {@link ConfigurationProperties#USER_COMMAND_PACKAGES} property.
   */
  @ConfigAttribute(type = String.class)
  String USER_COMMAND_PACKAGES_NAME = USER_COMMAND_PACKAGES;

  /**
   * The default value of the {@link ConfigurationProperties#USER_COMMAND_PACKAGES} property
   */
  String DEFAULT_USER_COMMAND_PACKAGES = "";

  /**
   * Returns the value of the {@link ConfigurationProperties#LOG_FILE} property
   *
   * @return <code>null</code> if logging information goes to standard out
   */
  @ConfigAttributeGetter(name = LOG_FILE)
  File getLogFile();

  /**
   * Sets the system's log file.
   * <p>
   * Non-absolute log files are relative to the system directory.
   * <p>
   * The system log file can not be changed while the system is running.
   *
   * @throws IllegalArgumentException if the specified value is not acceptable.
   * @throws org.apache.geode.UnmodifiableException if this attribute can not be modified.
   * @throws org.apache.geode.GemFireIOException if the set failure is caused by an error when
   *         writing to the system's configuration file.
   */

  @ConfigAttributeSetter(name = LOG_FILE)
  void setLogFile(File value);

  /**
   * The name of the {@link ConfigurationProperties#LOG_FILE} property
   */
  @ConfigAttribute(type = File.class)
  String LOG_FILE_NAME = LOG_FILE;

  /**
   * The default {@link ConfigurationProperties#LOG_FILE}.
   * <p>
   * Actual value of this constant is <code>""</code> which directs log message to standard output.
   */
  File DEFAULT_LOG_FILE = new File("");

  /**
   * Returns the value of the {@link ConfigurationProperties#LOG_LEVEL} property
   *
   * @see org.apache.geode.internal.logging.LogWriterImpl
   */
  @ConfigAttributeGetter(name = LOG_LEVEL)
  int getLogLevel();

  /**
   * Sets the value of the {@link ConfigurationProperties#LOG_LEVEL} property
   *
   * @see org.apache.geode.internal.logging.LogWriterImpl
   */
  @ConfigAttributeSetter(name = LOG_LEVEL)
  void setLogLevel(int value);

  /**
   * The default {@link ConfigurationProperties#LOG_LEVEL}.
   * <p>
   * Actual value of this constant is {@link InternalLogWriter#CONFIG_LEVEL}.
   */
  int DEFAULT_LOG_LEVEL = InternalLogWriter.CONFIG_LEVEL;
  /**
   * The minimum {@link ConfigurationProperties#LOG_LEVEL}.
   * <p>
   * Actual value of this constant is {@link InternalLogWriter#ALL_LEVEL}.
   */
  int MIN_LOG_LEVEL = InternalLogWriter.ALL_LEVEL;
  /**
   * The maximum {@link ConfigurationProperties#LOG_LEVEL}.
   * <p>
   * Actual value of this constant is {@link InternalLogWriter#NONE_LEVEL}.
   */
  int MAX_LOG_LEVEL = InternalLogWriter.NONE_LEVEL;

  /**
   * The name of the {@link ConfigurationProperties#LOG_LEVEL} property
   */
  // type is String because the config file contains "config", "debug", "fine" etc, not a code, but
  // the setter/getter accepts int
  @ConfigAttribute(type = String.class)
  String LOG_LEVEL_NAME = LOG_LEVEL;

  /**
   * Returns the value of the {@link ConfigurationProperties#STATISTIC_SAMPLING_ENABLED} property
   */
  @ConfigAttributeGetter(name = STATISTIC_SAMPLING_ENABLED)
  boolean getStatisticSamplingEnabled();

  /**
   * Sets {@link ConfigurationProperties#STATISTIC_SAMPLING_ENABLED}
   */
  @ConfigAttributeSetter(name = STATISTIC_SAMPLING_ENABLED)
  void setStatisticSamplingEnabled(boolean newValue);

  /**
   * The name of the {@link ConfigurationProperties#STATISTIC_SAMPLING_ENABLED} property
   */
  @ConfigAttribute(type = Boolean.class)
  String STATISTIC_SAMPLING_ENABLED_NAME = STATISTIC_SAMPLING_ENABLED;

  /**
   * The default value of the {@link ConfigurationProperties#STATISTIC_SAMPLING_ENABLED} property
   */
  boolean DEFAULT_STATISTIC_SAMPLING_ENABLED = true;

  /**
   * Returns the value of the {@link ConfigurationProperties#STATISTIC_SAMPLE_RATE} property
   */
  @ConfigAttributeGetter(name = STATISTIC_SAMPLE_RATE)
  int getStatisticSampleRate();

  /**
   * Sets the value of the {@link ConfigurationProperties#STATISTIC_SAMPLE_RATE} property
   */
  @ConfigAttributeSetter(name = STATISTIC_SAMPLE_RATE)
  void setStatisticSampleRate(int value);

  /**
   * The default {@link ConfigurationProperties#STATISTIC_SAMPLE_RATE}.
   * <p>
   * Actual value of this constant is <code>1000</code> milliseconds.
   */
  int DEFAULT_STATISTIC_SAMPLE_RATE = 1000;
  /**
   * The minimum {@link ConfigurationProperties#STATISTIC_SAMPLE_RATE}.
   * <p>
   * Actual value of this constant is <code>100</code> milliseconds.
   */
  int MIN_STATISTIC_SAMPLE_RATE = 100;
  /**
   * The maximum {@link ConfigurationProperties#STATISTIC_SAMPLE_RATE}.
   * <p>
   * Actual value of this constant is <code>60000</code> milliseconds.
   */
  int MAX_STATISTIC_SAMPLE_RATE = 60000;

  /**
   * The name of the {@link ConfigurationProperties#STATISTIC_SAMPLE_RATE} property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_STATISTIC_SAMPLE_RATE,
      max = MAX_STATISTIC_SAMPLE_RATE)
  String STATISTIC_SAMPLE_RATE_NAME = STATISTIC_SAMPLE_RATE;

  /**
   * Returns the value of the {@link ConfigurationProperties#STATISTIC_ARCHIVE_FILE} property.
   *
   * @return <code>null</code> if no file was specified
   */
  @ConfigAttributeGetter(name = STATISTIC_ARCHIVE_FILE)
  File getStatisticArchiveFile();

  /**
   * Sets the value of the {@link ConfigurationProperties#STATISTIC_ARCHIVE_FILE} property.
   */
  @ConfigAttributeSetter(name = STATISTIC_ARCHIVE_FILE)
  void setStatisticArchiveFile(File value);

  /**
   * The name of the {@link ConfigurationProperties#STATISTIC_ARCHIVE_FILE} property
   */
  @ConfigAttribute(type = File.class)
  String STATISTIC_ARCHIVE_FILE_NAME = STATISTIC_ARCHIVE_FILE;

  /**
   * The default {@link ConfigurationProperties#STATISTIC_ARCHIVE_FILE}.
   * <p>
   * Actual value of this constant is <code>""</code> which causes no archive file to be created.
   */
  File DEFAULT_STATISTIC_ARCHIVE_FILE = new File(""); // fix for bug 29786

  /**
   * Returns the value of the {@link ConfigurationProperties#CACHE_XML_FILE} property
   */
  @ConfigAttributeGetter(name = CACHE_XML_FILE)
  File getCacheXmlFile();

  /**
   * Sets the value of the {@link ConfigurationProperties#CACHE_XML_FILE} property
   */
  @ConfigAttributeSetter(name = CACHE_XML_FILE)
  void setCacheXmlFile(File value);

  /**
   * The name of the {@link ConfigurationProperties#CACHE_XML_FILE} property
   */
  @ConfigAttribute(type = File.class)
  String CACHE_XML_FILE_NAME = CACHE_XML_FILE;

  /**
   * The default value of the {@link ConfigurationProperties#CACHE_XML_FILE} property
   */
  File DEFAULT_CACHE_XML_FILE = new File("cache.xml");

  /**
   * Returns the value of the {@link ConfigurationProperties#ACK_WAIT_THRESHOLD} property
   */
  @ConfigAttributeGetter(name = ACK_WAIT_THRESHOLD)
  int getAckWaitThreshold();

  /**
   * Sets the value of the {@link ConfigurationProperties#ACK_WAIT_THRESHOLD} property Setting this
   * value too low will cause spurious alerts.
   */
  @ConfigAttributeSetter(name = ACK_WAIT_THRESHOLD)
  void setAckWaitThreshold(int newThreshold);

  /**
   * The default {@link ConfigurationProperties#ACK_WAIT_THRESHOLD}.
   * <p>
   * Actual value of this constant is <code>15</code> seconds.
   */
  int DEFAULT_ACK_WAIT_THRESHOLD = 15;
  /**
   * The minimum {@link ConfigurationProperties#ACK_WAIT_THRESHOLD}.
   * <p>
   * Actual value of this constant is <code>1</code> second.
   */
  int MIN_ACK_WAIT_THRESHOLD = 1;
  /**
   * The maximum {@link ConfigurationProperties#ACK_WAIT_THRESHOLD}.
   * <p>
   * Actual value of this constant is <code>MAX_INT</code> seconds.
   */
  int MAX_ACK_WAIT_THRESHOLD = Integer.MAX_VALUE;
  /**
   * The name of the {@link ConfigurationProperties#ACK_WAIT_THRESHOLD} property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_ACK_WAIT_THRESHOLD)
  String ACK_WAIT_THRESHOLD_NAME = ACK_WAIT_THRESHOLD;

  /**
   * Returns the value of the {@link ConfigurationProperties#ACK_SEVERE_ALERT_THRESHOLD} property
   */
  @ConfigAttributeGetter(name = ACK_SEVERE_ALERT_THRESHOLD)
  int getAckSevereAlertThreshold();

  /**
   * Sets the value of the {@link ConfigurationProperties#ACK_SEVERE_ALERT_THRESHOLD} property
   * Setting this value too low will cause spurious forced disconnects.
   */
  @ConfigAttributeSetter(name = ACK_SEVERE_ALERT_THRESHOLD)
  void setAckSevereAlertThreshold(int newThreshold);

  /**
   * The default {@link ConfigurationProperties#ACK_SEVERE_ALERT_THRESHOLD}.
   * <p>
   * Actual value of this constant is <code>0</code> seconds, which turns off shunning.
   */
  int DEFAULT_ACK_SEVERE_ALERT_THRESHOLD = 0;
  /**
   * The minimum {@link ConfigurationProperties#ACK_SEVERE_ALERT_THRESHOLD}.
   * <p>
   * Actual value of this constant is <code>0</code> second, which turns off shunning.
   */
  int MIN_ACK_SEVERE_ALERT_THRESHOLD = 0;
  /**
   * The maximum {@link ConfigurationProperties#ACK_SEVERE_ALERT_THRESHOLD}.
   * <p>
   * Actual value of this constant is <code>MAX_INT</code> seconds.
   */
  int MAX_ACK_SEVERE_ALERT_THRESHOLD = Integer.MAX_VALUE;
  /**
   * The name of the {@link ConfigurationProperties#ACK_SEVERE_ALERT_THRESHOLD} property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_ACK_SEVERE_ALERT_THRESHOLD)
  String ACK_SEVERE_ALERT_THRESHOLD_NAME = ACK_SEVERE_ALERT_THRESHOLD;

  /**
   * Returns the value of the {@link ConfigurationProperties#ARCHIVE_FILE_SIZE_LIMIT} property
   */
  @ConfigAttributeGetter(name = ARCHIVE_FILE_SIZE_LIMIT)
  int getArchiveFileSizeLimit();

  /**
   * Sets the value of the {@link ConfigurationProperties#ARCHIVE_FILE_SIZE_LIMIT} property
   */
  @ConfigAttributeSetter(name = ARCHIVE_FILE_SIZE_LIMIT)
  void setArchiveFileSizeLimit(int value);

  /**
   * The default {@link ConfigurationProperties#ARCHIVE_FILE_SIZE_LIMIT}.
   * <p>
   * Actual value of this constant is <code>0</code> megabytes.
   */
  int DEFAULT_ARCHIVE_FILE_SIZE_LIMIT = 0;
  /**
   * The minimum {@link ConfigurationProperties#ARCHIVE_FILE_SIZE_LIMIT}.
   * <p>
   * Actual value of this constant is <code>0</code> megabytes.
   */
  int MIN_ARCHIVE_FILE_SIZE_LIMIT = 0;
  /**
   * The maximum {@link ConfigurationProperties#ARCHIVE_FILE_SIZE_LIMIT}.
   * <p>
   * Actual value of this constant is <code>1000000</code> megabytes.
   */
  int MAX_ARCHIVE_FILE_SIZE_LIMIT = 1000000;

  /**
   * The name of the {@link ConfigurationProperties#ARCHIVE_FILE_SIZE_LIMIT} property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_ARCHIVE_FILE_SIZE_LIMIT,
      max = MAX_ARCHIVE_FILE_SIZE_LIMIT)
  String ARCHIVE_FILE_SIZE_LIMIT_NAME = ARCHIVE_FILE_SIZE_LIMIT;

  /**
   * Returns the value of the {@link ConfigurationProperties#ARCHIVE_DISK_SPACE_LIMIT} property
   */
  @ConfigAttributeGetter(name = ARCHIVE_DISK_SPACE_LIMIT)
  int getArchiveDiskSpaceLimit();

  /**
   * Sets the value of the {@link ConfigurationProperties#ARCHIVE_DISK_SPACE_LIMIT} property
   */
  @ConfigAttributeSetter(name = ARCHIVE_DISK_SPACE_LIMIT)
  void setArchiveDiskSpaceLimit(int value);

  /**
   * The default {@link ConfigurationProperties#ARCHIVE_DISK_SPACE_LIMIT}.
   * <p>
   * Actual value of this constant is <code>0</code> megabytes.
   */
  int DEFAULT_ARCHIVE_DISK_SPACE_LIMIT = 0;
  /**
   * The minimum {@link ConfigurationProperties#ARCHIVE_DISK_SPACE_LIMIT}.
   * <p>
   * Actual value of this constant is <code>0</code> megabytes.
   */
  int MIN_ARCHIVE_DISK_SPACE_LIMIT = 0;
  /**
   * The maximum {@link ConfigurationProperties#ARCHIVE_DISK_SPACE_LIMIT}.
   * <p>
   * Actual value of this constant is <code>1000000</code> megabytes.
   */
  int MAX_ARCHIVE_DISK_SPACE_LIMIT = 1000000;

  /**
   * The name of the {@link ConfigurationProperties#ARCHIVE_DISK_SPACE_LIMIT} property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_ARCHIVE_DISK_SPACE_LIMIT,
      max = MAX_ARCHIVE_DISK_SPACE_LIMIT)
  String ARCHIVE_DISK_SPACE_LIMIT_NAME = ARCHIVE_DISK_SPACE_LIMIT;

  /**
   * Returns the value of the {@link ConfigurationProperties#LOG_FILE_SIZE_LIMIT} property
   */
  @ConfigAttributeGetter(name = LOG_FILE_SIZE_LIMIT)
  int getLogFileSizeLimit();

  /**
   * Sets the value of the {@link ConfigurationProperties#LOG_FILE_SIZE_LIMIT} property
   */
  @ConfigAttributeSetter(name = LOG_FILE_SIZE_LIMIT)
  void setLogFileSizeLimit(int value);

  /**
   * The default {@link ConfigurationProperties#LOG_FILE_SIZE_LIMIT}.
   * <p>
   * Actual value of this constant is <code>0</code> megabytes.
   */
  int DEFAULT_LOG_FILE_SIZE_LIMIT = 0;
  /**
   * The minimum {@link ConfigurationProperties#LOG_FILE_SIZE_LIMIT}.
   * <p>
   * Actual value of this constant is <code>0</code> megabytes.
   */
  int MIN_LOG_FILE_SIZE_LIMIT = 0;
  /**
   * The maximum {@link ConfigurationProperties#LOG_FILE_SIZE_LIMIT}.
   * <p>
   * Actual value of this constant is <code>1000000</code> megabytes.
   */
  int MAX_LOG_FILE_SIZE_LIMIT = 1000000;

  /**
   * The name of the {@link ConfigurationProperties#LOG_FILE_SIZE_LIMIT} property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_LOG_FILE_SIZE_LIMIT,
      max = MAX_LOG_FILE_SIZE_LIMIT)
  String LOG_FILE_SIZE_LIMIT_NAME = LOG_FILE_SIZE_LIMIT;

  /**
   * Returns the value of the {@link ConfigurationProperties#LOG_DISK_SPACE_LIMIT} property
   */
  @ConfigAttributeGetter(name = LOG_DISK_SPACE_LIMIT)
  int getLogDiskSpaceLimit();

  /**
   * Sets the value of the {@link ConfigurationProperties#LOG_DISK_SPACE_LIMIT} property
   */
  @ConfigAttributeSetter(name = LOG_DISK_SPACE_LIMIT)
  void setLogDiskSpaceLimit(int value);

  /**
   * The default {@link ConfigurationProperties#LOG_DISK_SPACE_LIMIT}.
   * <p>
   * Actual value of this constant is <code>0</code> megabytes.
   */
  int DEFAULT_LOG_DISK_SPACE_LIMIT = 0;
  /**
   * The minimum {@link ConfigurationProperties#LOG_DISK_SPACE_LIMIT}.
   * <p>
   * Actual value of this constant is <code>0</code> megabytes.
   */
  int MIN_LOG_DISK_SPACE_LIMIT = 0;
  /**
   * The maximum {@link ConfigurationProperties#LOG_DISK_SPACE_LIMIT}.
   * <p>
   * Actual value of this constant is <code>1000000</code> megabytes.
   */
  int MAX_LOG_DISK_SPACE_LIMIT = 1000000;

  /**
   * The name of the {@link ConfigurationProperties#LOG_DISK_SPACE_LIMIT} property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_LOG_DISK_SPACE_LIMIT,
      max = MAX_LOG_DISK_SPACE_LIMIT)
  String LOG_DISK_SPACE_LIMIT_NAME = LOG_DISK_SPACE_LIMIT;

  /**
   * Returns the value of the {@link ConfigurationProperties#CLUSTER_SSL_ENABLED} property.
   */
  @Deprecated
  @ConfigAttributeGetter(name = CLUSTER_SSL_ENABLED)
  boolean getClusterSSLEnabled();

  /**
   * Sets the value of the {@link ConfigurationProperties#CLUSTER_SSL_ENABLED} property.
   */
  @Deprecated
  @ConfigAttributeSetter(name = CLUSTER_SSL_ENABLED)
  void setClusterSSLEnabled(boolean enabled);

  /**
   * The default {@link ConfigurationProperties#CLUSTER_SSL_ENABLED} state.
   * <p>
   * Actual value of this constant is <code>false</code>.
   */
  @Deprecated
  boolean DEFAULT_SSL_ENABLED = false;

  /**
   * The name of the {@link ConfigurationProperties#CLUSTER_SSL_ENABLED} property
   */
  @Deprecated
  @ConfigAttribute(type = Boolean.class)
  String CLUSTER_SSL_ENABLED_NAME = CLUSTER_SSL_ENABLED;

  /**
   * Returns the value of the {@link ConfigurationProperties#CLUSTER_SSL_PROTOCOLS} property.
   */
  @Deprecated
  @ConfigAttributeGetter(name = CLUSTER_SSL_PROTOCOLS)
  String getClusterSSLProtocols();

  /**
   * Sets the value of the {@link ConfigurationProperties#CLUSTER_SSL_PROTOCOLS} property.
   */
  @Deprecated
  @ConfigAttributeSetter(name = CLUSTER_SSL_PROTOCOLS)
  void setClusterSSLProtocols(String protocols);

  /**
   * The default {@link ConfigurationProperties#CLUSTER_SSL_PROTOCOLS} value.
   * <p>
   * Actual value of this constant is <code>any</code>.
   */
  String DEFAULT_SSL_PROTOCOLS = "any";

  /**
   * The name of the {@link ConfigurationProperties#CLUSTER_SSL_PROTOCOLS} property
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String CLUSTER_SSL_PROTOCOLS_NAME = CLUSTER_SSL_PROTOCOLS;

  /**
   * Returns the value of the {@link ConfigurationProperties#CLUSTER_SSL_CIPHERS} property.
   */
  @Deprecated
  @ConfigAttributeGetter(name = CLUSTER_SSL_CIPHERS)
  String getClusterSSLCiphers();

  /**
   * Sets the value of the {@link ConfigurationProperties#CLUSTER_SSL_CIPHERS} property.
   */
  @Deprecated
  @ConfigAttributeSetter(name = CLUSTER_SSL_CIPHERS)
  void setClusterSSLCiphers(String ciphers);

  /**
   * The default {@link ConfigurationProperties#CLUSTER_SSL_CIPHERS} value.
   * <p>
   * Actual value of this constant is <code>any</code>.
   */
  String DEFAULT_SSL_CIPHERS = "any";

  /**
   * The name of the {@link ConfigurationProperties#CLUSTER_SSL_CIPHERS} property
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String CLUSTER_SSL_CIPHERS_NAME = CLUSTER_SSL_CIPHERS;

  /**
   * Returns the value of the {@link ConfigurationProperties#CLUSTER_SSL_REQUIRE_AUTHENTICATION}
   * property.
   */
  @Deprecated
  @ConfigAttributeGetter(name = CLUSTER_SSL_REQUIRE_AUTHENTICATION)
  boolean getClusterSSLRequireAuthentication();

  /**
   * Sets the value of the {@link ConfigurationProperties#CLUSTER_SSL_REQUIRE_AUTHENTICATION}
   * property.
   */
  @Deprecated
  @ConfigAttributeSetter(name = CLUSTER_SSL_REQUIRE_AUTHENTICATION)
  void setClusterSSLRequireAuthentication(boolean enabled);

  /**
   * The default {@link ConfigurationProperties#CLUSTER_SSL_REQUIRE_AUTHENTICATION} value.
   * <p>
   * Actual value of this constant is <code>true</code>.
   */
  boolean DEFAULT_SSL_REQUIRE_AUTHENTICATION = true;

  /**
   * The name of the {@link ConfigurationProperties#CLUSTER_SSL_REQUIRE_AUTHENTICATION} property
   */
  @Deprecated
  @ConfigAttribute(type = Boolean.class)
  String CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME = CLUSTER_SSL_REQUIRE_AUTHENTICATION;

  /**
   * Returns the value of the {@link ConfigurationProperties#CLUSTER_SSL_KEYSTORE} property.
   */
  @Deprecated
  @ConfigAttributeGetter(name = CLUSTER_SSL_KEYSTORE)
  String getClusterSSLKeyStore();

  /**
   * Sets the value of the {@link ConfigurationProperties#CLUSTER_SSL_KEYSTORE} property.
   */
  @Deprecated
  @ConfigAttributeSetter(name = CLUSTER_SSL_KEYSTORE)
  void setClusterSSLKeyStore(String keyStore);

  /**
   * The default {@link ConfigurationProperties#CLUSTER_SSL_KEYSTORE} value.
   * <p>
   * Actual value of this constant is "".
   */
  String DEFAULT_SSL_KEYSTORE = "";

  /**
   * The name of the {@link ConfigurationProperties#CLUSTER_SSL_KEYSTORE} property
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String CLUSTER_SSL_KEYSTORE_NAME = CLUSTER_SSL_KEYSTORE;

  /**
   * Returns the value of the {@link ConfigurationProperties#CLUSTER_SSL_KEYSTORE_TYPE} property.
   */
  @Deprecated
  @ConfigAttributeGetter(name = CLUSTER_SSL_KEYSTORE_TYPE)
  String getClusterSSLKeyStoreType();

  /**
   * Sets the value of the {@link ConfigurationProperties#CLUSTER_SSL_KEYSTORE_TYPE} property.
   */
  @Deprecated
  @ConfigAttributeSetter(name = CLUSTER_SSL_KEYSTORE_TYPE)
  void setClusterSSLKeyStoreType(String keyStoreType);

  /**
   * The default {@link ConfigurationProperties#CLUSTER_SSL_KEYSTORE_TYPE} value.
   * <p>
   * Actual value of this constant is "".
   */
  String DEFAULT_CLUSTER_SSL_KEYSTORE_TYPE = "";

  /**
   * The name of the {@link ConfigurationProperties#CLUSTER_SSL_KEYSTORE_TYPE} property
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String CLUSTER_SSL_KEYSTORE_TYPE_NAME = CLUSTER_SSL_KEYSTORE_TYPE;

  /**
   * Returns the value of the {@link ConfigurationProperties#CLUSTER_SSL_KEYSTORE_PASSWORD}
   * property.
   */
  @Deprecated
  @ConfigAttributeGetter(name = CLUSTER_SSL_KEYSTORE_PASSWORD)
  String getClusterSSLKeyStorePassword();

  /**
   * Sets the value of the {@link ConfigurationProperties#CLUSTER_SSL_KEYSTORE_PASSWORD} property.
   */
  @Deprecated
  @ConfigAttributeSetter(name = CLUSTER_SSL_KEYSTORE_PASSWORD)
  void setClusterSSLKeyStorePassword(String keyStorePassword);

  /**
   * The default {@link ConfigurationProperties#CLUSTER_SSL_KEYSTORE_PASSWORD} value.
   * <p>
   * Actual value of this constant is "".
   */
  String DEFAULT_SSL_KEYSTORE_PASSWORD = "";

  /**
   * The name of the {@link ConfigurationProperties#CLUSTER_SSL_KEYSTORE_PASSWORD} property
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String CLUSTER_SSL_KEYSTORE_PASSWORD_NAME = CLUSTER_SSL_KEYSTORE_PASSWORD;

  /**
   * Returns the value of the {@link ConfigurationProperties#CLUSTER_SSL_TRUSTSTORE} property.
   */
  @Deprecated
  @ConfigAttributeGetter(name = CLUSTER_SSL_TRUSTSTORE)
  String getClusterSSLTrustStore();

  /**
   * Sets the value of the {@link ConfigurationProperties#CLUSTER_SSL_TRUSTSTORE} property.
   */
  @Deprecated
  @ConfigAttributeSetter(name = CLUSTER_SSL_TRUSTSTORE)
  void setClusterSSLTrustStore(String trustStore);

  /**
   * The default {@link ConfigurationProperties#CLUSTER_SSL_TRUSTSTORE} value.
   * <p>
   * Actual value of this constant is "".
   */
  String DEFAULT_SSL_TRUSTSTORE = "";

  /**
   * The name of the {@link ConfigurationProperties#CLUSTER_SSL_TRUSTSTORE} property
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String CLUSTER_SSL_TRUSTSTORE_NAME = CLUSTER_SSL_TRUSTSTORE;

  /**
   * Returns the value of the {@link ConfigurationProperties#CLUSTER_SSL_TRUSTSTORE_PASSWORD}
   * property.
   */
  @Deprecated
  @ConfigAttributeGetter(name = CLUSTER_SSL_TRUSTSTORE_PASSWORD)
  String getClusterSSLTrustStorePassword();

  /**
   * Sets the value of the {@link ConfigurationProperties#CLUSTER_SSL_TRUSTSTORE_PASSWORD} property.
   */
  @Deprecated
  @ConfigAttributeSetter(name = CLUSTER_SSL_TRUSTSTORE_PASSWORD)
  void setClusterSSLTrustStorePassword(String trusStorePassword);

  /**
   * The default {@link ConfigurationProperties#CLUSTER_SSL_TRUSTSTORE_PASSWORD} value.
   * <p>
   * Actual value of this constant is "".
   */
  String DEFAULT_SSL_TRUSTSTORE_PASSWORD = "";

  /**
   * The name of the {@link ConfigurationProperties#CLUSTER_SSL_TRUSTSTORE_PASSWORD} property
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String CLUSTER_SSL_TRUSTSTORE_PASSWORD_NAME = CLUSTER_SSL_TRUSTSTORE_PASSWORD;

  /**
   * The name of an internal property that specifies a {@link org.apache.geode.i18n.LogWriterI18n}
   * instance to log to. Set this property with put(), not with setProperty()
   *
   * @since GemFire 4.0
   */
  String LOG_WRITER_NAME = "log-writer";

  /**
   * The name of an internal property that specifies a a DistributionConfigImpl that the locator is
   * passing in to a ds connect. Set this property with put(), not with setProperty()
   *
   * @since GemFire 7.0
   */
  String DS_CONFIG_NAME = "ds-config";

  /**
   * The name of an internal property that specifies whether the distributed system is reconnecting
   * after a forced- disconnect.
   *
   * @since GemFire 8.1
   */
  String DS_RECONNECTING_NAME = "ds-reconnecting";

  /**
   * The name of an internal property that specifies the quorum checker for the system that was
   * forcibly disconnected. This should be used if the DS_RECONNECTING_NAME property is used.
   */
  String DS_QUORUM_CHECKER_NAME = "ds-quorum-checker";

  /**
   * The name of an internal property that specifies a {@link org.apache.geode.LogWriter} instance
   * to log security messages to. Set this property with put(), not with setProperty()
   *
   * @since GemFire 5.5
   */
  String SECURITY_LOG_WRITER_NAME = "security-log-writer";

  /**
   * The name of an internal property that specifies a FileOutputStream associated with the internal
   * property LOG_WRITER_NAME. If this property is set, the FileOutputStream will be closed when the
   * distributed system disconnects. Set this property with put(), not with setProperty()
   *
   * @since GemFire 5.0
   */
  String LOG_OUTPUTSTREAM_NAME = "log-output-stream";

  /**
   * The name of an internal property that specifies a FileOutputStream associated with the internal
   * property SECURITY_LOG_WRITER_NAME. If this property is set, the FileOutputStream will be closed
   * when the distributed system disconnects. Set this property with put(), not with setProperty()
   *
   * @since GemFire 5.5
   */
  String SECURITY_LOG_OUTPUTSTREAM_NAME = "security-log-output-stream";

  /**
   * Returns the value of the {@link ConfigurationProperties#SOCKET_LEASE_TIME} property
   */
  @ConfigAttributeGetter(name = SOCKET_LEASE_TIME)
  int getSocketLeaseTime();

  /**
   * Sets the value of the {@link ConfigurationProperties#SOCKET_LEASE_TIME} property
   */
  @ConfigAttributeSetter(name = SOCKET_LEASE_TIME)
  void setSocketLeaseTime(int value);

  /**
   * The default value of the {@link ConfigurationProperties#SOCKET_LEASE_TIME} property
   */
  int DEFAULT_SOCKET_LEASE_TIME = 60000;
  /**
   * The minimum {@link ConfigurationProperties#SOCKET_LEASE_TIME}.
   * <p>
   * Actual value of this constant is <code>0</code>.
   */
  int MIN_SOCKET_LEASE_TIME = 0;
  /**
   * The maximum {@link ConfigurationProperties#SOCKET_LEASE_TIME}.
   * <p>
   * Actual value of this constant is <code>600000</code>.
   */
  int MAX_SOCKET_LEASE_TIME = 600000;

  /**
   * The name of the {@link ConfigurationProperties#SOCKET_LEASE_TIME} property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_SOCKET_LEASE_TIME, max = MAX_SOCKET_LEASE_TIME)
  String SOCKET_LEASE_TIME_NAME = SOCKET_LEASE_TIME;

  /**
   * Returns the value of the {@link ConfigurationProperties#SOCKET_BUFFER_SIZE} property
   */
  @ConfigAttributeGetter(name = SOCKET_BUFFER_SIZE)
  int getSocketBufferSize();

  /**
   * Sets the value of the {@link ConfigurationProperties#SOCKET_BUFFER_SIZE} property
   */
  @ConfigAttributeSetter(name = SOCKET_BUFFER_SIZE)
  void setSocketBufferSize(int value);

  /**
   * The default value of the {@link ConfigurationProperties#SOCKET_BUFFER_SIZE} property
   */
  int DEFAULT_SOCKET_BUFFER_SIZE = 32768;
  /**
   * The minimum {@link ConfigurationProperties#SOCKET_BUFFER_SIZE}.
   * <p>
   * Actual value of this constant is <code>1024</code>.
   */
  int MIN_SOCKET_BUFFER_SIZE = 1024;
  /**
   * The maximum {@link ConfigurationProperties#SOCKET_BUFFER_SIZE}.
   * <p>
   * Actual value of this constant is <code>20000000</code>.
   */
  int MAX_SOCKET_BUFFER_SIZE = Connection.MAX_MSG_SIZE;

  boolean VALIDATE = Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "validateMessageSize");
  int VALIDATE_CEILING = Integer
      .getInteger(DistributionConfig.GEMFIRE_PREFIX + "validateMessageSizeCeiling", 8 * 1024 * 1024)
      .intValue();

  /**
   * The name of the {@link ConfigurationProperties#SOCKET_BUFFER_SIZE} property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_SOCKET_BUFFER_SIZE, max = MAX_SOCKET_BUFFER_SIZE)
  String SOCKET_BUFFER_SIZE_NAME = SOCKET_BUFFER_SIZE;

  /**
   * Get the value of the {@link ConfigurationProperties#MCAST_SEND_BUFFER_SIZE} property
   */
  @ConfigAttributeGetter(name = MCAST_SEND_BUFFER_SIZE)
  int getMcastSendBufferSize();

  /**
   * Set the value of the {@link ConfigurationProperties#MCAST_SEND_BUFFER_SIZE} property
   */
  @ConfigAttributeSetter(name = MCAST_SEND_BUFFER_SIZE)
  void setMcastSendBufferSize(int value);

  /**
   * The default value for {@link ConfigurationProperties#MCAST_SEND_BUFFER_SIZE} property
   */
  int DEFAULT_MCAST_SEND_BUFFER_SIZE = 65535;

  /**
   * The minimum size of the {@link ConfigurationProperties#MCAST_SEND_BUFFER_SIZE}, in bytes.
   * <p>
   * Actual value of this constant is <code>2048</code>.
   */
  int MIN_MCAST_SEND_BUFFER_SIZE = 2048;

  /**
   * The name of the {@link ConfigurationProperties#MCAST_SEND_BUFFER_SIZE} property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_MCAST_SEND_BUFFER_SIZE)
  String MCAST_SEND_BUFFER_SIZE_NAME = MCAST_SEND_BUFFER_SIZE;

  /**
   * Get the value of the {@link ConfigurationProperties#MCAST_RECV_BUFFER_SIZE} property
   */
  @ConfigAttributeGetter(name = MCAST_RECV_BUFFER_SIZE)
  int getMcastRecvBufferSize();

  /**
   * Set the value of the {@link ConfigurationProperties#MCAST_RECV_BUFFER_SIZE} property
   */
  @ConfigAttributeSetter(name = MCAST_RECV_BUFFER_SIZE)
  void setMcastRecvBufferSize(int value);

  /**
   * The default value of the {@link ConfigurationProperties#MCAST_RECV_BUFFER_SIZE} property
   */
  int DEFAULT_MCAST_RECV_BUFFER_SIZE = 1048576;

  /**
   * The minimum size of the {@link ConfigurationProperties#MCAST_RECV_BUFFER_SIZE}, in bytes.
   * <p>
   * Actual value of this constant is <code>2048</code>.
   */
  int MIN_MCAST_RECV_BUFFER_SIZE = 2048;

  /**
   * The name of the {@link ConfigurationProperties#MCAST_RECV_BUFFER_SIZE} property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_MCAST_RECV_BUFFER_SIZE)
  String MCAST_RECV_BUFFER_SIZE_NAME = MCAST_RECV_BUFFER_SIZE;

  /**
   * Get the value of the {@link ConfigurationProperties#MCAST_FLOW_CONTROL} property.
   */
  @ConfigAttributeGetter(name = MCAST_FLOW_CONTROL)
  FlowControlParams getMcastFlowControl();

  /**
   * Set the value of the {@link ConfigurationProperties#MCAST_FLOW_CONTROL} property
   */
  @ConfigAttributeSetter(name = MCAST_FLOW_CONTROL)
  void setMcastFlowControl(FlowControlParams values);

  /**
   * The name of the {@link ConfigurationProperties#MCAST_FLOW_CONTROL} property
   */
  @ConfigAttribute(type = FlowControlParams.class)
  String MCAST_FLOW_CONTROL_NAME = MCAST_FLOW_CONTROL;

  /**
   * The default value of the {@link ConfigurationProperties#MCAST_FLOW_CONTROL} property
   */
  FlowControlParams DEFAULT_MCAST_FLOW_CONTROL = new FlowControlParams(1048576, (float) 0.25, 5000);

  /**
   * The minimum byteAllowance for the{@link ConfigurationProperties#MCAST_FLOW_CONTROL} setting of
   * <code>100000</code>.
   */
  int MIN_FC_BYTE_ALLOWANCE = 10000;

  /**
   * The minimum rechargeThreshold for the {@link ConfigurationProperties#MCAST_FLOW_CONTROL}
   * setting of <code>0.1</code>
   */
  float MIN_FC_RECHARGE_THRESHOLD = (float) 0.1;

  /**
   * The maximum rechargeThreshold for the {@link ConfigurationProperties#MCAST_FLOW_CONTROL}
   * setting of <code>0.5</code>
   */
  float MAX_FC_RECHARGE_THRESHOLD = (float) 0.5;

  /**
   * The minimum rechargeBlockMs for the {@link ConfigurationProperties#MCAST_FLOW_CONTROL} setting
   * of <code>500</code>
   */
  int MIN_FC_RECHARGE_BLOCK_MS = 500;

  /**
   * The maximum rechargeBlockMs for the {@link ConfigurationProperties#MCAST_FLOW_CONTROL} setting
   * of <code>60000</code>
   */
  int MAX_FC_RECHARGE_BLOCK_MS = 60000;

  /**
   * Get the value of the {@link ConfigurationProperties#UDP_FRAGMENT_SIZE} property.
   */
  @ConfigAttributeGetter(name = UDP_FRAGMENT_SIZE)
  int getUdpFragmentSize();

  /**
   * Set the value of the {@link ConfigurationProperties#UDP_FRAGMENT_SIZE} property
   */
  @ConfigAttributeSetter(name = UDP_FRAGMENT_SIZE)
  void setUdpFragmentSize(int value);

  /**
   * The default value of the {@link ConfigurationProperties#UDP_FRAGMENT_SIZE} property
   */
  int DEFAULT_UDP_FRAGMENT_SIZE = 60000;

  /**
   * The minimum allowed {@link ConfigurationProperties#UDP_FRAGMENT_SIZE} setting of 1000
   */
  int MIN_UDP_FRAGMENT_SIZE = 1000;

  /**
   * The maximum allowed {@link ConfigurationProperties#UDP_FRAGMENT_SIZE} setting of 60000
   */
  int MAX_UDP_FRAGMENT_SIZE = 60000;

  /**
   * The name of the {@link ConfigurationProperties#UDP_FRAGMENT_SIZE} property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_UDP_FRAGMENT_SIZE, max = MAX_UDP_FRAGMENT_SIZE)
  String UDP_FRAGMENT_SIZE_NAME = UDP_FRAGMENT_SIZE;

  /**
   * Get the value of the {@link ConfigurationProperties#UDP_SEND_BUFFER_SIZE} property
   */
  @ConfigAttributeGetter(name = UDP_SEND_BUFFER_SIZE)
  int getUdpSendBufferSize();

  /**
   * Set the value of the {@link ConfigurationProperties#UDP_SEND_BUFFER_SIZE} property
   */
  @ConfigAttributeSetter(name = UDP_SEND_BUFFER_SIZE)
  void setUdpSendBufferSize(int value);

  /**
   * The default value of the {@link ConfigurationProperties#UDP_SEND_BUFFER_SIZE} property
   */
  int DEFAULT_UDP_SEND_BUFFER_SIZE = 65535;

  /**
   * The minimum size of the {@link ConfigurationProperties#UDP_SEND_BUFFER_SIZE}, in bytes.
   * <p>
   * Actual value of this constant is <code>2048</code>.
   */
  int MIN_UDP_SEND_BUFFER_SIZE = 2048;

  /**
   * The name of the {@link ConfigurationProperties#UDP_SEND_BUFFER_SIZE} property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_UDP_SEND_BUFFER_SIZE)
  String UDP_SEND_BUFFER_SIZE_NAME = UDP_SEND_BUFFER_SIZE;

  /**
   * Get the value of the {@link ConfigurationProperties#UDP_RECV_BUFFER_SIZE} property
   */
  @ConfigAttributeGetter(name = UDP_RECV_BUFFER_SIZE)
  int getUdpRecvBufferSize();

  /**
   * Set the value of the {@link ConfigurationProperties#UDP_RECV_BUFFER_SIZE} property
   */
  @ConfigAttributeSetter(name = UDP_RECV_BUFFER_SIZE)
  void setUdpRecvBufferSize(int value);

  /**
   * The default value of the {@link ConfigurationProperties#UDP_RECV_BUFFER_SIZE} property
   */
  int DEFAULT_UDP_RECV_BUFFER_SIZE = 1048576;

  /**
   * The default size of the {@link ConfigurationProperties#UDP_RECV_BUFFER_SIZE} if tcp/ip sockets
   * are enabled and multicast is disabled
   */
  int DEFAULT_UDP_RECV_BUFFER_SIZE_REDUCED = 65535;

  /**
   * The minimum size of the {@link ConfigurationProperties#UDP_RECV_BUFFER_SIZE}, in bytes.
   * <p>
   * Actual value of this constant is <code>2048</code>.
   */
  int MIN_UDP_RECV_BUFFER_SIZE = 2048;

  /**
   * The name of the {@link ConfigurationProperties#UDP_RECV_BUFFER_SIZE} property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_UDP_RECV_BUFFER_SIZE)
  String UDP_RECV_BUFFER_SIZE_NAME = UDP_RECV_BUFFER_SIZE;

  /**
   * Returns the value of the {@link ConfigurationProperties#DISABLE_TCP} property
   */
  @ConfigAttributeGetter(name = DISABLE_TCP)
  boolean getDisableTcp();

  /**
   * Sets the value of the {@link ConfigurationProperties#DISABLE_TCP} property.
   */
  @ConfigAttributeSetter(name = DISABLE_TCP)
  void setDisableTcp(boolean newValue);

  /**
   * The name of the {@link ConfigurationProperties#DISABLE_TCP} property
   */
  @ConfigAttribute(type = Boolean.class)
  String DISABLE_TCP_NAME = DISABLE_TCP;

  /**
   * The default value of the {@link ConfigurationProperties#DISABLE_TCP} property
   */
  boolean DEFAULT_DISABLE_TCP = false;

  /**
   * Turns on timing statistics for the distributed system
   */
  @ConfigAttributeSetter(name = ENABLE_TIME_STATISTICS)
  void setEnableTimeStatistics(boolean newValue);

  /**
   * Returns the value of {@link ConfigurationProperties#ENABLE_TIME_STATISTICS} property
   */
  @ConfigAttributeGetter(name = ENABLE_TIME_STATISTICS)
  boolean getEnableTimeStatistics();

  /**
   * the name of the {@link ConfigurationProperties#ENABLE_TIME_STATISTICS} property
   */
  @ConfigAttribute(type = Boolean.class)
  String ENABLE_TIME_STATISTICS_NAME = ENABLE_TIME_STATISTICS;

  /**
   * The default value of the {@link ConfigurationProperties#ENABLE_TIME_STATISTICS} property
   */
  boolean DEFAULT_ENABLE_TIME_STATISTICS = false;

  /**
   * Sets the value for {@link ConfigurationProperties#USE_CLUSTER_CONFIGURATION}
   */
  @ConfigAttributeSetter(name = USE_CLUSTER_CONFIGURATION)
  void setUseSharedConfiguration(boolean newValue);

  /**
   * Returns the value of {@link ConfigurationProperties#USE_CLUSTER_CONFIGURATION} property
   */
  @ConfigAttributeGetter(name = USE_CLUSTER_CONFIGURATION)
  boolean getUseSharedConfiguration();

  /**
   * the name of the {@link ConfigurationProperties#USE_CLUSTER_CONFIGURATION} property
   */
  @ConfigAttribute(type = Boolean.class)
  String USE_CLUSTER_CONFIGURATION_NAME = USE_CLUSTER_CONFIGURATION;

  /**
   * The default value of the {@link ConfigurationProperties#USE_CLUSTER_CONFIGURATION} property
   */
  boolean DEFAULT_USE_CLUSTER_CONFIGURATION = true;

  /**
   * Sets the value for {@link ConfigurationProperties#ENABLE_CLUSTER_CONFIGURATION}
   */
  @ConfigAttributeSetter(name = ENABLE_CLUSTER_CONFIGURATION)
  void setEnableClusterConfiguration(boolean newValue);

  /**
   * Returns the value of {@link ConfigurationProperties#ENABLE_CLUSTER_CONFIGURATION} property
   */
  @ConfigAttributeGetter(name = ENABLE_CLUSTER_CONFIGURATION)
  boolean getEnableClusterConfiguration();

  /**
   * the name of the {@link ConfigurationProperties#ENABLE_CLUSTER_CONFIGURATION} property
   */
  @ConfigAttribute(type = Boolean.class)
  String ENABLE_CLUSTER_CONFIGURATION_NAME = ENABLE_CLUSTER_CONFIGURATION;

  /**
   * The default value of the {@link ConfigurationProperties#ENABLE_CLUSTER_CONFIGURATION} property
   */
  boolean DEFAULT_ENABLE_CLUSTER_CONFIGURATION = true;

  @ConfigAttribute(type = Boolean.class)
  String LOAD_CLUSTER_CONFIG_FROM_DIR_NAME = LOAD_CLUSTER_CONFIGURATION_FROM_DIR;
  boolean DEFAULT_LOAD_CLUSTER_CONFIG_FROM_DIR = false;

  /**
   * Returns the value of {@link ConfigurationProperties#LOAD_CLUSTER_CONFIGURATION_FROM_DIR}
   * property
   */
  @ConfigAttributeGetter(name = LOAD_CLUSTER_CONFIGURATION_FROM_DIR)
  boolean getLoadClusterConfigFromDir();

  /**
   * Sets the value of {@link ConfigurationProperties#LOAD_CLUSTER_CONFIGURATION_FROM_DIR} property
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
   * Returns the value of the {@link ConfigurationProperties#ENABLE_NETWORK_PARTITION_DETECTION}
   * property
   */
  @ConfigAttributeGetter(name = ENABLE_NETWORK_PARTITION_DETECTION)
  boolean getEnableNetworkPartitionDetection();

  /**
   * the name of the {@link ConfigurationProperties#ENABLE_NETWORK_PARTITION_DETECTION} property
   */
  @ConfigAttribute(type = Boolean.class)
  String ENABLE_NETWORK_PARTITION_DETECTION_NAME = ENABLE_NETWORK_PARTITION_DETECTION;
  boolean DEFAULT_ENABLE_NETWORK_PARTITION_DETECTION = true;

  /**
   * Get the value of the {@link ConfigurationProperties#MEMBER_TIMEOUT} property
   */
  @ConfigAttributeGetter(name = MEMBER_TIMEOUT)
  int getMemberTimeout();

  /**
   * Set the value of the {@link ConfigurationProperties#MEMBER_TIMEOUT} property
   */
  @ConfigAttributeSetter(name = MEMBER_TIMEOUT)
  void setMemberTimeout(int value);

  /**
   * The default value of the {@link ConfigurationProperties#MEMBER_TIMEOUT} property
   */
  int DEFAULT_MEMBER_TIMEOUT = 5000;

  /**
   * The minimum {@link ConfigurationProperties#MEMBER_TIMEOUT} setting of 1000 milliseconds
   */
  int MIN_MEMBER_TIMEOUT = 10;

  /**
   * The maximum {@link ConfigurationProperties#MEMBER_TIMEOUT} setting of 600000 millieseconds
   */
  int MAX_MEMBER_TIMEOUT = 600000;
  /**
   * The name of the {@link ConfigurationProperties#MEMBER_TIMEOUT} property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_MEMBER_TIMEOUT, max = MAX_MEMBER_TIMEOUT)
  String MEMBER_TIMEOUT_NAME = MEMBER_TIMEOUT;

  @ConfigAttribute(type = int[].class)
  String MEMBERSHIP_PORT_RANGE_NAME = MEMBERSHIP_PORT_RANGE;

  /**
   * set this boolean to restrict membership/communications to use ports in the ephemeral range
   */
  String RESTRICT_MEMBERSHIP_PORT_RANGE = GEMFIRE_PREFIX + "use-ephemeral-ports";

  int[] DEFAULT_MEMBERSHIP_PORT_RANGE = Boolean.getBoolean(RESTRICT_MEMBERSHIP_PORT_RANGE)
      ? new int[] {32769, 61000} : new int[] {1024, 65535};

  @ConfigAttributeGetter(name = MEMBERSHIP_PORT_RANGE)
  int[] getMembershipPortRange();

  @ConfigAttributeSetter(name = MEMBERSHIP_PORT_RANGE)
  void setMembershipPortRange(int[] range);

  /**
   * Returns the value of the {@link ConfigurationProperties#CONSERVE_SOCKETS} property
   */
  @ConfigAttributeGetter(name = CONSERVE_SOCKETS)
  boolean getConserveSockets();

  /**
   * Sets the value of the {@link ConfigurationProperties#CONSERVE_SOCKETS} property.
   */
  @ConfigAttributeSetter(name = CONSERVE_SOCKETS)
  void setConserveSockets(boolean newValue);

  /**
   * The name of the {@link ConfigurationProperties#CONSERVE_SOCKETS} property
   */
  @ConfigAttribute(type = Boolean.class)
  String CONSERVE_SOCKETS_NAME = CONSERVE_SOCKETS;

  /**
   * The default value of the {@link ConfigurationProperties#CONSERVE_SOCKETS} property
   */
  boolean DEFAULT_CONSERVE_SOCKETS = true;

  /**
   * Returns the value of the {@link ConfigurationProperties#ROLES} property
   */
  @ConfigAttributeGetter(name = ROLES)
  String getRoles();

  /**
   * Sets the value of the {@link ConfigurationProperties#ROLES} property.
   */
  @ConfigAttributeSetter(name = ROLES)
  void setRoles(String roles);

  /**
   * The name of the {@link ConfigurationProperties#ROLES} property
   */
  @ConfigAttribute(type = String.class)
  String ROLES_NAME = ROLES;

  /**
   * The default value of the {@link ConfigurationProperties#ROLES} property
   */
  String DEFAULT_ROLES = "";

  /**
   * The name of the {@link ConfigurationProperties#MAX_WAIT_TIME_RECONNECT} property
   */
  @ConfigAttribute(type = Integer.class)
  String MAX_WAIT_TIME_FOR_RECONNECT_NAME = MAX_WAIT_TIME_RECONNECT;

  /**
   * Default value for {@link ConfigurationProperties#MAX_WAIT_TIME_RECONNECT}, 60,000 milliseconds.
   */
  int DEFAULT_MAX_WAIT_TIME_FOR_RECONNECT = 60000;

  /**
   * Sets the {@link ConfigurationProperties#MAX_WAIT_TIME_RECONNECT}, in milliseconds, for
   * reconnect.
   */
  @ConfigAttributeSetter(name = MAX_WAIT_TIME_RECONNECT)
  void setMaxWaitTimeForReconnect(int timeOut);

  /**
   * Returns the {@link ConfigurationProperties#MAX_WAIT_TIME_RECONNECT}, in milliseconds, for
   * reconnect.
   */
  @ConfigAttributeGetter(name = MAX_WAIT_TIME_RECONNECT)
  int getMaxWaitTimeForReconnect();

  /**
   * The name of the {@link ConfigurationProperties#MAX_NUM_RECONNECT_TRIES} property.
   */
  @ConfigAttribute(type = Integer.class)
  String MAX_NUM_RECONNECT_TRIES_NAME = MAX_NUM_RECONNECT_TRIES;

  /**
   * Default value for {@link ConfigurationProperties#MAX_NUM_RECONNECT_TRIES}.
   */
  int DEFAULT_MAX_NUM_RECONNECT_TRIES = 3;

  /**
   * Sets the {@link ConfigurationProperties#MAX_NUM_RECONNECT_TRIES}.
   */
  @ConfigAttributeSetter(name = MAX_NUM_RECONNECT_TRIES)
  void setMaxNumReconnectTries(int tries);

  /**
   * Returns the value for {@link ConfigurationProperties#MAX_NUM_RECONNECT_TRIES}.
   */
  @ConfigAttributeGetter(name = MAX_NUM_RECONNECT_TRIES)
  int getMaxNumReconnectTries();

  // ------------------- Asynchronous Messaging Properties -------------------

  /**
   * Returns the value of the {@link ConfigurationProperties#ASYNC_DISTRIBUTION_TIMEOUT} property.
   */
  @ConfigAttributeGetter(name = ASYNC_DISTRIBUTION_TIMEOUT)
  int getAsyncDistributionTimeout();

  /**
   * Sets the value of the {@link ConfigurationProperties#ASYNC_DISTRIBUTION_TIMEOUT} property.
   */
  @ConfigAttributeSetter(name = ASYNC_DISTRIBUTION_TIMEOUT)
  void setAsyncDistributionTimeout(int newValue);

  /**
   * The default value of {@link ConfigurationProperties#ASYNC_DISTRIBUTION_TIMEOUT} is
   * <code>0</code>.
   */
  int DEFAULT_ASYNC_DISTRIBUTION_TIMEOUT = 0;
  /**
   * The minimum value of {@link ConfigurationProperties#ASYNC_DISTRIBUTION_TIMEOUT} is
   * <code>0</code>.
   */
  int MIN_ASYNC_DISTRIBUTION_TIMEOUT = 0;
  /**
   * The maximum value of {@link ConfigurationProperties#ASYNC_DISTRIBUTION_TIMEOUT} is
   * <code>60000</code>.
   */
  int MAX_ASYNC_DISTRIBUTION_TIMEOUT = 60000;

  /**
   * The name of the {@link ConfigurationProperties#ASYNC_DISTRIBUTION_TIMEOUT} property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_ASYNC_DISTRIBUTION_TIMEOUT,
      max = MAX_ASYNC_DISTRIBUTION_TIMEOUT)
  String ASYNC_DISTRIBUTION_TIMEOUT_NAME = ASYNC_DISTRIBUTION_TIMEOUT;

  /**
   * Returns the value of the {@link ConfigurationProperties#ASYNC_QUEUE_TIMEOUT} property.
   */
  @ConfigAttributeGetter(name = ASYNC_QUEUE_TIMEOUT)
  int getAsyncQueueTimeout();

  /**
   * Sets the value of the {@link ConfigurationProperties#ASYNC_QUEUE_TIMEOUT} property.
   */
  @ConfigAttributeSetter(name = ASYNC_QUEUE_TIMEOUT)
  void setAsyncQueueTimeout(int newValue);

  /**
   * The default value of {@link ConfigurationProperties#ASYNC_QUEUE_TIMEOUT} is <code>60000</code>.
   */
  int DEFAULT_ASYNC_QUEUE_TIMEOUT = 60000;
  /**
   * The minimum value of {@link ConfigurationProperties#ASYNC_QUEUE_TIMEOUT} is <code>0</code>.
   */
  int MIN_ASYNC_QUEUE_TIMEOUT = 0;
  /**
   * The maximum value of {@link ConfigurationProperties#ASYNC_QUEUE_TIMEOUT} is
   * <code>86400000</code>.
   */
  int MAX_ASYNC_QUEUE_TIMEOUT = 86400000;
  /**
   * The name of the {@link ConfigurationProperties#ASYNC_QUEUE_TIMEOUT} property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_ASYNC_QUEUE_TIMEOUT,
      max = MAX_ASYNC_QUEUE_TIMEOUT)
  String ASYNC_QUEUE_TIMEOUT_NAME = ASYNC_QUEUE_TIMEOUT;

  /**
   * Returns the value of the {@link ConfigurationProperties#ASYNC_MAX_QUEUE_SIZE} property.
   */
  @ConfigAttributeGetter(name = ASYNC_MAX_QUEUE_SIZE)
  int getAsyncMaxQueueSize();

  /**
   * Sets the value of the {@link ConfigurationProperties#ASYNC_MAX_QUEUE_SIZE} property.
   */
  @ConfigAttributeSetter(name = ASYNC_MAX_QUEUE_SIZE)
  void setAsyncMaxQueueSize(int newValue);

  /**
   * The default value of {@link ConfigurationProperties#ASYNC_MAX_QUEUE_SIZE} is <code>8</code>.
   */
  int DEFAULT_ASYNC_MAX_QUEUE_SIZE = 8;
  /**
   * The minimum value of {@link ConfigurationProperties#ASYNC_MAX_QUEUE_SIZE} is <code>0</code>.
   */
  int MIN_ASYNC_MAX_QUEUE_SIZE = 0;
  /**
   * The maximum value of {@link ConfigurationProperties#ASYNC_MAX_QUEUE_SIZE} is <code>1024</code>.
   */
  int MAX_ASYNC_MAX_QUEUE_SIZE = 1024;

  /**
   * The name of the {@link ConfigurationProperties#ASYNC_MAX_QUEUE_SIZE} property
   */
  @ConfigAttribute(type = Integer.class, min = MIN_ASYNC_MAX_QUEUE_SIZE,
      max = MAX_ASYNC_MAX_QUEUE_SIZE)
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
   * Returns the value of the {@link ConfigurationProperties#CONFLATE_EVENTS} property.
   *
   * @since GemFire 5.7
   */
  @ConfigAttributeGetter(name = CONFLATE_EVENTS)
  String getClientConflation();

  /**
   * Sets the value of the {@link ConfigurationProperties#CONFLATE_EVENTS} property.
   *
   * @since GemFire 5.7
   */
  @ConfigAttributeSetter(name = CONFLATE_EVENTS)
  void setClientConflation(String clientConflation);
  // -------------------------------------------------------------------------

  /**
   * Returns the value of the {@link ConfigurationProperties#DURABLE_CLIENT_ID} property.
   */
  @ConfigAttributeGetter(name = DURABLE_CLIENT_ID)
  String getDurableClientId();

  /**
   * Sets the value of the {@link ConfigurationProperties#DURABLE_CLIENT_ID} property.
   */
  @ConfigAttributeSetter(name = DURABLE_CLIENT_ID)
  void setDurableClientId(String durableClientId);

  /**
   * The name of the {@link ConfigurationProperties#DURABLE_CLIENT_ID} property
   */
  @ConfigAttribute(type = String.class)
  String DURABLE_CLIENT_ID_NAME = DURABLE_CLIENT_ID;

  /**
   * The default {@link ConfigurationProperties#DURABLE_CLIENT_ID}.
   * <p>
   * Actual value of this constant is <code>""</code>.
   */
  String DEFAULT_DURABLE_CLIENT_ID = "";

  /**
   * Returns the value of the {@link ConfigurationProperties#DURABLE_CLIENT_TIMEOUT} property.
   */
  @ConfigAttributeGetter(name = DURABLE_CLIENT_TIMEOUT)
  int getDurableClientTimeout();

  /**
   * Sets the value of the {@link ConfigurationProperties#DURABLE_CLIENT_TIMEOUT} property.
   */
  @ConfigAttributeSetter(name = DURABLE_CLIENT_TIMEOUT)
  void setDurableClientTimeout(int durableClientTimeout);

  /**
   * The name of the {@link ConfigurationProperties#DURABLE_CLIENT_TIMEOUT} property
   */
  @ConfigAttribute(type = Integer.class)
  String DURABLE_CLIENT_TIMEOUT_NAME = DURABLE_CLIENT_TIMEOUT;

  /**
   * The default {@link ConfigurationProperties#DURABLE_CLIENT_TIMEOUT} in seconds.
   * <p>
   * Actual value of this constant is <code>"300"</code>.
   */
  int DEFAULT_DURABLE_CLIENT_TIMEOUT = 300;

  /**
   * Returns user module name for client authentication initializer in
   * {@link ConfigurationProperties#SECURITY_CLIENT_AUTH_INIT}
   */
  @ConfigAttributeGetter(name = SECURITY_CLIENT_AUTH_INIT)
  String getSecurityClientAuthInit();

  /**
   * Sets the user module name in {@link ConfigurationProperties#SECURITY_CLIENT_AUTH_INIT}
   * property.
   */
  @ConfigAttributeSetter(name = SECURITY_CLIENT_AUTH_INIT)
  void setSecurityClientAuthInit(String attValue);

  /**
   * The name of user defined method name for
   * {@link ConfigurationProperties#SECURITY_CLIENT_AUTH_INIT} property
   */
  @ConfigAttribute(type = String.class)
  String SECURITY_CLIENT_AUTH_INIT_NAME = SECURITY_CLIENT_AUTH_INIT;

  /**
   * The default {@link ConfigurationProperties#SECURITY_CLIENT_AUTH_INIT} method name.
   * <p>
   * Actual value of this is in format <code>"jar file:module name"</code>.
   */
  String DEFAULT_SECURITY_CLIENT_AUTH_INIT = "";

  /**
   * Returns user module name authenticating client credentials in
   * {@link ConfigurationProperties#SECURITY_CLIENT_AUTHENTICATOR}
   */
  @ConfigAttributeGetter(name = SECURITY_CLIENT_AUTHENTICATOR)
  String getSecurityClientAuthenticator();

  /**
   * Sets the user defined method name in
   * {@link ConfigurationProperties#SECURITY_CLIENT_AUTHENTICATOR} property.
   */
  @ConfigAttributeSetter(name = SECURITY_CLIENT_AUTHENTICATOR)
  void setSecurityClientAuthenticator(String attValue);

  /**
   * The name of factory method for {@link ConfigurationProperties#SECURITY_CLIENT_AUTHENTICATOR}
   * property
   */
  @ConfigAttribute(type = String.class)
  String SECURITY_CLIENT_AUTHENTICATOR_NAME = SECURITY_CLIENT_AUTHENTICATOR;

  /**
   * The default {@link ConfigurationProperties#SECURITY_CLIENT_AUTHENTICATOR} method name.
   * <p>
   * Actual value of this is fully qualified <code>"method name"</code>.
   */
  String DEFAULT_SECURITY_CLIENT_AUTHENTICATOR = "";

  /**
   * Returns user defined class name authenticating client credentials in
   * {@link ConfigurationProperties#SECURITY_MANAGER}
   */
  @ConfigAttributeGetter(name = SECURITY_MANAGER)
  String getSecurityManager();

  /**
   * Sets the user defined class name in {@link ConfigurationProperties#SECURITY_MANAGER} property.
   */
  @ConfigAttributeSetter(name = SECURITY_MANAGER)
  void setSecurityManager(String attValue);

  /**
   * The name of class for {@link ConfigurationProperties#SECURITY_MANAGER} property
   */
  @ConfigAttribute(type = String.class)
  String SECURITY_MANAGER_NAME = SECURITY_MANAGER;

  /**
   * The default {@link ConfigurationProperties#SECURITY_MANAGER} class name.
   * <p>
   * Actual value of this is fully qualified <code>"class name"</code>.
   */
  String DEFAULT_SECURITY_MANAGER = "";

  /**
   * Returns user defined post processor name in
   * {@link ConfigurationProperties#SECURITY_POST_PROCESSOR}
   */
  @ConfigAttributeGetter(name = SECURITY_POST_PROCESSOR)
  String getPostProcessor();

  /**
   * Sets the user defined class name in {@link ConfigurationProperties#SECURITY_POST_PROCESSOR}
   * property.
   */
  @ConfigAttributeSetter(name = SECURITY_POST_PROCESSOR)
  void setPostProcessor(String attValue);

  /**
   * The name of class for {@link ConfigurationProperties#SECURITY_POST_PROCESSOR} property
   */
  @ConfigAttribute(type = String.class)
  String SECURITY_POST_PROCESSOR_NAME = SECURITY_POST_PROCESSOR;

  /**
   * The default {@link ConfigurationProperties#SECURITY_POST_PROCESSOR} class name.
   * <p>
   * Actual value of this is fully qualified <code>"class name"</code>.
   */
  String DEFAULT_SECURITY_POST_PROCESSOR = "";

  /**
   * Returns name of algorithm to use for Diffie-Hellman key exchange
   * {@link ConfigurationProperties#SECURITY_CLIENT_DHALGO}
   */
  @ConfigAttributeGetter(name = SECURITY_CLIENT_DHALGO)
  String getSecurityClientDHAlgo();

  /**
   * Set the name of algorithm to use for Diffie-Hellman key exchange
   * {@link ConfigurationProperties#SECURITY_CLIENT_DHALGO} property.
   */
  @ConfigAttributeSetter(name = SECURITY_CLIENT_DHALGO)
  void setSecurityClientDHAlgo(String attValue);

  /**
   * Returns name of algorithm to use for Diffie-Hellman key exchange
   * <a href="../DistributedSystem.html#security-udp-dhalgo">"security-udp-dhalgo"</a>
   */
  @ConfigAttributeGetter(name = SECURITY_UDP_DHALGO)
  String getSecurityUDPDHAlgo();

  /**
   * Set the name of algorithm to use for Diffie-Hellman key exchange
   * <a href="../DistributedSystem.html#security-udp-dhalgo">"security-udp-dhalgo"</a> property.
   */
  @ConfigAttributeSetter(name = SECURITY_UDP_DHALGO)
  void setSecurityUDPDHAlgo(String attValue);

  /**
   * The name of the Diffie-Hellman symmetric algorithm
   * {@link ConfigurationProperties#SECURITY_CLIENT_DHALGO} property.
   */
  @ConfigAttribute(type = String.class)
  String SECURITY_CLIENT_DHALGO_NAME = SECURITY_CLIENT_DHALGO;

  /**
   * The name of the Diffie-Hellman symmetric algorithm "security-udp-dhalgo" property.
   *
   */
  @ConfigAttribute(type = String.class)
  String SECURITY_UDP_DHALGO_NAME = SECURITY_UDP_DHALGO;

  /**
   * The default Diffie-Hellman symmetric algorithm name.
   * <p>
   * Actual value of this is one of the available symmetric algorithm names in JDK like "AES:128" or
   * "Blowfish".
   */
  String DEFAULT_SECURITY_CLIENT_DHALGO = "";

  /**
   * The default Diffie-Hellman symmetric algorithm name.
   * <p>
   * Actual value of this is one of the available symmetric algorithm names in JDK like "AES:128" or
   * "Blowfish".
   */
  String DEFAULT_SECURITY_UDP_DHALGO = "";

  /**
   * Returns user defined method name for peer authentication initializer in
   * {@link ConfigurationProperties#SECURITY_PEER_AUTH_INIT}
   */
  @ConfigAttributeGetter(name = SECURITY_PEER_AUTH_INIT)
  String getSecurityPeerAuthInit();

  /**
   * Sets the user module name in {@link ConfigurationProperties#SECURITY_PEER_AUTH_INIT} property.
   */
  @ConfigAttributeSetter(name = SECURITY_PEER_AUTH_INIT)
  void setSecurityPeerAuthInit(String attValue);

  /**
   * The name of user module for {@link ConfigurationProperties#SECURITY_PEER_AUTH_INIT} property
   */
  @ConfigAttribute(type = String.class)
  String SECURITY_PEER_AUTH_INIT_NAME = SECURITY_PEER_AUTH_INIT;

  /**
   * The default {@link ConfigurationProperties#SECURITY_PEER_AUTH_INIT} method name.
   * <p>
   * Actual value of this is fully qualified <code>"method name"</code>.
   */
  String DEFAULT_SECURITY_PEER_AUTH_INIT = "";

  /**
   * Returns user defined method name authenticating peer's credentials in
   * {@link ConfigurationProperties#SECURITY_PEER_AUTHENTICATOR}
   */
  @ConfigAttributeGetter(name = SECURITY_PEER_AUTHENTICATOR)
  String getSecurityPeerAuthenticator();

  /**
   * Sets the user module name in {@link ConfigurationProperties#SECURITY_PEER_AUTHENTICATOR}
   * property.
   */
  @ConfigAttributeSetter(name = SECURITY_PEER_AUTHENTICATOR)
  void setSecurityPeerAuthenticator(String attValue);

  /**
   * The name of user defined method for {@link ConfigurationProperties#SECURITY_PEER_AUTHENTICATOR}
   * property
   */
  @ConfigAttribute(type = String.class)
  String SECURITY_PEER_AUTHENTICATOR_NAME = SECURITY_PEER_AUTHENTICATOR;

  /**
   * The default {@link ConfigurationProperties#SECURITY_PEER_AUTHENTICATOR} method.
   * <p>
   * Actual value of this is fully qualified <code>"method name"</code>.
   */
  String DEFAULT_SECURITY_PEER_AUTHENTICATOR = "";

  /**
   * Returns user module name authorizing client credentials in
   * {@link ConfigurationProperties#SECURITY_CLIENT_ACCESSOR}
   */
  @ConfigAttributeGetter(name = SECURITY_CLIENT_ACCESSOR)
  String getSecurityClientAccessor();

  /**
   * Sets the user defined method name in {@link ConfigurationProperties#SECURITY_CLIENT_ACCESSOR}
   * property.
   */
  @ConfigAttributeSetter(name = SECURITY_CLIENT_ACCESSOR)
  void setSecurityClientAccessor(String attValue);

  /**
   * The name of the factory method for {@link ConfigurationProperties#SECURITY_CLIENT_ACCESSOR}
   * property
   */
  @ConfigAttribute(type = String.class)
  String SECURITY_CLIENT_ACCESSOR_NAME = SECURITY_CLIENT_ACCESSOR;

  /**
   * The default {@link ConfigurationProperties#SECURITY_CLIENT_ACCESSOR} method name.
   * <p>
   * Actual value of this is fully qualified <code>"method name"</code>.
   */
  String DEFAULT_SECURITY_CLIENT_ACCESSOR = "";

  /**
   * Returns user module name authorizing client credentials in
   * {@link ConfigurationProperties#SECURITY_CLIENT_ACCESSOR_PP}
   */
  @ConfigAttributeGetter(name = SECURITY_CLIENT_ACCESSOR_PP)
  String getSecurityClientAccessorPP();

  /**
   * Sets the user defined method name in
   * {@link ConfigurationProperties#SECURITY_CLIENT_ACCESSOR_PP} property.
   */
  @ConfigAttributeSetter(name = SECURITY_CLIENT_ACCESSOR_PP)
  void setSecurityClientAccessorPP(String attValue);

  /**
   * The name of the factory method for {@link ConfigurationProperties#SECURITY_CLIENT_ACCESSOR_PP}
   * property
   */
  @ConfigAttribute(type = String.class)
  String SECURITY_CLIENT_ACCESSOR_PP_NAME = SECURITY_CLIENT_ACCESSOR_PP;

  /**
   * The default client post-operation {@link ConfigurationProperties#SECURITY_CLIENT_ACCESSOR_PP}
   * method name.
   * <p>
   * Actual value of this is fully qualified <code>"method name"</code>.
   */
  String DEFAULT_SECURITY_CLIENT_ACCESSOR_PP = "";

  /**
   * Get the current log-level for {@link ConfigurationProperties#SECURITY_LOG_LEVEL}.
   *
   * @return the current security log-level
   */
  @ConfigAttributeGetter(name = SECURITY_LOG_LEVEL)
  int getSecurityLogLevel();

  /**
   * Set the log-level for {@link ConfigurationProperties#SECURITY_LOG_LEVEL}.
   *
   * @param level the new security log-level
   */
  @ConfigAttributeSetter(name = SECURITY_LOG_LEVEL)
  void setSecurityLogLevel(int level);

  /**
   * The name of {@link ConfigurationProperties#SECURITY_LOG_LEVEL} property that sets the log-level
   * for security logger obtained using {@link DistributedSystem#getSecurityLogWriter()}
   */
  // type is String because the config file "config", "debug", "fine" etc, but the setter getter
  // accepts int
  @ConfigAttribute(type = String.class)
  String SECURITY_LOG_LEVEL_NAME = SECURITY_LOG_LEVEL;

  /**
   * Returns the value of the {@link ConfigurationProperties#SECURITY_LOG_FILE} property
   *
   * @return <code>null</code> if logging information goes to standard out
   */
  @ConfigAttributeGetter(name = SECURITY_LOG_FILE)
  File getSecurityLogFile();

  /**
   * Sets the system's {@link ConfigurationProperties#SECURITY_LOG_FILE} containing security related
   * messages.
   * <p>
   * Non-absolute log files are relative to the system directory.
   * <p>
   * The security log file can not be changed while the system is running.
   *
   * @throws IllegalArgumentException if the specified value is not acceptable.
   * @throws org.apache.geode.UnmodifiableException if this attribute can not be modified.
   * @throws org.apache.geode.GemFireIOException if the set failure is caused by an error when
   *         writing to the system's configuration file.
   */
  @ConfigAttributeSetter(name = SECURITY_LOG_FILE)
  void setSecurityLogFile(File value);

  /**
   * The name of the {@link ConfigurationProperties#SECURITY_LOG_FILE} property. This property is
   * the path of the file where security related messages are logged.
   */
  @ConfigAttribute(type = File.class)
  String SECURITY_LOG_FILE_NAME = SECURITY_LOG_FILE;

  /**
   * The default {@link ConfigurationProperties#SECURITY_LOG_FILE}.
   * <p>
   * *
   * <p>
   * Actual value of this constant is <code>""</code> which directs security log messages to the
   * same place as the system log file.
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
   * Set timeout for peer membership check when security is enabled. The timeout must be less than
   * peer handshake timeout.
   *
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
   * Returns all properties starting with {@link ConfigurationProperties#SECURITY_PREFIX}
   */
  Properties getSecurityProps();

  /**
   * Returns the value of security property {@link ConfigurationProperties#SECURITY_PREFIX} for an
   * exact attribute name match.
   */
  String getSecurity(String attName);

  /**
   * Sets the value of the {@link ConfigurationProperties#SECURITY_PREFIX} property.
   */
  void setSecurity(String attName, String attValue);

  String SECURITY_PREFIX_NAME = SECURITY_PREFIX;


  /**
   * The static String definition of the cluster ssl prefix <i>"cluster-ssl"</i> used in conjunction
   * with other <i>cluster-ssl-*</i> properties property <a name="cluster-ssl"/a>
   * </p>
   * <U>Description</U>: The cluster-ssl property prefix
   */
  @Deprecated
  String CLUSTER_SSL_PREFIX = "cluster-ssl";

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
   * Returns the value of the {@link ConfigurationProperties#REMOVE_UNRESPONSIVE_CLIENT} property.
   *
   * @since GemFire 6.0
   */
  @ConfigAttributeGetter(name = REMOVE_UNRESPONSIVE_CLIENT)
  boolean getRemoveUnresponsiveClient();

  /**
   * Sets the value of the {@link ConfigurationProperties#REMOVE_UNRESPONSIVE_CLIENT} property.
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
   * Returns the value of the {@link ConfigurationProperties#DELTA_PROPAGATION} property.
   *
   * @since GemFire 6.3
   */
  @ConfigAttributeGetter(name = DELTA_PROPAGATION)
  boolean getDeltaPropagation();

  /**
   * Sets the value of the {@link ConfigurationProperties#DELTA_PROPAGATION} property.
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
   * @deprecated Geode 1.0 use {@link #getClusterSSLProperties()}
   */
  Properties getJmxSSLProperties();

  /**
   * @since GemFire 6.6
   */
  @ConfigAttribute(type = Boolean.class)
  String ENFORCE_UNIQUE_HOST_NAME = ENFORCE_UNIQUE_HOST;
  /**
   * Using the system property to set the default here to retain backwards compatibility with
   * customers that are already using this system property.
   */
  boolean DEFAULT_ENFORCE_UNIQUE_HOST =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "EnforceUniqueHostStorageAllocation");

  @ConfigAttributeSetter(name = ENFORCE_UNIQUE_HOST)
  void setEnforceUniqueHost(boolean enforceUniqueHost);

  @ConfigAttributeGetter(name = ENFORCE_UNIQUE_HOST)
  boolean getEnforceUniqueHost();

  Properties getUserDefinedProps();

  /**
   * Returns the value of the {@link ConfigurationProperties#GROUPS} property
   * <p>
   * The default value is: {@link #DEFAULT_GROUPS}.
   *
   * @return the value of the property
   *
   * @since GemFire 7.0
   */
  @ConfigAttributeGetter(name = GROUPS)
  String getGroups();

  /**
   * Sets the {@link ConfigurationProperties#GROUPS} property.
   * <p>
   * The groups can not be changed while the system is running.
   *
   * @throws IllegalArgumentException if the specified value is not acceptable.
   * @throws org.apache.geode.UnmodifiableException if this attribute can not be modified.
   * @throws org.apache.geode.GemFireIOException if the set failure is caused by an error when
   *         writing to the system's configuration file.
   * @since GemFire 7.0
   */
  @ConfigAttributeSetter(name = GROUPS)
  void setGroups(String value);

  /**
   * The name of the {@link ConfigurationProperties#GROUPS} property
   *
   * @since GemFire 7.0
   */
  @ConfigAttribute(type = String.class)
  String GROUPS_NAME = GROUPS;
  /**
   * The default {@link ConfigurationProperties#GROUPS}.
   * <p>
   * Actual value of this constant is <code>""</code>.
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
   * The name of the {@link ConfigurationProperties#REMOTE_LOCATORS} property
   */
  @ConfigAttribute(type = String.class)
  String REMOTE_LOCATORS_NAME = REMOTE_LOCATORS;
  /**
   * The default value of the {@link ConfigurationProperties#REMOTE_LOCATORS} property
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
   * Returns the value of the {@link ConfigurationProperties#JMX_MANAGER_SSL_ENABLED} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLEnabled()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = JMX_MANAGER_SSL_ENABLED)
  boolean getJmxManagerSSLEnabled();

  /**
   * The default {@link ConfigurationProperties#JMX_MANAGER_SSL_ENABLED} state.
   * <p>
   * Actual value of this constant is <code>false</code>.
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_ENABLED}
   */
  @Deprecated
  boolean DEFAULT_JMX_MANAGER_SSL_ENABLED = false;

  /**
   * The name of the {@link ConfigurationProperties#JMX_MANAGER_SSL_ENABLED} property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_ENABLED}
   */
  @Deprecated
  @ConfigAttribute(type = Boolean.class)
  String JMX_MANAGER_SSL_ENABLED_NAME = JMX_MANAGER_SSL_ENABLED;

  /**
   * Sets the value of the {@link ConfigurationProperties#JMX_MANAGER_SSL_ENABLED} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLEnabled(boolean)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = JMX_MANAGER_SSL_ENABLED)
  void setJmxManagerSSLEnabled(boolean enabled);

  /**
   * Returns the value of the {@link ConfigurationProperties#OFF_HEAP_MEMORY_SIZE} property.
   *
   * @since Geode 1.0
   */
  @ConfigAttributeGetter(name = OFF_HEAP_MEMORY_SIZE)
  String getOffHeapMemorySize();

  /**
   * Sets the value of the {@link ConfigurationProperties#OFF_HEAP_MEMORY_SIZE} property.
   *
   * @since Geode 1.0
   */
  @ConfigAttributeSetter(name = OFF_HEAP_MEMORY_SIZE)
  void setOffHeapMemorySize(String value);

  /**
   * The name of the {@link ConfigurationProperties#OFF_HEAP_MEMORY_SIZE} property
   *
   * @since Geode 1.0
   */
  @ConfigAttribute(type = String.class)
  String OFF_HEAP_MEMORY_SIZE_NAME = OFF_HEAP_MEMORY_SIZE;
  /**
   * The default {@link ConfigurationProperties#OFF_HEAP_MEMORY_SIZE} value of <code>""</code>.
   *
   * @since Geode 1.0
   */
  String DEFAULT_OFF_HEAP_MEMORY_SIZE = "";

  /**
   * Returns the value of the {@link ConfigurationProperties#JMX_MANAGER_SSL_PROTOCOLS} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLProtocols()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = JMX_MANAGER_SSL_PROTOCOLS)
  String getJmxManagerSSLProtocols();

  /**
   * Sets the value of the {@link ConfigurationProperties#JMX_MANAGER_SSL_PROTOCOLS} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLProtocols(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = JMX_MANAGER_SSL_PROTOCOLS)
  void setJmxManagerSSLProtocols(String protocols);

  /**
   * The default {@link ConfigurationProperties#JMX_MANAGER_SSL_PROTOCOLS} value.
   * <p>
   * Actual value of this constant is <code>any</code>.
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_PROTOCOLS}
   */
  @Deprecated
  String DEFAULT_JMX_MANAGER_SSL_PROTOCOLS = "any";
  /**
   * The name of the {@link ConfigurationProperties#JMX_MANAGER_SSL_PROTOCOLS} property The name of
   * the {@link org.apache.geode.distributed.ConfigurationProperties#JMX_MANAGER_SSL_PROTOCOLS}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_PROTOCOLS}
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String JMX_MANAGER_SSL_PROTOCOLS_NAME = JMX_MANAGER_SSL_PROTOCOLS;

  /**
   * Returns the value of the {@link ConfigurationProperties#JMX_MANAGER_SSL_CIPHERS} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLCiphers()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = JMX_MANAGER_SSL_CIPHERS)
  String getJmxManagerSSLCiphers();

  /**
   * Sets the value of the {@link ConfigurationProperties#JMX_MANAGER_SSL_CIPHERS} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLCiphers(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = JMX_MANAGER_SSL_CIPHERS)
  void setJmxManagerSSLCiphers(String ciphers);

  /**
   * The default {@link ConfigurationProperties#JMX_MANAGER_SSL_CIPHERS} value.
   * <p>
   * Actual value of this constant is <code>any</code>.
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_CIPHERS}
   */
  @Deprecated
  String DEFAULT_JMX_MANAGER_SSL_CIPHERS = "any";
  /**
   * The name of the {@link ConfigurationProperties#JMX_MANAGER_SSL_CIPHERS} property The name of
   * the {@link org.apache.geode.distributed.ConfigurationProperties#JMX_MANAGER_SSL_CIPHERS}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_CIPHERS}
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String JMX_MANAGER_SSL_CIPHERS_NAME = JMX_MANAGER_SSL_CIPHERS;

  /**
   * Returns the value of the {@link ConfigurationProperties#JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION}
   * property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLRequireAuthentication()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION)
  boolean getJmxManagerSSLRequireAuthentication();

  /**
   * Sets the value of the {@link ConfigurationProperties#JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION}
   * property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLRequireAuthentication(boolean)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION)
  void setJmxManagerSSLRequireAuthentication(boolean enabled);

  /**
   * The default {@link ConfigurationProperties#JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION} value.
   * <p>
   * Actual value of this constant is <code>true</code>.
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_REQUIRE_AUTHENTICATION}
   */
  @Deprecated
  boolean DEFAULT_JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION = true;
  /**
   * The name of the {@link ConfigurationProperties#JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION} property
   * The name of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_REQUIRE_AUTHENTICATION}
   */
  @Deprecated
  @ConfigAttribute(type = Boolean.class)
  String JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION_NAME = JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION;

  /**
   * Returns the value of the {@link ConfigurationProperties#JMX_MANAGER_SSL_KEYSTORE} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLKeyStore()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = JMX_MANAGER_SSL_KEYSTORE)
  String getJmxManagerSSLKeyStore();

  /**
   * Sets the value of the {@link ConfigurationProperties#JMX_MANAGER_SSL_KEYSTORE} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLKeyStore(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = JMX_MANAGER_SSL_KEYSTORE)
  void setJmxManagerSSLKeyStore(String keyStore);

  /**
   * The default {@link ConfigurationProperties#JMX_MANAGER_SSL_KEYSTORE} value.
   * <p>
   * Actual value of this constant is "".
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_KEYSTORE}
   */
  @Deprecated
  String DEFAULT_JMX_MANAGER_SSL_KEYSTORE = "";

  /**
   * The name of the {@link ConfigurationProperties#JMX_MANAGER_SSL_KEYSTORE} property The name of
   * the {@link org.apache.geode.distributed.ConfigurationProperties#JMX_MANAGER_SSL_KEYSTORE}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_KEYSTORE}
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String JMX_MANAGER_SSL_KEYSTORE_NAME = JMX_MANAGER_SSL_KEYSTORE;

  /**
   * Returns the value of the {@link ConfigurationProperties#JMX_MANAGER_SSL_KEYSTORE_TYPE}
   * property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLKeyStoreType()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = JMX_MANAGER_SSL_KEYSTORE_TYPE)
  String getJmxManagerSSLKeyStoreType();

  /**
   * Sets the value of the {@link ConfigurationProperties#JMX_MANAGER_SSL_KEYSTORE_TYPE} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLKeyStoreType(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = JMX_MANAGER_SSL_KEYSTORE_TYPE)
  void setJmxManagerSSLKeyStoreType(String keyStoreType);

  /**
   * The default {@link ConfigurationProperties#JMX_MANAGER_SSL_KEYSTORE_TYPE} value.
   * <p>
   * Actual value of this constant is "".
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_CLUSTER_SSL_KEYSTORE_TYPE}
   */
  @Deprecated
  String DEFAULT_JMX_MANAGER_SSL_KEYSTORE_TYPE = "";

  /**
   * The name of the {@link ConfigurationProperties#JMX_MANAGER_SSL_KEYSTORE_TYPE} property The name
   * of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#JMX_MANAGER_SSL_KEYSTORE_TYPE}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_KEYSTORE_TYPE}
   */
  @ConfigAttribute(type = String.class)
  String JMX_MANAGER_SSL_KEYSTORE_TYPE_NAME = JMX_MANAGER_SSL_KEYSTORE_TYPE;

  /**
   * Returns the value of the {@link ConfigurationProperties#JMX_MANAGER_SSL_KEYSTORE_PASSWORD}
   * property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLKeyStorePassword()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = JMX_MANAGER_SSL_KEYSTORE_PASSWORD)
  String getJmxManagerSSLKeyStorePassword();

  /**
   * Sets the value of the {@link ConfigurationProperties#JMX_MANAGER_SSL_KEYSTORE_PASSWORD}
   * property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLKeyStorePassword(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = JMX_MANAGER_SSL_KEYSTORE_PASSWORD)
  void setJmxManagerSSLKeyStorePassword(String keyStorePassword);

  /**
   * The default {@link ConfigurationProperties#JMX_MANAGER_SSL_KEYSTORE_PASSWORD} value.
   * <p>
   * Actual value of this constant is "".
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_KEYSTORE_PASSWORD}
   */
  @Deprecated
  String DEFAULT_JMX_MANAGER_SSL_KEYSTORE_PASSWORD = "";

  /**
   * The name of the {@link ConfigurationProperties#JMX_MANAGER_SSL_KEYSTORE_PASSWORD} property The
   * name of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#JMX_MANAGER_SSL_KEYSTORE_PASSWORD}
   * propery
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_KEYSTORE_PASSWORD}
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String JMX_MANAGER_SSL_KEYSTORE_PASSWORD_NAME = JMX_MANAGER_SSL_KEYSTORE_PASSWORD;

  /**
   * Returns the value of the {@link ConfigurationProperties#JMX_MANAGER_SSL_TRUSTSTORE} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLTrustStore()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = JMX_MANAGER_SSL_TRUSTSTORE)
  String getJmxManagerSSLTrustStore();

  /**
   * Sets the value of the {@link ConfigurationProperties#JMX_MANAGER_SSL_TRUSTSTORE} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLTrustStore(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = JMX_MANAGER_SSL_TRUSTSTORE)
  void setJmxManagerSSLTrustStore(String trustStore);

  /**
   * The default {@link ConfigurationProperties#JMX_MANAGER_SSL_TRUSTSTORE} value.
   * <p>
   * Actual value of this constant is "".
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_TRUSTSTORE}
   */
  @Deprecated
  String DEFAULT_JMX_MANAGER_SSL_TRUSTSTORE = "";

  /**
   * The name of the {@link ConfigurationProperties#JMX_MANAGER_SSL_TRUSTSTORE} property The name of
   * the {@link org.apache.geode.distributed.ConfigurationProperties#JMX_MANAGER_SSL_TRUSTSTORE}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_TRUSTSTORE}
   */
  @ConfigAttribute(type = String.class)
  String JMX_MANAGER_SSL_TRUSTSTORE_NAME = JMX_MANAGER_SSL_TRUSTSTORE;

  /**
   * Returns the value of the {@link ConfigurationProperties#JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD}
   * property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLTrustStorePassword()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD)
  String getJmxManagerSSLTrustStorePassword();

  /**
   * Sets the value of the {@link ConfigurationProperties#JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD}
   * property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLTrustStorePassword(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD)
  void setJmxManagerSSLTrustStorePassword(String trusStorePassword);

  /**
   * The default {@link ConfigurationProperties#JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD} value.
   * <p>
   * Actual value of this constant is "".
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_TRUSTSTORE_PASSWORD}
   */
  @Deprecated
  String DEFAULT_JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD = "";

  /**
   * The name of the {@link ConfigurationProperties#JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD} property
   * The name of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_TRUSTSTORE_PASSWORD}
   */
  @Deprecated
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
   * Returns the value of the {@link ConfigurationProperties#JMX_MANAGER_HTTP_PORT} property
   * <p>
   * Returns the value of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#JMX_MANAGER_HTTP_PORT} property
   *
   * @deprecated as of 8.0 use {@link #getHttpServicePort()} instead.
   */
  @ConfigAttributeGetter(name = JMX_MANAGER_HTTP_PORT)
  int getJmxManagerHttpPort();

  /**
   * Set the {@link ConfigurationProperties#JMX_MANAGER_HTTP_PORT} for jmx-manager.
   * <p>
   * Set the {@link org.apache.geode.distributed.ConfigurationProperties#JMX_MANAGER_HTTP_PORT} for
   * jmx-manager.
   *
   * @param value the port number for jmx-manager HTTP service
   *
   * @deprecated as of 8.0 use {@link #setHttpServicePort(int)} instead.
   */
  @ConfigAttributeSetter(name = JMX_MANAGER_HTTP_PORT)
  void setJmxManagerHttpPort(int value);

  /**
   * The name of the {@link ConfigurationProperties#JMX_MANAGER_HTTP_PORT} property.
   * <p>
   * The name of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#JMX_MANAGER_HTTP_PORT} property.
   *
   * @deprecated as of 8.0 use {{@link #HTTP_SERVICE_PORT_NAME} instead.
   */
  @ConfigAttribute(type = Integer.class, min = 0, max = 65535)
  String JMX_MANAGER_HTTP_PORT_NAME = JMX_MANAGER_HTTP_PORT;

  /**
   * The default value of the {@link ConfigurationProperties#JMX_MANAGER_HTTP_PORT} property.
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
  @ConfigAttribute(type = Integer.class, min = MIN_JMX_MANAGER_UPDATE_RATE,
      max = MAX_JMX_MANAGER_UPDATE_RATE)
  String JMX_MANAGER_UPDATE_RATE_NAME = JMX_MANAGER_UPDATE_RATE;

  /**
   * Returns the value of the {@link ConfigurationProperties#MEMCACHED_PORT} property
   * <p>
   * Returns the value of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#MEMCACHED_PORT} property
   *
   * @return the port on which GemFireMemcachedServer should be started
   *
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
   * Returns the value of the {@link ConfigurationProperties#MEMCACHED_PROTOCOL} property
   * <p>
   * Returns the value of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#MEMCACHED_PROTOCOL} property
   *
   * @return the protocol for GemFireMemcachedServer
   *
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
   * Returns the value of the {@link ConfigurationProperties#MEMCACHED_BIND_ADDRESS} property
   * <p>
   * Returns the value of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#MEMCACHED_BIND_ADDRESS} property
   *
   * @return the bind address for GemFireMemcachedServer
   *
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
   * Returns the value of the {@link ConfigurationProperties#REDIS_PORT} property
   *
   * @return the port on which GeodeRedisServer should be started
   *
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
   * Returns the value of the {@link ConfigurationProperties#REDIS_BIND_ADDRESS} property
   * <p>
   * Returns the value of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#REDIS_BIND_ADDRESS} property
   *
   * @return the bind address for GemFireRedisServer
   *
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
   * Returns the value of the {@link ConfigurationProperties#REDIS_PASSWORD} property
   * <p>
   * Returns the value of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#REDIS_PASSWORD} property
   *
   * @return the authentication password for GemFireRedisServer
   *
   * @since GemFire 8.0
   */
  @ConfigAttributeGetter(name = REDIS_PASSWORD)
  String getRedisPassword();

  @ConfigAttributeSetter(name = REDIS_PASSWORD)
  void setRedisPassword(String password);

  @ConfigAttribute(type = String.class)
  String REDIS_PASSWORD_NAME = REDIS_PASSWORD;
  String DEFAULT_REDIS_PASSWORD = "";

  // Added for the HTTP service

  /**
   * Returns the value of the {@link ConfigurationProperties#HTTP_SERVICE_PORT} property
   * <p>
   * Returns the value of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#HTTP_SERVICE_PORT} property
   *
   * @return the HTTP service port
   *
   * @since GemFire 8.0
   */
  @ConfigAttributeGetter(name = HTTP_SERVICE_PORT)
  int getHttpServicePort();

  /**
   * Set the {@link ConfigurationProperties#HTTP_SERVICE_PORT} for HTTP service.
   * <p>
   * Set the {@link org.apache.geode.distributed.ConfigurationProperties#HTTP_SERVICE_PORT} for HTTP
   * service.
   *
   * @param value the port number for HTTP service
   *
   * @since GemFire 8.0
   */
  @ConfigAttributeSetter(name = HTTP_SERVICE_PORT)
  void setHttpServicePort(int value);

  /**
   * The name of the {@link ConfigurationProperties#HTTP_SERVICE_PORT} property
   * <p>
   * The name of the {@link org.apache.geode.distributed.ConfigurationProperties#HTTP_SERVICE_PORT}
   * property
   *
   * @since GemFire 8.0
   */
  @ConfigAttribute(type = Integer.class, min = 0, max = 65535)
  String HTTP_SERVICE_PORT_NAME = HTTP_SERVICE_PORT;

  /**
   * The default value of the {@link ConfigurationProperties#HTTP_SERVICE_PORT} property. Current
   * value is a <code>7070</code>
   *
   * @since GemFire 8.0
   */
  int DEFAULT_HTTP_SERVICE_PORT = 7070;

  /**
   * Returns the value of the {@link ConfigurationProperties#HTTP_SERVICE_BIND_ADDRESS} property
   * <p>
   * Returns the value of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#HTTP_SERVICE_BIND_ADDRESS} property
   *
   * @return the bind-address for HTTP service
   *
   * @since GemFire 8.0
   */
  @ConfigAttributeGetter(name = HTTP_SERVICE_BIND_ADDRESS)
  String getHttpServiceBindAddress();

  /**
   * Set the {@link ConfigurationProperties#HTTP_SERVICE_BIND_ADDRESS} for HTTP service.
   * <p>
   * Set the {@link org.apache.geode.distributed.ConfigurationProperties#HTTP_SERVICE_BIND_ADDRESS}
   * for HTTP service.
   *
   * @param value the bind-address for HTTP service
   *
   * @since GemFire 8.0
   */
  @ConfigAttributeSetter(name = HTTP_SERVICE_BIND_ADDRESS)
  void setHttpServiceBindAddress(String value);

  /**
   * The name of the {@link ConfigurationProperties#HTTP_SERVICE_BIND_ADDRESS} property
   *
   * @since GemFire 8.0
   */
  @ConfigAttribute(type = String.class)
  String HTTP_SERVICE_BIND_ADDRESS_NAME = HTTP_SERVICE_BIND_ADDRESS;

  /**
   * The default value of the {@link ConfigurationProperties#HTTP_SERVICE_BIND_ADDRESS} property.
   * Current value is an empty string <code>""</code>
   *
   * @since GemFire 8.0
   */
  String DEFAULT_HTTP_SERVICE_BIND_ADDRESS = "";

  // Added for HTTP Service SSL

  /**
   * Returns the value of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_ENABLED} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLEnabled()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = HTTP_SERVICE_SSL_ENABLED)
  boolean getHttpServiceSSLEnabled();

  /**
   * Sets the value of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_ENABLED} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLEnabled(boolean)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = HTTP_SERVICE_SSL_ENABLED)
  void setHttpServiceSSLEnabled(boolean httpServiceSSLEnabled);

  /**
   * The default {@link ConfigurationProperties#HTTP_SERVICE_SSL_ENABLED} state.
   * <p>
   * Actual value of this constant is <code>false</code>.
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_ENABLED}
   */
  @Deprecated
  boolean DEFAULT_HTTP_SERVICE_SSL_ENABLED = false;

  /**
   * The name of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_ENABLED} property The name of
   * the {@link org.apache.geode.distributed.ConfigurationProperties#HTTP_SERVICE_SSL_ENABLED}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_ENABLED}
   */
  @Deprecated
  @ConfigAttribute(type = Boolean.class)
  String HTTP_SERVICE_SSL_ENABLED_NAME = HTTP_SERVICE_SSL_ENABLED;

  /**
   * Returns the value of the
   * {@link ConfigurationProperties#HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLRequireAuthentication()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION)
  boolean getHttpServiceSSLRequireAuthentication();

  /**
   * Sets the value of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION}
   * property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLRequireAuthentication(boolean)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION)
  void setHttpServiceSSLRequireAuthentication(boolean httpServiceSSLRequireAuthentication);

  /**
   * The default {@link ConfigurationProperties#HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION} value.
   * <p>
   * Actual value of this constant is <code>true</code>.
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_REQUIRE_AUTHENTICATION}
   */
  @Deprecated
  boolean DEFAULT_HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION = false;

  /**
   * The name of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION}
   * property The name of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_REQUIRE_AUTHENTICATION}
   */
  @Deprecated
  @ConfigAttribute(type = Boolean.class)
  String HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION_NAME = HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION;

  /**
   * Returns the value of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_PROTOCOLS} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLProtocols()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = HTTP_SERVICE_SSL_PROTOCOLS)
  String getHttpServiceSSLProtocols();

  /**
   * Sets the value of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_PROTOCOLS} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLProtocols(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = HTTP_SERVICE_SSL_PROTOCOLS)
  void setHttpServiceSSLProtocols(String protocols);

  /**
   * The default {@link ConfigurationProperties#HTTP_SERVICE_SSL_PROTOCOLS} value.
   * <p>
   * Actual value of this constant is <code>any</code>.
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_PROTOCOLS}
   */
  @Deprecated
  String DEFAULT_HTTP_SERVICE_SSL_PROTOCOLS = "any";

  /**
   * The name of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_PROTOCOLS} property The name of
   * the {@link org.apache.geode.distributed.ConfigurationProperties#HTTP_SERVICE_SSL_PROTOCOLS}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_PROTOCOLS}
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String HTTP_SERVICE_SSL_PROTOCOLS_NAME = HTTP_SERVICE_SSL_PROTOCOLS;

  /**
   * Returns the value of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_CIPHERS} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLCiphers()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = HTTP_SERVICE_SSL_CIPHERS)
  String getHttpServiceSSLCiphers();

  /**
   * Sets the value of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_CIPHERS} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLCiphers(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = HTTP_SERVICE_SSL_CIPHERS)
  void setHttpServiceSSLCiphers(String ciphers);

  /**
   * The default {@link ConfigurationProperties#HTTP_SERVICE_SSL_CIPHERS} value.
   * <p>
   * Actual value of this constant is <code>any</code>.
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_CIPHERS}
   */
  @Deprecated
  String DEFAULT_HTTP_SERVICE_SSL_CIPHERS = "any";

  /**
   * The name of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_CIPHERS} property The name of
   * the {@link org.apache.geode.distributed.ConfigurationProperties#HTTP_SERVICE_SSL_CIPHERS}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_CIPHERS}
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String HTTP_SERVICE_SSL_CIPHERS_NAME = HTTP_SERVICE_SSL_CIPHERS;

  /**
   * Returns the value of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_KEYSTORE} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLKeyStore()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = HTTP_SERVICE_SSL_KEYSTORE)
  String getHttpServiceSSLKeyStore();

  /**
   * Sets the value of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_KEYSTORE} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLKeyStore(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = HTTP_SERVICE_SSL_KEYSTORE)
  void setHttpServiceSSLKeyStore(String keyStore);

  /**
   * The default {@link ConfigurationProperties#HTTP_SERVICE_SSL_KEYSTORE} value.
   * <p>
   * Actual value of this constant is "".
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_KEYSTORE}
   */
  @Deprecated
  String DEFAULT_HTTP_SERVICE_SSL_KEYSTORE = "";

  /**
   * The name of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_KEYSTORE} property The name of
   * the {@link org.apache.geode.distributed.ConfigurationProperties#HTTP_SERVICE_SSL_KEYSTORE}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_KEYSTORE}
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String HTTP_SERVICE_SSL_KEYSTORE_NAME = HTTP_SERVICE_SSL_KEYSTORE;

  /**
   * Returns the value of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_KEYSTORE_PASSWORD}
   * property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLKeyStorePassword()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = HTTP_SERVICE_SSL_KEYSTORE_PASSWORD)
  String getHttpServiceSSLKeyStorePassword();

  /**
   * Sets the value of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_KEYSTORE_PASSWORD}
   * property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLKeyStorePassword(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = HTTP_SERVICE_SSL_KEYSTORE_PASSWORD)
  void setHttpServiceSSLKeyStorePassword(String keyStorePassword);

  /**
   * The default {@link ConfigurationProperties#HTTP_SERVICE_SSL_KEYSTORE_PASSWORD} value.
   * <p>
   * Actual value of this constant is "".
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_KEYSTORE_PASSWORD}
   */
  @Deprecated
  String DEFAULT_HTTP_SERVICE_SSL_KEYSTORE_PASSWORD = "";

  /**
   * The name of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_KEYSTORE_PASSWORD} property The
   * name of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#HTTP_SERVICE_SSL_KEYSTORE_PASSWORD}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_KEYSTORE_PASSWORD}
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME = HTTP_SERVICE_SSL_KEYSTORE_PASSWORD;

  /**
   * Returns the value of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_KEYSTORE_TYPE}
   * property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLKeyStoreType()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = HTTP_SERVICE_SSL_KEYSTORE_TYPE)
  String getHttpServiceSSLKeyStoreType();

  /**
   * Sets the value of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_KEYSTORE_TYPE} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLKeyStoreType(String)}
   */
  @ConfigAttributeSetter(name = HTTP_SERVICE_SSL_KEYSTORE_TYPE)
  void setHttpServiceSSLKeyStoreType(String keyStoreType);

  /**
   * The default {@link ConfigurationProperties#HTTP_SERVICE_SSL_KEYSTORE_TYPE} value.
   * <p>
   * Actual value of this constant is "".
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_CLUSTER_SSL_KEYSTORE_TYPE}
   */
  @Deprecated
  String DEFAULT_HTTP_SERVICE_SSL_KEYSTORE_TYPE = "";

  /**
   * The name of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_KEYSTORE_TYPE} property The
   * name of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#HTTP_SERVICE_SSL_KEYSTORE_TYPE}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_KEYSTORE_TYPE}
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String HTTP_SERVICE_SSL_KEYSTORE_TYPE_NAME = HTTP_SERVICE_SSL_KEYSTORE_TYPE;

  /**
   * Returns the value of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_TRUSTSTORE} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLTrustStore()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = HTTP_SERVICE_SSL_TRUSTSTORE)
  String getHttpServiceSSLTrustStore();

  /**
   * Sets the value of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_TRUSTSTORE} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLTrustStore(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = HTTP_SERVICE_SSL_TRUSTSTORE)
  void setHttpServiceSSLTrustStore(String trustStore);

  /**
   * The default {@link ConfigurationProperties#HTTP_SERVICE_SSL_TRUSTSTORE} value.
   * <p>
   * Actual value of this constant is "".
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_TRUSTSTORE}
   */
  @Deprecated
  String DEFAULT_HTTP_SERVICE_SSL_TRUSTSTORE = "";

  /**
   * The name of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_TRUSTSTORE} property The name
   * of the {@link org.apache.geode.distributed.ConfigurationProperties#HTTP_SERVICE_SSL_TRUSTSTORE}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_TRUSTSTORE}
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String HTTP_SERVICE_SSL_TRUSTSTORE_NAME = HTTP_SERVICE_SSL_TRUSTSTORE;

  /**
   * Returns the value of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD}
   * property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLTrustStorePassword()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD)
  String getHttpServiceSSLTrustStorePassword();

  /**
   * Sets the value of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD}
   * property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLTrustStorePassword(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD)
  void setHttpServiceSSLTrustStorePassword(String trustStorePassword);

  /**
   * The default {@link ConfigurationProperties#HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD} value.
   * <p>
   * Actual value of this constant is "".
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_TRUSTSTORE_PASSWORD}
   */
  @Deprecated
  String DEFAULT_HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD = "";

  /**
   * The name of the {@link ConfigurationProperties#HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD} property
   * The name of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_TRUSTSTORE_PASSWORD}
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD_NAME = HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD;

  /**
   * @deprecated Geode 1.0 use {@link #getClusterSSLProperties()}
   */
  @Deprecated
  Properties getHttpServiceSSLProperties();

  // Added for API REST

  /**
   * Returns the value of the {@link ConfigurationProperties#START_DEV_REST_API} property
   * <p>
   * Returns the value of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#START_DEV_REST_API} property
   *
   * @return the value of the property
   *
   * @since GemFire 8.0
   */

  @ConfigAttributeGetter(name = START_DEV_REST_API)
  boolean getStartDevRestApi();

  /**
   * Set the {@link ConfigurationProperties#START_DEV_REST_API} for HTTP service.
   * <p>
   * Set the {@link org.apache.geode.distributed.ConfigurationProperties#START_DEV_REST_API} for
   * HTTP service.
   *
   * @param value for the property
   *
   * @since GemFire 8.0
   */
  @ConfigAttributeSetter(name = START_DEV_REST_API)
  void setStartDevRestApi(boolean value);

  /**
   * The name of the {@link ConfigurationProperties#START_DEV_REST_API} property
   * <p>
   * The name of the {@link org.apache.geode.distributed.ConfigurationProperties#START_DEV_REST_API}
   * property
   *
   * @since GemFire 8.0
   */
  @ConfigAttribute(type = Boolean.class)
  String START_DEV_REST_API_NAME = START_DEV_REST_API;

  /**
   * The default value of the {@link ConfigurationProperties#START_DEV_REST_API} property. Current
   * value is <code>"false"</code>
   *
   * @since GemFire 8.0
   */
  boolean DEFAULT_START_DEV_REST_API = false;

  /**
   * The name of the {@link ConfigurationProperties#DISABLE_AUTO_RECONNECT} property
   *
   * @since GemFire 8.0
   */
  @ConfigAttribute(type = Boolean.class)
  String DISABLE_AUTO_RECONNECT_NAME = DISABLE_AUTO_RECONNECT;

  /**
   * The default value of the {@link ConfigurationProperties#DISABLE_AUTO_RECONNECT} property
   */
  boolean DEFAULT_DISABLE_AUTO_RECONNECT = false;

  /**
   * Gets the value of {@link ConfigurationProperties#DISABLE_AUTO_RECONNECT}
   */
  @ConfigAttributeGetter(name = DISABLE_AUTO_RECONNECT)
  boolean getDisableAutoReconnect();

  /**
   * Sets the value of {@link ConfigurationProperties#DISABLE_AUTO_RECONNECT}
   *
   * @param value the new setting
   */
  @ConfigAttributeSetter(name = DISABLE_AUTO_RECONNECT)
  void setDisableAutoReconnect(boolean value);

  /**
   * @deprecated Geode 1.0 use {@link #getClusterSSLProperties()}
   */
  @Deprecated
  Properties getServerSSLProperties();

  /**
   * Returns the value of the {@link ConfigurationProperties#SERVER_SSL_ENABLED} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLEnabled()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = SERVER_SSL_ENABLED)
  boolean getServerSSLEnabled();

  /**
   * The default {@link ConfigurationProperties#SERVER_SSL_ENABLED} state.
   * <p>
   * Actual value of this constant is <code>false</code>.
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_ENABLED}
   */
  @Deprecated
  boolean DEFAULT_SERVER_SSL_ENABLED = false;
  /**
   * The name of the {@link ConfigurationProperties#SERVER_SSL_ENABLED} property The name of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#SERVER_SSL_ENABLED} property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#SERVER_SSL_ENABLED}
   */
  @Deprecated
  @ConfigAttribute(type = Boolean.class)
  String SERVER_SSL_ENABLED_NAME = SERVER_SSL_ENABLED;

  /**
   * Sets the value of the {@link ConfigurationProperties#SERVER_SSL_ENABLED} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLEnabled(boolean)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = SERVER_SSL_ENABLED)
  void setServerSSLEnabled(boolean enabled);

  /**
   * Returns the value of the {@link ConfigurationProperties#SERVER_SSL_PROTOCOLS} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLProtocols()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = SERVER_SSL_PROTOCOLS)
  String getServerSSLProtocols();

  /**
   * Sets the value of the {@link ConfigurationProperties#SERVER_SSL_PROTOCOLS} property.
   */
  @ConfigAttributeSetter(name = SERVER_SSL_PROTOCOLS)
  void setServerSSLProtocols(String protocols);

  /**
   * The default {@link ConfigurationProperties#SERVER_SSL_PROTOCOLS} value.
   * <p>
   * Actual value of this constant is <code>any</code>.
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_PROTOCOLS}
   */
  @Deprecated
  String DEFAULT_SERVER_SSL_PROTOCOLS = "any";
  /**
   * The name of the {@link ConfigurationProperties#SERVER_SSL_PROTOCOLS} property The name of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#SERVER_SSL_PROTOCOLS} property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_PROTOCOLS}
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String SERVER_SSL_PROTOCOLS_NAME = SERVER_SSL_PROTOCOLS;

  /**
   * Returns the value of the {@link ConfigurationProperties#SERVER_SSL_CIPHERS} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLCiphers()} 
   */
  @Deprecated
  @ConfigAttributeGetter(name = SERVER_SSL_CIPHERS)
  String getServerSSLCiphers();

  /**
   * Sets the value of the {@link ConfigurationProperties#SERVER_SSL_CIPHERS} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLCiphers(String)} 
   */
  @Deprecated
  @ConfigAttributeSetter(name = SERVER_SSL_CIPHERS)
  void setServerSSLCiphers(String ciphers);

  /**
   * The default {@link ConfigurationProperties#SERVER_SSL_CIPHERS} value.
   * <p>
   * Actual value of this constant is <code>any</code>.
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_CIPHERS} 
   */
  @Deprecated
  String DEFAULT_SERVER_SSL_CIPHERS = "any";
  /**
   * The name of the {@link ConfigurationProperties#SERVER_SSL_CIPHERS} property The name of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#SERVER_SSL_CIPHERS} property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_CIPHERS} 
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String SERVER_SSL_CIPHERS_NAME = SERVER_SSL_CIPHERS;

  /**
   * Returns the value of the {@link ConfigurationProperties#SERVER_SSL_REQUIRE_AUTHENTICATION}
   * property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLRequireAuthentication()} 
   */
  @Deprecated
  @ConfigAttributeGetter(name = SERVER_SSL_REQUIRE_AUTHENTICATION)
  boolean getServerSSLRequireAuthentication();

  /**
   * Sets the value of the {@link ConfigurationProperties#SERVER_SSL_REQUIRE_AUTHENTICATION}
   * property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLRequireAuthentication(boolean)} 
   */
  @Deprecated
  @ConfigAttributeSetter(name = SERVER_SSL_REQUIRE_AUTHENTICATION)
  void setServerSSLRequireAuthentication(boolean enabled);

  /**
   * The default {@link ConfigurationProperties#SERVER_SSL_REQUIRE_AUTHENTICATION} value.
   * <p>
   * Actual value of this constant is <code>true</code>.
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_REQUIRE_AUTHENTICATION}
   */
  @Deprecated
  boolean DEFAULT_SERVER_SSL_REQUIRE_AUTHENTICATION = true;
  /**
   * The name of the {@link ConfigurationProperties#SERVER_SSL_REQUIRE_AUTHENTICATION} property The
   * name of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#SERVER_SSL_REQUIRE_AUTHENTICATION}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_REQUIRE_AUTHENTICATION}
   */
  @Deprecated
  @ConfigAttribute(type = Boolean.class)
  String SERVER_SSL_REQUIRE_AUTHENTICATION_NAME = SERVER_SSL_REQUIRE_AUTHENTICATION;

  /**
   * Returns the value of the {@link ConfigurationProperties#SERVER_SSL_KEYSTORE} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLKeyStore()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = SERVER_SSL_KEYSTORE)
  String getServerSSLKeyStore();

  /**
   * Sets the value of the {@link ConfigurationProperties#SERVER_SSL_KEYSTORE} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLKeyStore(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = SERVER_SSL_KEYSTORE)
  void setServerSSLKeyStore(String keyStore);

  /**
   * The default {@link ConfigurationProperties#SERVER_SSL_KEYSTORE} value.
   * <p>
   * Actual value of this constant is "".
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_KEYSTORE}
   */
  @Deprecated
  String DEFAULT_SERVER_SSL_KEYSTORE = "";

  /**
   * The name of the {@link ConfigurationProperties#SERVER_SSL_KEYSTORE} property The name of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#SERVER_SSL_KEYSTORE} property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_KEYSTORE}
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String SERVER_SSL_KEYSTORE_NAME = SERVER_SSL_KEYSTORE;

  /**
   * Returns the value of the {@link ConfigurationProperties#SERVER_SSL_KEYSTORE_TYPE} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLKeyStoreType()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = SERVER_SSL_KEYSTORE_TYPE)
  String getServerSSLKeyStoreType();

  /**
   * Sets the value of the {@link ConfigurationProperties#SERVER_SSL_KEYSTORE_TYPE} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLKeyStoreType(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = SERVER_SSL_KEYSTORE_TYPE)
  void setServerSSLKeyStoreType(String keyStoreType);

  /**
   * The default {@link ConfigurationProperties#SERVER_SSL_KEYSTORE_TYPE} value.
   * <p>
   * Actual value of this constant is "".
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_CLUSTER_SSL_KEYSTORE_TYPE}
   */
  @Deprecated
  String DEFAULT_SERVER_SSL_KEYSTORE_TYPE = "";

  /**
   * The name of the {@link ConfigurationProperties#SERVER_SSL_KEYSTORE_TYPE} property The name of
   * the {@link org.apache.geode.distributed.ConfigurationProperties#SERVER_SSL_KEYSTORE_TYPE}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_KEYSTORE_TYPE}
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String SERVER_SSL_KEYSTORE_TYPE_NAME = SERVER_SSL_KEYSTORE_TYPE;

  /**
   * Returns the value of the {@link ConfigurationProperties#SERVER_SSL_KEYSTORE_PASSWORD} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLKeyStorePassword()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = SERVER_SSL_KEYSTORE_PASSWORD)
  String getServerSSLKeyStorePassword();

  /**
   * Sets the value of the {@link ConfigurationProperties#SERVER_SSL_KEYSTORE_PASSWORD} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLKeyStorePassword(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = SERVER_SSL_KEYSTORE_PASSWORD)
  void setServerSSLKeyStorePassword(String keyStorePassword);

  /**
   * The default {@link ConfigurationProperties#SERVER_SSL_KEYSTORE_PASSWORD} value.
   * <p>
   * Actual value of this constant is "".
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_KEYSTORE_PASSWORD}
   */
  @Deprecated
  String DEFAULT_SERVER_SSL_KEYSTORE_PASSWORD = "";

  /**
   * The name of the {@link ConfigurationProperties#SERVER_SSL_KEYSTORE_PASSWORD} property The name
   * of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#SERVER_SSL_KEYSTORE_PASSWORD}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_KEYSTORE_PASSWORD}
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String SERVER_SSL_KEYSTORE_PASSWORD_NAME = SERVER_SSL_KEYSTORE_PASSWORD;

  /**
   * Returns the value of the {@link ConfigurationProperties#SERVER_SSL_TRUSTSTORE} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLTrustStore()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = SERVER_SSL_TRUSTSTORE)
  String getServerSSLTrustStore();

  /**
   * Sets the value of the {@link ConfigurationProperties#SERVER_SSL_TRUSTSTORE} property.
   *
   * @deprecated Geode 1.0 use {@link #setServerSSLTrustStore(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = SERVER_SSL_TRUSTSTORE)
  void setServerSSLTrustStore(String trustStore);

  /**
   * The default {@link ConfigurationProperties#SERVER_SSL_TRUSTSTORE} value.
   * <p>
   * Actual value of this constant is "".
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_TRUSTSTORE}
   */
  String DEFAULT_SERVER_SSL_TRUSTSTORE = "";

  /**
   * The name of the {@link ConfigurationProperties#SERVER_SSL_TRUSTSTORE} property The name of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#SERVER_SSL_TRUSTSTORE} property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_TRUSTSTORE}
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String SERVER_SSL_TRUSTSTORE_NAME = SERVER_SSL_TRUSTSTORE;

  /**
   * Returns the value of the {@link ConfigurationProperties#SERVER_SSL_TRUSTSTORE_PASSWORD}
   * property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLTrustStorePassword()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = SERVER_SSL_TRUSTSTORE_PASSWORD)
  String getServerSSLTrustStorePassword();

  /**
   * Sets the value of the {@link ConfigurationProperties#SERVER_SSL_TRUSTSTORE_PASSWORD} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLTrustStorePassword(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = SERVER_SSL_TRUSTSTORE_PASSWORD)
  void setServerSSLTrustStorePassword(String trusStorePassword);

  /**
   * The default {@link ConfigurationProperties#SERVER_SSL_TRUSTSTORE_PASSWORD} value.
   * <p>
   * Actual value of this constant is "".
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_TRUSTSTORE_PASSWORD}
   */
  @Deprecated
  String DEFAULT_SERVER_SSL_TRUSTSTORE_PASSWORD = "";

  /**
   * The name of the {@link ConfigurationProperties#SERVER_SSL_TRUSTSTORE_PASSWORD} property The
   * name of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#SERVER_SSL_TRUSTSTORE_PASSWORD}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_TRUSTSTORE_PASSWORD}
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String SERVER_SSL_TRUSTSTORE_PASSWORD_NAME = SERVER_SSL_TRUSTSTORE_PASSWORD;

  /**
   * Returns the value of the {@link ConfigurationProperties#GATEWAY_SSL_ENABLED} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLEnabled()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = GATEWAY_SSL_ENABLED)
  boolean getGatewaySSLEnabled();

  /**
   * The default {@link ConfigurationProperties#GATEWAY_SSL_ENABLED} state.
   * <p>
   * Actual value of this constant is <code>false</code>.
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_ENABLED}
   */
  @Deprecated
  boolean DEFAULT_GATEWAY_SSL_ENABLED = false;
  /**
   * The name of the {@link ConfigurationProperties#GATEWAY_SSL_ENABLED} property The name of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#GATEWAY_SSL_ENABLED} property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_ENABLED}
   */
  @Deprecated
  @ConfigAttribute(type = Boolean.class)
  String GATEWAY_SSL_ENABLED_NAME = GATEWAY_SSL_ENABLED;

  /**
   * Sets the value of the {@link ConfigurationProperties#GATEWAY_SSL_ENABLED} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLEnabled()}
   */
  @Deprecated
  @ConfigAttributeSetter(name = GATEWAY_SSL_ENABLED)
  void setGatewaySSLEnabled(boolean enabled);

  /**
   * Returns the value of the {@link ConfigurationProperties#GATEWAY_SSL_PROTOCOLS} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLProtocols()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = GATEWAY_SSL_PROTOCOLS)
  String getGatewaySSLProtocols();

  /**
   * Sets the value of the {@link ConfigurationProperties#GATEWAY_SSL_PROTOCOLS} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLTrustStorePassword(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = GATEWAY_SSL_PROTOCOLS)
  void setGatewaySSLProtocols(String protocols);

  /**
   * The default {@link ConfigurationProperties#GATEWAY_SSL_PROTOCOLS} value.
   * <p>
   * Actual value of this constant is <code>any</code>.
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_PROTOCOLS}
   */
  String DEFAULT_GATEWAY_SSL_PROTOCOLS = "any";
  /**
   * The name of the {@link ConfigurationProperties#GATEWAY_SSL_PROTOCOLS} property The name of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#GATEWAY_SSL_PROTOCOLS} property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_PROTOCOLS}
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String GATEWAY_SSL_PROTOCOLS_NAME = GATEWAY_SSL_PROTOCOLS;

  /**
   * Returns the value of the {@link ConfigurationProperties#GATEWAY_SSL_CIPHERS} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLCiphers()} 
   */
  @Deprecated
  @ConfigAttributeGetter(name = GATEWAY_SSL_CIPHERS)
  String getGatewaySSLCiphers();

  /**
   * Sets the value of the {@link ConfigurationProperties#GATEWAY_SSL_CIPHERS} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLCiphers(String)} 
   */
  @Deprecated
  @ConfigAttributeSetter(name = GATEWAY_SSL_CIPHERS)
  void setGatewaySSLCiphers(String ciphers);

  /**
   * The default {@link ConfigurationProperties#GATEWAY_SSL_CIPHERS} value.
   * <p>
   * Actual value of this constant is <code>any</code>.
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_CIPHERS} 
   */
  String DEFAULT_GATEWAY_SSL_CIPHERS = "any";
  /**
   * The name of the {@link ConfigurationProperties#GATEWAY_SSL_CIPHERS} property The name of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#GATEWAY_SSL_CIPHERS} property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_CIPHERS} 
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String GATEWAY_SSL_CIPHERS_NAME = GATEWAY_SSL_CIPHERS;

  /**
   * Returns the value of the {@link ConfigurationProperties#GATEWAY_SSL_REQUIRE_AUTHENTICATION}
   * property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLRequireAuthentication()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = GATEWAY_SSL_REQUIRE_AUTHENTICATION)
  boolean getGatewaySSLRequireAuthentication();

  /**
   * Sets the value of the {@link ConfigurationProperties#GATEWAY_SSL_REQUIRE_AUTHENTICATION}
   * property.
   *
   * @deprecated Geode 1.0 use {@link #setGatewaySSLRequireAuthentication(boolean)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = GATEWAY_SSL_REQUIRE_AUTHENTICATION)
  void setGatewaySSLRequireAuthentication(boolean enabled);

  /**
   * The default {@link ConfigurationProperties#GATEWAY_SSL_REQUIRE_AUTHENTICATION} value.
   * <p>
   * Actual value of this constant is <code>true</code>.
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_REQUIRE_AUTHENTICATION}
   */
  @Deprecated
  boolean DEFAULT_GATEWAY_SSL_REQUIRE_AUTHENTICATION = true;
  /**
   * The name of the {@link ConfigurationProperties#GATEWAY_SSL_REQUIRE_AUTHENTICATION} property The
   * name of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#GATEWAY_SSL_REQUIRE_AUTHENTICATION}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_REQUIRE_AUTHENTICATION}
   */
  @Deprecated
  @ConfigAttribute(type = Boolean.class)
  String GATEWAY_SSL_REQUIRE_AUTHENTICATION_NAME = GATEWAY_SSL_REQUIRE_AUTHENTICATION;

  /**
   * Returns the value of the {@link ConfigurationProperties#GATEWAY_SSL_KEYSTORE} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLKeyStore()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = GATEWAY_SSL_KEYSTORE)
  String getGatewaySSLKeyStore();

  /**
   * Sets the value of the {@link ConfigurationProperties#GATEWAY_SSL_KEYSTORE} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLKeyStore(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = GATEWAY_SSL_KEYSTORE)
  void setGatewaySSLKeyStore(String keyStore);

  /**
   * The default {@link ConfigurationProperties#GATEWAY_SSL_KEYSTORE} value.
   * <p>
   * Actual value of this constant is "".
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_KEYSTORE}
   */
  @Deprecated
  String DEFAULT_GATEWAY_SSL_KEYSTORE = "";

  /**
   * The name of the {@link ConfigurationProperties#GATEWAY_SSL_KEYSTORE} property The name of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#GATEWAY_SSL_KEYSTORE} property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_KEYSTORE}
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String GATEWAY_SSL_KEYSTORE_NAME = GATEWAY_SSL_KEYSTORE;

  /**
   * Returns the value of the {@link ConfigurationProperties#GATEWAY_SSL_KEYSTORE_TYPE} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLKeyStoreType()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = GATEWAY_SSL_KEYSTORE_TYPE)
  String getGatewaySSLKeyStoreType();

  /**
   * Sets the value of the {@link ConfigurationProperties#GATEWAY_SSL_KEYSTORE_TYPE} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLKeyStoreType(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = GATEWAY_SSL_KEYSTORE_TYPE)
  void setGatewaySSLKeyStoreType(String keyStoreType);

  /**
   * The default {@link ConfigurationProperties#GATEWAY_SSL_KEYSTORE_TYPE} value.
   * <p>
   * Actual value of this constant is "".
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_CLUSTER_SSL_KEYSTORE_TYPE}
   */
  @Deprecated
  String DEFAULT_GATEWAY_SSL_KEYSTORE_TYPE = "";

  /**
   * The name of the {@link ConfigurationProperties#GATEWAY_SSL_KEYSTORE_TYPE} property The name of
   * the {@link org.apache.geode.distributed.ConfigurationProperties#GATEWAY_SSL_KEYSTORE_TYPE}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_KEYSTORE_TYPE}
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String GATEWAY_SSL_KEYSTORE_TYPE_NAME = GATEWAY_SSL_KEYSTORE_TYPE;

  /**
   * Returns the value of the {@link ConfigurationProperties#GATEWAY_SSL_KEYSTORE_PASSWORD}
   * property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLKeyStorePassword()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = GATEWAY_SSL_KEYSTORE_PASSWORD)
  String getGatewaySSLKeyStorePassword();

  /**
   * Sets the value of the {@link ConfigurationProperties#GATEWAY_SSL_KEYSTORE_PASSWORD} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLKeyStorePassword(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = GATEWAY_SSL_KEYSTORE_PASSWORD)
  void setGatewaySSLKeyStorePassword(String keyStorePassword);

  /**
   * The default {@link ConfigurationProperties#GATEWAY_SSL_KEYSTORE_PASSWORD} value.
   * <p>
   * Actual value of this constant is "".
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_KEYSTORE_PASSWORD}
   */
  @Deprecated
  String DEFAULT_GATEWAY_SSL_KEYSTORE_PASSWORD = "";

  /**
   * The name of the {@link ConfigurationProperties#GATEWAY_SSL_KEYSTORE_PASSWORD} property The name
   * of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#GATEWAY_SSL_KEYSTORE_PASSWORD}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_KEYSTORE_PASSWORD}
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String GATEWAY_SSL_KEYSTORE_PASSWORD_NAME = GATEWAY_SSL_KEYSTORE_PASSWORD;

  /**
   * Returns the value of the {@link ConfigurationProperties#GATEWAY_SSL_TRUSTSTORE} property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLTrustStore()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = GATEWAY_SSL_TRUSTSTORE)
  String getGatewaySSLTrustStore();

  /**
   * Sets the value of the {@link ConfigurationProperties#GATEWAY_SSL_TRUSTSTORE} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLTrustStore(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = GATEWAY_SSL_TRUSTSTORE)
  void setGatewaySSLTrustStore(String trustStore);

  /**
   * The default {@link ConfigurationProperties#GATEWAY_SSL_TRUSTSTORE} value.
   * <p>
   * Actual value of this constant is "".
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_TRUSTSTORE}
   */
  @Deprecated
  String DEFAULT_GATEWAY_SSL_TRUSTSTORE = "";

  /**
   * The name of the {@link ConfigurationProperties#GATEWAY_SSL_TRUSTSTORE} property The name of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#GATEWAY_SSL_TRUSTSTORE} property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_TRUSTSTORE}
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String GATEWAY_SSL_TRUSTSTORE_NAME = GATEWAY_SSL_TRUSTSTORE;

  /**
   * Returns the value of the {@link ConfigurationProperties#GATEWAY_SSL_TRUSTSTORE_PASSWORD}
   * property.
   *
   * @deprecated Geode 1.0 use {@link #getClusterSSLTrustStorePassword()}
   */
  @Deprecated
  @ConfigAttributeGetter(name = GATEWAY_SSL_TRUSTSTORE_PASSWORD)
  String getGatewaySSLTrustStorePassword();

  /**
   * Sets the value of the {@link ConfigurationProperties#GATEWAY_SSL_TRUSTSTORE_PASSWORD} property.
   *
   * @deprecated Geode 1.0 use {@link #setClusterSSLKeyStorePassword(String)}
   */
  @Deprecated
  @ConfigAttributeSetter(name = GATEWAY_SSL_TRUSTSTORE_PASSWORD)
  void setGatewaySSLTrustStorePassword(String trusStorePassword);

  /**
   * The default {@link ConfigurationProperties#GATEWAY_SSL_TRUSTSTORE_PASSWORD} value.
   * <p>
   * Actual value of this constant is "".
   *
   * @deprecated Geode 1.0 use {@link #DEFAULT_SSL_TRUSTSTORE_PASSWORD}
   */
  @Deprecated
  String DEFAULT_GATEWAY_SSL_TRUSTSTORE_PASSWORD = "";

  /**
   * The name of the {@link ConfigurationProperties#GATEWAY_SSL_TRUSTSTORE_PASSWORD} property The
   * name of the
   * {@link org.apache.geode.distributed.ConfigurationProperties#GATEWAY_SSL_TRUSTSTORE_PASSWORD}
   * property
   *
   * @deprecated Geode 1.0 use
   *             {@link org.apache.geode.distributed.ConfigurationProperties#CLUSTER_SSL_TRUSTSTORE_PASSWORD}
   */
  @Deprecated
  @ConfigAttribute(type = String.class)
  String GATEWAY_SSL_TRUSTSTORE_PASSWORD_NAME = GATEWAY_SSL_TRUSTSTORE_PASSWORD;

  /**
   * @deprecated Geode 1.0 use {@link #getClusterSSLProperties()}
   */
  @Deprecated
  Properties getGatewaySSLProperties();

  ConfigSource getConfigSource(String attName);

  /**
   * The name of the {@link ConfigurationProperties#LOCK_MEMORY} property. Used to cause pages to be
   * locked into memory, thereby preventing them from being swapped to disk.
   *
   * @since Geode 1.0
   */
  @ConfigAttribute(type = Boolean.class)
  String LOCK_MEMORY_NAME = LOCK_MEMORY;
  boolean DEFAULT_LOCK_MEMORY = false;

  /**
   * Gets the value of {@link ConfigurationProperties#LOCK_MEMORY}
   * <p>
   * Gets the value of {@link org.apache.geode.distributed.ConfigurationProperties#LOCK_MEMORY}
   *
   * @since Geode 1.0
   */
  @ConfigAttributeGetter(name = LOCK_MEMORY)
  boolean getLockMemory();

  /**
   * Set the value of {@link ConfigurationProperties#LOCK_MEMORY}
   *
   * @param value the new setting
   *
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


  /**
   * Returns the value of the {@link ConfigurationProperties#SSL_CLUSTER_ALIAS} property.
   *
   * @since Geode 1.0
   */
  @ConfigAttributeGetter(name = SSL_CLUSTER_ALIAS)
  String getClusterSSLAlias();

  /**
   * Sets the value of the {@link ConfigurationProperties#SSL_CLUSTER_ALIAS} property.
   *
   * @since Geode 1.0
   */
  @ConfigAttributeSetter(name = SSL_CLUSTER_ALIAS)
  void setClusterSSLAlias(String alias);

  /**
   * The Default Cluster SSL alias
   *
   * @since Geode 1.0
   */
  String DEFAULT_SSL_ALIAS = "";

  /**
   * The name of the {@link ConfigurationProperties#SSL_CLUSTER_ALIAS} property
   *
   * @since Geode 1.0
   */
  @ConfigAttribute(type = String.class)
  String CLUSTER_SSL_ALIAS_NAME = SSL_CLUSTER_ALIAS;

  /**
   * Returns the value of the {@link ConfigurationProperties#SSL_LOCATOR_ALIAS} property.
   *
   * @since Geode 1.0
   */
  @ConfigAttributeGetter(name = SSL_LOCATOR_ALIAS)
  String getLocatorSSLAlias();

  /**
   * Sets the value of the {@link ConfigurationProperties#SSL_LOCATOR_ALIAS} property.
   *
   * @since Geode 1.0
   */
  @ConfigAttributeSetter(name = SSL_LOCATOR_ALIAS)
  void setLocatorSSLAlias(String alias);

  /**
   * The name of the {@link ConfigurationProperties#SSL_LOCATOR_ALIAS} property
   *
   * @since Geode 1.0
   */
  @ConfigAttribute(type = String.class)
  String LOCATOR_SSL_ALIAS_NAME = SSL_LOCATOR_ALIAS;

  /**
   * Returns the value of the {@link ConfigurationProperties#SSL_GATEWAY_ALIAS} property.
   *
   * @since Geode 1.0
   */
  @ConfigAttributeGetter(name = SSL_GATEWAY_ALIAS)
  String getGatewaySSLAlias();

  /**
   * Sets the value of the {@link ConfigurationProperties#SSL_GATEWAY_ALIAS} property.
   *
   * @since Geode 1.0
   */
  @ConfigAttributeSetter(name = SSL_GATEWAY_ALIAS)
  void setGatewaySSLAlias(String alias);

  /**
   * The name of the {@link ConfigurationProperties#SSL_GATEWAY_ALIAS} property
   *
   * @since Geode 1.0
   */
  @ConfigAttribute(type = String.class)
  String GATEWAY_SSL_ALIAS_NAME = SSL_GATEWAY_ALIAS;

  /**
   * Returns the value of the {@link ConfigurationProperties#SSL_CLUSTER_ALIAS} property.
   *
   * @since Geode 1.0
   */
  @ConfigAttributeGetter(name = SSL_WEB_ALIAS)
  String getHTTPServiceSSLAlias();

  /**
   * Sets the value of the {@link ConfigurationProperties#SSL_WEB_ALIAS} property.
   *
   * @since Geode 1.0
   */
  @ConfigAttributeSetter(name = SSL_WEB_ALIAS)
  void setHTTPServiceSSLAlias(String alias);

  /**
   * The name of the {@link ConfigurationProperties#SSL_WEB_ALIAS} property
   *
   * @since Geode 1.0
   */
  @ConfigAttribute(type = String.class)
  String HTTP_SERVICE_SSL_ALIAS_NAME = SSL_WEB_ALIAS;

  /**
   * Returns the value of the {@link ConfigurationProperties#SSL_JMX_ALIAS} property.
   *
   * @since Geode 1.0
   */
  @ConfigAttributeGetter(name = SSL_JMX_ALIAS)
  String getJMXSSLAlias();

  /**
   * Sets the value of the {@link ConfigurationProperties#SSL_JMX_ALIAS} property.
   *
   * @since Geode 1.0
   */
  @ConfigAttributeSetter(name = SSL_JMX_ALIAS)
  void setJMXSSLAlias(String alias);

  /**
   * The name of the {@link ConfigurationProperties#SSL_JMX_ALIAS} property
   *
   * @since Geode 1.0
   */
  @ConfigAttribute(type = String.class)
  String JMX_SSL_ALIAS_NAME = SSL_JMX_ALIAS;

  /**
   * Returns the value of the {@link ConfigurationProperties#SSL_SERVER_ALIAS} property.
   *
   * @since Geode 1.0
   */
  @ConfigAttributeGetter(name = SSL_SERVER_ALIAS)
  String getServerSSLAlias();

  /**
   * Sets the value of the {@link ConfigurationProperties#SSL_SERVER_ALIAS} property.
   *
   * @since Geode 1.0
   */
  @ConfigAttributeSetter(name = SSL_SERVER_ALIAS)
  void setServerSSLAlias(String alias);

  /**
   * The name of the {@link ConfigurationProperties#SSL_SERVER_ALIAS} property
   *
   * @since Geode 1.0
   */
  @ConfigAttribute(type = String.class)
  String SERVER_SSL_ALIAS_NAME = SSL_SERVER_ALIAS;

  /**
   * Returns the value of the {@link ConfigurationProperties#SSL_ENABLED_COMPONENTS} property.
   *
   * @since Geode 1.0
   */
  @ConfigAttributeGetter(name = SSL_ENABLED_COMPONENTS)
  SecurableCommunicationChannel[] getSecurableCommunicationChannels();

  /**
   * Sets the value of the {@link ConfigurationProperties#SSL_ENABLED_COMPONENTS} property.
   *
   * @since Geode 1.0
   */
  @ConfigAttributeSetter(name = SSL_ENABLED_COMPONENTS)
  void setSecurableCommunicationChannels(SecurableCommunicationChannel[] sslEnabledComponents);

  /**
   * The name of the {@link ConfigurationProperties#SSL_ENABLED_COMPONENTS} property
   *
   * @since Geode 1.0
   */
  @ConfigAttribute(type = SecurableCommunicationChannel[].class)
  String SSL_ENABLED_COMPONENTS_NAME = SSL_ENABLED_COMPONENTS;

  /**
   * The default ssl enabled components
   *
   * @since Geode 1.0
   */
  SecurableCommunicationChannel[] DEFAULT_SSL_ENABLED_COMPONENTS =
      new SecurableCommunicationChannel[] {};

  /**
   * Returns the value of the {@link ConfigurationProperties#SSL_PROTOCOLS} property.
   */
  @ConfigAttributeGetter(name = SSL_PROTOCOLS)
  String getSSLProtocols();

  /**
   * Sets the value of the {@link ConfigurationProperties#SSL_PROTOCOLS} property.
   */
  @ConfigAttributeSetter(name = SSL_PROTOCOLS)
  void setSSLProtocols(String protocols);

  /**
   * The name of the {@link ConfigurationProperties#SSL_PROTOCOLS} property
   */
  @ConfigAttribute(type = String.class)
  String SSL_PROTOCOLS_NAME = SSL_PROTOCOLS;

  /**
   * Returns the value of the {@link ConfigurationProperties#SSL_CIPHERS} property.
   */
  @ConfigAttributeGetter(name = SSL_CIPHERS)
  String getSSLCiphers();

  /**
   * Sets the value of the {@link ConfigurationProperties#SSL_CIPHERS} property.
   */
  @ConfigAttributeSetter(name = SSL_CIPHERS)
  void setSSLCiphers(String ciphers);

  /**
   * The name of the {@link ConfigurationProperties#SSL_CIPHERS} property
   */
  @ConfigAttribute(type = String.class)
  String SSL_CIPHERS_NAME = SSL_CIPHERS;

  /**
   * Returns the value of the {@link ConfigurationProperties#SSL_REQUIRE_AUTHENTICATION} property.
   */
  @ConfigAttributeGetter(name = SSL_REQUIRE_AUTHENTICATION)
  boolean getSSLRequireAuthentication();

  /**
   * Sets the value of the {@link ConfigurationProperties#SSL_REQUIRE_AUTHENTICATION} property.
   */
  @ConfigAttributeSetter(name = SSL_REQUIRE_AUTHENTICATION)
  void setSSLRequireAuthentication(boolean enabled);

  /**
   * The name of the {@link ConfigurationProperties#SSL_REQUIRE_AUTHENTICATION} property
   */
  @ConfigAttribute(type = Boolean.class)
  String SSL_REQUIRE_AUTHENTICATION_NAME = SSL_REQUIRE_AUTHENTICATION;

  /**
   * Returns the value of the {@link ConfigurationProperties#SSL_KEYSTORE} property.
   */
  @ConfigAttributeGetter(name = SSL_KEYSTORE)
  String getSSLKeyStore();

  /**
   * Sets the value of the {@link ConfigurationProperties#SSL_KEYSTORE} property.
   */
  @ConfigAttributeSetter(name = SSL_KEYSTORE)
  void setSSLKeyStore(String keyStore);

  /**
   * The name of the {@link ConfigurationProperties#SSL_KEYSTORE} property
   */
  @ConfigAttribute(type = String.class)
  String SSL_KEYSTORE_NAME = SSL_KEYSTORE;

  /**
   * Returns the value of the {@link ConfigurationProperties#SSL_KEYSTORE_TYPE} property.
   */
  @ConfigAttributeGetter(name = SSL_KEYSTORE_TYPE)
  String getSSLKeyStoreType();

  /**
   * Sets the value of the {@link ConfigurationProperties#SSL_KEYSTORE_TYPE} property.
   */
  @ConfigAttributeSetter(name = SSL_KEYSTORE_TYPE)
  void setSSLKeyStoreType(String keyStoreType);

  /**
   * The name of the {@link ConfigurationProperties#SSL_KEYSTORE_TYPE} property
   */
  @ConfigAttribute(type = String.class)
  String SSL_KEYSTORE_TYPE_NAME = SSL_KEYSTORE_TYPE;

  /**
   * Returns the value of the {@link ConfigurationProperties#SSL_KEYSTORE_PASSWORD} property.
   */
  @ConfigAttributeGetter(name = SSL_KEYSTORE_PASSWORD)
  String getSSLKeyStorePassword();

  /**
   * Sets the value of the {@link ConfigurationProperties#SSL_KEYSTORE_PASSWORD} property.
   */
  @ConfigAttributeSetter(name = SSL_KEYSTORE_PASSWORD)
  void setSSLKeyStorePassword(String keyStorePassword);

  /**
   * The name of the {@link ConfigurationProperties#SSL_KEYSTORE_PASSWORD} property
   */
  @ConfigAttribute(type = String.class)
  String SSL_KEYSTORE_PASSWORD_NAME = SSL_KEYSTORE_PASSWORD;

  /**
   * Returns the value of the {@link ConfigurationProperties#SSL_TRUSTSTORE} property.
   */
  @ConfigAttributeGetter(name = SSL_TRUSTSTORE)
  String getSSLTrustStore();

  /**
   * Sets the value of the {@link ConfigurationProperties#SSL_TRUSTSTORE} property.
   */
  @ConfigAttributeSetter(name = SSL_TRUSTSTORE)
  void setSSLTrustStore(String trustStore);

  /**
   * The name of the {@link ConfigurationProperties#SSL_TRUSTSTORE} property
   */
  @ConfigAttribute(type = String.class)
  String SSL_TRUSTSTORE_NAME = SSL_TRUSTSTORE;

  /**
   * Returns the value of the {@link ConfigurationProperties#SSL_DEFAULT_ALIAS} property.
   */
  @ConfigAttributeGetter(name = SSL_DEFAULT_ALIAS)
  String getSSLDefaultAlias();

  /**
   * Sets the value of the {@link ConfigurationProperties#SSL_DEFAULT_ALIAS} property.
   */
  @ConfigAttributeSetter(name = SSL_DEFAULT_ALIAS)
  void setSSLDefaultAlias(String sslDefaultAlias);

  /**
   * The name of the {@link ConfigurationProperties#SSL_DEFAULT_ALIAS} property
   */
  @ConfigAttribute(type = String.class)
  String SSL_DEFAULT_ALIAS_NAME = SSL_DEFAULT_ALIAS;

  /**
   * Returns the value of the {@link ConfigurationProperties#SSL_TRUSTSTORE_PASSWORD} property.
   */
  @ConfigAttributeGetter(name = SSL_TRUSTSTORE_PASSWORD)
  String getSSLTrustStorePassword();

  /**
   * Sets the value of the {@link ConfigurationProperties#SSL_TRUSTSTORE_PASSWORD} property.
   */
  @ConfigAttributeSetter(name = SSL_TRUSTSTORE_PASSWORD)
  void setSSLTrustStorePassword(String trustStorePassword);

  /**
   * The name of the {@link ConfigurationProperties#SSL_TRUSTSTORE_PASSWORD} property
   */
  @ConfigAttribute(type = String.class)
  String SSL_TRUSTSTORE_PASSWORD_NAME = SSL_TRUSTSTORE_PASSWORD;

  /**
   * Returns the value of the {@link ConfigurationProperties#SSL_TRUSTSTORE_TYPE} property.
   */
  @ConfigAttributeGetter(name = SSL_TRUSTSTORE_TYPE)
  String getSSLTrustStoreType();

  /**
   * Sets the value of the {@link ConfigurationProperties#SSL_TRUSTSTORE_TYPE} property.
   */
  @ConfigAttributeSetter(name = SSL_TRUSTSTORE_TYPE)
  void setSSLTrustStoreType(String trustStoreType);


  /**
   * The name of the {@link ConfigurationProperties#SSL_TRUSTSTORE_TYPE} property
   */
  @ConfigAttribute(type = String.class)
  String SSL_TRUSTSTORE_TYPE_NAME = SSL_TRUSTSTORE_TYPE;

  /**
   * Returns the value of the {@link ConfigurationProperties#SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION}
   * property.
   */
  @ConfigAttributeGetter(name = SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION)
  boolean getSSLWebRequireAuthentication();

  /**
   * Sets the value of the {@link ConfigurationProperties#SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION}
   * property.
   */
  @ConfigAttributeSetter(name = SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION)
  void setSSLWebRequireAuthentication(boolean requiresAuthentication);

  /**
   * The name of the {@link ConfigurationProperties#SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION} property
   */
  @ConfigAttribute(type = Boolean.class)
  String SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION_NAME = SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION;

  /**
   * The default value for http service ssl mutual authentication
   */
  boolean DEFAULT_SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION = false;

  /**
   * Returns the value of the {@link ConfigurationProperties#VALIDATE_SERIALIZABLE_OBJECTS} property
   */
  @ConfigAttributeGetter(name = VALIDATE_SERIALIZABLE_OBJECTS)
  boolean getValidateSerializableObjects();

  /**
   * Sets the value of the {@link ConfigurationProperties#VALIDATE_SERIALIZABLE_OBJECTS} property
   */
  @ConfigAttributeSetter(name = VALIDATE_SERIALIZABLE_OBJECTS)
  void setValidateSerializableObjects(boolean value);

  /**
   * The name of the {@link ConfigurationProperties#VALIDATE_SERIALIZABLE_OBJECTS} property
   */
  @ConfigAttribute(type = Boolean.class)
  String VALIDATE_SERIALIZABLE_OBJECTS_NAME = VALIDATE_SERIALIZABLE_OBJECTS;

  /**
   * The default value of the {@link ConfigurationProperties#VALIDATE_SERIALIZABLE_OBJECTS}
   * property.
   */
  boolean DEFAULT_VALIDATE_SERIALIZABLE_OBJECTS = false;

  /**
   * Returns the value of the {@link ConfigurationProperties#SERIALIZABLE_OBJECT_FILTER} property
   */
  @ConfigAttributeGetter(name = SERIALIZABLE_OBJECT_FILTER)
  String getSerializableObjectFilter();

  /**
   * Sets the value of the {@link ConfigurationProperties#SERIALIZABLE_OBJECT_FILTER} property
   */
  @ConfigAttributeSetter(name = SERIALIZABLE_OBJECT_FILTER)
  void setSerializableObjectFilter(String value);

  /**
   * The name of the {@link ConfigurationProperties#SERIALIZABLE_OBJECT_FILTER} property
   */
  @ConfigAttribute(type = String.class)
  String SERIALIZABLE_OBJECT_FILTER_NAME = SERIALIZABLE_OBJECT_FILTER;

  /**
   * The default value of the {@link ConfigurationProperties#SERIALIZABLE_OBJECT_FILTER} property.
   * Current value is a pattern for rejecting everything <code>"!*"</code>
   */
  String DEFAULT_SERIALIZABLE_OBJECT_FILTER = "!*";

  // *************** Initializers to gather all the annotations in this class
  // ************************

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

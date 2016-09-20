/*
 *
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
 *
 */

package org.apache.geode.tools.pulse.internal;

import org.apache.geode.tools.pulse.internal.controllers.PulseController;
import org.apache.geode.tools.pulse.internal.data.PulseConfig;
import org.apache.geode.tools.pulse.internal.data.PulseConstants;
import org.apache.geode.tools.pulse.internal.data.Repository;
import org.apache.geode.tools.pulse.internal.log.PulseLogWriter;
import org.apache.geode.tools.pulse.internal.util.StringUtils;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.logging.Level;

/**
 * This class is used for checking the application running mode i.e. Embedded or
 * not
 * 
 * @since GemFire version 7.0.Beta 2012-09-23
 * 
 */
// @WebListener
public class PulseAppListener implements ServletContextListener {
  private PulseLogWriter LOGGER;
  private final ResourceBundle resourceBundle = Repository.get()
      .getResourceBundle();

  // String object to store all messages which needs to be logged into the log
  // file before logger gets initialized
  private String messagesToBeLogged = "";

  private Properties pulseProperties;
  private Properties pulseSecurityProperties;
  private Boolean sysPulseUseLocator;
  private String sysPulseHost;
  private String sysPulsePort;
  private String jmxUserName;
  private String jmxUserPassword;
  
  private boolean sysPulseUseSSLLocator;
  private boolean sysPulseUseSSLManager;
  
  //This property determines if pulse webApp login is authenticated against
  //GemFire integrated security or custom spring-security config provided 
  //in pulse-authentication-custom.xml 
  private boolean useGemFireCredentials;

  @Override
  public void contextDestroyed(ServletContextEvent event) {

    // Stop all running threads those are created in Pulse
    // Stop cluster threads
    Repository.get().removeAllClusters();

    if (LOGGER.infoEnabled()) {
      LOGGER.info(resourceBundle.getString("LOG_MSG_CONTEXT_DESTROYED")
          + event.getServletContext().getContextPath());
    }
  }

  @Override
  public void contextInitialized(ServletContextEvent event) {
    
    messagesToBeLogged = messagesToBeLogged
        .concat(formatLogString(resourceBundle
            .getString("LOG_MSG_CONTEXT_INITIALIZED")));

    // Load Pulse version details
    loadPulseVersionDetails();

    // Load Pulse Properties
    pulseProperties = loadProperties(PulseConstants.PULSE_PROPERTIES_FILE);

    if (pulseProperties.isEmpty()) {
      messagesToBeLogged = messagesToBeLogged
          .concat(formatLogString(resourceBundle
              .getString("LOG_MSG_PROPERTIES_NOT_FOUND")));
    } else {
      messagesToBeLogged = messagesToBeLogged
          .concat(formatLogString(resourceBundle
              .getString("LOG_MSG_PROPERTIES_FOUND")));

      // set Pulse product support into the Pulse controller for access from
      // client side
      // to display the appropriate ui depending on which product is supported
      // in present deployment
      String pulseProduct = pulseProperties.getProperty(PulseConstants.APPLICATION_PROPERTY_PULSE_PRODUCTSUPPORT);
      // default is gemfire

      if ((pulseProduct != null) && (pulseProduct.trim().equalsIgnoreCase(PulseConstants.PRODUCT_NAME_SQLFIRE))) {
        PulseController.setPulseProductSupport(PulseConstants.PRODUCT_NAME_SQLFIRE);
      }
    }
    
    pulseSecurityProperties = loadProperties(PulseConstants.PULSE_SECURITY_PROPERTIES_FILE);

    // Initialize logger
    initializeLogger();

    // Reference to repository
    Repository repository = Repository.get();

    if (LOGGER.infoEnabled()) {
      LOGGER.info(resourceBundle.getString("LOG_MSG_CHECK_APP_RUNNING_MODE"));
    }

    boolean sysIsEmbedded = Boolean
        .getBoolean(PulseConstants.SYSTEM_PROPERTY_PULSE_EMBEDDED);

    if (sysIsEmbedded) {
      // Application Pulse is running in Embedded Mode
      if (LOGGER.infoEnabled()) {
        LOGGER.info(resourceBundle
            .getString("LOG_MSG_APP_RUNNING_EMBEDDED_MODE"));
      }
      repository.setIsEmbeddedMode(true);

      sysPulseUseLocator = Boolean.FALSE;
	  try{
				// Get host name of machine running pulse in embedded mode
		   sysPulseHost = InetAddress.getLocalHost().getCanonicalHostName();
		} catch (UnknownHostException e) {
			if (LOGGER.fineEnabled()) {
				LOGGER.fine(resourceBundle
							.getString("LOG_MSG_JMX_CONNECTION_UNKNOWN_HOST")
							+ e.getMessage());
		    }
				// Set default host name
		    sysPulseHost = PulseConstants.GEMFIRE_DEFAULT_HOST;
		} catch (Exception e) {
			if (LOGGER.fineEnabled()) {
					LOGGER.fine(resourceBundle
							.getString("LOG_MSG_JMX_CONNECTION_UNKNOWN_HOST")
							+ e.getMessage());
			}
				// Set default host name
			sysPulseHost = PulseConstants.GEMFIRE_DEFAULT_HOST;
		}
      sysPulsePort = PulseConstants.GEMFIRE_DEFAULT_PORT;
      
      boolean pulseEmbededSqlf = Boolean.getBoolean(PulseConstants.SYSTEM_PROPERTY_PULSE_EMBEDDED_SQLF);
      if(pulseEmbededSqlf){
        PulseController.setPulseProductSupport(PulseConstants.PRODUCT_NAME_SQLFIRE);
        if (LOGGER.infoEnabled()) {
          LOGGER.info(resourceBundle
              .getString("LOG_MSG_APP_RUNNING_EMBEDDED_SQLF_MODE"));
        }
      }

    } else {
      // Application Pulse is running in Non-Embedded Mode
      if (LOGGER.infoEnabled()) {
        LOGGER.info(resourceBundle
            .getString("LOG_MSG_APP_RUNNING_NONEMBEDDED_MODE"));
      }
      repository.setIsEmbeddedMode(false);

      // Load JMX User Details
      loadJMXUserDetails();
      // Load locator and/or manager details
      loadLocatorManagerDetails();
       
      useGemFireCredentials = areWeUsingGemFireSecurityProfile(event); 
    }

    // Set user details in repository    
    repository.setJmxUserName(jmxUserName);
    repository.setJmxUserPassword(jmxUserPassword);

    // Set locator/Manager details in repository
    repository.setJmxUseLocator(sysPulseUseLocator);
    repository.setJmxHost(sysPulseHost);
    repository.setJmxPort(sysPulsePort);
    
    //set SSL info
    initializeSSL();
    repository.setUseSSLLocator(sysPulseUseSSLLocator);
    repository.setUseSSLManager(sysPulseUseSSLManager);
    
    repository.setUseGemFireCredentials(useGemFireCredentials);

  }

  /**
   * Return true if pulse is configure to authenticate using gemfire
   * integrated security
   * 
   * @param event
   * @return
   */
  private boolean areWeUsingGemFireSecurityProfile(ServletContextEvent event) {
    String profile = null;
    WebApplicationContext ctx = WebApplicationContextUtils.getWebApplicationContext(event.getServletContext());
    if (ctx.getEnvironment() != null) {
      String[] profiles = ctx.getEnvironment().getActiveProfiles();
      if (profiles != null && profiles.length > 0) {
        StringBuilder sb = new StringBuilder();
        for (String p : profiles)
          sb.append(p).append(",");
        LOGGER.info("#SpringProfilesConfigured : " + sb.toString());
        profile = ctx.getEnvironment().getActiveProfiles()[0];
        LOGGER.info("#First Profile : " + profile);
      } else {
        LOGGER.info("No SpringProfileConfigured using default spring profile");
        return false;
      }
    }
    if (PulseConstants.APPLICATION_PROPERTY_PULSE_SEC_PROFILE_GEMFIRE.equals(profile)) {
      LOGGER.info("Using gemfire integrated security profile");
      return true;
    }      
    return false;
  }

  // Function to load pulse version details from properties file
  private void loadPulseVersionDetails() {

    // Read version details from version property file
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    InputStream inputStream = classLoader
        .getResourceAsStream(PulseConstants.PULSE_VERSION_PROPERTIES_FILE);

    if (inputStream != null) {
      Properties properties = new Properties();
      try {
        properties.load(inputStream);
      } catch (IOException e) {
        messagesToBeLogged = messagesToBeLogged
            .concat(formatLogString(resourceBundle
                .getString("LOG_MSG_EXCEPTION_LOADING_PROPERTIES_FILE")));
      } finally {
        try {
          inputStream.close();
        } catch (IOException e) {
          messagesToBeLogged = messagesToBeLogged
              .concat(formatLogString(resourceBundle
                  .getString("LOG_MSG_EXCEPTION_CLOSING_INPUT_STREAM")));
        }
      }
      // Set pulse version details in common object
      PulseController.pulseVersion.setPulseVersion(properties.getProperty(
          PulseConstants.PROPERTY_PULSE_VERSION, ""));
      PulseController.pulseVersion.setPulseBuildId(properties.getProperty(
          PulseConstants.PROPERTY_BUILD_ID, ""));
      PulseController.pulseVersion.setPulseBuildDate(properties.getProperty(
          PulseConstants.PROPERTY_BUILD_DATE, ""));
      PulseController.pulseVersion.setPulseSourceDate(properties.getProperty(
          PulseConstants.PROPERTY_SOURCE_DATE, ""));
      PulseController.pulseVersion.setPulseSourceRevision(properties
          .getProperty(PulseConstants.PROPERTY_SOURCE_REVISION, ""));
      PulseController.pulseVersion.setPulseSourceRepository(properties
          .getProperty(PulseConstants.PROPERTY_SOURCE_REPOSITORY, ""));
    }

    // Log Pulse Version details into log file
    messagesToBeLogged = messagesToBeLogged
        .concat(formatLogString(PulseController.pulseVersion
            .getPulseVersionLogMessage()));
  }

  private void initializeLogger() {

    // Override default log configuration by properties which are provided in
    // properties file.
    loadLogDetailsFromPropertyFile();

    // Override log configuration by properties which are provided in
    // through system properties.
    loadLogDetailsFromSystemProperties();

    // Initialize logger object
    LOGGER = PulseLogWriter.getLogger();

    // Log messages stored in messagesToBeLogged
    if (LOGGER.infoEnabled()) {
      LOGGER.info(messagesToBeLogged);
      messagesToBeLogged = "";
    }
  }

  // Function to load pulse properties from pulse.properties file
  private Properties loadProperties(String propertyFile) {

    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    InputStream inputStream = classLoader.getResourceAsStream(propertyFile);
    Properties properties = new Properties();

    if (inputStream != null) {
      messagesToBeLogged = messagesToBeLogged.concat(formatLogString(propertyFile + " "
          + resourceBundle.getString("LOG_MSG_FILE_FOUND")));

      try {
        // Load properties from input stream
        properties.load(inputStream);
      } catch (IOException e1) {
        messagesToBeLogged = messagesToBeLogged.concat(formatLogString(resourceBundle
            .getString("LOG_MSG_EXCEPTION_LOADING_PROPERTIES_FILE")
            + " " + propertyFile));
      } finally {
        // Close input stream
        try {
          inputStream.close();
        } catch (IOException e) {
          messagesToBeLogged = messagesToBeLogged.concat(formatLogString(resourceBundle
              .getString("LOG_MSG_EXCEPTION_CLOSING_INPUT_STREAM")
              + " " + propertyFile));
        }
      }

    } else {
      messagesToBeLogged = messagesToBeLogged.concat(formatLogString(resourceBundle
          .getString("LOG_MSG_COULD_NOT_READ_FILE")
          + " " + propertyFile));
    }
    return properties;
  }

  // Function to load Logging details from properties file
  private void loadLogDetailsFromPropertyFile() {

    // return, if Pulse Properties are not provided
    if (pulseProperties.size() == 0) {
      return;
    }

    messagesToBeLogged = messagesToBeLogged
        .concat(formatLogString(resourceBundle
            .getString("LOG_MSG_CHECK_LOG_PROPERTIES_IN_FILE")));

    HashMap<String, String> logPropertiesHM = new HashMap<String, String>();

    logPropertiesHM.put(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILENAME,
        pulseProperties.getProperty(
            PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILENAME, ""));

    logPropertiesHM.put(
        PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILELOCATION,
        pulseProperties.getProperty(
            PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILELOCATION, ""));

    logPropertiesHM.put(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILESIZE,
        pulseProperties.getProperty(
            PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILESIZE, ""));

    logPropertiesHM.put(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILECOUNT,
        pulseProperties.getProperty(
            PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILECOUNT, ""));

    logPropertiesHM.put(
        PulseConstants.APPLICATION_PROPERTY_PULSE_LOGDATEPATTERN,
        pulseProperties.getProperty(
            PulseConstants.APPLICATION_PROPERTY_PULSE_LOGDATEPATTERN, ""));

    logPropertiesHM.put(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGLEVEL,
        pulseProperties.getProperty(
            PulseConstants.APPLICATION_PROPERTY_PULSE_LOGLEVEL, ""));

    logPropertiesHM.put(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGAPPEND,
        pulseProperties.getProperty(
            PulseConstants.APPLICATION_PROPERTY_PULSE_LOGAPPEND, ""));

    if (logPropertiesHM.size() == 0) {
      messagesToBeLogged = messagesToBeLogged
          .concat(formatLogString(resourceBundle
              .getString("LOG_MSG_LOG_PROPERTIES_NOT_FOUND_IN_FILE")));
    } else {
      messagesToBeLogged = messagesToBeLogged
          .concat(formatLogString(resourceBundle
              .getString("LOG_MSG_LOG_PROPERTIES_FOUND_IN_FILE")));
    }

    setLogConfigurations(logPropertiesHM);
  }

  // Function to load Logging details from system properties
  private void loadLogDetailsFromSystemProperties() {

    messagesToBeLogged = messagesToBeLogged
        .concat(formatLogString(resourceBundle
            .getString("LOG_MSG_CHECK_LOG_PROPERTIES_IN_SYSTEM_PROPERTIES")));

    HashMap<String, String> logPropertiesHM = new HashMap<String, String>();

    String sysLogFileName = System
        .getProperty(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILENAME);
    String sysLogFileLocation = System
        .getProperty(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILELOCATION);
    String sysLogFileSize = System
        .getProperty(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILESIZE);
    String sysLogFileCount = System
        .getProperty(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILECOUNT);
    String sysLogDatePattern = System
        .getProperty(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGDATEPATTERN);
    String sysLogLevel = System
        .getProperty(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGLEVEL);
    String sysLogAppend = System
        .getProperty(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGAPPEND);

    if (sysLogFileName == null || sysLogFileName.isEmpty()) {
      logPropertiesHM.put(
          PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILENAME, "");
    } else {
      logPropertiesHM
          .put(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILENAME,
              sysLogFileName);
    }

    if (sysLogFileLocation == null || sysLogFileLocation.isEmpty()) {
      logPropertiesHM.put(
          PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILELOCATION, "");
    } else {
      logPropertiesHM.put(
          PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILELOCATION,
          sysLogFileLocation);
    }

    if (sysLogFileSize == null || sysLogFileSize.isEmpty()) {
      logPropertiesHM.put(
          PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILESIZE, "");
    } else {
      logPropertiesHM
          .put(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILESIZE,
              sysLogFileSize);
    }

    if (sysLogFileCount == null || sysLogFileCount.isEmpty()) {
      logPropertiesHM.put(
          PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILECOUNT, "");
    } else {
      logPropertiesHM.put(
          PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILECOUNT,
          sysLogFileCount);
    }

    if (sysLogDatePattern == null || sysLogDatePattern.isEmpty()) {
      logPropertiesHM.put(
          PulseConstants.APPLICATION_PROPERTY_PULSE_LOGDATEPATTERN, "");
    } else {
      logPropertiesHM.put(
          PulseConstants.APPLICATION_PROPERTY_PULSE_LOGDATEPATTERN,
          sysLogDatePattern);
    }

    if (sysLogLevel == null || sysLogLevel.isEmpty()) {
      logPropertiesHM.put(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGLEVEL,
          "");
    } else {
      logPropertiesHM.put(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGLEVEL,
          sysLogLevel);
    }

    if (sysLogAppend == null || sysLogAppend.isEmpty()) {
      logPropertiesHM.put(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGAPPEND,
          "");
    } else {
      logPropertiesHM.put(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGAPPEND,
          sysLogAppend);
    }

    if (logPropertiesHM.size() == 0) {
      messagesToBeLogged = messagesToBeLogged
          .concat(formatLogString(resourceBundle
              .getString("LOG_MSG_LOG_PROPERTIES_NOT_FOUND_IN_SYSTEM_PROPERTIES")));
    } else {
      messagesToBeLogged = messagesToBeLogged
          .concat(formatLogString(resourceBundle
              .getString("LOG_MSG_LOG_PROPERTIES_FOUND_IN_SYSTEM_PROPERTIES")));
    }

    setLogConfigurations(logPropertiesHM);
  }

  private void setLogConfigurations(HashMap<String, String> logPropertiesHM) {

    PulseConfig pulseConfig = Repository.get().getPulseConfig();

    // log file name
    if (StringUtils.isNotNullNotEmptyNotWhiteSpace(logPropertiesHM
        .get(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILENAME))) {
      pulseConfig.setLogFileName(logPropertiesHM
          .get(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILENAME));
    }

    // log file location
    if (StringUtils.isNotNullNotEmptyNotWhiteSpace(logPropertiesHM
        .get(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILELOCATION))) {
      pulseConfig.setLogFileLocation(logPropertiesHM
          .get(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILELOCATION));
    }

    // log file size
    if (StringUtils.isNotNullNotEmptyNotWhiteSpace(logPropertiesHM
        .get(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILESIZE))) {
      pulseConfig.setLogFileSize(Integer.parseInt(logPropertiesHM
          .get(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILESIZE)));
    }

    // log file count
    if (StringUtils.isNotNullNotEmptyNotWhiteSpace(logPropertiesHM
        .get(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILECOUNT))) {
      pulseConfig.setLogFileCount(Integer.parseInt(logPropertiesHM
          .get(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGFILECOUNT)));
    }

    // log message date pattern
    if (StringUtils.isNotNullNotEmptyNotWhiteSpace(logPropertiesHM
        .get(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGDATEPATTERN))) {
      pulseConfig.setLogDatePattern(logPropertiesHM
          .get(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGDATEPATTERN));
    }

    // log level
    if (StringUtils.isNotNullNotEmptyNotWhiteSpace(logPropertiesHM
        .get(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGLEVEL))) {
      pulseConfig.setLogLevel(Level.parse(logPropertiesHM.get(
          PulseConstants.APPLICATION_PROPERTY_PULSE_LOGLEVEL).toUpperCase()));
    }

    // log append
    if (StringUtils.isNotNullNotEmptyNotWhiteSpace(logPropertiesHM
        .get(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGAPPEND))) {
      pulseConfig.setLogAppend(Boolean.valueOf(logPropertiesHM
          .get(PulseConstants.APPLICATION_PROPERTY_PULSE_LOGAPPEND)));
    }

  }

  // Function to load JMX User details from properties
  private void loadJMXUserDetails() {

    if (LOGGER.infoEnabled()) {
      LOGGER.info(resourceBundle.getString("LOG_MSG_GET_JMX_USER_DETAILS"));
    }

    if (pulseProperties.isEmpty()) {
      if (LOGGER.infoEnabled()) {
        LOGGER
            .info(resourceBundle
                .getString("LOG_MSG_JMX_USER_DETAILS_NOT_FOUND")
                + resourceBundle
                    .getString("LOG_MSG_REASON_USER_DETAILS_NOT_FOUND"));
      }
    } else {
      jmxUserName = pulseProperties.getProperty(
          PulseConstants.APPLICATION_PROPERTY_PULSE_JMXUSERNAME, "");
      jmxUserPassword = pulseProperties.getProperty(
          PulseConstants.APPLICATION_PROPERTY_PULSE_JMXPASSWORD, "");

      if (jmxUserName.isEmpty() || jmxUserPassword.isEmpty()) {
        if (LOGGER.infoEnabled()) {
          LOGGER.info(resourceBundle
              .getString("LOG_MSG_JMX_USER_DETAILS_NOT_FOUND")
              + resourceBundle
                  .getString("LOG_MSG_REASON_USER_DETAILS_NOT_FOUND"));
        }
      } else {
        if (LOGGER.infoEnabled()) {
          LOGGER.info(resourceBundle
              .getString("LOG_MSG_JMX_USER_DETAILS_FOUND"));
        }
      }
    }
  }
  
//Function to set SSL VM arguments
  private void initializeSSL() {
    if (LOGGER.infoEnabled()) {
      LOGGER.info(resourceBundle.getString("LOG_MSG_GET_SSL_DETAILS"));
    }

     
    this.sysPulseUseSSLLocator = Boolean.valueOf(pulseProperties.getProperty(
        PulseConstants.SYSTEM_PROPERTY_PULSE_USESSL_LOCATOR, "false"));

    this.sysPulseUseSSLManager = Boolean.valueOf(pulseProperties.getProperty(
        PulseConstants.SYSTEM_PROPERTY_PULSE_USESSL_MANAGER, "false"));


    if ((sysPulseUseSSLLocator || sysPulseUseSSLManager)) {
      Properties sslProperties = new Properties();
      if (!pulseSecurityProperties.isEmpty()) {
        Set entrySet = pulseSecurityProperties.entrySet();
        for (Iterator it = entrySet.iterator(); it.hasNext();) {
          Entry<String, String> entry = (Entry<String, String>) it.next();
          String key = entry.getKey();
          if (key.startsWith("javax.net.ssl.")) {

            String val = entry.getValue();
            System.setProperty(key, val);
            sslProperties.setProperty(key, val);
          }
        }
      }
      if (sslProperties.isEmpty()) {
        if (LOGGER.warningEnabled()) {
          LOGGER.warning(resourceBundle.getString("LOG_MSG_SSL_NOT_SET"));
        }
      }
    }

  }

  // Function to load locator and/or manager details
  private void loadLocatorManagerDetails() {

    // Get locator details through System Properties
    if (LOGGER.infoEnabled()) {
      LOGGER.info(resourceBundle.getString("LOG_MSG_GET_LOCATOR_DETAILS_1"));
    }

    // Required System properties are
    // -Dpulse.embedded="false" -Dpulse.useLocator="false"
    // -Dpulse.host="192.168.2.11" -Dpulse.port="2099"
    sysPulseUseLocator = Boolean
        .getBoolean(PulseConstants.SYSTEM_PROPERTY_PULSE_USELOCATOR);
    sysPulseHost = System
        .getProperty(PulseConstants.SYSTEM_PROPERTY_PULSE_HOST);
    sysPulsePort = System
        .getProperty(PulseConstants.SYSTEM_PROPERTY_PULSE_PORT);

    if (sysPulseHost == null || sysPulseHost.isEmpty() || sysPulsePort == null
        || sysPulsePort.isEmpty()) {
      if (LOGGER.infoEnabled()) {
        LOGGER.info(resourceBundle
            .getString("LOG_MSG_LOCATOR_DETAILS_NOT_FOUND")
            + resourceBundle
                .getString("LOG_MSG_REASON_LOCATOR_DETAILS_NOT_FOUND_1"));
        LOGGER.info(resourceBundle.getString("LOG_MSG_GET_LOCATOR_DETAILS_2"));
      }

      if (pulseProperties.isEmpty()) {
        if (LOGGER.infoEnabled()) {
          LOGGER.info(resourceBundle
              .getString("LOG_MSG_LOCATOR_DETAILS_NOT_FOUND")
              + resourceBundle
                  .getString("LOG_MSG_REASON_LOCATOR_DETAILS_NOT_FOUND_2"));
        }

        sysPulseHost = "";
        sysPulsePort = "";
      } else {
        if (LOGGER.infoEnabled()) {
          LOGGER
              .info(resourceBundle.getString("LOG_MSG_LOCATOR_DETAILS_FOUND"));
        }

        sysPulseUseLocator = Boolean.valueOf(pulseProperties.getProperty(
            PulseConstants.APPLICATION_PROPERTY_PULSE_USELOCATOR, ""));
        sysPulseHost = pulseProperties.getProperty(
            PulseConstants.APPLICATION_PROPERTY_PULSE_HOST, "");
        sysPulsePort = pulseProperties.getProperty(
            PulseConstants.APPLICATION_PROPERTY_PULSE_PORT, "");
      }
    } else {
      if (LOGGER.infoEnabled()) {
        LOGGER.info(resourceBundle.getString("LOG_MSG_LOCATOR_DETAILS_FOUND"));
      }
    }
  }

  private String formatLogString(String logMessage) {

    DateFormat df = new SimpleDateFormat(
        PulseConstants.LOG_MESSAGE_DATE_PATTERN);
    // DateFormat df = new
    // SimpleDateFormat(Repository.get().getPulseConfig().getLogDatePattern());
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);

    pw.println();
    pw.print("[");
    pw.print("INFO");
    pw.print(" ");
    pw.print(df.format(new Date(System.currentTimeMillis())));
    String threadName = Thread.currentThread().getName();
    if (threadName != null) {
      pw.print(" ");
      pw.print(threadName);
    }
    pw.print(" tid=0x");
    pw.print(Long.toHexString(Thread.currentThread().getId()));
    pw.print("] ");
    pw.print("(msgTID=");
    pw.print("");

    pw.print(" msgSN=");
    pw.print("");

    pw.print(") ");

    pw.println("[" + PulseConstants.APP_NAME + "]");

    pw.println(PulseLogWriter.class.getName());

    pw.println(logMessage);

    pw.close();
    try {
      sw.close();
    } catch (IOException ignore) {
    }
    String result = sw.toString();
    return result;

  }

}

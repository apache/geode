/*
 * ========================================================================= (c)
 * Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved. 1260 NW
 * Waterhouse Ave., Suite 200, Beaverton, OR 97006 All Rights Reserved.
 * ========================================================================
 */
package com.gemstone.gemfire.mgmt.DataBrowser.prefs;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

import com.gemstone.gemfire.mgmt.DataBrowser.app.VersionInfo;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.SecurityPropComposite.SecurityProp;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;


/**
 * This class represents the Preferences to be set for Data Browser. 
 * 
 * @author mjha
 */
public class DataBrowserPreferences extends PreferencesAdapter {
  // Connection Preferences key
  public static final String KEY_NO_CON_RETRY_ATTEMPTS          = "connection-retry-attempts";

  public static final String KEY_CON_RETRY_INTERVAL             = "connection-retry-interval";

  public static final String KEY_CONNECTION_HOSTS               = "connection-default-host";

  public static final String KEY_CONNECTION_PORTS               = "connection-default-port";

  public static final String KEY_MRU_HOST_PORT_LIST_LIMIT       = "mru-host-port-list-limit";

  public static final String KEY_CONNECTION_TIMEOUT             = "connection-timeout";

  // Query Preferences key
  public static final String KEY_QUERY_TIMEOUT                  = "query-timeout"; //TODO: This property is currently unused. Why was it added?
  
  public static final String KEY_SCOKET_READ_TIMEOUT            = "socket-read-timeout";

  public static final String KEY_QUERY_RESULT_LIMIT             = "query-result-limit";

  public static final String KEY_RESULT_AUTO_SCROLL             = "result-auto-scroll";

  public static final String KEY_ENFORCE_LIMIT_TO_QUERY_RESULT  = "enforce-limit-to-query";

  // Security Preferences key
  public static final String KEY_SEURITY_PLUGIN                 = "security-plugin";

  public static final String KEY_SEURITY_PROPS_HIDDEN           = "security-proprties-hidden";

  // Misc Preferences key
  public static final String KEY_LOGGING_LEVEL                  = "logging-level";

  public static final String KEY_DS_REFRESH_INTERVAL            = "ds-refresh-interval";

  public static final String KEY_APPLICATION_CLASSES            = "application-classes";

  public static final String KEY_LOG_DIR                        = "log-dir";
  
  public static final String KEY_LOG_FILE_SIZE                  = "log-file-size"; //In MB
  
  public static final String KEY_LOG_FILE_COUNT                 = "log-file-count";
  
  //Prefernce node
  private static final String CURRENT_USER                      = System.getProperty("user.name");

  private static final Preferences USER_ROOT                    = Preferences.userRoot();
  
  private static final String GEMSTONE_NAME                     = "GemStone";
  
  private static final String DATA_BROWSER_NAME                 = "Data Browser";
  
  private static final String DATA_BROWSER_VERSION              = VersionInfo.getInstance().toString();

  private static final Preferences DATA_BROWSER_NODE            = USER_ROOT.node(GEMSTONE_NAME).node(DATA_BROWSER_NAME).node(DATA_BROWSER_VERSION);
  
  private static final Preferences CURRENT_USER_NODE            = DATA_BROWSER_NODE.node(CURRENT_USER);
  
  private static final Preferences SECURITY_NODE                = CURRENT_USER_NODE.node("Security-Prop");
  
  private static final Preferences APPLICATION_JAR              = CURRENT_USER_NODE.node("Application-Jars");

  private static DataBrowserPreferences dbPrefernces_           = new DataBrowserPreferences(CURRENT_USER_NODE);
  
  private static DataBrowserPreferences dbSecurityProps_        = new DataBrowserPreferences(SECURITY_NODE);
  
  private static DataBrowserPreferences dbApplicationJars_      = new DataBrowserPreferences(APPLICATION_JAR);
  
  /**
   * Fix added for BUG711 to make sure that logging configuration is loaded only once during the application startup and any 
   * changes to the logging preferences are activated only after tool restart.
   **/
  private static String log_dir                                 = null;
  
  private static String log_level                               = null;  
  
  private static int log_file_size                              = DefaultPreferences.DEFAULT_LOG_FILE_SIZE;   
  
  private static int log_file_count                             = DefaultPreferences.DEFAULT_LOG_FILE_COUNT;   
  
  static {
    try {
      log_dir = dbPrefernces_.get(KEY_LOG_DIR, DefaultPreferences.DEFAULT_LOG_DIR);
    } catch (Throwable e) {
      //System.out.println("Failed to get the logging directory preference :"+e); // Since the Logger is not yet initialized...
      log_dir = DefaultPreferences.DEFAULT_LOG_DIR;
    }
    
    try {
      log_level = dbPrefernces_.get(KEY_LOGGING_LEVEL, DefaultPreferences.DEFAULT_LOGGING_LEVEL);
    } catch (Throwable e) {
      //System.out.println("Failed to get the log level preference :"+e); // Since the Logger is not yet initialized...
      log_level = DefaultPreferences.DEFAULT_LOGGING_LEVEL;
    }
    
    try {
      log_file_size = dbPrefernces_.getInt(KEY_LOG_FILE_SIZE, DefaultPreferences.DEFAULT_LOG_FILE_SIZE);
    } catch (Throwable e) {
      //System.out.println("Failed to get the log file size preference :"+e); // Since the Logger is not yet initialized...
      log_file_size = DefaultPreferences.DEFAULT_LOG_FILE_SIZE;
    }
    
    try {
      log_file_count = dbPrefernces_.getInt(KEY_LOG_FILE_COUNT, DefaultPreferences.DEFAULT_LOG_FILE_COUNT);
    } catch (Throwable e) {
      //System.out.println("Failed to get the log file count preference :"+e); // Since the Logger is not yet initialized...
      log_file_count = DefaultPreferences.DEFAULT_LOG_FILE_COUNT;
    }
  }
  
  private DataBrowserPreferences(Preferences toBeWrapped) {
    super(toBeWrapped);
  }
  
  public static void setConRetryAttempts(int val) {
    dbPrefernces_.put(KEY_NO_CON_RETRY_ATTEMPTS, String.valueOf(val));
  }

  public static int getConRetryAttempts() {
    return dbPrefernces_.getInt(KEY_NO_CON_RETRY_ATTEMPTS, DefaultPreferences.DEFAULT_NO_CON_RETRY_ATTEMPTS);
  }
  
  public static void setDefaultConRetryAttempts() {
    dbPrefernces_.put(KEY_NO_CON_RETRY_ATTEMPTS, String.valueOf(DefaultPreferences.DEFAULT_NO_CON_RETRY_ATTEMPTS));
  }

  public static void setConRetryInterval(long val) {
    dbPrefernces_.put(KEY_CON_RETRY_INTERVAL, String.valueOf(val));
  }

  public static long getConRetryInterval() {
    return dbPrefernces_.getLong(KEY_CON_RETRY_INTERVAL, DefaultPreferences.DEFAULT_CON_RETRY_INTERVAL);
  }
  
  public static void setDefaultConRetryInterval() {
    dbPrefernces_.put(KEY_CON_RETRY_INTERVAL, String.valueOf(DefaultPreferences.DEFAULT_CON_RETRY_INTERVAL));
  }
  
  public static long getConnectionTimeoutInterval() {
    return dbPrefernces_.getLong(KEY_CONNECTION_TIMEOUT, DefaultPreferences.DEFAULT_CONNECTION_TIMEOUT);
  }

  public static void setConnectionTimeoutInterval(long val) {
    dbPrefernces_.put(KEY_CONNECTION_TIMEOUT, String.valueOf(val));
  }
  
  public static void setDefaultConnectionTimeoutInterval() {
    dbPrefernces_.put(KEY_CONNECTION_TIMEOUT, String.valueOf(DefaultPreferences.DEFAULT_CONNECTION_TIMEOUT));
  }
  
  public static int getSocketReadTimeoutInterval() {
    return dbPrefernces_.getInt(KEY_SCOKET_READ_TIMEOUT, DefaultPreferences.DEFAULT_SOCKET_READ_TIMEOUT);
  }

  public static void setSocketReadTimeoutInterval(int val) {
    dbPrefernces_.put(KEY_SCOKET_READ_TIMEOUT, String.valueOf(val));
  }
  
  public static void setDefaultSocketReadTimeoutInterval() {
    dbPrefernces_.put(KEY_SCOKET_READ_TIMEOUT, String.valueOf(DefaultPreferences.DEFAULT_SOCKET_READ_TIMEOUT));
  }
  
  public static void setConnectionMRUHListLimit(int val) {
    dbPrefernces_.put(KEY_MRU_HOST_PORT_LIST_LIMIT, String.valueOf(val));
  }
  
  public static int getConnectionMRUHListLimit() {
   return dbPrefernces_.getInt(KEY_MRU_HOST_PORT_LIST_LIMIT,DefaultPreferences.DEFAULT_MRU_HOST_PORT_LIST_LIMIT);
  }
  
  public static void setDefaultConnectionMRUHListLimit() {
    dbPrefernces_.put(KEY_MRU_HOST_PORT_LIST_LIMIT, String.valueOf(DefaultPreferences.DEFAULT_MRU_HOST_PORT_LIST_LIMIT));
  }
  
  public static void setConnectionMRUPorts(LinkedList<String> val) {
    dbPrefernces_.put(KEY_CONNECTION_PORTS, 
                              getSelectedStringSetAsCommaSeperatedString(val));
  }
  
  public static LinkedList<String> getConnectionMRUPorts() {
    String value = dbPrefernces_.get(KEY_CONNECTION_PORTS, 
                                            DefaultPreferences.DEFAULT_CONNECTION_PORTS);
    return getCommaSeperatedStringAsStringList(value);
  }
  
  public static LinkedList<String> getConnectionMRUHosts() {
    String value = dbPrefernces_.get(KEY_CONNECTION_HOSTS, 
                                        DefaultPreferences.DEFAULT_CONNECTION_HOSTS);
    return getCommaSeperatedStringAsStringList(value);
  }
  
  public static void setConnectionMRUHosts(LinkedList<String> val) {
    dbPrefernces_.put(KEY_CONNECTION_HOSTS, 
                              getSelectedStringSetAsCommaSeperatedString(val));
  }
  
  public static boolean getEnforceLimitToQuery() {
    return dbPrefernces_.getBoolean(KEY_ENFORCE_LIMIT_TO_QUERY_RESULT, DefaultPreferences.DEFAULT_ENFORCE_LIMIT_TO_QUERY_RESULT);
  }
  
  public static void setEnforceLimitToQuery(boolean limit) {
    dbPrefernces_.putBoolean(KEY_ENFORCE_LIMIT_TO_QUERY_RESULT, Boolean.valueOf(limit));
  }
  
  public static void setDefaultEnforceLimitToQuery() {
    dbPrefernces_.putBoolean(KEY_ENFORCE_LIMIT_TO_QUERY_RESULT, Boolean.valueOf(DefaultPreferences.DEFAULT_ENFORCE_LIMIT_TO_QUERY_RESULT));
  }
  
  public static long getQueryLimit() {
    return dbPrefernces_.getLong(KEY_QUERY_RESULT_LIMIT, DefaultPreferences.DEFAULT_QUERY_RESULT_LIMIT);
  }
  
  public static void setQueryLimit(long limit) {
    dbPrefernces_.put(KEY_QUERY_RESULT_LIMIT, String.valueOf(limit));
  }
  
  public static void setDefaultQueryLimit() {
    dbPrefernces_.put(KEY_QUERY_RESULT_LIMIT, String.valueOf(DefaultPreferences.DEFAULT_QUERY_RESULT_LIMIT));
  }
  
  public static long getQueryExecutionTimeout() {
    return dbPrefernces_.getLong(KEY_QUERY_TIMEOUT, DefaultPreferences.DEFAULT_QUERY_TIMEOUT);
  }
  
  public static void setQueryExecutionTimeout(long interval) {
    dbPrefernces_.put(KEY_QUERY_TIMEOUT, String.valueOf(interval));
  }
  
  public static void setDefaultQueryExecutionTimeout() {
    dbPrefernces_.put(KEY_QUERY_TIMEOUT, String.valueOf(DefaultPreferences.DEFAULT_QUERY_TIMEOUT));
  }
  
  public static boolean getResultAutoScroll() {
    return dbPrefernces_.getBoolean(KEY_RESULT_AUTO_SCROLL, DefaultPreferences.DEFAULT_RESULT_AUTO_SCROLL);
  }
  
  public static void setResultAutoScroll(boolean auto) {
    dbPrefernces_.putBoolean(KEY_RESULT_AUTO_SCROLL, Boolean.valueOf(auto));
  }
  
  public static void setDefaultResultAutoScroll() {
    dbPrefernces_.putBoolean(KEY_RESULT_AUTO_SCROLL, Boolean.valueOf(DefaultPreferences.DEFAULT_RESULT_AUTO_SCROLL));
  }
  
  public static String getSecurityPlugin() {
    return dbPrefernces_.get(KEY_SEURITY_PLUGIN, DefaultPreferences.DEFAULT_SEURITY_PLUGIN);
  }
  
  public static void setSecurityPlugin(String plugin) {
    dbPrefernces_.put(KEY_SEURITY_PLUGIN, plugin);
  }
  
  public static void setDefaultSecurityPlugin() {
    dbPrefernces_.put(KEY_SEURITY_PLUGIN, DefaultPreferences.DEFAULT_SEURITY_PLUGIN);
  }
  
  public static boolean getSecurityPropsHidden() {
    return dbPrefernces_.getBoolean(KEY_SEURITY_PROPS_HIDDEN, DefaultPreferences.DEFAULT_SEURITY_PROPS_HIDDEN);
  }
  
  public static void setSecurityPropsHidden(boolean hidden) {
    dbPrefernces_.putBoolean(KEY_SEURITY_PROPS_HIDDEN, hidden);
  }
  
  public static void setDefaultSecurityPropsHidden() {
    dbPrefernces_.putBoolean(KEY_SEURITY_PROPS_HIDDEN, DefaultPreferences.DEFAULT_SEURITY_PROPS_HIDDEN);
  }
  
  public static List<SecurityProp> getSecurityProperties() throws BackingStoreException {
    String[] childrenNames = dbSecurityProps_.keys();
    List<SecurityProp> props = new ArrayList<SecurityProp>();
    boolean hidden = getSecurityPropsHidden();
    for (int i = 0; i < childrenNames.length; i++) {
      String key = childrenNames[i];
      String value = dbSecurityProps_.get(key, null);
      if(value != null){
        SecurityProp prop = new SecurityProp(key,value,hidden);
        props.add(prop);
      }
    }
    return props;
  }
  
  public static void setSecurityProperties(List<SecurityProp> props) {
    try {
      dbSecurityProps_.clear();
      
      for (SecurityProp prop : props) {
        String key = prop.getKey();
        String value = prop.getValue();
        dbSecurityProps_.put(key, value);
      }
    }
    catch (BackingStoreException e) {
     LogUtil.warning("Exception while saving security preferences.", e); 
    }
  }
  
  public static void setDefaultSecurityProperties() throws BackingStoreException {
    List<SecurityProp> defaultSeurityProperties = DefaultPreferences.DEFAULT_SEURITY_PROPERTIES;
    if(defaultSeurityProperties != null)
      setSecurityProperties(defaultSeurityProperties);
    else{
      String[] childrenNames = dbSecurityProps_.keys();
      for (int i = 0; i < childrenNames.length; i++) {
        String key = childrenNames[i];
        dbSecurityProps_.remove(key);
      }
    }
  }
    
  public static void save() throws BackingStoreException{
    dbPrefernces_.flush();
    dbSecurityProps_.flush();
  }
  
  public static String getLogDirectory() {
    return log_dir;
  }
  
  public static String getLogDirectoryPref() {
    return dbPrefernces_.get(KEY_LOG_DIR, DefaultPreferences.DEFAULT_LOG_DIR);
  }
  
  public static void setLogDirectory(String dir) {
    dbPrefernces_.put(KEY_LOG_DIR, dir);
  }
  
  public static void setDefaultLogDirectory() {
    dbPrefernces_.put(KEY_LOG_DIR, DefaultPreferences.DEFAULT_LOG_DIR);
  }
    
  public static String getLoggingLevel() {
    return log_level;
  }
  
  public static String getLoggingLevelPref() {
    return dbPrefernces_.get(KEY_LOGGING_LEVEL, DefaultPreferences.DEFAULT_LOGGING_LEVEL);
  }
  
  public static void setLoggingLevel(String level) {
    dbPrefernces_.put(KEY_LOGGING_LEVEL, level);
  }
  
  public static void setDefaultLoggingLevel() {
    dbPrefernces_.put(KEY_LOGGING_LEVEL, DefaultPreferences.DEFAULT_LOGGING_LEVEL);
  }
  
  public static int getLogFileSize() {
    return log_file_size;
  }
  
  public static int getLogFileSizePref() {
    return dbPrefernces_.getInt(KEY_LOG_FILE_SIZE, DefaultPreferences.DEFAULT_LOG_FILE_SIZE);
  }
  
  public static void setLogFileSize(int size) {
    dbPrefernces_.putInt(KEY_LOG_FILE_SIZE, size);
  }
  
  public static void setDefaultLogFileSize() {
    dbPrefernces_.putInt(KEY_LOG_FILE_SIZE, DefaultPreferences.DEFAULT_LOG_FILE_SIZE);
  }

  public static int getLogFileCount() {
    return log_file_count;
  }
  
  public static int getLogFileCountPref() {
    return dbPrefernces_.getInt(KEY_LOG_FILE_COUNT, DefaultPreferences.DEFAULT_LOG_FILE_COUNT);
  }
  
  public static void setLogFileCount(int count) {
    dbPrefernces_.putInt(KEY_LOG_FILE_COUNT, count);
  }
  
  public static void setDefaultLogFileCount() {
    dbPrefernces_.putInt(KEY_LOG_FILE_COUNT, DefaultPreferences.DEFAULT_LOG_FILE_COUNT);
  }
  
  public static long getDSRefreshInterval() {
    return dbPrefernces_.getLong(KEY_DS_REFRESH_INTERVAL, DefaultPreferences.DEFAULT_DS_REFRESH_INTERVAL);
  }
  
  public static void setDSRefreshInterval(long interval) {
    dbPrefernces_.put(KEY_DS_REFRESH_INTERVAL, String.valueOf(interval));
  }
  
  public static void setDefaultDSRefreshInterval() {
    dbPrefernces_.put(KEY_DS_REFRESH_INTERVAL, String.valueOf(DefaultPreferences.DEFAULT_DS_REFRESH_INTERVAL));
  }
  
  public static String[] getApplicationClasses() {
    String[] keys;
    String[]applicationClasses = new String[0];
    try {
      keys = dbApplicationJars_.keys();
      applicationClasses = new String[keys.length];
      for (int i = 0; i < keys.length; i++) {
        applicationClasses[i]= dbApplicationJars_.get(keys[i], null);
      }
    }
    catch (BackingStoreException e) {
      LogUtil.warning("Exception while fetching application class from prefrences.", e); 
    }
    
    return applicationClasses;
  }
  
  public static void setApplicationClasses(String[] jars){
    try {
      dbApplicationJars_.clear();
      java.util.List<String> jarList = new ArrayList<String>();
      for (int i = 0; i < jars.length; i++) {
        jarList.add(jars[i]);
        dbApplicationJars_.put("ApplicationJar" + i, jars[i]);
      }
    }
    catch (BackingStoreException e) {
      LogUtil.warning("Exception while saving application class.", e); 
     }
  }
  
  public static void setDefaultApplicationClasses() {
    setApplicationClasses(DefaultPreferences.DEFAULT_APPLICATION_CLASSES);
  }
  
  private static String getSelectedStringSetAsCommaSeperatedString(List<String> selections) {
    StringBuffer buf = new StringBuffer();
    for (String next: selections) {
      buf.append(next + ",");
    }
    return buf.toString();
  }

  private static LinkedList<String> getCommaSeperatedStringAsStringList(String selection) {
    LinkedList<String> list = new LinkedList<String>();
    StringTokenizer tokenizer = new StringTokenizer(selection, ",");
    int limit = getConnectionMRUHListLimit();
    int index = 0;
    while (tokenizer.hasMoreTokens() && (index < limit)) {
      String info = tokenizer.nextToken().trim();
      list.add(info);
      index++;
    }
    return list;
  }
}

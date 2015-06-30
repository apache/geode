/*
 * ========================================================================= (c)
 * Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved. 1260 NW
 * Waterhouse Ave., Suite 200, Beaverton, OR 97006 All Rights Reserved.
 * ========================================================================
 */
package com.gemstone.gemfire.mgmt.DataBrowser.prefs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.admin.jmx.AgentConfig;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.SecurityPropComposite.SecurityProp;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;


/**
 * This class represents the Defaults Preferences to be set for Data Browser
 *
 * @author mjha
 */
public class DefaultPreferences implements Serializable {
  private static final long serialVersionUID                            = -8501486898786022599L;

  // Connection Preferences
  public static final int DEFAULT_NO_CON_RETRY_ATTEMPTS                 = 5;

  public static final long DEFAULT_CON_RETRY_INTERVAL                    = 2000;

  public static final String DEFAULT_CONNECTION_HOSTS                   = "localhost";

  public static final String DEFAULT_CONNECTION_PORTS                   = String.valueOf(AgentConfig.DEFAULT_RMI_PORT);

  public static final int DEFAULT_MRU_HOST_PORT_LIST_LIMIT              = 5;

  public static final long DEFAULT_CONNECTION_TIMEOUT                   = 60000;
  
  public static final int DEFAULT_SOCKET_READ_TIMEOUT                   = PoolFactory.DEFAULT_READ_TIMEOUT;

  // Query Preferences
  public static final long DEFAULT_QUERY_TIMEOUT                        = 60000;

  public static final long DEFAULT_QUERY_RESULT_LIMIT                   = 1000;

  public static final boolean DEFAULT_RESULT_AUTO_SCROLL                = true;

  public static final boolean DEFAULT_ENFORCE_LIMIT_TO_QUERY_RESULT     = true;

  // Security Preferences
  public static final String DEFAULT_SEURITY_PLUGIN                     = "";

  public static final List<SecurityProp> DEFAULT_SEURITY_PROPERTIES     = new ArrayList<SecurityProp>();

  public static final boolean DEFAULT_SEURITY_PROPS_HIDDEN              = false;

  // Misc Preferences
  public static final String DEFAULT_LOGGING_LEVEL                      = LogUtil.LOG_LEVEL_INFO.getLocalizedName();

  public static final long DEFAULT_DS_REFRESH_INTERVAL                  = 60000;

  public static final String[] DEFAULT_APPLICATION_CLASSES                = new String[0];

  private static final String ENV_FILE_SYS_SEPARATOR                    = System
                                                                            .getProperty("file.separator");

  private static final String ENV_JAVA_LOGDIR                           = System.getProperty("log.dir");

  private static final String ENV_USER_HOME                             = System.getProperty("user.home");

  private static final String ENV_CWD                                   = System.getProperty("user.dir");

  private static final String ENV_JAVA_TMPDIR                           = System.getProperty("java.io.tmpdir");

  public static final String DEFAULT_LOG_DIR;

  public static final int DEFAULT_LOG_FILE_SIZE                         = 1  ; //In MB
  
  public static final int  DEFAULT_LOG_FILE_COUNT                      = 10;
  
  static{
    String logDir = ENV_JAVA_LOGDIR;
    if (null == logDir || 0 == logDir.length()) {
      logDir = ENV_USER_HOME;

      if (null == logDir || 0 == logDir.length()) {
        logDir = ENV_CWD;
      }

      if (null == logDir || 0 == logDir.length()) {
        logDir = ENV_JAVA_TMPDIR;
      }

      if (null == logDir) {
        logDir = "";
      } else {
        logDir = logDir.concat(ENV_FILE_SYS_SEPARATOR);
      }

      // TODO MGH - May need to localize the file name for non-english systems?
      logDir = logDir.concat("GemStone").concat(ENV_FILE_SYS_SEPARATOR)
          .concat("GemFire").concat(ENV_FILE_SYS_SEPARATOR).concat(
              "DataBrowser").concat(ENV_FILE_SYS_SEPARATOR).concat( "logs" );
    }
    DEFAULT_LOG_DIR = logDir;
  }

}

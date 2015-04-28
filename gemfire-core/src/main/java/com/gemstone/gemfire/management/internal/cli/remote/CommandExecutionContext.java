/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.remote;

import java.util.Collections;
import java.util.Map;

import com.gemstone.gemfire.management.internal.cli.CommandResponseWriter;
import com.gemstone.gemfire.management.internal.cli.GfshParser;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;

/**
 * 
 * @author Abhishek Chaudhari
 * @since 7.0
 */
public class CommandExecutionContext {
  // ThreadLocal variables that can be uses by commands
  private static final ThreadLocal<Map<String, String>> ENV = new ThreadLocal<Map<String, String>>();
  private static final ThreadLocal<Boolean>             FROM_SHELL = new ThreadLocal<Boolean>();
  private static final ThreadLocal<byte[][]>            SHELL_BYTES_DATA = new ThreadLocal<byte[][]>();

  private static final WrapperThreadLocal<CommandResponseWriter> WRITER_WRAPPER = 
      new WrapperThreadLocal<CommandResponseWriter>() {
        @Override
        protected CommandResponseWriter createWrapped() {
          return new CommandResponseWriter();
        }
      };

  public static String getShellEnvProperty(String propertyName, String defaultValue) {
    String propertyValue = null;
    Map<String, String> gfshEnv = ENV.get();
    if (gfshEnv != null) {
      propertyValue = gfshEnv.get(propertyName);
    }
    return propertyValue != null ? propertyValue : defaultValue;
  }
// Enable when "use region" command is required. See #46110
//  public static String getShellContextPath() {
//    return getShellEnvProperty(CliConstants.ENV_APP_CONTEXT_PATH, null);
//  }

  public static int getShellFetchSize() {
    int fetchSize = Gfsh.DEFAULT_APP_FETCH_SIZE;
    String fetchSizeStr = getShellEnvProperty(Gfsh.ENV_APP_FETCH_SIZE, null);
    if (fetchSizeStr != null) {
      fetchSize = Integer.valueOf(fetchSizeStr);
    }
    return fetchSize;
  }

  public static String getShellLineSeparator() {
    return getShellEnvProperty(Gfsh.ENV_SYS_OS_LINE_SEPARATOR, GfshParser.LINE_SEPARATOR);
  }

  public static Map<String, String> getShellEnv() {
    Map<String, String> envMap = ENV.get();
    if (envMap != null) {
      return Collections.unmodifiableMap(envMap);
    } else {
      return Collections.emptyMap();
    }
  }

  // TODO - Abhishek make this protected & move caller code of this method 
  // from MemberMBeanBridge to MemberCommandService
  public static void setShellEnv(Map<String, String> env) {
    ENV.set(env);
  }

  public static byte[][] getBytesFromShell() {
    return SHELL_BYTES_DATA.get();
  }

  public static void setBytesFromShell(byte[][] data) {
    SHELL_BYTES_DATA.set(data);
  }

  public static boolean isShellRequest() {
    return FROM_SHELL.get() != null && FROM_SHELL.get();
  }

  // TODO - Abhishek make this protected & move caller code of this method 
  // from MemberMBeanBridge to MemberCommandService
  public static void setShellRequest() {
    FROM_SHELL.set(true);
  }

  public static boolean isSetWrapperThreadLocal() {
    return WRITER_WRAPPER.isSet();
  }

  public static CommandResponseWriter getCommandResponseWriter() {
    return WRITER_WRAPPER.get();
  }

  public static CommandResponseWriter getAndCreateIfAbsentCommandResponseWriter() {
    return WRITER_WRAPPER.getAndCreateIfAbsent();
  }

  public static void clear() {
    Map<String, String> map = ENV.get();
    if (map != null) {
      map.clear();
    }
    ENV.set(null);
    
    FROM_SHELL.set(false);
    SHELL_BYTES_DATA.set(null);
    WRITER_WRAPPER.set(null);
  }
}

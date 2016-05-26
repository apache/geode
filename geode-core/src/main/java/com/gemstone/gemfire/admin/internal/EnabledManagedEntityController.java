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
package com.gemstone.gemfire.admin.internal;

import java.io.File;
import java.util.Iterator;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.admin.ManagedEntity;
import com.gemstone.gemfire.admin.ManagedEntityConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.ProcessOutputReader;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * Implements the actual administration (starting, stopping, etc.) of
 * GemFire {@link ManagedEntity}s.  It {@link Runtime#exec(java.lang.String) executes}
 * commands to administer the entities based on information provided
 * by the {@link InternalManagedEntity} object.  Note that it does not
 * use <code>SystemAdmin</code> to manage "local" entities; it always
 * execs the scripts.
 *
 * <P>
 *
 * This class is a refactoring of <code>Systemcontroller</code>,
 * <code>RemoteCommand</code>, and <code>LocatorRemoteCommand</code>.
 *
 * @since GemFire 4.0
 */
class EnabledManagedEntityController implements ManagedEntityController {
  private static final Logger logger = LogService.getLogger();

//  /** A lock to ensure that only entity is managed at a time.  See bug
//   * 31374. */
//  private static Object startStopLock = new Object();

  /** Known strings found in output indicating error. */
  private static final String[] ERROR_OUTPUTS = new String[] {
    "No such file or directory",
    "The system cannot find the file specified.",
    "Access is denied.",
    "cannot open",
    "ERROR"
  };

  /** Token in command prefix to be replaced with actual HOST */
  private static final String HOST = "{HOST}";

  /** Token in command prefix to be replaced with actual execution CMD */
  private static final String CMD = "{CMD}";

  //////////////////////  Instance Fields  //////////////////////

  /** The thread group in which threads launched by this system
   * controller reside. */
  private final ThreadGroup threadGroup;

  /** System to which the managed entities belong */
  private final AdminDistributedSystem system;

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>ManagedEntityController</code> for entities
   * in the given distributed system.
   */
  EnabledManagedEntityController(AdminDistributedSystem system) {
    this.system = system;
    this.threadGroup =
      LoggingThreadGroup.createThreadGroup("ManagedEntityController threads", logger);
  }

  /////////////////////  Instance Methods  /////////////////////

  /**
   * Returns <code>true</code> if the <code>output</code> string
   * contains a known error message.
   */
  private boolean outputIsError(String output) {
    if (output == null) return false;
    boolean error = false;
    for (int i = 0; i < ERROR_OUTPUTS.length; i++) {
      error = output.indexOf(ERROR_OUTPUTS[i]) > -1;
      if (error) return error;
    }
    return error;
  }

  /**
   * Executes a command using {@link Runtime#exec(java.lang.String)}.
   *
   * @param command
   *        The full command to remotely execute
   *
   * @return Output from the command that was executed or
   *         <code>null</code> if the executing the command failed.
   */
  protected String execute(String command,
                         InternalManagedEntity entity) {
    /* TODO: this is getting ugly... clients of this method really need to
       have the ability to do their own parsing/checking of 'output' */
    if (command == null || command.length() == 0) {
      throw new IllegalArgumentException(LocalizedStrings.ManagedEntityController_EXECUTION_COMMAND_IS_EMPTY.toLocalizedString());
    }

    File workingDir =
      new File(entity.getEntityConfig().getWorkingDirectory());
    logger.info(LocalizedMessage.create(LocalizedStrings.ManagedEntityController_EXECUTING_REMOTE_COMMAND_0_IN_DIRECTORY_1, new Object[] {command, workingDir}));
    Process p = null;
    try {
      p = Runtime.getRuntime().exec(command, null /* env */,
                                    workingDir);

    } catch (java.io.IOException e) {
      logger.fatal(LocalizedMessage.create(LocalizedStrings.ManagedEntityController_WHILE_EXECUTING_0, command), e);
      return null;
    }

    final ProcessOutputReader pos = new ProcessOutputReader(p);
    int retCode = pos.getExitCode();
    final String output = pos.getOutput();
    logger.info(LocalizedMessage.create(LocalizedStrings.ManagedEntityController_RESULT_OF_EXECUTING_0_IS_1, new Object[] {command, Integer.valueOf(retCode)}));
    logger.info(LocalizedMessage.create(LocalizedStrings.ManagedEntityController_OUTPUT_OF_0_IS_1, new Object[] {command, output}));

    if (retCode != 0 || outputIsError(output)) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.ManagedEntityController_REMOTE_EXECUTION_OF_0_FAILED, command));
      return null;
    }

    return output;
  }

  /** Returns true if the path ends with a path separator. */
  private boolean endsWithSeparator(String path) {
    return path.endsWith("/") || path.endsWith("\\");
  }

  /** Translates the path between Windows and UNIX. */
  private String getOSPath(String path) {
    if (pathIsWindows(path)) {
      return path.replace('/', '\\');
    } else {
      return path.replace('\\', '/');
    }
  }

//  /** Returns true if the path is on Windows. */
//  private boolean pathIsWindows(File path) {
//    return pathIsWindows(path.toString());
//  }

  /** Returns true if the path is on Windows. */
  private boolean pathIsWindows(String path) {
    if (path != null && path.length() > 1) {
      return (Character.isLetter(path.charAt(0)) && path.charAt(1) == ':') ||
        (path.startsWith("//") || path.startsWith("\\\\"));
    }
    return false;
  }

  /**
   * If the managed entity resides on a remote host, then
   * <code>command</code> is munged to take the remote command into account.
   *
   * @throws IllegalStateException
   *        If a remote command is required, but one has not been
   *        specified.
   */
  private String arrangeRemoteCommand(InternalManagedEntity entity,
                                      String cmd) {

    String host = entity.getEntityConfig().getHost();
    if (InetAddressUtil.isLocalHost(host)) {
      // No arranging necessary
      return cmd;
    }

    String prefix = entity.getEntityConfig().getRemoteCommand();
    if (prefix == null || prefix.length() <= 0) {
      prefix = entity.getDistributedSystem().getRemoteCommand();
    }

    if (prefix == null || prefix.length() <= 0) {
      throw new IllegalStateException(LocalizedStrings.ManagedEntityController_A_REMOTE_COMMAND_MUST_BE_SPECIFIED_TO_OPERATE_ON_A_MANAGED_ENTITY_ON_HOST_0
          .toLocalizedString(host));
    }

    int hostIdx = prefix.indexOf(HOST);
    int cmdIdx = prefix.indexOf(CMD);
    if (hostIdx == -1 && cmdIdx == -1) {
      return prefix + " " + host + " " + cmd;
    }

    if (hostIdx >= 0) {
      String start = prefix.substring(0, hostIdx);
      String end = null;
      if (hostIdx + HOST.length() >= prefix.length()) {
        end = "";
      } else {
        end = prefix.substring(hostIdx + HOST.length());
      }
      prefix = start + host + end;
      cmdIdx = prefix.indexOf(CMD); //recalculate;
    }

    if (cmdIdx >= 0) {
      String start = prefix.substring(0, cmdIdx);
      String end = null;
      if (cmdIdx + CMD.length() >= prefix.length()) {
        end = "";
      } else {
        end = prefix.substring(cmdIdx + CMD.length());
      }
      prefix = start + cmd + end;
    }
    return prefix;
  }

  /**
   * Returns the full path to the executable in
   * <code>$GEMFIRE/bin</code> taking into account the {@linkplain
   * ManagedEntityConfig#getProductDirectory product directory} and the
   * platform's file separator.
   *
   * <P>
   *
   * Note: we should probably do a better job of determine whether or
   * not the machine on which the entity runs is Windows or Linux.
   *
   * @param executable
   *        The name of the executable that resides in
   *        <code>$GEMFIRE/bin</code>.
   */
  public String getProductExecutable(InternalManagedEntity entity,
                                     String executable) {
    String productDirectory =
      entity.getEntityConfig().getProductDirectory();
    String path = null;
    File productDir = new File(productDirectory);
//    if (productDir != null) (cannot be null)
    {
      path = productDir.getPath();
      if (!endsWithSeparator(path)) {
        path += File.separator;
      }
      path += "bin" + File.separator;
    }
//    else {
//      path = "";
//    }

    String bat = "";
    if (pathIsWindows(path)) {
      bat = ".bat";
    }
    return getOSPath(path) + executable + bat;
  }

  /**
   * Builds optional SSL command-line arguments.  Returns null if SSL is not
   * enabled for the distributed system.
   */
  public String buildSSLArguments(DistributedSystemConfig config) {
    Properties sslProps = buildSSLProperties(config, true);
    if (sslProps == null) return null;

    StringBuffer sb = new StringBuffer();
    for (Iterator iter = sslProps.keySet().iterator(); iter.hasNext();) {
      String key = (String) iter.next();
      String value = sslProps.getProperty(key);
      sb.append(" -J-D" + key + "=" + value);
    }

    return sb.toString();
  }

  /**
   * Builds optional SSL properties for DistributionLocator. Returns null if SSL
   * is not enabled for the distributed system.
   *
   * @param forCommandLine
   *                true indicates that
   *                {@link DistributionConfig#GEMFIRE_PREFIX} should be
   *                prepended so the argument will become -Dgemfire.xxxx
   */
  private Properties buildSSLProperties(DistributedSystemConfig config,
                                        boolean forCommandLine) {
    if (!config.isSSLEnabled()) return null;

    String prefix = "";
    if (forCommandLine) prefix = DistributionConfig.GEMFIRE_PREFIX;

    Properties sslProps = (Properties) config.getSSLProperties().clone();
    // add ssl-enabled, etc...
    sslProps.setProperty(prefix +
                         DistributionConfig.MCAST_PORT_NAME,
                         "0");
    sslProps.setProperty(prefix +
                         DistributionConfig.SSL_ENABLED_NAME,
                         String.valueOf(config.isSSLEnabled()));
    sslProps.setProperty(prefix +
                         DistributionConfig.SSL_CIPHERS_NAME,
                         config.getSSLCiphers());
    sslProps.setProperty(prefix +
                         DistributionConfig.SSL_PROTOCOLS_NAME,
                         config.getSSLProtocols());
    sslProps.setProperty(prefix +
                         DistributionConfig.SSL_REQUIRE_AUTHENTICATION_NAME,
                         String.valueOf(config.isSSLAuthenticationRequired()));
    return sslProps;
  }


  /**
   * Starts a managed entity.
   */
  public void start(final InternalManagedEntity entity) {
    final String command =
      arrangeRemoteCommand(entity, entity.getStartCommand());
    Thread start = new Thread(this.threadGroup, new Runnable() {
        public void run() {
          execute(command, entity);
        }
      }, "Start " + entity.getEntityType());
    start.start();
  }

  /**
   * Stops a managed entity.
   */
  public void stop(final InternalManagedEntity entity) {
    final String command =
      arrangeRemoteCommand(entity, entity.getStopCommand());
    Thread stop = new Thread(this.threadGroup, new Runnable() {
        public void run() {
          execute(command, entity);
        }
      }, "Stop " + entity.getEntityType());
    stop.start();
  }

  /**
   * Returns whether or not a managed entity is running
   */
  public boolean isRunning(InternalManagedEntity entity) {
    final String command =
      arrangeRemoteCommand(entity, entity.getIsRunningCommand());
    String output = execute(command, entity);

    if (output == null ||
        (output.indexOf("stop" /* "ing" "ped" */) != -1) ||
        (output.indexOf("killed") != -1) ||
        (output.indexOf("starting") != -1)) {
      return false;

    } else if (output.indexOf("running") != -1) {
      return true;

    } else {
      throw new IllegalStateException(LocalizedStrings.ManagedEntityController_COULD_NOT_DETERMINE_IF_MANAGED_ENTITY_WAS_RUNNING_0
          .toLocalizedString(output));
    }
  }

  /**
   * Returns the contents of a locator's log file.  Other APIs are
   * used to get the log file of managed entities that are also system
   * members.
   */
  public String getLog(DistributionLocatorImpl locator) {
    String command =
      arrangeRemoteCommand(locator, locator.getLogCommand());
    return execute(command, locator);
  }

  /**
   * Returns the contents of the given directory using the given
   * managed entity to determine the host and remote command.
   */
  private String listDirectory(InternalManagedEntity entity,
                               String dir) {
    ManagedEntityConfig config = entity.getEntityConfig();
    String listFile =
        pathIsWindows(config.getProductDirectory()) ? "dir " : "ls ";
    String command =
      arrangeRemoteCommand(entity, listFile + dir);
    return execute(command, entity);
  }
}

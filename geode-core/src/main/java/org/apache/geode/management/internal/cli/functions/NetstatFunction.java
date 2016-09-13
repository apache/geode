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
package org.apache.geode.management.internal.cli.functions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.CliUtil.DeflaterInflaterData;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

/**
 * Executes 'netstat' OS command & returns the result as compressed bytes.
 * 
 * @since GemFire 7.0
 */
public class NetstatFunction implements Function, InternalEntity {
  private static final long serialVersionUID = 1L;

  private static final String OS_NAME_LINUX   = "Linux";
  private static final String OS_NAME_MACOS   = "darwin";
  private static final String OS_NAME_SOLARIS = "SunOS";
  private static final String OS_NAME_PROP    = "os.name";
  private static final String OS_ARCH_PROP    = "os.arch";
  private static final String OS_VERSION_PROP = "os.version";
  
  public static final NetstatFunction INSTANCE = new NetstatFunction();
  
  private static final String ID = NetstatFunction.class.getName();

  private static final String NETSTAT_COMMAND = "netstat";
  private static final String LSOF_COMMAND    = "lsof";

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public void execute(final FunctionContext context) {
    InternalDistributedSystem ds = InternalDistributedSystem.getConnectedInstance();
    if (ds.isConnected()) {
      InternalDistributedMember distributedMember = ds.getDistributedMember();
      String host = distributedMember.getHost();
      NetstatFunctionArgument args = (NetstatFunctionArgument)context.getArguments();
      boolean withlsof = args.isWithlsof();
      String lineSeparator = args.getLineSeparator();
      String netstatOutput = executeCommand(lineSeparator, withlsof);

      StringBuilder netstatInfo = new StringBuilder();
      addMemberHostHeader(netstatInfo, "{0}", host, lineSeparator); //{0} will be replaced on Manager

      context.getResultSender().lastResult(new NetstatFunctionResult(host, netstatInfo.toString(), CliUtil.compressBytes(netstatOutput.getBytes())));
    }
  }

  private static void addMemberHostHeader(final StringBuilder netstatInfo, final String id, final String host, final String lineSeparator) {
    StringBuilder memberPlatFormInfo = new StringBuilder();
    String osInfo = System.getProperty(OS_NAME_PROP) +" " + System.getProperty(OS_VERSION_PROP) + " " + System.getProperty(OS_ARCH_PROP);
    memberPlatFormInfo.append(CliStrings.format(CliStrings.NETSTAT__MSG__FOR_HOST_1_OS_2_MEMBER_0, new Object[] {id, host, osInfo, lineSeparator}));

    int nameIdLength = Math.max(Math.max(id.length(), host.length()), osInfo.length()) * 2;

    StringBuilder netstatInfoBottom = new StringBuilder();
    for (int i = 0; i < nameIdLength; i++) {
      netstatInfo.append("#");
      netstatInfoBottom.append("#");
    }

    netstatInfo.append(lineSeparator)
      .append(memberPlatFormInfo.toString())
      .append(lineSeparator)
      .append(netstatInfoBottom.toString())
      .append(lineSeparator);
  }

  private static void addNetstatDefaultOptions(final List<String> cmdOptionsList) {
    String osName = System.getProperty(OS_NAME_PROP);
    if (OS_NAME_LINUX.equalsIgnoreCase(osName)) {
      cmdOptionsList.add("-v");
      cmdOptionsList.add("-a");
      cmdOptionsList.add("-e");
    } else if (OS_NAME_MACOS.equalsIgnoreCase(osName)) {
      cmdOptionsList.add("-v");
      cmdOptionsList.add("-a");
      cmdOptionsList.add("-e");
    } else if (OS_NAME_SOLARIS.equalsIgnoreCase(osName)) {
      cmdOptionsList.add("-v");
      cmdOptionsList.add("-a");
    } else { // default to Windows
      cmdOptionsList.add("-v");
      cmdOptionsList.add("-a");
    }
  }

  private static void executeNetstat(final StringBuilder netstatInfo, final String lineSeparator) {
    List<String> cmdOptionsList = new ArrayList<String>();
    cmdOptionsList.add(NETSTAT_COMMAND);
    addNetstatDefaultOptions(cmdOptionsList);

    ProcessBuilder procBuilder = new ProcessBuilder(cmdOptionsList);
    try {
      Process netstat = procBuilder.start();
      InputStreamReader reader = new InputStreamReader(netstat.getInputStream());
      BufferedReader breader = new BufferedReader(reader);
      String line = "";

      while ((line = breader.readLine()) != null) {
        netstatInfo.append(line).append(lineSeparator);
      }
      netstat.destroy();
    } catch (IOException e) {
      // Send error also, if any
      netstatInfo.append(CliStrings.format(CliStrings.NETSTAT__MSG__COULD_NOT_EXECUTE_0_REASON_1, new Object[] {NETSTAT_COMMAND, e.getMessage()}));
    } finally {
      netstatInfo.append(lineSeparator); //additional new line
    }
  }

  private static void executeLsof(final StringBuilder existingNetstatInfo, final String lineSeparator) {
    String osName = System.getProperty(OS_NAME_PROP);
    existingNetstatInfo.append("################ "+LSOF_COMMAND+" output ###################").append(lineSeparator);
    if (OS_NAME_LINUX.equalsIgnoreCase(osName) || OS_NAME_MACOS.equalsIgnoreCase(osName) || OS_NAME_SOLARIS.equalsIgnoreCase(osName)) {
      ProcessBuilder procBuilder = new ProcessBuilder(LSOF_COMMAND);
      try {
        Process lsof = procBuilder.start();
        InputStreamReader reader = new InputStreamReader(lsof.getInputStream());
        BufferedReader breader = new BufferedReader(reader);
        String line = "";

        while ((line = breader.readLine()) != null) {
          existingNetstatInfo.append(line).append(lineSeparator);
        }
        lsof.destroy();
      } catch (IOException e) {
        // Send error also, if any
        String message = e.getMessage();
        if (message.contains("error=2, No such file or directory")) {
          existingNetstatInfo.append(CliStrings.format(
              CliStrings.NETSTAT__MSG__COULD_NOT_EXECUTE_0_REASON_1,
              new Object[] { LSOF_COMMAND, CliStrings.NETSTAT__MSG__LSOF_NOT_IN_PATH }));
        } else {
          existingNetstatInfo.append(CliStrings.format(
              CliStrings.NETSTAT__MSG__COULD_NOT_EXECUTE_0_REASON_1,
              new Object[] { LSOF_COMMAND, e.getMessage() }));
        }        
      } finally {
        existingNetstatInfo.append(lineSeparator); //additional new line
      }
    } else {
      existingNetstatInfo.append(CliStrings.NETSTAT__MSG__NOT_AVAILABLE_FOR_WINDOWS).append(lineSeparator);
    }
  }

  public static String executeCommand(final String lineSeparator, final boolean withlsof) {
    StringBuilder netstatInfo = new StringBuilder();

    executeNetstat(netstatInfo, lineSeparator);

    if (withlsof) {
      executeLsof(netstatInfo, lineSeparator);
    }

    return netstatInfo.toString();
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  }
  
  public static class NetstatFunctionArgument implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String  lineSeparator;
    private final boolean withlsof;

    public NetstatFunctionArgument(String lineSeparator, boolean withlsof) {
      this.lineSeparator = lineSeparator;
      this.withlsof = withlsof;
    }

    /**
     * @return the lineSeparator
     */
    public String getLineSeparator() {
      return lineSeparator;
    }

    /**
     * @return the withlsof
     */
    public boolean isWithlsof() {
      return withlsof;
    }
  }
  
  public static class NetstatFunctionResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String host;
    private final String headerInfo;
    private final DeflaterInflaterData compressedBytes;

    public NetstatFunctionResult(String host, String headerInfo, DeflaterInflaterData compressedBytes) {
      this.host = host;
      this.headerInfo = headerInfo;
      this.compressedBytes = compressedBytes;
    }

    /**
     * @return the host
     */
    public String getHost() {
      return host;
    }

    /**
     * @return the headerInfo
     */
    public String getHeaderInfo() {
      return headerInfo;
    }

    /**
     * @return the compressedBytes
     */
    public DeflaterInflaterData getCompressedBytes() {
      return compressedBytes;
    }
  }

  public static void main(String[] args) {
    String netstat = executeCommand(GfshParser.LINE_SEPARATOR, true);
    System.out.println(netstat);
  }
}

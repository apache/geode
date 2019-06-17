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
package org.apache.geode.internal.statistics;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.Statistics;
import org.apache.geode.internal.PureJavaMode;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.statistics.platform.LinuxProcFsStatistics;
import org.apache.geode.internal.statistics.platform.LinuxProcessStats;
import org.apache.geode.internal.statistics.platform.LinuxSystemStats;
import org.apache.geode.internal.statistics.platform.OSXProcessStats;
import org.apache.geode.internal.statistics.platform.OSXSystemStats;
import org.apache.geode.internal.statistics.platform.OsStatisticsFactory;
import org.apache.geode.internal.statistics.platform.ProcessStats;
import org.apache.geode.internal.statistics.platform.SolarisProcessStats;
import org.apache.geode.internal.statistics.platform.SolarisSystemStats;
import org.apache.geode.internal.statistics.platform.WindowsProcessStats;
import org.apache.geode.internal.statistics.platform.WindowsSystemStats;

/**
 * Provides native methods which fetch operating system statistics.
 */
public class HostStatHelper {
  private static final int SOLARIS_CODE = 1; // Sparc Solaris
  private static final int WINDOWS_CODE = 2;
  private static final int LINUX_CODE = 3; // x86 Linux
  private static final int OSX_CODE = 4; // Mac OS X

  private static final int PROCESS_STAT_FLAG = 1;
  private static final int SYSTEM_STAT_FLAG = 2;

  private static final int osCode;

  static {
    String osName = System.getProperty("os.name", "unknown");
    if (!PureJavaMode.osStatsAreAvailable()) {
      throw new RuntimeException("HostStatHelper not allowed in pure java mode");
    } else if (osName.equals("SunOS")) {
      osCode = SOLARIS_CODE;
    } else if (osName.startsWith("Windows")) {
      osCode = WINDOWS_CODE;
    } else if (osName.startsWith("Linux")) {
      osCode = LINUX_CODE;
    } else if (osName.equals("Mac OS X")) {
      osCode = OSX_CODE;
    } else {
      throw new InternalGemFireException(
          String.format(
              "Unsupported OS %s. Supported OSs are: SunOS(sparc Solaris), Linux(x86) and Windows.",
              osName));
    }
  }

  public static boolean isWindows() {
    return osCode == WINDOWS_CODE;
  }

  public static boolean isUnix() {
    return osCode != WINDOWS_CODE;
  }

  public static boolean isSolaris() {
    return osCode == SOLARIS_CODE;
  }

  public static boolean isLinux() {
    return osCode == LINUX_CODE;
  }

  public static boolean isOSX() {
    return osCode == OSX_CODE;
  }

  private HostStatHelper() {
    // instances are not allowed
  }

  static int initOSStats() {
    if (isLinux()) {
      return LinuxProcFsStatistics.init();
    } else {
      return init();
    }
  }

  static void closeOSStats() {
    if (isLinux()) {
      LinuxProcFsStatistics.close();
    } else {
      close();
    }
  }

  static void readyRefreshOSStats() {
    if (isLinux()) {
      LinuxProcFsStatistics.readyRefresh();
    } else {
      readyRefresh();
    }
  }

  /**
   * Allocates and initializes any resources required to sample operating system statistics. returns
   * 0 if initialization succeeded
   */
  private static native int init();

  /**
   * Frees up resources used by this class. Once close is called this class can no longer be used.
   */
  private static native void close();

  /**
   * Should be called before any calls to the refresh methods. On some platforms if this is not
   * called then the refesh methods will just keep returning the same old data.
   */
  private static native void readyRefresh();

  /**
   * Refreshes the specified process stats instance by fetching the current OS values for the given
   * stats and storing them in the instance.
   */
  private static void refreshProcess(LocalStatisticsImpl statistics) {
    int pid = (int) statistics.getNumericId();
    if (isLinux()) {
      LinuxProcFsStatistics.refreshProcess(pid, statistics._getIntStorage(),
          statistics._getLongStorage(), statistics._getDoubleStorage());
    } else {
      refreshProcess(pid, statistics._getIntStorage(), statistics._getLongStorage(),
          statistics._getDoubleStorage());
    }
  }

  private static native void refreshProcess(int pid, int[] ints, long[] longs, double[] doubles);

  /**
   * Refreshes the specified system stats instance by fetching the current OS values for the local
   * machine and storing them in the instance.
   */
  private static void refreshSystem(LocalStatisticsImpl statistics) {
    if (isLinux()) {
      LinuxProcFsStatistics.refreshSystem(statistics._getIntStorage(), statistics._getLongStorage(),
          statistics._getDoubleStorage());
    } else {
      refreshSystem(statistics._getIntStorage(), statistics._getLongStorage(),
          statistics._getDoubleStorage());
    }
  }

  private static native void refreshSystem(int[] ints, long[] longs, double[] doubles);

  /**
   * The call should have already checked to make sure usesSystemCalls returns true.
   */
  public static void refresh(LocalStatisticsImpl statistics) {
    int flags = statistics.getOsStatFlags();
    if ((flags & PROCESS_STAT_FLAG) != 0) {
      refreshProcess(statistics);
    } else if ((flags & SYSTEM_STAT_FLAG) != 0) {
      refreshSystem(statistics);
    } else {
      throw new RuntimeException(String.format("Unexpected os stats flags %s", flags));
    }
  }

  /**
   * Creates and returns a {@link Statistics} with the given pid and name. The resource's stats will
   * contain a snapshot of the current statistic values for the specified process.
   */
  static Statistics newProcess(OsStatisticsFactory osStatisticsFactory, long pid, String name) {
    Statistics statistics;
    switch (osCode) {
      case SOLARIS_CODE:
        statistics =
            osStatisticsFactory.createOsStatistics(SolarisProcessStats.getType(), name, pid,
                PROCESS_STAT_FLAG);
        break;
      case LINUX_CODE:
        statistics = osStatisticsFactory.createOsStatistics(LinuxProcessStats.getType(), name, pid,
            PROCESS_STAT_FLAG);
        break;
      case OSX_CODE:
        statistics = osStatisticsFactory.createOsStatistics(OSXProcessStats.getType(), name, pid,
            PROCESS_STAT_FLAG);
        break;
      case WINDOWS_CODE:
        statistics =
            osStatisticsFactory.createOsStatistics(WindowsProcessStats.getType(), name, pid,
                PROCESS_STAT_FLAG);
        break;
      default:
        throw new InternalGemFireException(
            String.format("unhandled osCode= %s HostStatHelper:newProcess", osCode));
    }
    // Note we don't call refreshProcess since we only want the manager to do that
    return statistics;
  }

  /**
   * Creates a new <code>ProcessStats</code> instance that wraps the given <code>Statistics</code>.
   *
   * @see #newProcess
   * @since GemFire 3.5
   */
  static ProcessStats newProcessStats(Statistics statistics) {
    switch (osCode) {
      case SOLARIS_CODE:
        return SolarisProcessStats.createProcessStats(statistics);

      case LINUX_CODE:
        return LinuxProcessStats.createProcessStats(statistics);

      case WINDOWS_CODE:
        return WindowsProcessStats.createProcessStats(statistics);

      case OSX_CODE:
        return OSXProcessStats.createProcessStats(statistics);

      default:
        throw new InternalGemFireException(
            String.format("unhandled osCode= %s HostStatHelper:newProcessStats",
                osCode));
    }
  }

  /**
   * Creates and returns a {@link Statistics} with the current machine's stats. The resource's stats
   * will contain a snapshot of the current statistic values for the local machine.
   */
  static void newSystem(OsStatisticsFactory osStatisticsFactory, long id) {
    Statistics statistics;
    switch (osCode) {
      case SOLARIS_CODE:
        statistics = osStatisticsFactory.createOsStatistics(SolarisSystemStats.getType(),
            getHostSystemName(), id, SYSTEM_STAT_FLAG);
        break;
      case LINUX_CODE:
        statistics = osStatisticsFactory.createOsStatistics(LinuxSystemStats.getType(),
            getHostSystemName(), id, SYSTEM_STAT_FLAG);
        break;
      case WINDOWS_CODE:
        statistics = osStatisticsFactory.createOsStatistics(WindowsSystemStats.getType(),
            getHostSystemName(), id, SYSTEM_STAT_FLAG);
        break;
      case OSX_CODE:
        statistics = osStatisticsFactory.createOsStatistics(OSXSystemStats.getType(),
            getHostSystemName(), id, SYSTEM_STAT_FLAG);
        break;
      default:
        throw new InternalGemFireException(
            String.format("unhandled osCode= %s HostStatHelper:newSystem", osCode));
    }
    if (statistics instanceof LocalStatisticsImpl) {
      refreshSystem((LocalStatisticsImpl) statistics);
    } // otherwise its a Dummy implementation so do nothing
  }

  /**
   * @return this machine's fully qualified hostname or "unknownHostName" if one cannot be found.
   */
  private static String getHostSystemName() {
    String hostname = "unknownHostName";
    try {
      InetAddress inetAddress = SocketCreator.getLocalHost();
      hostname = inetAddress.getCanonicalHostName();
    } catch (UnknownHostException ignored) {
    }
    return hostname;
  }
}

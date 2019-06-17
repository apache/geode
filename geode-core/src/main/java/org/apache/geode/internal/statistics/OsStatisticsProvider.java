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
import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.statistics.platform.LinuxProcFsStatistics;
import org.apache.geode.internal.statistics.platform.LinuxProcessStats;
import org.apache.geode.internal.statistics.platform.LinuxSystemStats;
import org.apache.geode.internal.statistics.platform.OsStatisticsFactory;
import org.apache.geode.internal.statistics.platform.ProcessStats;

/**
 * Provides methods which fetch operating system statistics.
 * Only Linux OS is currently allowed.
 */
public class OsStatisticsProvider {
  private static final int PROCESS_STAT_FLAG = 1;
  private static final int SYSTEM_STAT_FLAG = 2;
  private final boolean osStatsSupported;
  private final String currentOs;

  public boolean osStatsSupported() {
    return osStatsSupported;
  }

  private OsStatisticsProvider() {
    currentOs = SystemUtils.getOsName();
    osStatsSupported = SystemUtils.isLinux();
    if (!isSupportedOs(currentOs)) {
      throw new InternalGemFireException(
          String.format("Unsupported OS %s. Only Linux(x86) OSs supports OS statistics.",
              currentOs));
    }
  }

  public static OsStatisticsProvider build() {
    return new OsStatisticsProvider();
  }

  private boolean isSupportedOs(String os) {
    boolean result = true;
    if (!SystemUtils.isLinux() && !SystemUtils.isWindows() && !SystemUtils.isMacOSX()
        && !SystemUtils.isSolaris()) {
      result = false;
    }
    return result;
  }

  int initOSStats() {
    return LinuxProcFsStatistics.init();
  }

  void closeOSStats() {
    LinuxProcFsStatistics.close();
  }

  void readyRefreshOSStats() {
    LinuxProcFsStatistics.readyRefresh();
  }

  /**
   * Refreshes the specified process stats instance by fetching the current OS values for the given
   * stats and storing them in the instance.
   */
  private void refreshProcess(LocalStatisticsImpl statistics) {
    int pid = (int) statistics.getNumericId();
    LinuxProcFsStatistics.refreshProcess(pid, statistics._getIntStorage(),
        statistics._getLongStorage(), statistics._getDoubleStorage());
  }

  /**
   * Refreshes the specified system stats instance by fetching the current OS values for the local
   * machine and storing them in the instance.
   */
  private void refreshSystem(LocalStatisticsImpl statistics) {
    LinuxProcFsStatistics.refreshSystem(statistics._getIntStorage(), statistics._getLongStorage(),
        statistics._getDoubleStorage());
  }

  /**
   * The call should have already checked to make sure usesSystemCalls returns true.
   */
  public void refresh(LocalStatisticsImpl statistics) {
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
  Statistics newProcess(OsStatisticsFactory osStatisticsFactory, long pid, String name) {
    Statistics statistics;
    statistics = osStatisticsFactory.createOsStatistics(LinuxProcessStats.getType(), name, pid,
        PROCESS_STAT_FLAG);
    // Note we don't call refreshProcess since we only want the manager to do that
    return statistics;
  }

  /**
   * Creates a new <code>ProcessStats</code> instance that wraps the given <code>Statistics</code>.
   *
   * @see #newProcess
   * @since GemFire 3.5
   */
  ProcessStats newProcessStats(Statistics statistics) {
    if (statistics instanceof LocalStatisticsImpl) {
      refresh((LocalStatisticsImpl) statistics);
    } // otherwise its a Dummy implementation so do nothing
    return LinuxProcessStats.createProcessStats(statistics);
  }

  /**
   * Creates a {@link Statistics} with the current machine's stats. The resource's stats
   * will contain a snapshot of the current statistic values for the local machine.
   */
  void newSystem(OsStatisticsFactory osStatisticsFactory, long id) {
    Statistics statistics;
    statistics = osStatisticsFactory.createOsStatistics(LinuxSystemStats.getType(),
        getHostSystemName(), id, SYSTEM_STAT_FLAG);
    if (statistics instanceof LocalStatisticsImpl) {
      refreshSystem((LocalStatisticsImpl) statistics);
    } // otherwise its a Dummy implementation so do nothing
  }

  /**
   * @return this machine's fully qualified hostname or "unknownHostName" if one cannot be found.
   */
  private String getHostSystemName() {
    String hostname = "unknownHostName";
    try {
      InetAddress inetAddress = SocketCreator.getLocalHost();
      hostname = inetAddress.getCanonicalHostName();
    } catch (UnknownHostException ignored) {
    }
    return hostname;
  }
}

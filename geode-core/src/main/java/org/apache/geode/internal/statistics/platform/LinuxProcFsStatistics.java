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
package org.apache.geode.internal.statistics.platform;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.DistributionConfig;

public class LinuxProcFsStatistics {
  private enum CPU {
    USER,
    NICE,
    SYSTEM,
    IDLE,
    IOWAIT,
    IRQ,
    SOFTIRQ,
    STEAL,
    /** stands for aggregation of all columns not present in the enum list */
    OTHER
  }

  private static final int DEFAULT_PAGESIZE = 4 * 1024;
  private static final int OneMeg = 1024 * 1024;
  private static final String pageSizeProperty =
      DistributionConfig.GEMFIRE_PREFIX + "statistics.linux.pageSize";
  private static CpuStat cpuStatSingleton;
  private static int pageSize;
  private static int sys_cpus;
  private static boolean hasProcVmStat;
  private static boolean hasDiskStats;
  static SpaceTokenizer st;

  /** The number of non-process files in /proc */
  private static int nonPidFilesInProc;

  /** /proc/stat tokens */
  private static final String CPU_TOKEN = "cpu ";
  private static final String PAGE = "page ";
  private static final String SWAP = "swap ";
  private static final String CTXT = "ctxt ";
  private static final String PROCESSES = "processes ";

  /** /proc/vmstat tokens */
  private static final String PGPGIN = "pgpgin ";
  private static final String PGPGOUT = "pgpgout ";
  private static final String PSWPIN = "pswpin ";
  private static final String PSWPOUT = "pswpout ";

  // Do not create instances of this class
  private LinuxProcFsStatistics() {}

  public static int init() { // TODO: was package-protected
    nonPidFilesInProc = getNumberOfNonProcessProcFiles();
    sys_cpus = Runtime.getRuntime().availableProcessors();
    pageSize = Integer.getInteger(pageSizeProperty, DEFAULT_PAGESIZE);
    cpuStatSingleton = new CpuStat();
    hasProcVmStat = new File("/proc/vmstat").exists();
    hasDiskStats = new File("/proc/diskstats").exists();
    st = new SpaceTokenizer();
    return 0;
  }

  public static void close() { // TODO: was package-protected
    cpuStatSingleton = null;
    st = null;
  }

  public static void readyRefresh() { // TODO: was package-protected
  }

  /*
   * get the statistics for the specified process. ( pid_rssSize, pid_imageSize ) vsize is assumed
   * to be in units of kbytes System property gemfire.statistics.pagesSize can be used to configure
   * pageSize. This is the mem_unit member of the struct returned by sysinfo()
   *
   */
  public static void refreshProcess(int pid, int[] ints, long[] longs, double[] doubles) { // TODO:
                                                                                           // was
                                                                                           // package-protected
    // Just incase a pid is not available
    if (pid == 0)
      return;
    InputStreamReader isr = null;
    BufferedReader br = null;
    try {
      File file = new File("/proc/" + pid + "/stat");
      isr = new InputStreamReader(new FileInputStream(file));
      br = new BufferedReader(isr, 2048);
      String line = br.readLine();
      if (line == null) {
        return;
      }
      st.setString(line);
      st.skipTokens(22);
      ints[LinuxProcessStats.imageSizeINT] = (int) (st.nextTokenAsLong() / OneMeg);
      ints[LinuxProcessStats.rssSizeINT] = (int) ((st.nextTokenAsLong() * pageSize) / OneMeg);
    } catch (NoSuchElementException nsee) {
      // It might just be a case of the process going away while we
      // where trying to get its stats.
      // So for now lets just ignore the failure and leave the stats
      // as they are.
    } catch (IOException ioe) {
      // It might just be a case of the process going away while we
      // where trying to get its stats.
      // So for now lets just ignore the failure and leave the stats
      // as they are.
    } finally {
      st.releaseResources();
      if (br != null)
        try {
          br.close();
        } catch (IOException ignore) {
        }
    }
  }

  public static void refreshSystem(int[] ints, long[] longs, double[] doubles) { // TODO: was
                                                                                 // package-protected
    ints[LinuxSystemStats.processesINT] = getProcessCount();
    ints[LinuxSystemStats.cpusINT] = sys_cpus;
    InputStreamReader isr = null;
    BufferedReader br = null;
    try {
      isr = new InputStreamReader(new FileInputStream("/proc/stat"));
      br = new BufferedReader(isr);
      String line = null;
      while ((line = br.readLine()) != null) {
        try {
          if (line.startsWith(CPU_TOKEN)) {
            int[] cpuData = cpuStatSingleton.calculateStats(line);
            ints[LinuxSystemStats.cpuIdleINT] = cpuData[CPU.IDLE.ordinal()];
            ints[LinuxSystemStats.cpuNiceINT] = cpuData[CPU.NICE.ordinal()];
            ints[LinuxSystemStats.cpuSystemINT] = cpuData[CPU.SYSTEM.ordinal()];
            ints[LinuxSystemStats.cpuUserINT] = cpuData[CPU.USER.ordinal()];
            ints[LinuxSystemStats.cpuStealINT] = cpuData[CPU.STEAL.ordinal()];
            ints[LinuxSystemStats.iowaitINT] = cpuData[CPU.IOWAIT.ordinal()];
            ints[LinuxSystemStats.irqINT] = cpuData[CPU.IRQ.ordinal()];
            ints[LinuxSystemStats.softirqINT] = cpuData[CPU.SOFTIRQ.ordinal()];
            ints[LinuxSystemStats.cpuActiveINT] = 100 - cpuData[CPU.IDLE.ordinal()];
            ints[LinuxSystemStats.cpuNonUserINT] = cpuData[CPU.OTHER.ordinal()]
                + cpuData[CPU.SYSTEM.ordinal()] + cpuData[CPU.IOWAIT.ordinal()]
                + cpuData[CPU.IRQ.ordinal()] + cpuData[CPU.SOFTIRQ.ordinal()];
          } else if (!hasProcVmStat && line.startsWith(PAGE)) {
            int secondIndex = line.indexOf(" ", PAGE.length());
            longs[LinuxSystemStats.pagesPagedInLONG] =
                SpaceTokenizer.parseAsLong(line.substring(PAGE.length(), secondIndex));
            longs[LinuxSystemStats.pagesPagedOutLONG] =
                SpaceTokenizer.parseAsLong(line.substring(secondIndex + 1));
          } else if (!hasProcVmStat && line.startsWith(SWAP)) {
            int secondIndex = line.indexOf(" ", SWAP.length());
            longs[LinuxSystemStats.pagesSwappedInLONG] =
                SpaceTokenizer.parseAsLong(line.substring(SWAP.length(), secondIndex));
            longs[LinuxSystemStats.pagesSwappedOutLONG] =
                SpaceTokenizer.parseAsLong(line.substring(secondIndex + 1));
          } else if (line.startsWith(CTXT)) {
            longs[LinuxSystemStats.contextSwitchesLONG] =
                SpaceTokenizer.parseAsLong(line.substring(CTXT.length()));
          } else if (line.startsWith(PROCESSES)) {
            longs[LinuxSystemStats.processCreatesLONG] =
                SpaceTokenizer.parseAsInt(line.substring(PROCESSES.length()));
          }
        } catch (NoSuchElementException nsee) {
          // this is the result of reading a partially formed file
          // just do not update what ever entry had the problem
        }
      }
    } catch (IOException ioe) {
    } finally {
      if (br != null)
        try {
          br.close();
        } catch (IOException ignore) {
        }
    }
    getLoadAvg(doubles);
    getMemInfo(ints);
    getDiskStats(longs);
    getNetStats(longs);
    if (hasProcVmStat) {
      getVmStats(longs);
    }
    st.releaseResources();
  }

  // Example of /proc/loadavg
  // 0.00 0.00 0.07 1/218 7907
  private static void getLoadAvg(double[] doubles) {
    InputStreamReader isr = null;
    BufferedReader br = null;
    try {
      isr = new InputStreamReader(new FileInputStream("/proc/loadavg"));
      br = new BufferedReader(isr, 512);
      String line = br.readLine();
      if (line == null) {
        return;
      }
      st.setString(line);
      doubles[LinuxSystemStats.loadAverage1DOUBLE] = st.nextTokenAsDouble();
      doubles[LinuxSystemStats.loadAverage5DOUBLE] = st.nextTokenAsDouble();
      doubles[LinuxSystemStats.loadAverage15DOUBLE] = st.nextTokenAsDouble();
    } catch (NoSuchElementException nsee) {
    } catch (IOException ioe) {
    } finally {
      st.releaseResources();
      if (br != null)
        try {
          br.close();
        } catch (IOException ignore) {
        }
    }
  }

  /**
   * Returns the available system memory (free + cached).
   *
   * @param logger the logger
   * @return the available memory in bytes
   */
  public static long getAvailableMemory(Logger logger) {
    try {
      BufferedReader br =
          new BufferedReader(new InputStreamReader(new FileInputStream("/proc/meminfo")));
      try {
        long free = 0;
        Pattern p = Pattern.compile("(.*)?:\\s+(\\d+)( kB)?");

        String line;
        while ((line = br.readLine()) != null) {
          Matcher m = p.matcher(line);
          if (m.matches() && ("MemFree".equals(m.group(1)) || "Cached".equals(m.group(1)))) {
            free += Long.parseLong(m.group(2));
          }
        }

        // convert to bytes
        return 1024 * free;

      } finally {
        br.close();
      }
    } catch (IOException e) {
      logger.warn("Error determining free memory", e);
      return Long.MAX_VALUE;
    }
  }

  // Example of /proc/meminfo
  // total: used: free: shared: buffers: cached:
  // Mem: 4118380544 3816050688 302329856 0 109404160 3060326400
  // Swap: 4194881536 127942656 4066938880
  private static void getMemInfo(int[] ints) {
    InputStreamReader isr = null;
    BufferedReader br = null;
    try {
      isr = new InputStreamReader(new FileInputStream("/proc/meminfo"));
      br = new BufferedReader(isr);
      // Assume all values read in are in kB, convert to MB
      String line = null;
      while ((line = br.readLine()) != null) {
        try {
          if (line.startsWith("MemTotal: ")) {
            st.setString(line);
            st.skipToken(); // Burn initial token
            ints[LinuxSystemStats.physicalMemoryINT] = (int) (st.nextTokenAsLong() / 1024);
          } else if (line.startsWith("MemFree: ")) {
            st.setString(line);
            st.skipToken(); // Burn initial token
            ints[LinuxSystemStats.freeMemoryINT] = (int) (st.nextTokenAsLong() / 1024);
          } else if (line.startsWith("SharedMem: ")) {
            st.setString(line);
            st.skipToken(); // Burn initial token
            ints[LinuxSystemStats.sharedMemoryINT] = (int) (st.nextTokenAsLong() / 1024);
          } else if (line.startsWith("Buffers: ")) {
            st.setString(line);
            st.nextToken(); // Burn initial token
            ints[LinuxSystemStats.bufferMemoryINT] = (int) (st.nextTokenAsLong() / 1024);
          } else if (line.startsWith("SwapTotal: ")) {
            st.setString(line);
            st.skipToken(); // Burn initial token
            ints[LinuxSystemStats.allocatedSwapINT] = (int) (st.nextTokenAsLong() / 1024);
          } else if (line.startsWith("SwapFree: ")) {
            st.setString(line);
            st.skipToken(); // Burn initial token
            ints[LinuxSystemStats.unallocatedSwapINT] = (int) (st.nextTokenAsLong() / 1024);
          } else if (line.startsWith("Cached: ")) {
            st.setString(line);
            st.skipToken(); // Burn initial token
            ints[LinuxSystemStats.cachedMemoryINT] = (int) (st.nextTokenAsLong() / 1024);
          } else if (line.startsWith("Dirty: ")) {
            st.setString(line);
            st.skipToken(); // Burn initial token
            ints[LinuxSystemStats.dirtyMemoryINT] = (int) (st.nextTokenAsLong() / 1024);
          } else if (line.startsWith("Inact_dirty: ")) { // 2.4 kernels
            st.setString(line);
            st.skipToken(); // Burn initial token
            ints[LinuxSystemStats.dirtyMemoryINT] = (int) (st.nextTokenAsLong() / 1024);
          }
        } catch (NoSuchElementException nsee) {
          // ignore and let that stat not to be updated this time
        }
      }
    } catch (IOException ioe) {
    } finally {
      st.releaseResources();
      if (br != null)
        try {
          br.close();
        } catch (IOException ignore) {
        }
    }
  }

  /*
   * Inter-| Receive | Transmit face |bytes packets errs drop fifo frame compressed multicast|bytes
   * packets errs drop fifo colls carrier compressed lo:1908275823 326949246 0 0 0 0 0 0 1908275823
   * 326949246 0 0 0 0 0 0
   */

  private static void getNetStats(long[] longs) {
    InputStreamReader isr = null;
    BufferedReader br = null;
    try {
      isr = new InputStreamReader(new FileInputStream("/proc/net/dev"));
      br = new BufferedReader(isr);
      br.readLine(); // Discard header info
      br.readLine(); // Discard header info
      long lo_recv_packets = 0, lo_recv_bytes = 0;
      long other_recv_packets = 0, other_recv_bytes = 0;
      long other_recv_errs = 0, other_recv_drop = 0;
      long other_xmit_packets = 0, other_xmit_bytes = 0;
      long other_xmit_errs = 0, other_xmit_drop = 0, other_xmit_colls = 0;
      String line = null;
      while ((line = br.readLine()) != null) {
        int index = line.indexOf(":");
        boolean isloopback = (line.indexOf("lo:") != -1);
        st.setString(line.substring(index + 1).trim());
        long recv_bytes = st.nextTokenAsLong();
        long recv_packets = st.nextTokenAsLong();
        long recv_errs = st.nextTokenAsLong();
        long recv_drop = st.nextTokenAsLong();
        st.skipTokens(4); // fifo, frame, compressed, multicast
        long xmit_bytes = st.nextTokenAsLong();
        long xmit_packets = st.nextTokenAsLong();
        long xmit_errs = st.nextTokenAsLong();
        long xmit_drop = st.nextTokenAsLong();
        st.skipToken(); // fifo
        long xmit_colls = st.nextTokenAsLong();

        if (isloopback) {
          lo_recv_packets = recv_packets;
          lo_recv_bytes = recv_bytes;
        } else {
          other_recv_packets += recv_packets;
          other_recv_bytes += recv_bytes;
        }
        other_recv_errs += recv_errs;
        other_recv_drop += recv_drop;

        if (isloopback) {
          /* loopback_xmit_packets = xmit_packets; */
        } else {
          other_xmit_packets += xmit_packets;
          other_xmit_bytes += xmit_bytes;
        }
        other_xmit_errs += xmit_errs;
        other_xmit_drop += xmit_drop;
        other_xmit_colls += xmit_colls;
      }
      // fix for bug 43860
      longs[LinuxSystemStats.loopbackPacketsLONG] = lo_recv_packets;
      longs[LinuxSystemStats.loopbackBytesLONG] = lo_recv_bytes;
      longs[LinuxSystemStats.recvPacketsLONG] = other_recv_packets;
      longs[LinuxSystemStats.recvBytesLONG] = other_recv_bytes;
      longs[LinuxSystemStats.recvErrorsLONG] = other_recv_errs;
      longs[LinuxSystemStats.recvDropsLONG] = other_recv_drop;
      longs[LinuxSystemStats.xmitPacketsLONG] = other_xmit_packets;
      longs[LinuxSystemStats.xmitBytesLONG] = other_xmit_bytes;
      longs[LinuxSystemStats.xmitErrorsLONG] = other_xmit_errs;
      longs[LinuxSystemStats.xmitDropsLONG] = other_xmit_drop;
      longs[LinuxSystemStats.xmitCollisionsLONG] = other_xmit_colls;
    } catch (NoSuchElementException nsee) {
    } catch (IOException ioe) {
    } finally {
      st.releaseResources();
      if (br != null)
        try {
          br.close();
        } catch (IOException ignore) {
        }
    }
  }

  // example of /proc/diskstats
  // 1 0 ram0 0 0 0 0 0 0 0 0 0 0 0
  // 1 1 ram1 0 0 0 0 0 0 0 0 0 0 0
  // 1 2 ram2 0 0 0 0 0 0 0 0 0 0 0
  // 1 3 ram3 0 0 0 0 0 0 0 0 0 0 0
  // 1 4 ram4 0 0 0 0 0 0 0 0 0 0 0
  // 1 5 ram5 0 0 0 0 0 0 0 0 0 0 0
  // 1 6 ram6 0 0 0 0 0 0 0 0 0 0 0
  // 1 7 ram7 0 0 0 0 0 0 0 0 0 0 0
  // 1 8 ram8 0 0 0 0 0 0 0 0 0 0 0
  // 1 9 ram9 0 0 0 0 0 0 0 0 0 0 0
  // 1 10 ram10 0 0 0 0 0 0 0 0 0 0 0
  // 1 11 ram11 0 0 0 0 0 0 0 0 0 0 0
  // 1 12 ram12 0 0 0 0 0 0 0 0 0 0 0
  // 1 13 ram13 0 0 0 0 0 0 0 0 0 0 0
  // 1 14 ram14 0 0 0 0 0 0 0 0 0 0 0
  // 1 15 ram15 0 0 0 0 0 0 0 0 0 0 0
  // 8 0 sda 1628761 56603 37715982 5690640 6073889 34091137 330349716 279787924 0 25235208
  // 285650572
  // 8 1 sda1 151 638 45 360
  // 8 2 sda2 674840 11202608 8591346 68716852
  // 8 3 sda3 1010409 26512312 31733575 253868616
  // 8 16 sdb 12550386 47814 213085738 60429448 5529812 210792345 1731459040 1962038752 0 33797176
  // 2024138028
  // 8 17 sdb1 12601113 213085114 216407197 1731257800
  // 3 0 hda 0 0 0 0 0 0 0 0 0 0 0
  private static void getDiskStats(long[] longs) {
    InputStreamReader isr = null;
    BufferedReader br = null;
    String line = null;
    try {
      if (hasDiskStats) {
        // 2.6 kernel
        isr = new InputStreamReader(new FileInputStream("/proc/diskstats"));
      } else {
        // 2.4 kernel
        isr = new InputStreamReader(new FileInputStream("/proc/partitions"));
      }
      br = new BufferedReader(isr);
      long readsCompleted = 0, readsMerged = 0;
      long sectorsRead = 0, timeReading = 0;
      long writesCompleted = 0, writesMerged = 0;
      long sectorsWritten = 0, timeWriting = 0;
      long iosInProgress = 0;
      long timeIosInProgress = 0;
      long ioTime = 0;
      if (!hasDiskStats) {
        br.readLine(); // Discard header info
        br.readLine(); // Discard header info
      }
      while ((line = br.readLine()) != null) {
        st.setString(line);
        {
          // " 8 1 sdb" on 2.6
          // " 8 1 452145145 sdb" on 2.4
          String tok = st.nextToken();
          if (tok.length() == 0 || Character.isWhitespace(tok.charAt(0))) {
            // skip over first token since it is whitespace
            tok = st.nextToken();
          }
          // skip first token it is some number
          tok = st.nextToken();
          // skip second token it is some number
          tok = st.nextToken();
          if (!hasDiskStats) {
            // skip third token it is some number
            tok = st.nextToken();
          }
          // Now tok should be the device name.
          if (Character.isDigit(tok.charAt(tok.length() - 1))) {
            // If the last char is a digit
            // skip this line since it is a partition of a device; not a device.
            continue;
          }
        }
        long tmp_readsCompleted = st.nextTokenAsLong();
        long tmp_readsMerged = st.nextTokenAsLong();
        long tmp_sectorsRead = st.nextTokenAsLong();
        long tmp_timeReading = st.nextTokenAsLong();
        if (st.hasMoreTokens()) {
          // If we are on 2.6 then we might only have 4 longs; if so ignore this line
          // Otherwise we should have 11 long tokens.
          long tmp_writesCompleted = st.nextTokenAsLong();
          long tmp_writesMerged = st.nextTokenAsLong();
          long tmp_sectorsWritten = st.nextTokenAsLong();
          long tmp_timeWriting = st.nextTokenAsLong();
          long tmp_iosInProgress = st.nextTokenAsLong();
          long tmp_timeIosInProgress = st.nextTokenAsLong();
          long tmp_ioTime = st.nextTokenAsLong();
          readsCompleted += tmp_readsCompleted;
          readsMerged += tmp_readsMerged;
          sectorsRead += tmp_sectorsRead;
          timeReading += tmp_timeReading;
          writesCompleted += tmp_writesCompleted;
          writesMerged += tmp_writesMerged;
          sectorsWritten += tmp_sectorsWritten;
          timeWriting += tmp_timeWriting;
          iosInProgress += tmp_iosInProgress;
          timeIosInProgress += tmp_timeIosInProgress;
          ioTime += tmp_ioTime;
        }
      } // while
      final int SECTOR_SIZE = 512;
      longs[LinuxSystemStats.readsCompletedLONG] = readsCompleted;
      longs[LinuxSystemStats.readsMergedLONG] = readsMerged;
      longs[LinuxSystemStats.bytesReadLONG] = sectorsRead * SECTOR_SIZE;
      longs[LinuxSystemStats.timeReadingLONG] = timeReading;
      longs[LinuxSystemStats.writesCompletedLONG] = writesCompleted;
      longs[LinuxSystemStats.writesMergedLONG] = writesMerged;
      longs[LinuxSystemStats.bytesWrittenLONG] = sectorsWritten * SECTOR_SIZE;
      longs[LinuxSystemStats.timeWritingLONG] = timeWriting;
      longs[LinuxSystemStats.iosInProgressLONG] = iosInProgress;
      longs[LinuxSystemStats.timeIosInProgressLONG] = timeIosInProgress;
      longs[LinuxSystemStats.ioTimeLONG] = ioTime;
    } catch (NoSuchElementException nsee) {
      // org.apache.geode.distributed.internal.InternalDistributedSystem.getAnyInstance().getLogger().fine("unexpected
      // NoSuchElementException line=" + line, nsee);
    } catch (IOException ioe) {
    } finally {
      st.releaseResources();
      if (br != null)
        try {
          br.close();
        } catch (IOException ignore) {
        }
    }
  }

  // Example of /proc/vmstat
  // ...
  // pgpgin 294333738
  // pgpgout 1057420300
  // pswpin 19422
  // pswpout 14495
  private static void getVmStats(long[] longs) {
    assert hasProcVmStat != false : "getVmStats called when hasVmStat was false";
    InputStreamReader isr = null;
    BufferedReader br = null;
    try {
      isr = new InputStreamReader(new FileInputStream("/proc/vmstat"));
      br = new BufferedReader(isr);
      String line = null;
      while ((line = br.readLine()) != null) {
        if (line.startsWith(PGPGIN)) {
          longs[LinuxSystemStats.pagesPagedInLONG] =
              SpaceTokenizer.parseAsLong(line.substring(PGPGIN.length()));
        } else if (line.startsWith(PGPGOUT)) {
          longs[LinuxSystemStats.pagesPagedOutLONG] =
              SpaceTokenizer.parseAsLong(line.substring(PGPGOUT.length()));
        } else if (line.startsWith(PSWPIN)) {
          longs[LinuxSystemStats.pagesSwappedInLONG] =
              SpaceTokenizer.parseAsLong(line.substring(PSWPIN.length()));
        } else if (line.startsWith(PSWPOUT)) {
          longs[LinuxSystemStats.pagesSwappedOutLONG] =
              SpaceTokenizer.parseAsLong(line.substring(PSWPOUT.length()));
        }
      }
    } catch (NoSuchElementException nsee) {
    } catch (IOException ioe) {
    } finally {
      if (br != null)
        try {
          br.close();
        } catch (IOException ignore) {
        }
    }
  }

  /**
   * Count the number of files in /proc that do not represent processes. This value is cached to
   * make counting the number of running process a cheap operation. The assumption is that the
   * contents of /proc will not change on a running system.
   *
   * @return the files in /proc that do NOT match /proc/[0-9]*
   */
  private static int getNumberOfNonProcessProcFiles() {
    File proc = new File("/proc");
    String[] procFiles = proc.list();
    int count = 0;
    if (procFiles != null) {
      for (String filename : procFiles) {
        char c = filename.charAt(0);
        if (!Character.isDigit(c)) {
          if (c == '.') {
            // see if the next char is a digit
            if (filename.length() > 1) {
              char c2 = filename.charAt(1);
              if (Character.isDigit(c2)) {
                // for bug 42091 do not count files that begin with a '.' followed by digits
                continue;
              }
            }
          }
          count++;
        }
      }
    }
    return count;
  }

  /**
   * @return the number of running processes on the system
   */
  private static int getProcessCount() {
    File proc = new File("/proc");
    String[] procFiles = proc.list();
    if (procFiles == null) {
      // unknown error, continue without this stat
      return 0;
    }
    return procFiles.length - nonPidFilesInProc;
  }

  // The array indices must be ordered as they appear in /proc/stats
  // (user) (nice) (system) (idle) (iowait) (irq) (softirq)
  // cpu 42813766 10844 8889075 1450764512 49963779 808244 3084872
  //
  private static class CpuStat {
    private static boolean lastCpuStatsInvalid;
    private static List<Long> lastCpuStats;

    public CpuStat() {
      lastCpuStatsInvalid = true;
    }

    public int[] calculateStats(String newStatLine) {
      st.setString(newStatLine);
      st.skipToken(); // cpu name
      final int MAX_CPU_STATS = CPU.values().length;
      /*
       * newer kernels now have 10 columns for cpu in /proc/stat. This number may increase even
       * further, hence we now use List in place of long[]. We add up entries from all columns after
       * MAX_CPU_STATS into CPU.OTHER
       */
      List<Long> newStats = new ArrayList<Long>(10);
      List<Long> diffs = new ArrayList<Long>(10);
      long total_change = 0;
      int actualCpuStats = 0;
      long unaccountedCpuUtilization = 0;

      while (st.hasMoreTokens()) {
        newStats.add(st.nextTokenAsLong());
        actualCpuStats++;
      }

      if (lastCpuStatsInvalid) {
        lastCpuStats = newStats;
        lastCpuStatsInvalid = false;
        for (int i = 0; i < MAX_CPU_STATS; i++) {
          diffs.add(0L);
        }
        diffs.set(CPU.IDLE.ordinal(), 100L);
      } else {
        for (int i = 0; i < actualCpuStats; i++) {
          diffs.add(newStats.get(i) - lastCpuStats.get(i));
          total_change += diffs.get(i);
          lastCpuStats.set(i, newStats.get(i));
        }
        if (total_change == 0) {
          // avoid divide by zero
          total_change = 1;
        }
        for (int i = 0; i < MAX_CPU_STATS; i++) {
          if (i < actualCpuStats) {
            diffs.set(i, (diffs.get(i) * 100) / total_change);
          }
        }
        for (int i = MAX_CPU_STATS; i < actualCpuStats; i++) {
          unaccountedCpuUtilization += (diffs.get(i) * 100) / total_change;
        }
      }
      int[] ret = new int[MAX_CPU_STATS];
      for (int i = 0; i < MAX_CPU_STATS; i++) {
        if (i < actualCpuStats) {
          ret[i] = diffs.get(i).intValue();
        }
      }
      ret[CPU.OTHER.ordinal()] += (int) unaccountedCpuUtilization;
      return ret;
    }
  }

  private static class SpaceTokenizer {
    private String str;
    private char[] rawChars;
    private int beginIdx;
    private int endIdx;
    private int nextIdx;

    protected SpaceTokenizer() {
      endIdx = -1;
      nextIdx = -1;
    }

    protected void releaseResources() {
      str = null;
      rawChars = null;
      endIdx = -1;
      nextIdx = -1;
    }

    private void nextIdx() {
      int origin = nextIdx;
      if (endIdx == rawChars.length || beginIdx == -1) {
        endIdx = -1;
        nextIdx = -1;
        return;
      }
      endIdx = -1;
      nextIdx = -1;
      for (int i = origin + 1; i < rawChars.length; i++) {
        char c = rawChars[i];
        // Add all delimiters here
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
          if (endIdx == -1) {
            endIdx = i;
          }
        } else {
          // this handles multiple consecutive delimiters
          if (endIdx != -1) {
            nextIdx = i;
            return;
          }
        }
      }
      if (endIdx == -1) {
        // indicates we were still reading white space at the end of the string
        endIdx = rawChars.length;
      }
    }

    protected boolean hasMoreTokens() {
      return endIdx != -1;
    }

    protected void setString(String data) {
      str = data;
      rawChars = new char[str.length()];
      str.getChars(0, str.length(), rawChars, 0);
      beginIdx = 0;
      endIdx = -1;
      nextIdx = -1;
      nextIdx();
    }

    protected boolean skipToken() {
      if (hasMoreTokens()) {
        beginIdx = nextIdx;
        nextIdx();
        return true;
      }
      return false;
    }

    protected String nextToken() {
      if (hasMoreTokens()) {
        final String ret = str.substring(beginIdx, endIdx);
        beginIdx = nextIdx;
        nextIdx();
        return ret;
      }
      throw new NoSuchElementException();
    }

    protected String peekToken() {
      if (hasMoreTokens()) {
        return str.substring(beginIdx, endIdx);
      }
      throw new NoSuchElementException();
    }

    protected void skipTokens(int numberToSkip) {
      int remaining = numberToSkip + 1;
      while (--remaining > 0 && skipToken());
    }

    protected static long parseAsLong(String number) {
      long l = 0L;
      try {
        l = Long.parseLong(number);
      } catch (NumberFormatException nfe) {
      }
      return l;
    }

    protected static int parseAsInt(String number) {
      int i = 0;
      try {
        i = Integer.parseInt(number);
      } catch (NumberFormatException nfe) {
      }
      return i;
    }

    protected int nextTokenAsInt() {
      int i = 0;
      try {
        i = Integer.parseInt(nextToken());
      } catch (NumberFormatException nfe) {
      }
      return i;
    }

    protected long nextTokenAsLong() {
      long l = 0L;
      try {
        l = Long.parseLong(nextToken());
      } catch (NumberFormatException nfe) {
      }
      return l;
    }

    protected double nextTokenAsDouble() {
      double d = 0;
      try {
        d = Double.parseDouble(nextToken());
      } catch (NumberFormatException nfe) {
      }
      return d;
    }
  }
}

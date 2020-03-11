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
package org.apache.geode.internal;

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.GemFireException;
import org.apache.geode.GemFireIOException;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.NoSystemException;
import org.apache.geode.SystemFailure;
import org.apache.geode.UncreatedSystemException;
import org.apache.geode.UnstartedSystemException;
import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.internal.AdminDistributedSystemImpl;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.HighPriorityAckedMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.internal.admin.remote.TailLogResponse;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.backup.BackupOperation;
import org.apache.geode.internal.logging.DateFormatter;
import org.apache.geode.internal.logging.MergeLogFiles;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.internal.statistics.StatArchiveReader;
import org.apache.geode.internal.statistics.StatArchiveReader.ResourceInst;
import org.apache.geode.internal.statistics.StatArchiveReader.StatValue;
import org.apache.geode.internal.util.JavaCommandBuilder;
import org.apache.geode.internal.util.PluckStacks;
import org.apache.geode.internal.util.PluckStacks.ThreadStack;
import org.apache.geode.management.BackupStatus;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Provides static methods for various system administation tasks.
 */
public class SystemAdmin {

  /**
   * Finds the gemfire jar path element in the given classpath and returns the directory that jar is
   * in.
   */
  public static File findGemFireLibDir() {
    URL jarURL = GemFireVersion.getJarURL();
    if (jarURL == null)
      return null;
    String path = jarURL.getPath();
    // Decode URL to get rid of escaped characters. See bug 32465.
    path = URLDecoder.decode(path);
    File f = new File(path);
    if (f.isDirectory()) {
      return f;
    }
    return f.getParentFile();
  }

  // ------------------- begin: Locator methods ------------------------------

  public void locatorStart(File directory, String portOption, String addressOption,
      String gemfirePropertiesFileOption, boolean peerOption, boolean serverOption,
      String hostnameForClientsOption) throws InterruptedException {
    // if (Thread.interrupted()) throw new InterruptedException(); not necessary checked in
    // locatorStart
    locatorStart(directory, portOption, addressOption, gemfirePropertiesFileOption, null, null,
        peerOption, serverOption, hostnameForClientsOption);
  }

  public void locatorStart(File directory, String portOption, String addressOption,
      String gemfirePropertiesFileOption, Properties propertyOptionArg, List xoptions,
      boolean peerOption, boolean serverOption, String hostnameForClientsOption)
      throws InterruptedException {
    if (Thread.interrupted())
      throw new InterruptedException();
    int port = DistributionLocator.parsePort(portOption);

    if (addressOption == null)
      addressOption = "";

    if (!addressOption.equals("")) {
      // make sure its a valid ip address
      if (!validLocalAddress(addressOption)) {
        throw new IllegalArgumentException(
            String.format("' %s ' is not a valid IP address for this machine",
                addressOption));
      }
    }
    // check to see if locator is already running
    File logFile = new File(directory, DistributionLocator.DEFAULT_STARTUP_LOG_FILE);
    try {
      // make sure file can be opened for writing
      (new FileOutputStream(logFile.getPath(), true)).close();
    } catch (IOException ex) {
      throw new GemFireIOException(
          String.format(
              "Logfile %s could not be opened for writing. Verify file permissions are correct and that another locator is not already running in the same directory.",
              logFile.getPath()),
          ex);
    }

    if (gemfirePropertiesFileOption != null) {
      Properties newPropOptions = new Properties();// see #43731
      newPropOptions.putAll(propertyOptionArg);
      newPropOptions.setProperty("gemfirePropertyFile", gemfirePropertiesFileOption);
      propertyOptionArg = newPropOptions;
    }

    // read ssl properties
    Map<String, String> env = new HashMap<String, String>();
    SocketCreator.readSSLProperties(env);

    List cmdVec = JavaCommandBuilder.buildCommand(getDistributionLocatorPath(), null,
        propertyOptionArg, xoptions);

    cmdVec.add(String.valueOf(port));
    cmdVec.add(addressOption);
    cmdVec.add(Boolean.toString(peerOption));
    cmdVec.add(Boolean.toString(serverOption));
    if (hostnameForClientsOption == null) {
      hostnameForClientsOption = "";
    }
    cmdVec.add(hostnameForClientsOption);

    String[] cmd = (String[]) cmdVec.toArray(new String[0]);

    // start with a fresh log each time
    if (!logFile.delete() && logFile.exists()) {
      throw new GemFireIOException("Unable to delete " + logFile.getAbsolutePath());
    }
    boolean treatAsPure = true;
    /*
     * A counter used by PureJava to determine when its waited too long to start the locator
     * process. countDown * 250 = how many seconds to wait before giving up.
     */
    int countDown = 60;
    // NYI: wait around until we can attach
    while (!ManagerInfo.isLocatorStarted(directory)) {
      countDown--;
      Thread.sleep(250);
      if (countDown < 0) {
        try {
          String msg = tailFile(logFile, false);
          throw new GemFireIOException(
              String.format("Start of locator failed. The end of %s contained this message: %s.",
                  logFile, msg),
              null);
        } catch (IOException ignore) {
          throw new GemFireIOException(
              String.format("Start of locator failed. Check end of %s for reason.",
                  logFile),
              null);
        }
      }
      Thread.sleep(500);
    }
  }


  /** get the path to the distribution locator class */
  protected String getDistributionLocatorPath() {
    return "org.apache.geode.internal.DistributionLocator";
  }

  /**
   * enumerates all available local network addresses to find a match with the given address.
   * Returns false if the address is not usable on the current machine
   */
  public static boolean validLocalAddress(String bindAddress) {
    InetAddress addr = null;
    try {
      addr = InetAddress.getByName(bindAddress);
    } catch (UnknownHostException ex) {
      return false;
    }
    try {
      Enumeration en = NetworkInterface.getNetworkInterfaces();
      while (en.hasMoreElements()) {
        NetworkInterface ni = (NetworkInterface) en.nextElement();
        Enumeration en2 = ni.getInetAddresses();
        while (en2.hasMoreElements()) {
          InetAddress check = (InetAddress) en2.nextElement();
          if (check.equals(addr)) {
            return true;
          }
        }
      }
    } catch (SocketException sex) {
      return true; // can't query the interfaces - punt
    }
    return false;
  }

  @SuppressWarnings("hiding")
  public void locatorStop(File directory, String portOption, String addressOption,
      Properties propertyOption) throws InterruptedException {
    if (Thread.interrupted())
      throw new InterruptedException();
    InetAddress addr = null; // fix for bug 30810
    if (addressOption == null) {
      addressOption = "";
    }
    addressOption = addressOption.trim();
    if (!addressOption.equals("")) {
      // make sure its a valid ip address
      try {
        addr = InetAddress.getByName(addressOption);
      } catch (UnknownHostException ex) {
        throw new IllegalArgumentException(
            String.format("-address value was not a known IP address: %s",
                ex));
      }
    }

    if (propertyOption != null) {
      Iterator iter = propertyOption.keySet().iterator();
      while (iter.hasNext()) {
        String key = (String) iter.next();
        System.setProperty(key, propertyOption.getProperty(key));
      }
    }
    int port = DistributionLocator.parsePort(portOption);
    int pid = 0;
    try {
      ManagerInfo info = ManagerInfo.loadLocatorInfo(directory);
      pid = info.getManagerProcessId();
      if (portOption == null || portOption.trim().length() == 0) {
        port = info.getManagerPort();
      }
      if (addr == null) {
        addr = info.getManagerAddress();
      }

      try {
        new TcpClient(SocketCreatorFactory
            .getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR),
            InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
            InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer())
                .stop(new HostAndPort(addr.getHostName(), port));
      } catch (java.net.ConnectException ce) {
        System.out.println(
            "Unable to connect to Locator process. Possible causes are that an incorrect bind address/port combination was specified to the stop-locator command or the process is unresponsive.");
        return;
      }
      // wait for the locator process to go away
      // format and change message
      if (!quiet) {
        System.out.println(
            "Waiting 5 seconds for locator process to terminate...");
      }
      Thread.sleep(5000);
    } catch (UnstartedSystemException ex) {
      // fix for bug 28133
      throw new UnstartedSystemException(
          String.format("Locator in directory %s is not running.",
              directory));
    } catch (NoSystemException ex) {
      // before returning see if a stale lock file/shared memory can be cleaned up
      cleanupAfterKilledLocator(directory);
      throw ex;
    }
  }

  /**
   * Gets the status of a locator.
   *
   * @param directory the locator's directory
   * @return the status string. Will be one of the following: "running", "killed", "stopped",
   *         "stopping", or "starting".
   * @throws UncreatedSystemException if the locator <code>directory</code> does not exist or is not
   *         a directory.
   * @throws GemFireIOException if the manager info exists but could not be read. This probably
   *         means that the info file is corrupt.
   */
  public String locatorStatus(File directory) {
    return ManagerInfo.getLocatorStatusCodeString(directory);
  }

  /**
   * Gets information on the locator.
   *
   * @param directory the locator's directory
   * @return information string.
   * @throws UncreatedSystemException if the locator <code>directory</code> does not exist or is not
   *         a directory.
   * @throws GemFireIOException if the manager info exists but could not be read. This probably
   *         means that the info file is corrupt.
   */
  public String locatorInfo(File directory) {
    int statusCode = ManagerInfo.getLocatorStatusCode(directory);
    String statusString = ManagerInfo.statusToString(statusCode);
    try {
      ManagerInfo mi = ManagerInfo.loadLocatorInfo(directory);
      if (statusCode == ManagerInfo.KILLED_STATUS_CODE) {
        return String.format("Locator in %s was killed while it was %s. Locator process id was %s.",
            directory, ManagerInfo.statusToString(mi.getManagerStatus()),
            Integer.valueOf(mi.getManagerProcessId()));
      } else {
        return String.format("Locator in %s is %s. Locator process id is %s.",
            directory, statusString, Integer.valueOf(mi.getManagerProcessId()));
      }
    } catch (UnstartedSystemException ex) {
      return String.format("Locator in %s is stopped.", directory);
    } catch (GemFireIOException ex) {
      return String.format("Locator in %s is starting.", directory);
    }
  }

  /**
   * Cleans up any artifacts left by a killed locator. Namely the info file is deleted.
   */
  private static void cleanupAfterKilledLocator(File directory) {
    try {
      if (ManagerInfo.getLocatorStatusCode(directory) == ManagerInfo.KILLED_STATUS_CODE) {
        File infoFile = ManagerInfo.getLocatorInfoFile(directory);
        if (infoFile.exists()) {
          if (!infoFile.delete() && infoFile.exists()) {
            System.out.println("WARNING: unable to delete " + infoFile.getAbsolutePath());
          }
          if (!quiet) {
            System.out.println(
                "Cleaned up artifacts left by the previous killed locator.");
          }
        }
      }
    } catch (GemFireException ignore) {
    }
  }

  /**
   * Tails the end of the locator's log file
   *
   * @since GemFire 4.0
   */
  public String locatorTailLog(File directory) {
    File logFile = new File(directory, DistributionLocator.DEFAULT_LOG_FILE);
    if (!logFile.exists()) {
      return String.format("Log file %s does not exist.", logFile);
    }

    try {
      return TailLogResponse.tailSystemLog(logFile);
    } catch (IOException ex) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw, true);
      sw.write(String.format("An IOException was thrown while tailing %s",
          logFile));
      ex.printStackTrace(pw);
      pw.flush();
      return sw.toString();
    }
  }

  // -------------------- end: Locator methods -------------------------------

  public static void compactDiskStore(String... args) {
    compactDiskStore(Arrays.asList(args));
  }

  public static void compactDiskStore(List args) {
    String diskStoreName = (String) args.get(0);
    List dirList = args.subList(1, args.size());
    File[] dirs = new File[dirList.size()];
    Iterator it = dirList.iterator();
    int idx = 0;
    while (it.hasNext()) {
      dirs[idx] = new File((String) it.next());
      idx++;
    }
    try {
      DiskStoreImpl.offlineCompact(diskStoreName, dirs, false, maxOplogSize);
    } catch (Exception ex) {
      throw new GemFireIOException(" disk-store=" + diskStoreName + ": " + ex, ex);
    }
  }

  public static void upgradeDiskStore(List args) {
    String diskStoreName = (String) args.get(0);
    List dirList = args.subList(1, args.size());
    File[] dirs = new File[dirList.size()];
    Iterator it = dirList.iterator();
    int idx = 0;
    while (it.hasNext()) {
      dirs[idx] = new File((String) it.next());
      idx++;
    }
    try {
      DiskStoreImpl.offlineCompact(diskStoreName, dirs, true, maxOplogSize);
    } catch (Exception ex) {
      throw new GemFireIOException(" disk-store=" + diskStoreName + ": " + ex, ex);
    }
  }

  public static void compactAllDiskStores(List args) throws AdminException {
    InternalDistributedSystem ads = getAdminCnx();
    Map<DistributedMember, Set<PersistentID>> status =
        AdminDistributedSystemImpl.compactAllDiskStores(ads.getDistributionManager());

    System.out.println("Compaction complete.");
    System.out.println("The following disk stores compacted some files:");
    for (Set<PersistentID> memberStores : status.values()) {
      for (PersistentID store : memberStores) {
        System.out.println("\t" + store);
      }
    }
  }

  public static void validateDiskStore(String... args) {
    validateDiskStore(Arrays.asList(args));
  }

  public static void validateDiskStore(List args) {
    String diskStoreName = (String) args.get(0);
    List dirList = args.subList(1, args.size());
    File[] dirs = new File[dirList.size()];
    Iterator it = dirList.iterator();
    int idx = 0;
    while (it.hasNext()) {
      dirs[idx] = new File((String) it.next());
      idx++;
    }
    try {
      DiskStoreImpl.validate(diskStoreName, dirs);
    } catch (Exception ex) {
      throw new GemFireIOException(" disk-store=" + diskStoreName + ": " + ex, ex);
    }
  }

  private static InternalDistributedSystem getAdminCnx() {
    InternalDistributedSystem.setCommandLineAdmin(true);
    Properties props = propertyOption;
    props.setProperty(LOG_LEVEL, "warning");
    DistributionConfigImpl dsc = new DistributionConfigImpl(props);
    System.out.print("Connecting to distributed system:");
    if (!"".equals(dsc.getLocators())) {
      System.out.println(" locators=" + dsc.getLocators());
    } else {
      System.out.println(" mcast=" + dsc.getMcastAddress() + ":" + dsc.getMcastPort());
    }
    InternalDistributedSystem ds =
        (InternalDistributedSystem) InternalDistributedSystem.connectForAdmin(props);
    Set existingMembers = ds.getDistributionManager().getDistributionManagerIds();
    if (existingMembers.isEmpty()) {
      throw new RuntimeException("There are no members in the distributed system");
    }
    return ds;
  }

  public static void shutDownAll(ArrayList<String> cmdLine) {
    try {
      long timeout = 0;
      if (cmdLine.size() > 0) {
        timeout = Long.parseLong(cmdLine.get(0));
      }
      InternalDistributedSystem ads = getAdminCnx();
      Set members =
          AdminDistributedSystemImpl.shutDownAllMembers(ads.getDistributionManager(), timeout);
      int count = members == null ? 0 : members.size();
      if (members == null) {
        System.err
            .println("Unable to shut down the distributed system in the specified amount of time.");
      } else if (count == 0) {
        System.err.println("The distributed system had no members to shut down.");
      } else if (count == 1) {
        System.out.println("Successfully shut down one member");
      } else {
        System.out.println("Successfully shut down " + count + " members");
      }
    } catch (Exception ex) {
      throw new GemFireIOException(ex.toString(), ex);
    }
  }

  /**
   * this is a test hook to allow us to drive SystemAdmin functions without invoking main(), which
   * can call System.exit().
   *
   */
  public static void setDistributedSystemProperties(Properties props) {
    propertyOption = props;
  }

  public static void printStacks(List<String> cmdLine, boolean allStacks) {
    try {
      InternalDistributedSystem ads = getAdminCnx();
      HighPriorityAckedMessage msg = new HighPriorityAckedMessage();
      OutputStream os;
      PrintWriter ps;
      File outputFile = null;

      if (cmdLine.size() > 0) {
        outputFile = new File(cmdLine.get(0));
        os = new FileOutputStream(outputFile);
        ps = new PrintWriter(os);
      } else {
        os = System.out;
        ps = new PrintWriter(System.out);
      }

      Map<InternalDistributedMember, byte[]> dumps =
          msg.dumpStacks(ads.getDistributionManager().getAllOtherMembers(), false, true);
      for (Map.Entry<InternalDistributedMember, byte[]> entry : dumps.entrySet()) {
        ps.append("--- dump of stack for member " + entry.getKey()
            + " ------------------------------------------------------------------------------\n");
        ps.flush();
        GZIPInputStream zipIn = new GZIPInputStream(new ByteArrayInputStream(entry.getValue()));
        if (allStacks) {
          BufferedInputStream bin = new BufferedInputStream(zipIn);
          byte[] buffer = new byte[10000];
          int count;
          while ((count = bin.read(buffer)) != -1) {
            os.write(buffer, 0, count);
          }
          ps.append('\n');
        } else {
          BufferedReader reader = new BufferedReader(new InputStreamReader(zipIn));
          List<ThreadStack> stacks = (new PluckStacks()).getStacks(reader);
          for (ThreadStack s : stacks) {
            s.writeTo(ps);
            ps.append('\n');
          }
          ps.append('\n');
        }
      }
      ps.flush();
      os.close();
      if (outputFile != null) {
        System.out.println(dumps.size() + " stack dumps written to " + outputFile.getName());
      }
    } catch (Exception ex) {
      throw new GemFireIOException(ex.toString(), ex);
    }
  }

  public static void backup(String targetDir) throws AdminException {
    InternalDistributedSystem ads = getAdminCnx();

    // Baseline directory should be null if it was not provided on the command line
    BackupStatus status =
        new BackupOperation(ads.getDistributionManager(), ads.getCache()).backupAllMembers(
            targetDir,
            SystemAdmin.baselineDir);

    boolean incomplete = !status.getOfflineDiskStores().isEmpty();

    System.out.println("The following disk stores were backed up:");
    for (Set<PersistentID> memberStores : status.getBackedUpDiskStores().values()) {
      for (PersistentID store : memberStores) {
        System.out.println("\t" + store);
      }
    }
    if (incomplete) {
      System.err.println("The backup may be incomplete. The following disk stores are not online:");
      for (PersistentID store : status.getOfflineDiskStores()) {
        System.err.println("\t" + store);
      }
    } else {
      System.out.println("Backup successful.");
    }
  }

  public static void listMissingDiskStores() throws AdminException {
    InternalDistributedSystem ads = getAdminCnx();
    Set s = AdminDistributedSystemImpl.getMissingPersistentMembers(ads.getDistributionManager());
    if (s.isEmpty()) {
      System.out.println("The distributed system did not have any missing disk stores");
    } else {
      for (Object o : s) {
        System.out.println(o);
      }
    }
  }

  private static File[] argsToFile(Collection<String> args) {
    File[] dirs = new File[args.size()];

    int i = 0;
    for (String dir : args) {
      dirs[i++] = new File(dir);
    }
    return dirs;
  }

  public static void showDiskStoreMetadata(ArrayList<String> args) {
    String dsName = args.get(0);
    File[] dirs = argsToFile(args.subList(1, args.size()));

    try {
      DiskStoreImpl.dumpMetadata(dsName, dirs, showBuckets);
    } catch (Exception ex) {
      throw new GemFireIOException(" disk-store=" + dsName + ": " + ex, ex);
    }
  }

  public static void exportDiskStore(ArrayList<String> args, String outputDir) {
    File out = outputDir == null ? new File(".") : new File(outputDir);
    if (!out.exists()) {
      out.mkdirs();
    }

    String dsName = args.get(0);
    File[] dirs = argsToFile(args.subList(1, args.size()));

    try {
      DiskStoreImpl.exportOfflineSnapshot(dsName, dirs, out);
    } catch (Exception ex) {
      throw new GemFireIOException(" disk-store=" + dsName + ": " + ex, ex);
    }
  }

  public static void revokeMissingDiskStores(ArrayList<String> cmdLine)
      throws UnknownHostException, AdminException {
    String uuidString = cmdLine.get(0);
    UUID uuid = UUID.fromString(uuidString);
    InternalDistributedSystem ads = getAdminCnx();
    AdminDistributedSystemImpl.revokePersistentMember(ads.getDistributionManager(), uuid);
    Set<PersistentID> s =
        AdminDistributedSystemImpl.getMissingPersistentMembers(ads.getDistributionManager());


    // Fix for 42607 - wait to see if the revoked member goes way if it is still in the set of
    // missing members. It may take a moment to clear the missing member set after the revoke.
    long start = System.currentTimeMillis();
    while (containsRevokedMember(s, uuid)) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignore) {
      }
      s = AdminDistributedSystemImpl.getMissingPersistentMembers(ads.getDistributionManager());
      if (start + 30000 < System.currentTimeMillis()) {
        break;
      }
    }
    if (s.isEmpty()) {
      System.out.println("revocation was successful and no disk stores are now missing");
    } else {
      System.out.println("The following disk stores are still missing:");
      for (Object o : s) {
        System.out.println(o);
      }
    }
  }

  private static boolean containsRevokedMember(Set<PersistentID> missing, UUID revokedUUID) {
    for (PersistentID id : missing) {
      if (id.getUUID().equals(revokedUUID)) {
        return true;
      }
    }
    return false;
  }

  public static void modifyDiskStore(String... args) {
    modifyDiskStore(Arrays.asList(args));
  }

  public static void modifyDiskStore(List args) {
    String diskStoreName = (String) args.get(0);
    List dirList = args.subList(1, args.size());
    File[] dirs = new File[dirList.size()];
    Iterator it = dirList.iterator();
    int idx = 0;
    while (it.hasNext()) {
      dirs[idx] = new File((String) it.next());
      idx++;
    }
    try {
      if (lruOption != null || lruActionOption != null || lruLimitOption != null
          || concurrencyLevelOption != null || initialCapacityOption != null
          || loadFactorOption != null || compressorClassNameOption != null
          || statisticsEnabledOption != null) {
        if (regionOption == null) {
          throw new IllegalArgumentException("modify-disk-store requires -region=<regionName>");
        }
        if (remove) {
          throw new IllegalArgumentException(
              "the -remove option can not be used with the other modify options.");
        }
        DiskStoreImpl.modifyRegion(diskStoreName, dirs, regionOption, lruOption, lruActionOption,
            lruLimitOption, concurrencyLevelOption, initialCapacityOption, loadFactorOption,
            compressorClassNameOption, statisticsEnabledOption, null/* offHeap */, true);
        System.out.println("The region " + regionOption
            + " was successfully modified in the disk store " + diskStoreName);
      } else if (remove) {
        if (regionOption == null) {
          throw new IllegalArgumentException("modify-disk-store requires -region=<regionName>");
        }
        DiskStoreImpl.destroyRegion(diskStoreName, dirs, regionOption);
        System.out.println("The region " + regionOption
            + " was successfully removed from the disk store " + diskStoreName);
      } else {
        DiskStoreImpl.dumpInfo(System.out, diskStoreName, dirs, regionOption, null);
        if (regionOption == null) {
          System.out.println("Please specify -region=<regionName> and a modify option.");
        } else {
          System.out.println("Please specify a modify option.");
        }
      }
    } catch (Exception ex) {
      throw new GemFireIOException(" disk-store=" + diskStoreName + ": " + ex, ex);
    }
  }

  public void mergeLogs(String outOption, List<String> args) {
    Map<String, InputStream> inputs = new HashMap<>();

    PrintStream ps;
    if (outOption != null) {
      try {
        ps = new PrintStream(new FileOutputStream(outOption));
      } catch (FileNotFoundException ex) {
        throw new GemFireIOException(
            String.format("Could not create file %s for output because %s",
                outOption, getExceptionMessage(ex)));
      }
    } else {
      ps = System.out;
    }
    PrintWriter mergedFile = new PrintWriter(ps, true);

    List<String> normalizedFiles =
        args.stream().map(f -> Paths.get(f).toAbsolutePath().toString()).collect(
            Collectors.toList());
    int prefixLength =
        StringUtils.getCommonPrefix(normalizedFiles.toArray(new String[] {})).length();

    if (!quiet) {
      ps.println("Merging the following log files:");
    }

    for (String fileName : normalizedFiles) {
      try {
        String shortName = fileName.substring(prefixLength);
        inputs.put(shortName, new FileInputStream(fileName));
      } catch (FileNotFoundException ex) {
        throw new GemFireIOException(
            String.format("Could not open to %s for reading because %s",
                fileName, getExceptionMessage(ex)));
      }
      if (!quiet) {
        ps.println("  " + fileName);
      }
    }

    if (MergeLogFiles.mergeLogFiles(inputs, mergedFile)) {
      throw new GemFireIOException(
          "trouble merging log files.");
    }
    mergedFile.flush();
    if (outOption != null) {
      mergedFile.close();
    }
    if (!quiet) {
      System.out
          .println(String.format("Completed merge of %s logs to %s.",
              args.size(),
              ((outOption != null) ? outOption : "stdout")));
    }
  }

  /**
   * Returns the contents located at the end of the file as a string.
   *
   * @throws IOException if the file can not be opened or read
   */
  public String tailFile(File file, boolean problemsOnly) throws IOException {
    byte buffer[] = new byte[128000];
    int readSize = buffer.length;
    RandomAccessFile f = new RandomAccessFile(file, "r");
    long length = f.length();
    if (length < readSize) {
      readSize = (int) length;
    }
    long seekOffset = length - readSize;
    f.seek(seekOffset);
    if (readSize != f.read(buffer, 0, readSize)) {
      throw new EOFException(
          "Failed to read " + readSize + " bytes from " + file.getAbsolutePath());
    }
    f.close();
    // Now look for the last message header
    int msgStart = -1;
    int msgEnd = readSize;
    for (int i = readSize - 1; i >= 0; i--) {
      if (buffer[i] == '[' && (buffer[i + 1] == 's' || buffer[i + 1] == 'e' || buffer[i
          + 1] == 'w' /* ignore all messages except severe, error, and warning to fix bug 28968 */)
          && i > 0 && (buffer[i - 1] == '\n' || buffer[i - 1] == '\r')) {
        msgStart = i;
        break;
      }
    }
    if (msgStart == -1) {
      if (problemsOnly) {
        return null;
      }
      // Could not find message start. Show last line instead.
      for (int i = readSize - 3; i >= 0; i--) {
        if (buffer[i] == '\n' || buffer[i] == '\r') {
          msgStart = (buffer[i] == '\n') ? (i + 1) : (i + 2);
          break;
        }
      }
      if (msgStart == -1) {
        // Could not find line so show entire buffer
        msgStart = 0;
      }
    } else {
      // try to find the start of the next message and get rid of it
      for (int i = msgStart + 1; i < readSize; i++) {
        if (buffer[i] == '[' && (buffer[i - 1] == '\n' || buffer[i - 1] == '\r')) {
          msgEnd = i;
          break;
        }
      }
    }
    for (int i = msgStart; i < msgEnd; i++) {
      if (buffer[i] == '\n' || buffer[i] == '\r') {
        buffer[i] = ' ';
      }
    }
    return new String(buffer, msgStart, msgEnd - msgStart);
  }

  protected void format(PrintWriter pw, String msg, String linePrefix, int initialLength) {
    final int maxWidth = 79;
    boolean firstLine = true;
    int lineLength = 0;
    final int prefixLength = linePrefix.length();
    int idx = 0;
    while (idx < msg.length()) {
      if (lineLength == 0) {
        if (isBreakChar(msg, idx)) {
          // skip over only lead break characters
          idx++;
          continue;
        }
        pw.print(linePrefix);
        lineLength += prefixLength;
        if (firstLine) {
          firstLine = false;
          lineLength += initialLength;
        }
      }
      if (msg.charAt(idx) == '\n' || msg.charAt(idx) == '\r') {
        pw.println();
        lineLength = 0;
        if (msg.charAt(idx) == '\r') {
          idx += 2;
        } else {
          idx++;
        }
      } else if (msg.charAt(idx) == ' ' && idx > 0 && msg.charAt(idx - 1) == '.'
          && idx < (msg.length() - 1) && msg.charAt(idx + 1) == ' ') {
        // treat ". " as a hardbreak
        pw.println();
        lineLength = 0;
        idx += 2;
      } else {
        String word = msg.substring(idx, findWordBreak(msg, idx));
        if (lineLength == prefixLength || (word.length() + lineLength) <= maxWidth) {
          pw.print(word);
          lineLength += word.length();
          idx += word.length();
        } else {
          // no room on current line. So start a new line.
          pw.println();
          lineLength = 0;
        }
      }
    }
    if (lineLength != 0) {
      pw.println();
    }
  }

  private static final char breakChars[] = new char[] {' ', '\t', '\n', '\r'};

  private static boolean isBreakChar(String str, int idx) {
    char c = str.charAt(idx);
    for (int i = 0; i < breakChars.length; i++) {
      if (c == breakChars[i]) {
        return true;
      }
    }
    return false;
  }

  private static int findWordBreak(String str, int fromIdx) {
    int result = str.length();
    for (int i = 0; i < breakChars.length; i++) {
      int tmp = str.indexOf(breakChars[i], fromIdx + 1);
      if (tmp > fromIdx && tmp < result) {
        result = tmp;
      }
    }
    return result;
  }

  public static class StatSpec implements StatArchiveReader.StatSpec {
    public final String cmdLineSpec;
    public final String typeId;
    public final String instanceId;
    public final String statId;
    private final Pattern tp;
    private final Pattern sp;
    private final Pattern ip;
    private final int combineType;

    public StatSpec(String cmdLineSpec) {
      this.cmdLineSpec = cmdLineSpec;
      if (cmdLineSpec.charAt(0) == '+') {
        cmdLineSpec = cmdLineSpec.substring(1);
        if (cmdLineSpec.charAt(0) == '+') {
          cmdLineSpec = cmdLineSpec.substring(1);
          this.combineType = GLOBAL;
        } else {
          this.combineType = FILE;
        }
      } else {
        this.combineType = NONE;
      }
      int dotIdx = cmdLineSpec.lastIndexOf('.');
      String typeId = null;
      String instanceId = null;
      String statId = null;
      if (dotIdx != -1) {
        statId = cmdLineSpec.substring(dotIdx + 1);
        cmdLineSpec = cmdLineSpec.substring(0, dotIdx);
      }
      int commaIdx = cmdLineSpec.indexOf(':');
      if (commaIdx != -1) {
        instanceId = cmdLineSpec.substring(0, commaIdx);
        typeId = cmdLineSpec.substring(commaIdx + 1);
      } else {
        instanceId = cmdLineSpec;
      }

      if (statId == null || statId.length() == 0) {
        this.statId = "";
        this.sp = null;
      } else {
        this.statId = statId;
        this.sp = Pattern.compile(statId, Pattern.CASE_INSENSITIVE);
      }
      if (typeId == null || typeId.length() == 0) {
        this.typeId = "";
        this.tp = null;
      } else {
        this.typeId = typeId;
        this.tp = Pattern.compile(".*" + typeId, Pattern.CASE_INSENSITIVE);
      }
      if (instanceId == null || instanceId.length() == 0) {
        this.instanceId = "";
        this.ip = null;
      } else {
        this.instanceId = instanceId;
        this.ip = Pattern.compile(instanceId, Pattern.CASE_INSENSITIVE);
      }
    }

    @Override
    public String toString() {
      return "StatSpec instanceId=" + this.instanceId + " typeId=" + this.typeId + " statId="
          + this.statId;
    }

    @Override
    public int getCombineType() {
      return this.combineType;
    }

    @Override
    public boolean archiveMatches(File archive) {
      return true;
    }

    @Override
    public boolean statMatches(String statName) {
      if (this.sp == null) {
        return true;
      } else {
        Matcher m = this.sp.matcher(statName);
        return m.matches();
      }
    }

    @Override
    public boolean typeMatches(String typeName) {
      if (this.tp == null) {
        return true;
      } else {
        Matcher m = this.tp.matcher(typeName);
        return m.matches();
      }
    }

    @Override
    public boolean instanceMatches(String textId, long numericId) {
      if (this.ip == null) {
        return true;
      } else {
        Matcher m = this.ip.matcher(textId);
        if (m.matches()) {
          return true;
        }
        m = this.ip.matcher(String.valueOf(numericId));
        return m.matches();
      }
    }
  }

  private static StatSpec[] createSpecs(List cmdLineSpecs) {
    StatSpec[] result = new StatSpec[cmdLineSpecs.size()];
    Iterator it = cmdLineSpecs.iterator();
    int idx = 0;
    while (it.hasNext()) {
      result[idx] = new StatSpec((String) it.next());
      idx++;
    }
    return result;
  }

  private static void printStatValue(StatArchiveReader.StatValue v, long startTime, long endTime,
      boolean nofilter, boolean persec, boolean persample, boolean prunezeros, boolean details) {
    v = v.createTrimmed(startTime, endTime);
    if (nofilter) {
      v.setFilter(StatArchiveReader.StatValue.FILTER_NONE);
    } else if (persec) {
      v.setFilter(StatArchiveReader.StatValue.FILTER_PERSEC);
    } else if (persample) {
      v.setFilter(StatArchiveReader.StatValue.FILTER_PERSAMPLE);
    }
    if (prunezeros) {
      if (v.getSnapshotsMinimum() == 0.0 && v.getSnapshotsMaximum() == 0.0) {
        return;
      }
    }
    System.out.println("  " + v.toString());
    if (details) {
      System.out.print("  values=");
      double[] snapshots = v.getSnapshots();
      for (int i = 0; i < snapshots.length; i++) {
        System.out.print(' ');
        System.out.print(snapshots[i]);
      }
      System.out.println();
      String desc = v.getDescriptor().getDescription();
      if (desc != null && desc.length() > 0) {
        System.out.println("    " + desc);
      }
    }
  }

  /**
   * List the statistics of a running system.
   *
   * @param directory the system directory of the system to list.
   * @param archiveNames the archive file(s) to read.
   * @param details if true the statistic descriptions will also be listed.
   * @param nofilter if true then printed stat values will all be raw unfiltered.
   * @param persec if true then printed stat values will all be the rate of change, per second, of
   *        the raw values.
   * @param persample if true then printed stat values will all be the rate of change, per sample,
   *        of the raw values.
   * @param prunezeros if true then stat values whose samples are all zero will not be printed.
   *
   * @throws UncreatedSystemException if the system <code>sysDir</code> does not exist, is not a
   *         directory, or does not contain a configuration file.
   * @throws NoSystemException if the system is not running or could not be connected to.
   * @throws IllegalArgumentException if a statSpec does not match a resource and/or statistic.
   * @throws GemFireIOException if the archive could not be read
   */
  public void statistics(File directory, List archiveNames, boolean details, boolean nofilter,
      boolean persec, boolean persample, boolean prunezeros, boolean monitor, long startTime,
      long endTime, List cmdLineSpecs) {
    if (persec && nofilter) {
      throw new IllegalArgumentException(
          "The -nofilter and -persec options are mutually exclusive.");
    }
    if (persec && persample) {
      throw new IllegalArgumentException(
          "The -persample and -persec options are mutually exclusive.");
    }
    if (nofilter && persample) {
      throw new IllegalArgumentException(
          "The -persample and -nofilter options are mutually exclusive.");
    }
    StatSpec[] specs = createSpecs(cmdLineSpecs);
    if (archiveOption != null) {
      if (directory != null) {
        throw new IllegalArgumentException(
            "The -archive= and -dir= options are mutually exclusive.");
      }
      StatArchiveReader reader = null;
      boolean interrupted = false;
      try {
        reader = new StatArchiveReader((File[]) archiveNames.toArray(new File[0]),
            specs, !monitor);
        // Runtime.getRuntime().gc(); System.out.println("DEBUG: heap size=" +
        // (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
        if (specs.length == 0) {
          if (details) {
            StatArchiveReader.StatArchiveFile[] archives = reader.getArchives();
            for (int i = 0; i < archives.length; i++) {
              System.out.println(archives[i].getArchiveInfo().toString());
            }
          }
        }
        do {
          if (specs.length == 0) {
            Iterator it = reader.getResourceInstList().iterator();
            while (it.hasNext()) {
              ResourceInst inst = (ResourceInst) it.next();
              StatValue values[] = inst.getStatValues();
              boolean firstTime = true;
              for (int i = 0; i < values.length; i++) {
                if (values[i] != null && values[i].hasValueChanged()) {
                  if (firstTime) {
                    firstTime = false;
                    System.out.println(inst.toString());
                  }
                  printStatValue(values[i], startTime, endTime, nofilter, persec, persample,
                      prunezeros, details);
                }
              }
            }
          } else {
            Map<CombinedResources, List<StatValue>> allSpecsMap =
                new HashMap<CombinedResources, List<StatValue>>();
            for (int i = 0; i < specs.length; i++) {
              StatValue[] values = reader.matchSpec(specs[i]);
              if (values.length == 0) {
                if (!quiet) {
                  System.err.println(String.format("[warning] No stats matched %s.",
                      specs[i].cmdLineSpec));
                }
              } else {
                Map<CombinedResources, List<StatValue>> specMap =
                    new HashMap<CombinedResources, List<StatValue>>();
                for (StatValue v : values) {
                  CombinedResources key = new CombinedResources(v);
                  List<StatArchiveReader.StatValue> list = specMap.get(key);
                  if (list != null) {
                    list.add(v);
                  } else {
                    specMap.put(key, new ArrayList<StatValue>(Collections.singletonList(v)));
                  }
                }
                if (!quiet) {
                  System.out.println(
                      String.format("[info] Found %s instances matching %s:",
                          new Object[] {Integer.valueOf(specMap.size()), specs[i].cmdLineSpec}));
                }
                for (Map.Entry<CombinedResources, List<StatValue>> me : specMap.entrySet()) {
                  List<StatArchiveReader.StatValue> list = allSpecsMap.get(me.getKey());
                  if (list != null) {
                    list.addAll(me.getValue());
                  } else {
                    allSpecsMap.put(me.getKey(), me.getValue());
                  }
                }
              }
            }
            for (Map.Entry<CombinedResources, List<StatValue>> me : allSpecsMap.entrySet()) {
              System.out.println(me.getKey());
              for (StatValue v : me.getValue()) {
                printStatValue(v, startTime, endTime, nofilter, persec, persample, prunezeros,
                    details);
              }
            }
          }
          if (monitor) {
            while (!reader.update()) {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException ignore) {
                interrupted = true;
              }
            }
          }
        } while (monitor && !interrupted);
      } catch (IOException ex) {
        throw new GemFireIOException(
            String.format("Failed reading %s", archiveOption), ex);
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException ignore) {
          }
        }
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /**
   * Represents a list of ResourceInst that have been combined together. Note the most common case
   * is for this class to only own a single ResourceInst.
   *
   */
  @SuppressWarnings("serial")
  private static class CombinedResources extends ArrayList<ResourceInst> {
    public CombinedResources(StatValue v) {
      super(Arrays.asList(v.getResources()));
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      boolean first = true;
      for (ResourceInst inst : this) {
        if (first) {
          first = false;
        } else {
          sb.append(" + ");
        }
        sb.append(inst);
      }
      return sb.toString();
    }
  }

  public SystemAdmin() {}

  private static final String[] helpTopics =
      new String[] {"all", "overview", "commands", "options", "usage", "configuration"};

  protected void printHelpTopic(String topic, PrintWriter pw) {
    if (topic.equalsIgnoreCase("all")) {
      for (int i = 0; i < helpTopics.length; i++) {
        if (!helpTopics[i].equals("all")) {
          pw.println("-------- " + helpTopics[i] + " --------");
          printHelpTopic(helpTopics[i], pw);
        }
      }
    } else if (topic.equalsIgnoreCase("overview")) {
      pw.println(
          String.format(
              "This program allows GemFire to be managed from the command line. It expects a command to execute.See the help topic %s. For a summary of supported options see the help topic %s.For a concise description of command line syntax see the help topic %s.For a description of system configuration see the help topic %s.For help on a specific command use the %s option with the command name.",

              new Object[] {"commands", "options", "usage", "configuration", "-h"}));
    } else if (topic.equalsIgnoreCase("commands")) {
      pw.println(usageMap.get("gemfire") + " <command> ...");
      format(pw, (String) helpMap.get("gemfire"), "  ", 0);
      for (int i = 0; i < validCommands.length; i++) {
        pw.println((String) usageMap.get(validCommands[i]));
        if (helpMap.get(validCommands[i]) == null) {
          pw.println("  (help message missing for " + validCommands[i] + ")");
        } else {
          format(pw, (String) helpMap.get(validCommands[i]), "  ", 0);
        }
      }
    } else if (topic.equalsIgnoreCase("options")) {
      pw.println(
          "All command line options start with a - and are not required.Each option has a default that will be used when its not specified.Options that take an argument always use a single = character, with no spaces, to delimit where the option name ends and the argument begins.Options that precede the command word can be used with any command and are also permitted to follow the command word.");
      for (int i = 0; i < validOptions.length; i++) {
        pw.print(validOptions[i] + ":");
        try {
          format(pw, (String) helpMap.get(validOptions[i]), "  ", validOptions[i].length() + 1);
        } catch (RuntimeException ex) {
          System.err.println(
              String.format("no help for option %s]", validOptions[i]));
          throw ex;
        }
      }
    } else if (topic.equalsIgnoreCase("usage")) {
      pw.println(
          "The following synax is used in the usage strings:[] designate an optional item() are used to group items<> designate non-literal text. Used to designate logical items* suffix means zero or more of the previous item| means the item to the left or right is required");
      for (int i = 0; i < validCommands.length; i++) {
        pw.println(getUsageString(validCommands[i]));
      }
    }
  }

  protected void help(List args) {
    String topic = "overview";
    if (args.size() > 0) {
      topic = (String) args.get(0);
      if (!Arrays.asList(helpTopics).contains(topic.toLowerCase())) {
        System.err.println(
            String.format("ERROR: Invalid help topic %s.", topic));
        usage();
      }
    }
    PrintWriter pw = new PrintWriter(System.out);
    printHelpTopic(topic, pw);
    pw.flush();
    if (args.size() == 0) {
      usage("help");
    }
  }

  protected void usage() {
    usage(null);
  }

  protected String getUsageString(String cmd) {
    StringBuffer result = new StringBuffer(80);
    result.append(usageMap.get("gemfire")).append(' ');
    if (cmd == null || cmd.equalsIgnoreCase("gemfire")) {
      result.append(join(Arrays.asList(validCommands), "|")).append(" ...");
    } else {
      result.append(usageMap.get(cmd.toLowerCase()));
    }
    return result.toString();
  }

  protected void usage(String cmd) {
    System.err.println(
        "Usage " + getUsageString(cmd));
    ExitCode.FATAL.doSystemExit();
  }

  private static final String[] validCommands = new String[] {"version", "stats", START_LOCATOR,
      "stop-locator", "status-locator", "info-locator", "tail-locator-log", "merge-logs",
      "revoke-missing-disk-store", "list-missing-disk-stores", "validate-disk-store",
      "upgrade-disk-store", "compact-disk-store", "compact-all-disk-stores", "modify-disk-store",
      "show-disk-store-metadata", "export-disk-store", "shut-down-all", "backup", "print-stacks",
      "help"};

  protected static String[] getValidCommands() {
    return validCommands.clone();
  }

  private static final String[] aliasCommands = new String[] {"locator-start", "locator-stop",
      "locator-status", "locator-info", "locator-tail-log", "logs-merge", "shutdown-all",
      "shutdownall", "compact", "modify", "validate"};
  private static final String[] validOptions =
      new String[] {"-address=", "-archive=", "-concurrencyLevel=", "-debug", "-remove", "-details",
          "-dir=", "-endtime=", "-h", "-help", "-initialCapacity=", "-loadFactor=", "-lru=",
          "-lruAction=", "-lruLimit=", "-maxOplogSize=", "-properties=", "-monitor", "-nofilter",
          "-persample", "-persec", "-out=", "-port=", "-prunezeros", "-region=", "-starttime=",
          "-statisticsEnabled=", "-peer=", "-server=", "-q", "-D", "-X", "-outputDir="};

  protected String checkCmd(String theCmd) {
    String cmd = theCmd;
    if (!Arrays.asList(validCommands).contains(cmd.toLowerCase())) {
      if (!Arrays.asList(aliasCommands).contains(cmd.toLowerCase())) {
        System.err
            .println(String.format("ERROR: Invalid command %s.", cmd));
        usage();
      } else {
        if (cmd.equalsIgnoreCase("locator-start")) {
          cmd = START_LOCATOR;
        } else if (cmd.equalsIgnoreCase("locator-stop")) {
          cmd = "stop-locator";
        } else if (cmd.equalsIgnoreCase("locator-info")) {
          cmd = "info-locator";
        } else if (cmd.equalsIgnoreCase("locator-status")) {
          cmd = "status-locator";
        } else if (cmd.equalsIgnoreCase("locator-tail-log")) {
          cmd = "tail-locator-log";
        } else if (cmd.equalsIgnoreCase("logs-merge")) {
          cmd = "merge-logs";
        } else if (cmd.equalsIgnoreCase("shutdownall")) {
          cmd = "shut-down-all";
        } else if (cmd.equalsIgnoreCase("shutdown-all")) {
          cmd = "shut-down-all";
        } else if (cmd.equalsIgnoreCase("upgrade")) {
          cmd = "upgrade-disk-store";
        } else if (cmd.equalsIgnoreCase("compact")) {
          cmd = "compact-disk-store";
        } else if (cmd.equalsIgnoreCase("modify")) {
          cmd = "modify-disk-store";
        } else if (cmd.equalsIgnoreCase("validate")) {
          cmd = "validate-disk-store";
        } else {
          throw new InternalGemFireException(
              String.format("Unhandled alias %s", cmd));
        }
      }
    }
    return cmd;
  }

  public static String join(Object[] a) {
    return join(a, " ");
  }

  public static String join(Object[] a, String joinString) {
    return join(Arrays.asList(a), joinString);
  }

  public static String join(List l) {
    return join(l, " ");
  }

  public static String join(List l, String joinString) {
    StringBuffer result = new StringBuffer(80);
    boolean firstTime = true;
    Iterator it = l.iterator();
    while (it.hasNext()) {
      if (firstTime) {
        firstTime = false;
      } else {
        result.append(joinString);
      }
      result.append(it.next());
    }
    return result.toString();
  }

  protected final Map helpMap = new HashMap();

  protected void initHelpMap() {
    helpMap.put("gemfire",
        "GemFire requires one of the following command strings: " + join(validCommands)
            + ". For additional help on a command specify it along with the '-h' option. "
            + "The '-debug' option causes gemfire to print out extra information when it fails.  "
            + "The '-h' and '-help' are synonyms that cause gemfire to print out help information instead of performing a task.  "
            + "The '-q' option quiets GemFire down by suppressing extra messages.  The '-J<vmOpt>' option passes <vmOpt> to the Java VM's command line.");
    helpMap.put("version", "Prints GemFire product version information.");
    helpMap.put("help",
        "Prints information on how to use this executable.  If an optional help topic is specified then more detailed help is printed.");
    helpMap.put("stats",
        "Prints statistic values from a statistic archive.  By default all statistics are printed.\n"
            + "The 'statSpec' arguments can be used to print individual resources or a specific statistic.\n"
            + "The format of a 'statSpec' is: an optional combine operator, followed by an optional instanceId, followed by an optional typeId, followed by an optional statId.\n"
            + "The '+' operator combines all matches in the same file.  The '++' operator combines all matches across all files.\n"
            + "An instanceId must be the name or id of a resource.  A typeId is a ':' followed by the name of a resource type.\n"
            + "A statId is a '.' followed by the name of a statistic.  A typeId or instanceId with no statId prints out all the matching resources and all their statistics.\n"
            + "A typeId or instanceId with a statId prints out just the named statistic on the matching resources.  A statId with no typeId or instanceId matches all statistics with that name.\n"
            + "The '-details' option causes statistic descriptions to also be printed.\n"
            + "The '-nofilter' option, in conjunction with '-archive=', causes the printed statistics to all be raw, unfiltered, values.\n"
            + "The '-persec' option, in conjunction with '-archive=', causes the printed statistics to be the rate of change, per second, of the raw values.\n"
            + "The '-persample' option, in conjunction with '-archive=', causes the printed statistics to be the rate of change, per sample, of the raw values.\n"
            + "The '-prunezeros' option', in conjunction with '-archive=', causes statistics whose values are all zero to not be printed.\n"
            + "The '-starttime=' option, in conjunction with '-archive=', causes statistics samples taken before this time to be ignored. The argument format must match "
            + DateFormatter.FORMAT_STRING + ".\n"
            + "The '-endtime' option, in conjunction with '-archive=', causes statistics samples taken after this time to be ignored. The argument format must match "
            + DateFormatter.FORMAT_STRING + ".\n"
            + "The '-archive=' option causes the data to come from an archive file.");
    helpMap.put(START_LOCATOR,
        "Starts a locator.\n"
            + "The 'port=' option specifies the port the locator will listen on.  It defaults to "
            + DistributionLocator.DEFAULT_LOCATOR_PORT + "\n."
            + "The '-address=' option specifies the address the locator will listen on.  It defaults to listening on all local addresses.\n"
            + "The '-dir=' option can be used to specify the directory the locator will run in.\n"
            + "The '-properties=' option can be used to specify the gemfire.properties file for configuring the locator's distributed system.  The file's path should be absolute, or relative to the locator's directory ('-dir=').\n"
            + "The '-peer=' option can be used to specify whether peer locator service should be enabled.  True (the default) will enable the service.\n"
            + "The '-server=' option can be used to specify whether server locator service should be enabled. True (the default) will enable the service.\n"
            + "The '-hostname-for-clients=' option can be used to specify a host name or ip address that will be sent to clients so they can connect to this locator. The default is to use the address the locator is listening on.\n"
            + "The '-D' option can be used to set system properties for the locator VM.  "
            + "The '-X' option can be used to set vendor-specific VM options and is usually used to increase the size of the locator VM when using multicast.");
    helpMap.put("stop-locator",
        "Stops a locator.\n"
            + "The '-port=' option specifies the port the locator is listening on. It defaults to "
            + DistributionLocator.DEFAULT_LOCATOR_PORT + ".\n"
            + "The '-address=' option specifies the address the locator is listening on. It defaults to the local host's address.\n"
            + "The '-dir=' option can be used to specify the directory the locator is running in.");
    helpMap.put("status-locator",
        "Prints the status of a locator. The status string will one of the following: "
            + join(ManagerInfo.statusNames) + ".\n"
            + "The '-dir=' option can be used to specify the directory of the locator whose status is desired.");
    helpMap.put("info-locator",
        "Prints information on a locator.  The information includes the process id of the locator, if the product is not running in PureJava mode.  "
            + "The '-dir=' option can be used to specify the directory of the locator whose information is desired.");
    helpMap.put("tail-locator-log",
        "Prints the last 64K bytes of the locator's log file.  The '-dir=' option can be used to specify the directory of the locator whose information is desired.");
    helpMap.put("merge-logs",
        "Merges multiple logs files into a single log.The -out option can be used to specify the file to write the merged log to. The default is stdout.");
    helpMap.put("validate-disk-store",
        "Checks to make sure files of a disk store are valid. The name of the disk store and the directories its files are stored in are required arguments.");
    helpMap.put("upgrade-disk-store", "Upgrade an offline disk store with new version format.\n"
        + "'-maxOplogSize=<long> causes the oplogs created by compaction to be no larger than the specified size in megabytes.");
    helpMap.put("compact-disk-store",
        "Compacts an offline disk store. Compaction removes all unneeded records from the persistent files.\n"
            + "-maxOplogSize=<long> causes the oplogs created by compaction to be no larger than the specified size in megabytes.");
    helpMap.put("compact-all-disk-stores",
        "Connects to a running system and tells its members to compact their disk stores. "
            + "This command uses the compaction threshold that each member has "
            + "configured for its disk stores. The disk store must have allow-force-compaction "
            + "set to true in order for this command to work.\n"
            + "This command will use the gemfire.properties file to determine what distributed system to connect to.");
    helpMap.put("modify-disk-store",
        "Modifies the contents stored in a disk store. Note that this operation writes to the disk store files so use it with care. Requires that a region name by specified using -region=<regionName> Options:   -remove will remove the region from the disk store causing any data stored in the disk store for this region to no longer exist. Subregions of the removed region will not be removed.   -lru=<type> Sets region's lru algorithm. Valid types are: none, lru-entry-count, lru-heap-percentage, or lru-memory-size   -lruAction=<action> Sets the region's lru action. Valid actions are: none, overflow-to-disk, local-destroy   -lruLimit=<int> Sets the region's lru limit. Valid values are >= 0   -concurrencyLevel=<int> Sets the region's concurrency level. Valid values are >= 0   -initialCapacity=<int> Sets the region's initial capacity. Valid values are >= 0.   -loadFactor=<float> Sets the region's load factory. Valid values are >= 0.0   -statisticsEnabled=<boolean> Sets the region's statistics enabled. Value values are true or false. The name of the disk store and the directories its files are stored in and the region to target are all required arguments.");
    helpMap.put("revoke-missing-disk-store",
        "Connects to a running system and tells its members to stop waiting for the "
            + "specified disk store to be available. Only revoke a disk store if its files "
            + "are lost. Once a disk store is revoked its files can no longer be loaded so be "
            + "careful. Use the list-missing-disk-stores command to get descriptions of the"
            + "missing disk stores.\n"
            + "You must pass the in the unique id for the disk store to revoke. The unique id is listed in the output "
            + "of the list-missing-disk-stores command, for example a63d7d99-f8f8-4907-9eb7-cca965083dbb.\n"
            + "This command will use the gemfire.properties file to determine what distributed system to connect to.");
    helpMap.put("list-missing-disk-stores",
        "Prints out a description of the disk stores that are currently missing from a distributed system\n\\n."
            + "This command will use the gemfire.properties file to determine what distributed system to connect to.");
    helpMap.put("export-disk-store",
        "Exports an offline disk store.  The persistent data is written to a binary format.\n"
            + "  -outputDir=<directory> specifies the location of the exported snapshot files.");
    helpMap.put("shut-down-all",
        "Connects to a running system and asks all its members that have a cache to close the cache and disconnect from system."
            + "The timeout parameter allows you to specify that the system should be shutdown forcibly after the time has exceeded.\n"
            + "This command will use the gemfire.properties file to determine what distributed system to connect to.");
    helpMap.put("backup",
        "Connects to a running system and asks all its members that have persistent data "
            + "to backup their data to the specified directory. The directory specified must exist "
            + "on all members, but it can be a local directory on each machine. This command "
            + "takes care to ensure that the backup files will not be corrupted by concurrent "
            + "operations. Backing up a running system with filesystem copy is not recommended.\n"
            + "This command will use the gemfire.properties file to determine what distributed system to connect to.");
    helpMap.put("print-stacks",
        "fetches stack dumps of all processes.  By default an attempt"
            + " is made to remove idle GemFire threads from the dump.  "
            + "Use -all-threads to include these threads in the dump.  "
            + "An optional filename may be given for storing the dumps.");
    helpMap.put("-out=",
        "Causes gemfire to write output to the specified file. The file is overwritten if it already exists.");
    helpMap.put("-debug",
        "Causes gemfire to print out extra information when it fails. This option is supported by all commands.");
    helpMap.put("-details",
        "Causes gemfire to print detailed information.  With the 'stats' command it means statistic descriptions.");
    helpMap.put("-nofilter",
        "Causes gemfire 'stats' command to print unfiltered, raw, statistic values. This is the default for non-counter statistics.");
    helpMap.put("-persec",
        "Causes gemfire 'stats' command to print the rate of change, per second, for statistic values. This is the default for counter statistics.");
    helpMap.put("-persample",
        "Causes gemfire 'stats' command to print the rate of change, per sample, for statistic values.");
    helpMap.put("-prunezeros",
        "Causes gemfire 'stats' command to not print statistics whose values are all zero.");
    helpMap.put("-port=",
        "Used to specify a non-default port when starting or stopping a locator.");
    helpMap.put("-address=",
        "Used to specify a specific IP address to listen on when starting or stopping a locator.");
    helpMap.put("-hostname-for-clients=",
        "Used to specify a host name or IP address to give to clients so they can connect to a locator.");
    helpMap.put("-properties=",
        "Used to specify the " + GeodeGlossary.GEMFIRE_PREFIX
            + "properties file to be used in configuring the locator's DistributedSystem.");
    helpMap.put("-archive=",
        "The argument is the statistic archive file the 'stats' command should read.");
    helpMap.put("-h",
        "Causes GemFire to print out information instead of performing the command. This option is supported by all commands.");
    helpMap.put("-help", helpMap.get("-h"));
    helpMap.put("-q",
        "Turns on quiet mode. This option is supported by all commands.");
    helpMap.put("-starttime=",
        "Causes the 'stats' command to ignore statistics samples taken before this time. The argument format must match "
            + DateFormatter.FORMAT_STRING + ".");
    helpMap.put("-endtime=",
        "Causes the 'stats' command to ignore statistics samples taken after this time. The argument format must match "
            + DateFormatter.FORMAT_STRING + ".");
    helpMap.put("-dir=",
        "The argument is the system directory the command should operate on.  If the argument is empty then a default system directory will be search for.\n"
            + "However the search will not include the " + GeodeGlossary.GEMFIRE_PREFIX
            + "properties file.  By default if a command needs a system directory, and one is not specified, then a search is done.\n"
            + "If a " + GeodeGlossary.GEMFIRE_PREFIX + "properties file can be located then "
            + GeodeGlossary.GEMFIRE_PREFIX
            + "systemDirectory property from that file is used.\n"
            + "Otherwise if the GEMFIRE environment variable is set to a directory that contains a subdirectory named defaultSystem then that directory is used.\n"
            + "The property file is searched for in the following locations:\n"
            + "1. The current working directory.\n"
            + "2. The user's home directory.\n"
            + "3. The class path.  All commands except 'version' use the system directory.");
    helpMap.put("-D",
        "Sets a Java system property in the locator VM.  Used most often for configuring SSL communication.");
    helpMap.put("-X",
        "Sets a Java VM X setting in the locator VM.  Used most often for increasing the size of the virtual machine.");
    helpMap.put("-remove",
        "Causes the region specified by the -region=<regionName> to be removed from a disk store. Any records in the disk store for this region become garbage and will be deleted from the disk store files if compact-disk-store is called. Note that this option writes to the disk store files so use it with care.");
    helpMap.put("-maxOplogSize=",
        "Limits the size of any oplogs that are created to the specified size in megabytes.");
    helpMap.put("-lru=",
        "-lru=<type> Sets region's lru algorithm. Valid types are: none, lru-entry-count, lru-heap-percentage, or lru-memory-size");
    helpMap.put("-lruAction=",
        "-lruAction=<action> Sets the region's lru action. Valid actions are: none, overflow-to-disk, local-destroy");
    helpMap.put("-lruLimit=",
        "-lruLimit=<int> Sets the region's lru limit. Valid values are >= 0");
    helpMap.put("-concurrencyLevel=",
        "-concurrencyLevel=<int> Sets the region's concurrency level. Valid values are >= 0");
    helpMap.put("-initialCapacity=",
        "-initialCapacity=<int> Sets the region's initial capacity. Valid values are >= 0");
    helpMap.put("-loadFactor=",
        "-loadFactor=<float> Sets the region's load factory. Valid values are >= 0.0");
    helpMap.put("-statisticsEnabled=",
        "-statisticsEnabled=<boolean> Sets the region's statistics enabled. Value values are true or false");
    helpMap.put("-region=", "Used to specify what region an operation is to be done on.");
    helpMap.put("-monitor",
        "-monitor Causes the stats command to keep periodically checking its statistic archives for updates.");
    helpMap.put("-peer=",
        "-peer=<true|false> True, the default, causes the locator to find peers for other peers. False will cause the locator to not locate peers.");
    helpMap.put("-server=",
        "-server=<true|false> True, the default, causes the locator to find servers for clients. False will cause the locator to not locate servers for clients.");
    helpMap.put("-outputDir=", "The directory where the disk store should be exported.");
  }

  protected final Map usageMap = new HashMap();

  protected void initUsageMap() {
    usageMap.put("gemfire", "gemfire [-debug] [-h[elp]] [-q] [-J<vmOpt>]*");
    usageMap.put("version", "version");
    usageMap.put("help", "help [" + join(helpTopics, "|") + "]");
    usageMap.put("stats",
        "stats ([<instanceId>][:<typeId>][.<statId>])* [-details] [-nofilter|-persec|-persample] [-prunezeros] [-starttime=<time>] [-endtime=<time>] -archive=<statFile>");
    usageMap.put(START_LOCATOR,
        "start-locator [-port=<port>] [-address=<ipAddr>] [-dir=<locatorDir>] [-properties=<gemfire.properties>] [-peer=<true|false>] [-server=<true|false>] [-hostname-for-clients=<ipAddr>] [-D<system.property>=<value>] [-X<vm-setting>]");
    usageMap.put("stop-locator",
        "stop-locator [-port=<port>] [-address=<ipAddr>] [-dir=<locatorDir>]");
    usageMap.put("status-locator", "status-locator [-dir=<locatorDir>]");
    usageMap.put("info-locator", "info-locator [-dir=<locatorDir>]");
    usageMap.put("tail-locator-log", "tail-locator-log [-dir=<locatorDir>]");
    usageMap.put("merge-logs", "merge-logs <logFile>+ [-out=<outFile>]");
    usageMap.put("validate-disk-store", "validate-disk-store <diskStoreName> <directory>+");
    usageMap.put("upgrade-disk-store",
        "upgrade-disk-store <diskStoreName> <directory>+ [-maxOplogSize=<int>]");
    usageMap.put("compact-disk-store",
        "compact-disk-store <diskStoreName> <directory>+ [-maxOplogSize=<int>]");
    usageMap.put("compact-all-disk-stores", "compact-all-disk-stores");
    usageMap.put("modify-disk-store",
        "modify-disk-store <diskStoreName> <directory>+ [-region=<regionName> [-remove|(-lru=<none|lru-entry-count|lru-heap-percentage|lru-memory-size>|-lruAction=<none|overflow-to-disk|local-destroy>|-lruLimit=<int>|-concurrencyLevel=<int>|-initialCapacity=<int>|-loadFactor=<float>|-statisticsEnabled=<boolean>)*]]");
    usageMap.put("list-missing-disk-stores", "list-missing-disk-stores");
    usageMap.put("export-disk-store",
        "export-disk-store <diskStoreName> <directory>+ [-outputDir=<directory>]");
    usageMap.put("shut-down-all", "shut-down-all [timeout_in_ms]");
    usageMap.put("backup", "backup [-baseline=<baseline directory>] <target directory>");
    usageMap.put("revoke-missing-disk-store", "revoke-missing-disk-store <disk-store-id>");
    usageMap.put("print-stacks", "print-stacks [-all-threads] [<filename>]");
  }

  // option statics
  @MakeNotStatic
  private static boolean debug = false;
  @MakeNotStatic
  private static boolean details = false;
  @MakeNotStatic
  private static boolean nofilter = false;
  @MakeNotStatic
  private static boolean persec = false;
  @MakeNotStatic
  private static boolean persample = false;
  @MakeNotStatic
  private static boolean prunezeros = false;
  @MakeNotStatic
  private static boolean quiet = false;
  @MakeNotStatic
  private static boolean help = false;
  @MakeNotStatic
  private static boolean monitor = false;
  @MakeNotStatic
  private static boolean showBuckets = false;
  @MakeNotStatic
  private static long startTime = -1;
  @MakeNotStatic
  private static long endTime = -1;
  @MakeNotStatic
  private static String portOption = null;
  @MakeNotStatic
  private static String addressOption = "";
  @MakeNotStatic
  private static String regionOption = null;
  @MakeNotStatic
  private static long maxOplogSize = -1L;
  @MakeNotStatic
  private static String lruOption = null;
  @MakeNotStatic
  private static String lruActionOption = null;
  @MakeNotStatic
  private static String lruLimitOption = null;
  @MakeNotStatic
  private static String concurrencyLevelOption = null;
  @MakeNotStatic
  private static String initialCapacityOption = null;
  @MakeNotStatic
  private static String loadFactorOption = null;
  @MakeNotStatic
  private static String compressorClassNameOption = null;
  @MakeNotStatic
  private static String statisticsEnabledOption = null;
  @MakeNotStatic
  private static boolean remove = false;
  @MakeNotStatic
  private static String sysDirName = null;
  @MakeNotStatic
  private static final ArrayList archiveOption = new ArrayList();
  @MakeNotStatic
  private static String printStacksOption = null;
  @MakeNotStatic
  private static String outOption = null;
  @MakeNotStatic
  private static Properties propertyOption = new Properties();
  @MakeNotStatic
  private static boolean serverOption = true;
  @MakeNotStatic
  private static boolean peerOption = true;
  @MakeNotStatic
  private static String gemfirePropertiesFileOption = null;
  @MakeNotStatic
  private static final ArrayList xoptions = new ArrayList();
  @MakeNotStatic
  private static String hostnameForClientsOption = null;
  @MakeNotStatic
  private static String baselineDir = null; // Baseline directory option value for backup command
  @MakeNotStatic
  private static String outputDir = null;

  @Immutable
  private static final Map<String, String[]> cmdOptionsMap;
  static {
    Map<String, String[]> optionsMap = new HashMap<>();
    optionsMap.put("gemfire", new String[] {"--help", "-h", "-help", "-debug", "-q"});
    optionsMap.put("version", new String[] {});
    optionsMap.put("help", new String[] {});
    optionsMap.put("merge-logs", new String[] {"-out="});
    optionsMap.put("stats", new String[] {"-details", "-monitor", "-nofilter", "-persec",
        "-persample", "-prunezeros", "-archive=", "-starttime=", "-endtime="});
    optionsMap.put(START_LOCATOR, new String[] {"-port=", "-dir=", "-address=", "-properties=",
        "-D", "-X", "-peer=", "-server=", "-hostname-for-clients="});
    optionsMap.put("stop-locator", new String[] {"-port=", "-dir=", "-address=", "-D"});
    optionsMap.put("status-locator", new String[] {"-dir=", "-D"});
    optionsMap.put("info-locator", new String[] {"-dir=", "-D"});
    optionsMap.put("tail-locator-log", new String[] {"-dir=", "-D"});
    optionsMap.put("validate-disk-store", new String[] {});
    optionsMap.put("upgrade-disk-store", new String[] {"-maxOplogSize="});
    optionsMap.put("compact-disk-store", new String[] {"-maxOplogSize="});
    optionsMap.put("modify-disk-store",
        new String[] {"-region=", "-remove", "-lru=", "-lruAction=", "-lruLimit=",
            "-concurrencyLevel=", "-initialCapacity=", "-loadFactor=", "-statisticsEnabled="});
    optionsMap.put("list-missing-disk-stores", new String[] {});
    optionsMap.put("compact-all-disk-stores", new String[] {});
    optionsMap.put("revoke-missing-disk-store", new String[] {});
    optionsMap.put("show-disk-store-metadata", new String[] {"-buckets"});
    optionsMap.put("export-disk-store", new String[] {"-outputDir="});
    optionsMap.put("shut-down-all", new String[] {});
    optionsMap.put("backup", new String[] {"-baseline="});
    optionsMap.put("print-stacks", new String[] {"-all-threads"});

    cmdOptionsMap = Collections.unmodifiableMap(optionsMap);
  }

  private static long parseLong(String arg) {
    try {
      return Long.parseLong(arg);
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException(
          "Could not parse -maxOplogSize=" + arg + " because: " + ex.getMessage());
    }
  }

  private static long parseTime(String arg) {
    DateFormat fmt = DateFormatter.createDateFormat();
    try {
      Date d = fmt.parse(arg);
      return d.getTime();
    } catch (ParseException ex) {
      throw new IllegalArgumentException(
          String.format("Time was not in this format %s. %s",
              new Object[] {DateFormatter.FORMAT_STRING, ex}));
    }
  }

  protected boolean matchCmdArg(String cmd, String arg) {
    String[] validArgs = (String[]) cmdOptionsMap.get(cmd.toLowerCase());
    for (int i = 0; i < validArgs.length; i++) {
      if (validArgs[i].endsWith("=") || validArgs[i].equals("-D") || validArgs[i].equals("-X")) {
        if (arg.toLowerCase().startsWith(validArgs[i]) || arg.startsWith(validArgs[i])) {
          String argValue = arg.substring(validArgs[i].length());
          if (validArgs[i].equals("-dir=")) {
            sysDirName = argValue;
          } else if (validArgs[i].equals("-archive=")) {
            archiveOption.add(new File(argValue));
          } else if (validArgs[i].equals("-port=")) {
            portOption = argValue;
          } else if (validArgs[i].equals("-address=")) {
            addressOption = argValue;
          } else if (validArgs[i].equals("-region=")) {
            regionOption = argValue;
          } else if (validArgs[i].equals("-maxOplogSize=")) {
            maxOplogSize = parseLong(argValue);
          } else if (validArgs[i].equals("-lru=")) {
            lruOption = argValue;
          } else if (validArgs[i].equals("-lruAction=")) {
            lruActionOption = argValue;
          } else if (validArgs[i].equals("-lruLimit=")) {
            lruLimitOption = argValue;
          } else if (validArgs[i].equals("-concurrencyLevel=")) {
            concurrencyLevelOption = argValue;
          } else if (validArgs[i].equals("-initialCapacity=")) {
            initialCapacityOption = argValue;
          } else if (validArgs[i].equals("-loadFactor=")) {
            loadFactorOption = argValue;
          } else if (validArgs[i].equals("-compressor=")) {
            compressorClassNameOption = argValue;
          } else if (validArgs[i].equals("-statisticsEnabled=")) {
            statisticsEnabledOption = argValue;
          } else if (validArgs[i].equals("-properties=")) {
            gemfirePropertiesFileOption = argValue;
          } else if (validArgs[i].equals("-out=")) {
            outOption = argValue;
          } else if (validArgs[i].equals("-starttime=")) {
            startTime = parseTime(argValue);
          } else if (validArgs[i].equals("-endtime=")) {
            endTime = parseTime(argValue);
          } else if (validArgs[i].equals("-peer=")) {
            peerOption = "true".equalsIgnoreCase(argValue);
          } else if (validArgs[i].equals("-server=")) {
            serverOption = "true".equalsIgnoreCase(argValue);
          } else if (validArgs[i].equals("-hostname-for-clients=")) {
            hostnameForClientsOption = argValue;
          } else if (validArgs[i].equals("-D")) {
            int idx = argValue.indexOf('=');
            String key = argValue.substring(0, idx);
            String value = argValue.substring(idx + 1);
            propertyOption.setProperty(key, value);
          } else if (validArgs[i].equals("-X")) {
            xoptions.add(arg);
          } else if (validArgs[i].equals("-baseline=")) {
            baselineDir = argValue;
          } else if (validArgs[i].equals("-outputDir=")) {
            outputDir = argValue;
          } else {
            throw new InternalGemFireException(
                String.format("unexpected valid option %s",
                    validArgs[i]));
          }
          return true;
        }
      } else if (validArgs[i].equalsIgnoreCase(arg)) {
        if (validArgs[i].equals("-h") || validArgs[i].toLowerCase().matches("-{0,2}help")) {
          help = true;
        } else if (validArgs[i].equals("-debug")) {
          debug = true;
        } else if (validArgs[i].equals("-remove")) {
          remove = true;
        } else if (validArgs[i].equals("-q")) {
          quiet = true;
        } else if (validArgs[i].equals("-details")) {
          details = true;
        } else if (validArgs[i].equals("-nofilter")) {
          nofilter = true;
        } else if (validArgs[i].equals("-persec")) {
          persec = true;
        } else if (validArgs[i].equals("-persample")) {
          persample = true;
        } else if (validArgs[i].equals("-prunezeros")) {
          prunezeros = true;
        } else if (validArgs[i].equals("-monitor")) {
          monitor = true;
        } else if (validArgs[i].equalsIgnoreCase("-buckets")) {
          showBuckets = true;
        } else if (validArgs[i].equals("-all-threads")) {
          printStacksOption = arg;
        } else {
          throw new InternalGemFireException(String.format("unexpected valid option %s",
              validArgs[i]));
        }
        return true;
      }
    }
    return false;
  }

  protected void printHelp(String cmd) {
    List<String> lines = format((String) helpMap.get(cmd.toLowerCase()), 80);
    for (String line : lines) {
      System.err.println(line);
    }
    usage(cmd);
  }

  public static List<String> format(String string, int width) {
    List<String> results = new ArrayList<String>();
    String[] realLines = string.split("\n");
    for (String line : realLines) {
      results.addAll(lineWrapOut(line, width));
      results.add("");
    }

    return results;
  }

  public static List<String> lineWrapOut(String string, int width) {
    Pattern pattern =
        Pattern.compile("(.{0," + (width - 1) + "}\\S|\\S{" + (width) + ",})(\n|\\s+|$)");

    Matcher matcher = pattern.matcher(string);
    List<String> lines = new ArrayList<String>();
    while (matcher.find()) {
      lines.add(matcher.group(1));
    }

    return lines;
  }

  private static String getExceptionMessage(Throwable ex) {
    String result = ex.getMessage();
    if (result == null || result.length() == 0) {
      result = ex.toString();
    }
    return result;
  }

  private static boolean needsSysDir(String cmd) {
    return !(cmd.equalsIgnoreCase("stats") || cmd.equalsIgnoreCase("merge-logs")
        || cmd.equalsIgnoreCase("version") || cmd.equalsIgnoreCase("help"));
  }

  public static File getProductDir() {
    File libdir = findGemFireLibDir();
    if (libdir == null) {
      return new File("").getAbsoluteFile(); // default to current directory
    } else {
      return libdir.getParentFile();
    }
  }

  public static File getHiddenDir() throws IOException {
    File prodDir = getProductDir();
    if (prodDir == null) {
      return null;
    }
    File hiddenDir = new File(prodDir.getParentFile(), "hidden");
    if (!hiddenDir.exists()) {
      hiddenDir = new File(prodDir.getCanonicalFile().getParentFile(), "hidden");
    }
    if (!hiddenDir.exists()) {
      // If we don't have a jar file (eg, when running in eclipse), look for a hidden
      // directory in same same directory as our classes.
      File libDir = findGemFireLibDir();
      File oldHiddenDir = hiddenDir;
      if (libDir != null && libDir.exists()) {
        hiddenDir = new File(libDir, "hidden");
      }
      if (!hiddenDir.exists()) {
        hiddenDir = oldHiddenDir;
      }
    }

    return hiddenDir;
  }

  public static void main(String[] args) {
    try {
      final SystemAdmin admin = new SystemAdmin();
      admin.initHelpMap();
      admin.initUsageMap();
      admin.invoke(args);
    } finally {
      InternalDistributedSystem sys = InternalDistributedSystem.getConnectedInstance();
      if (sys != null) {
        sys.disconnect();
      }
    }
  }

  public void invoke(String[] args) {
    String cmd = null;
    ArrayList cmdLine = new ArrayList(Arrays.asList(args));
    try {
      Iterator it = cmdLine.iterator();
      while (it.hasNext()) {
        String arg = (String) it.next();
        if (arg.startsWith("-")) {
          if (matchCmdArg("gemfire", arg)) {
            it.remove();
          } else {
            System.err.println(
                String.format("ERROR: Unknown option %s.", arg));
            usage();
          }
        } else {
          break;
        }
      }
    } catch (IllegalArgumentException ex) {
      System.err.println(
          "ERROR: " + getExceptionMessage(ex));
      if (debug) {
        ex.printStackTrace(System.err);
      }
      ExitCode.FATAL.doSystemExit(); // fix for bug 28351
    }
    if (cmdLine.size() == 0) {
      if (help) {
        printHelp("gemfire");
      } else {
        System.err.println("ERROR: Wrong number of command line args.");
        usage();
      }
    }
    cmd = (String) cmdLine.remove(0);
    cmd = checkCmd(cmd);
    File sysDir = null;
    // File configFile = null;
    try {
      Iterator it = cmdLine.iterator();
      while (it.hasNext()) {
        String arg = (String) it.next();
        if (arg.startsWith("-")) {
          if (matchCmdArg(cmd, arg) || matchCmdArg("gemfire", arg)) {
            it.remove();
          } else {
            System.err.println(
                String.format("ERROR: Unknown option %s.", arg));
            usage(cmd);
          }
        }
      }
    } catch (IllegalArgumentException ex) {
      System.err.println(
          "ERROR: " + getExceptionMessage(ex));
      if (debug) {
        ex.printStackTrace(System.err);
      }
      ExitCode.FATAL.doSystemExit(); // fix for bug 28351
    }
    if (needsSysDir(cmd) && !help) {
      if (sysDirName != null && sysDirName.length() > 0) {
        sysDir = new File(sysDirName).getAbsoluteFile();
      } else {
        sysDir = new File(System.getProperty("user.dir")).getAbsoluteFile();
      }
    }
    SystemFailure.loadEmergencyClasses();
    if (help) {
      printHelp(cmd);
    }
    try {
      if (cmd.equalsIgnoreCase("stats")) {
        statistics(sysDir, archiveOption, details, nofilter, persec, persample, prunezeros, monitor,
            startTime, endTime, cmdLine);
      } else if (cmd.equalsIgnoreCase("version")) {
        boolean optionOK = (cmdLine.size() == 0);
        if (cmdLine.size() == 1) {
          optionOK = false;

          String option = (String) cmdLine.get(0);
          if ("CREATE".equals(option) || "FULL".equalsIgnoreCase(option)) {
            // CREATE and FULL are secret for internal use only
            optionOK = true;
          }
        }

        if (!optionOK) {
          System.err.println(String.format("ERROR: unexpected command line arguments: %s.",
              join(cmdLine)));
          usage(cmd);
        }
        System.out.println(String.format("GemFire product directory: %s",
            getProductDir()));

        GemFireVersion.print(System.out);

      } else if (cmd.equalsIgnoreCase("help")) {
        if (cmdLine.size() > 1) {
          System.err.println(String.format("ERROR: unexpected command line arguments: %s.",
              join(cmdLine)));
          usage(cmd);
        }
        help(cmdLine);
      } else if (cmd.equalsIgnoreCase(START_LOCATOR)) {
        if (cmdLine.size() != 0) {
          System.err.println(String.format("ERROR: unexpected command line arguments: %s.",
              join(cmdLine)));
          usage(cmd);
        }
        locatorStart(sysDir, portOption, addressOption, gemfirePropertiesFileOption, propertyOption,
            xoptions, peerOption, serverOption, hostnameForClientsOption);
        if (!quiet) {
          System.out
              .println("Locator start complete.");
        }
      } else if (cmd.equalsIgnoreCase("stop-locator")) {
        if (cmdLine.size() != 0) {
          System.err.println(String.format("ERROR: unexpected command line arguments: %s.",
              join(cmdLine)));
          usage(cmd);
        }
        locatorStop(sysDir, portOption, addressOption, propertyOption);
        if (!quiet) {
          System.out
              .println("Locator stop complete.");
        }
      } else if (cmd.equalsIgnoreCase("status-locator")) {
        if (cmdLine.size() != 0) {
          System.err.println(String.format("ERROR: unexpected command line arguments: %s.",
              join(cmdLine)));
          usage(cmd);
        }
        if (!quiet) {
          System.out.println(locatorStatus(sysDir));
        }
      } else if (cmd.equalsIgnoreCase("info-locator")) {
        if (cmdLine.size() != 0) {
          System.err.println(String.format("ERROR: unexpected command line arguments: %s.",
              join(cmdLine)));
          usage(cmd);
        }
        System.out.println(locatorInfo(sysDir));
      } else if (cmd.equalsIgnoreCase("tail-locator-log")) {
        if (cmdLine.size() != 0) {
          System.err.println(String.format("ERROR: unexpected command line arguments: %s.",
              join(cmdLine)));
          usage(cmd);
        }
        System.out.println(locatorTailLog(sysDir));
      } else if (cmd.equalsIgnoreCase("merge-logs")) {
        if (cmdLine.size() == 0) {
          System.err
              .println("ERROR: expected at least one log file to merge.");
          usage(cmd);
        }
        mergeLogs(outOption, cmdLine);
      } else if (cmd.equalsIgnoreCase("validate-disk-store")) {
        if (cmdLine.size() == 0) {
          System.err.println("Expected disk store name and at least one directory");
          usage(cmd);
        } else if (cmdLine.size() == 1) {
          System.err.println("Expected at least one directory");
          usage(cmd);
        }
        validateDiskStore(cmdLine);
      } else if (cmd.equalsIgnoreCase("upgrade-disk-store")) {
        if (cmdLine.size() == 0) {
          System.err.println("Expected disk store name and at least one directory");
          usage(cmd);
        } else if (cmdLine.size() == 1) {
          System.err.println("Expected at least one directory");
          usage(cmd);
        }
        upgradeDiskStore(cmdLine);
      } else if (cmd.equalsIgnoreCase("compact-disk-store")) {
        if (cmdLine.size() == 0) {
          System.err.println("Expected disk store name and at least one directory");
          usage(cmd);
        } else if (cmdLine.size() == 1) {
          System.err.println("Expected at least one directory");
          usage(cmd);
        }
        compactDiskStore(cmdLine);
      } else if (cmd.equalsIgnoreCase("compact-all-disk-stores")) {
        if (cmdLine.size() != 0) {
          System.err.println("Did not expect any command line arguments");
          usage(cmd);
        }
        compactAllDiskStores(cmdLine);
      } else if (cmd.equalsIgnoreCase("modify-disk-store")) {
        if (cmdLine.size() == 0) {
          System.err.println("Expected disk store name and at least one directory");
          usage(cmd);
        } else if (cmdLine.size() == 1) {
          System.err.println("Expected at least one directory");
          usage(cmd);
        }
        modifyDiskStore(cmdLine);
      } else if (cmd.equalsIgnoreCase("list-missing-disk-stores")) {
        if (cmdLine.size() != 0) {
          System.err.println("Did not expect any command line arguments");
          usage(cmd);
        }
        listMissingDiskStores();
      } else if (cmd.equalsIgnoreCase("revoke-missing-disk-store")) {
        if (cmdLine.size() != 1) {
          System.err.println("Expected a disk store id");
          usage(cmd);
        }
        revokeMissingDiskStores(cmdLine);
      } else if (cmd.equalsIgnoreCase("show-disk-store-metadata")) {
        if (cmdLine.size() == 0) {
          System.err.println("Expected disk store name and at least one directory");
          usage(cmd);
        } else if (cmdLine.size() == 1) {
          System.err.println("Expected at least one directory");
          usage(cmd);
        }
        showDiskStoreMetadata(cmdLine);
      } else if (cmd.equalsIgnoreCase("export-disk-store")) {
        if (cmdLine.size() == 0) {
          System.err.println("Expected disk store name and at least one directory");
          usage(cmd);
        } else if (cmdLine.size() == 1) {
          System.err.println("Expected at least one directory");
          usage(cmd);
        }
        exportDiskStore(cmdLine, outputDir);
      } else if (cmd.equalsIgnoreCase("shut-down-all")) {
        if (cmdLine.size() > 1) {
          System.err.println("Expected an optional timeout (ms)");
          usage(cmd);
        }
        shutDownAll(cmdLine);
      } else if (cmd.equalsIgnoreCase("backup")) {
        if (cmdLine.size() != 1) {
          usage(cmd);
        }
        backup((String) cmdLine.get(0));
      } else if (cmd.equalsIgnoreCase("print-stacks")) {
        printStacks(cmdLine, printStacksOption != null);
      } else {
        System.err
            .println(String.format("ERROR: Unknown command %s.", cmd));
        usage();
      }
    } catch (InterruptedException ex) {
      System.err.println(String.format("ERROR: Operation %s failed because: %s.",
          new Object[] {cmd, getExceptionMessage(ex)}));
      if (debug) {
        ex.printStackTrace(System.err);
      }
      ExitCode.FATAL.doSystemExit(); // fix for bug 28351
    } catch (IllegalArgumentException ex) {
      System.err.println(String.format("ERROR: Operation %s failed because: %s.",
          new Object[] {cmd, getExceptionMessage(ex)}));

      if (debug) {
        ex.printStackTrace(System.err);
      }
      ExitCode.FATAL.doSystemExit(); // fix for bug 28351
    } catch (Exception ex) {
      System.err.println(String.format("ERROR: Operation %s failed because: %s.",
          new Object[] {cmd, getExceptionMessage(ex)}));
      if (debug) {
        ex.printStackTrace(System.err);
      }
      ExitCode.FATAL.doSystemExit(); // fix for bug 28351
    }
  }
}

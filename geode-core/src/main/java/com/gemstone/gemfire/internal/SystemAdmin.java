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
package com.gemstone.gemfire.internal;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.BackupStatus;
import com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl;
import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.StatArchiveReader.ResourceInst;
import com.gemstone.gemfire.internal.StatArchiveReader.StatValue;
import com.gemstone.gemfire.internal.admin.remote.TailLogResponse;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.DateFormatter;
import com.gemstone.gemfire.internal.logging.MergeLogFiles;
import com.gemstone.gemfire.internal.net.SocketCreator;
import com.gemstone.gemfire.internal.util.JavaCommandBuilder;
import com.gemstone.gemfire.internal.util.PasswordUtil;
import com.gemstone.gemfire.internal.util.PluckStacks;
import com.gemstone.gemfire.internal.util.PluckStacks.ThreadStack;

import java.io.*;
import java.net.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.START_LOCATOR;

/**
 * Provides static methods for various system administation tasks.
 */
public class SystemAdmin {

  /**
   * Finds the gemfire jar path element in the given classpath
   * and returns the directory that jar is in.
   */
  public static File findGemFireLibDir() {
    URL jarURL = GemFireVersion.getJarURL();
    if (jarURL == null) return null;
    String path = jarURL.getPath();
    // Decode URL to get rid of escaped characters.  See bug 32465.
    path = URLDecoder.decode(path);
    File f = new File(path);
    if(f.isDirectory()) {
      return f;
    }
    return f.getParentFile();
  }
  
  // ------------------- begin: Locator methods ------------------------------
  
  public void locatorStart(
    File directory, 
    String portOption, 
    String addressOption, 
    String gemfirePropertiesFileOption,
    boolean peerOption, 
    boolean serverOption,
    String hostnameForClientsOption
    )
    throws InterruptedException
  {
//    if (Thread.interrupted()) throw new InterruptedException(); not necessary checked in locatorStart
    locatorStart( directory, portOption, addressOption, gemfirePropertiesFileOption, null, null, peerOption, serverOption, hostnameForClientsOption );
  }

  public void locatorStart(
    File directory, 
    String portOption, 
    String addressOption, 
    String gemfirePropertiesFileOption,
    Properties propertyOptionArg,
    List xoptions,
    boolean peerOption, 
    boolean serverOption,
    String hostnameForClientsOption
    )
    throws InterruptedException
  {
    if (Thread.interrupted()) throw new InterruptedException();
    int port = DistributionLocator.parsePort(portOption);

    if (addressOption == null) addressOption = "";

    if (!addressOption.equals("")) {
      // make sure its a valid ip address
      if (!validLocalAddress(addressOption)) {
        throw new IllegalArgumentException(LocalizedStrings.SystemAdmin__0_IS_NOT_A_VALID_IP_ADDRESS_FOR_THIS_MACHINE.toLocalizedString(addressOption));
      }
    }
    // check to see if locator is already running
    File logFile = new File(directory, DistributionLocator.DEFAULT_STARTUP_LOG_FILE);
    try {
      // make sure file can be opened for writing
      (new FileOutputStream(logFile.getPath(), true)).close();
    } catch (IOException ex) {
      throw new GemFireIOException(LocalizedStrings.SystemAdmin_LOGFILE_0_COULD_NOT_BE_OPENED_FOR_WRITING_VERIFY_FILE_PERMISSIONS_AND_THAT_ANOTHER_LOCATOR_IS_NOT_ALREADY_RUNNING.toLocalizedString(logFile.getPath()), ex);
    }
    
    if (gemfirePropertiesFileOption != null) {
      Properties newPropOptions = new Properties();//see #43731
      newPropOptions.putAll(propertyOptionArg);
      newPropOptions.setProperty("gemfirePropertyFile", gemfirePropertiesFileOption);
      propertyOptionArg = newPropOptions;
    }

    // read ssl properties
    Map<String, String> env = new HashMap<String, String>();
    SocketCreator.readSSLProperties(env);
    
    List cmdVec = JavaCommandBuilder.buildCommand(getDistributionLocatorPath(),
        null, propertyOptionArg, xoptions);

    cmdVec.add( String.valueOf(port) );
    cmdVec.add( addressOption );
    cmdVec.add(Boolean.toString(peerOption));
    cmdVec.add(Boolean.toString(serverOption));
    if (hostnameForClientsOption == null) {
      hostnameForClientsOption = "";
    }
    cmdVec.add(hostnameForClientsOption);

    String[] cmd = (String[]) cmdVec.toArray(new String[cmdVec.size()]);

    try {
      // start with a fresh log each time
      if (!logFile.delete() && logFile.exists()) {
        throw new GemFireIOException("Unable to delete " + logFile.getAbsolutePath());
      }
      int managerPid = OSProcess.bgexec(cmd, directory, logFile, false, env);
      boolean treatAsPure = (env.size() > 0) || PureJavaMode.isPure();
      /** 
       * A counter used by PureJava to determine when its waited too long
       * to start the locator process. 
       * countDown * 250 = how many seconds to wait before giving up.
       **/
      int countDown = 60; 
      // NYI: wait around until we can attach
      while (!ManagerInfo.isLocatorStarted(directory)) {
        if ( treatAsPure) {
          countDown--;
          Thread.sleep(250);       
        }
        if (countDown < 0 || 
           !(treatAsPure || OSProcess.exists(managerPid)))
        {
          try {
            String msg = tailFile(logFile, false);
            throw new GemFireIOException(LocalizedStrings.SystemAdmin_START_OF_LOCATOR_FAILED_THE_END_OF_0_CONTAINED_THIS_MESSAGE_1.toLocalizedString(new Object[] {logFile, msg}), null);
          } catch (IOException ignore) {
            throw new GemFireIOException(LocalizedStrings.SystemAdmin_START_OF_LOCATOR_FAILED_CHECK_END_OF_0_FOR_REASON.toLocalizedString(logFile), null);
          }
        }
        Thread.sleep(500);
      }
    } catch (IOException io) {
      throw new GemFireIOException(LocalizedStrings.SystemAdmin_COULD_NOT_EXEC_0.toLocalizedString(cmd[0]), io);
    }
  }

  
  /** get the path to the distribution locator class */
  protected String getDistributionLocatorPath() {
    return "com.gemstone.gemfire.internal.DistributionLocator";
  }

  /** enumerates all available local network addresses to find a match with
      the given address.  Returns false if the address is not usable on
      the current machine */
  public static boolean validLocalAddress(String bindAddress) {
    InetAddress addr = null;
    try {
      addr = InetAddress.getByName(bindAddress);
    }
    catch (UnknownHostException ex) {
      return false;
    }
    try {
      Enumeration en = NetworkInterface.getNetworkInterfaces();
      while (en.hasMoreElements()) {
        NetworkInterface ni = (NetworkInterface)en.nextElement();
        Enumeration en2 = ni.getInetAddresses();
        while (en2.hasMoreElements()) {
          InetAddress check = (InetAddress)en2.nextElement();
          if (check.equals(addr)) {
            return true;
          }
        }
      }
    }
    catch (SocketException sex) {
      return true; // can't query the interfaces - punt
    }
    return false;
  }

  @SuppressWarnings("hiding")
  public void locatorStop(File directory, String portOption, String addressOption, Properties propertyOption) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException();
    InetAddress addr = null;  // fix for bug 30810
    if (addressOption == null) addressOption = "";
    if (!addressOption.equals("")) {
      // make sure its a valid ip address
      try {
        addr = InetAddress.getByName(addressOption);
      } catch (UnknownHostException ex) {
        throw new IllegalArgumentException(LocalizedStrings.SystemAdmin_ADDRESS_VALUE_WAS_NOT_A_KNOWN_IP_ADDRESS_0.toLocalizedString(ex));
      }
    }

    if ( propertyOption != null ) {
      Iterator iter = propertyOption.keySet().iterator();
      while( iter.hasNext() ) {
        String key = (String) iter.next();
        System.setProperty( key, propertyOption.getProperty( key ) );
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
      if (addressOption.trim().length() == 0) {
        addr = info.getManagerAddress();
      }
//      File infoFile = ManagerInfo.getLocatorInfoFile(directory);

      try {
        InternalLocator.stopLocator(port, addr);
      } 
      catch ( java.net.ConnectException ce ) {
        if( PureJavaMode.isPure() || OSProcess.exists(pid) ) {
          System.out.println("Unable to connect to Locator process. Possible causes are that an incorrect bind address/port combination was specified to the stop-locator command or the process is unresponsive.");
        }
        return;
      }
      // wait for the locator process to go away
      if ( PureJavaMode.isPure() ) {
        //format and change message
        if (!quiet) {
          System.out.println(LocalizedStrings.SystemAdmin_WAITING_5_SECONDS_FOR_LOCATOR_PROCESS_TO_TERMINATE.toLocalizedString());
        }
        Thread.sleep(5000);
      } else {
        int sleepCount = 0;
        final int maxSleepCount = 15; 
        while (++sleepCount < maxSleepCount && OSProcess.exists(pid) ) {
          Thread.sleep(1000);
          if (sleepCount == maxSleepCount/3 && !quiet) {
            System.out.println(LocalizedStrings.SystemAdmin_WAITING_FOR_LOCATOR_PROCESS_WITH_PID_0_TO_TERMINATE.toLocalizedString(Integer.valueOf(pid)));
          }
        }
        if (sleepCount > maxSleepCount && !quiet) {
          System.out.println(LocalizedStrings.SystemAdmin_LOCATOR_PROCESS_HAS_TERMINATED.toLocalizedString());
        } else if( OSProcess.exists(pid) ) {
          System.out.println("Locator process did not terminate within " + maxSleepCount + " seconds.");
        }
      }
  } catch (UnstartedSystemException ex) {
      // fix for bug 28133
      throw new UnstartedSystemException(LocalizedStrings.SystemAdmin_LOCATOR_IN_DIRECTORY_0_IS_NOT_RUNNING.toLocalizedString(directory));
    } catch (NoSystemException ex) {
      // before returning see if a stale lock file/shared memory can be cleaned up
      cleanupAfterKilledLocator(directory);
      throw ex;
    }
  }
  /**
   * Gets the status of a locator.
   * @param directory the locator's directory
   * @return the status string. Will be one of the following:
   *   "running", "killed", "stopped", "stopping", or "starting".
   * @throws UncreatedSystemException if the locator <code>directory</code>
   *   does not exist or is not a directory.
   * @throws GemFireIOException if the manager info exists but could not be read. This probably means that the info file is corrupt.
   */
  public String locatorStatus(File directory) {
    return ManagerInfo.getLocatorStatusCodeString(directory);
  }
  /**
   * Gets information on the locator.
   * @param directory the locator's directory
   * @return information string.
   * @throws UncreatedSystemException if the locator <code>directory</code>
   *   does not exist or is not a directory.
   * @throws GemFireIOException if the manager info exists but could not be read. This probably means that the info file is corrupt.
   */
  public String locatorInfo(File directory) {
    int statusCode = ManagerInfo.getLocatorStatusCode(directory);
    String statusString = ManagerInfo.statusToString(statusCode);
    try {
      ManagerInfo mi = ManagerInfo.loadLocatorInfo(directory);
      if (statusCode == ManagerInfo.KILLED_STATUS_CODE) {
        return LocalizedStrings.SystemAdmin_LOCATOR_IN_0_WAS_KILLED_WHILE_IT_WAS_1_LOCATOR_PROCESS_ID_WAS_2
         .toLocalizedString(
           new Object[] {
             directory,
             ManagerInfo.statusToString(mi.getManagerStatus()),
             Integer.valueOf(mi.getManagerProcessId())
         });
      } else {
        return LocalizedStrings.SystemAdmin_LOCATOR_IN_0_IS_1_LOCATOR_PROCESS_ID_IS_2 
         .toLocalizedString(
           new Object[] {
             directory,
             statusString,
             Integer.valueOf(mi.getManagerProcessId())});
      }
    } catch (UnstartedSystemException ex) {
      return LocalizedStrings.SystemAdmin_LOCATOR_IN_0_IS_STOPPED.toLocalizedString(directory);
    } catch (GemFireIOException ex) {
      return LocalizedStrings.SystemAdmin_LOCATOR_IN_0_IS_STARTING.toLocalizedString(directory); 
    }
  }
  /**
   * Cleans up any artifacts left by a killed locator.
   * Namely the info file is deleted.
   */
  private static void cleanupAfterKilledLocator(File directory) {
    try {
      if(ManagerInfo.getLocatorStatusCode(directory) == ManagerInfo.KILLED_STATUS_CODE) {
        File infoFile = ManagerInfo.getLocatorInfoFile(directory);
        if (infoFile.exists()) {
          if (!infoFile.delete() && infoFile.exists()) {
            System.out.println("WARNING: unable to delete " + infoFile.getAbsolutePath());
          }
          if (!quiet) {
            System.out.println(LocalizedStrings.SystemAdmin_CLEANED_UP_ARTIFACTS_LEFT_BY_THE_PREVIOUS_KILLED_LOCATOR.toLocalizedString());
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
    File logFile =
      new File(directory, DistributionLocator.DEFAULT_LOG_FILE);
    if (!logFile.exists()) {
      return LocalizedStrings.SystemAdmin_LOG_FILE_0_DOES_NOT_EXIST.toLocalizedString(logFile);
    }

    try {
      return TailLogResponse.tailSystemLog(logFile);
    } catch (IOException ex) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw, true);
      sw.write(LocalizedStrings.SystemAdmin_AN_IOEXCEPTION_WAS_THROWN_WHILE_TAILING_0.toLocalizedString(logFile));
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
    String diskStoreName = (String)args.get(0);
    List dirList = args.subList(1, args.size());
    File[] dirs = new File[dirList.size()];
    Iterator it = dirList.iterator();
    int idx = 0;
    while (it.hasNext()) {
      dirs[idx] = new File((String)it.next());
      idx++;
    }
    try {
      DiskStoreImpl.offlineCompact(diskStoreName, dirs, false, maxOplogSize);
    } catch (Exception ex) {
      throw new GemFireIOException(" disk-store=" + diskStoreName + ": " + ex, ex); 
    }
  }

  public static void upgradeDiskStore(List args) {
    String diskStoreName = (String)args.get(0);
    List dirList = args.subList(1, args.size());
    File[] dirs = new File[dirList.size()];
    Iterator it = dirList.iterator();
    int idx = 0;
    while (it.hasNext()) {
      dirs[idx] = new File((String)it.next());
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
    Map<DistributedMember, Set<PersistentID>> status = AdminDistributedSystemImpl.compactAllDiskStores(ads.getDistributionManager());
    
    System.out.println("Compaction complete.");
    System.out.println("The following disk stores compacted some files:");
    for(Set<PersistentID> memberStores : status.values()) {
      for(PersistentID store : memberStores) {
        System.out.println("\t" + store);
      }
    }
  }

  public static void validateDiskStore(String... args) {
    validateDiskStore(Arrays.asList(args));
  }
  public static void validateDiskStore(List args) {
    String diskStoreName = (String)args.get(0);
    List dirList = args.subList(1, args.size());
    File[] dirs = new File[dirList.size()];
    Iterator it = dirList.iterator();
    int idx = 0;
    while (it.hasNext()) {
      dirs[idx] = new File((String)it.next());
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
      System.out.println(" locators=" +dsc.getLocators());
    } else {
      System.out.println(" mcast=" + dsc.getMcastAddress()
          + ":" + dsc.getMcastPort());
    }
    InternalDistributedSystem ds = (InternalDistributedSystem) InternalDistributedSystem.connectForAdmin(props);
    Set existingMembers = ds.getDistributionManager().getDistributionManagerIds();
    if(existingMembers.isEmpty()) {
      throw new RuntimeException("There are no members in the distributed system");
    }
    return ds;
  }
  
  public static void shutDownAll(ArrayList<String> cmdLine)  {
    try {
      long timeout = 0;
      if(cmdLine.size() > 0) {
        timeout = Long.parseLong(cmdLine.get(0));
      }
      InternalDistributedSystem ads = getAdminCnx();
      Set members = AdminDistributedSystemImpl.shutDownAllMembers(ads.getDistributionManager(), timeout);
      int count = members==null?0:members.size();
      if(members == null) {
        System.err.println("Unable to shut down the distributed system in the specified amount of time.");
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
   * this is a test hook to allow us to drive SystemAdmin functions without
   * invoking main(), which can call System.exit().
   * 
   * @param props
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
        outputFile = new File((String)cmdLine.get(0));
        os = new FileOutputStream(outputFile);
        ps = new PrintWriter(os);
      } else {
        os = System.out;
        ps = new PrintWriter(System.out);
      }
      
      Map<InternalDistributedMember, byte[]> dumps = msg.dumpStacks(ads.getDistributionManager().getAllOtherMembers(), false, true);
      for (Map.Entry<InternalDistributedMember, byte[]> entry: dumps.entrySet()) {
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
          for (ThreadStack s: stacks) {
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
  
  public static void backup(String targetDir) throws AdminException  {
    InternalDistributedSystem ads = getAdminCnx();

    // Baseline directory should be null if it was not provided on the command line
    BackupStatus status = AdminDistributedSystemImpl.backupAllMembers(ads.getDistributionManager(), 
        new File(targetDir),
        (SystemAdmin.baselineDir == null ? null : new File(SystemAdmin.baselineDir)));
    
    boolean incomplete = !status.getOfflineDiskStores().isEmpty();

    System.out.println("The following disk stores were backed up:");
    for(Set<PersistentID> memberStores : status.getBackedUpDiskStores().values()) {
      for(PersistentID store : memberStores) {
        System.out.println("\t" + store);
      }
    }
    if(incomplete) {
      System.err.println("The backup may be incomplete. The following disk stores are not online:");
      for(PersistentID store : status.getOfflineDiskStores()) {
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
      for (Object o: s) {
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
    String dsName = (String) args.get(0);
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
    
    String dsName = (String) args.get(0);
    File[] dirs = argsToFile(args.subList(1, args.size()));

    try {
      DiskStoreImpl.exportOfflineSnapshot(dsName, dirs, out);
    } catch (Exception ex) {
      throw new GemFireIOException(" disk-store=" + dsName + ": " + ex, ex); 
    }
  }
  
  public static void revokeMissingDiskStores(ArrayList<String> cmdLine) throws UnknownHostException, AdminException {
    String uuidString = cmdLine.get(0);
    UUID uuid = UUID.fromString(uuidString);
    InternalDistributedSystem ads = getAdminCnx();
    AdminDistributedSystemImpl.revokePersistentMember(ads.getDistributionManager(), uuid);
    Set<PersistentID> s = AdminDistributedSystemImpl.getMissingPersistentMembers(ads.getDistributionManager());
    
    
    //Fix for 42607 - wait to see if the revoked member goes way if it is still in the set of
    //missing members. It may take a moment to clear the missing member set after the revoke.
    long start = System.currentTimeMillis();
    while(containsRevokedMember(s, uuid)) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignore) {
      }
      s = AdminDistributedSystemImpl.getMissingPersistentMembers(ads.getDistributionManager());
      if(start + 30000 < System.currentTimeMillis()) {
        break;
      }
    }
    if (s.isEmpty()) {
      System.out.println("revocation was successful and no disk stores are now missing");
    } else {
      System.out.println("The following disk stores are still missing:");
      for (Object o: s) {
        System.out.println(o);
      }
    }
  }
  
  private static boolean containsRevokedMember(Set<PersistentID> missing, UUID revokedUUID) {
    for(PersistentID id : missing) {
      if(id.getUUID().equals(revokedUUID)) {
        return true;
      }
    }
    return false;
  }

  public static void modifyDiskStore(String... args) {
    modifyDiskStore(Arrays.asList(args));
  }
  public static void modifyDiskStore(List args) {
    String diskStoreName = (String)args.get(0);
    List dirList = args.subList(1, args.size());
    File[] dirs = new File[dirList.size()];
    Iterator it = dirList.iterator();
    int idx = 0;
    while (it.hasNext()) {
      dirs[idx] = new File((String)it.next());
      idx++;
    }
    try {
      if (lruOption != null
          || lruActionOption != null
          || lruLimitOption != null
          || concurrencyLevelOption != null
          || initialCapacityOption != null
          || loadFactorOption != null
          || compressorClassNameOption != null
          || statisticsEnabledOption != null) {
        if (regionOption == null) {
          throw new IllegalArgumentException("modify-disk-store requires -region=<regionName>");
        }
        if (remove) {
          throw new IllegalArgumentException("the -remove option can not be used with the other modify options.");
        }
        DiskStoreImpl.modifyRegion(diskStoreName, dirs, regionOption,
                                   lruOption, lruActionOption, lruLimitOption,
                                   concurrencyLevelOption, initialCapacityOption, loadFactorOption,
                                   compressorClassNameOption, statisticsEnabledOption, null/*offHeap*/, true);
        System.out.println("The region " + regionOption + " was successfully modified in the disk store " + diskStoreName);
      } else if (remove) {
        if (regionOption == null) {
          throw new IllegalArgumentException("modify-disk-store requires -region=<regionName>");
        }
        DiskStoreImpl.destroyRegion(diskStoreName, dirs, regionOption);
        System.out.println("The region " + regionOption + " was successfully removed from the disk store " + diskStoreName);
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

  public void mergeLogs(String outOption, List args) {
    FileInputStream[] input = new FileInputStream[args.size()];
    String[] inputNames = new String[args.size()]; // note we don't want any extra names printed.
    
    PrintStream ps;
    if (outOption != null) {
      try {
        ps = new PrintStream(new FileOutputStream(outOption));
      } catch (FileNotFoundException ex) {
        throw new GemFireIOException(LocalizedStrings.SystemAdmin_COULD_NOT_CREATE_FILE_0_FOR_OUTPUT_BECAUSE_1.toLocalizedString(new Object[] {outOption, getExceptionMessage(ex)}));
      }
    } else {
      ps = System.out;
    }
    PrintWriter mergedFile = new PrintWriter(ps, true);

    Iterator it = args.iterator();
    int idx = 0;
    if (!quiet) {
      ps.println(LocalizedStrings.SystemAdmin_MERGING_THE_FOLLOWING_LOG_FILES.toLocalizedString());
    }
    while (it.hasNext()) {
      String fileName = (String)it.next();
      try {
        input[idx] = new FileInputStream(fileName);
        inputNames[idx] = (new File(fileName)).getAbsolutePath();
        idx++;
      } catch (FileNotFoundException ex) {
        throw new GemFireIOException(LocalizedStrings.SystemAdmin_COULD_NOT_OPEN_TO_0_FOR_READING_BECAUSE_1.toLocalizedString(new Object[] {fileName, getExceptionMessage(ex)}));
      }
      if (!quiet) {
        ps.println("  " + fileName);
      }
    }

    if (idx > 0) {
      // strip off any common filename prefix
      boolean strip = true;
      do {
        if (inputNames[0].length() == 0) {
          break;
        }
        if (inputNames[0].indexOf('/') == -1
            && inputNames[0].indexOf('\\') == -1) {
          // no more directories to strip off
          break;
        }
        char c = inputNames[0].charAt(0);
        for (int i=1; i < idx; i++) {
          if (inputNames[i].charAt(0) != c) {
            strip = false;
            break;
          }
        }
        for (int i=0; i < idx; i++) {
          inputNames[i] = inputNames[i].substring(1);
        }
      } while (strip);
    }

    if (MergeLogFiles.mergeLogFiles(input, inputNames, mergedFile)) {
      throw new GemFireIOException(LocalizedStrings.SystemAdmin_TROUBLE_MERGING_LOG_FILES.toLocalizedString()); 
    }
    mergedFile.flush();
    if (outOption != null) {
      mergedFile.close();
    }
    if (!quiet) {
      System.out.println(
        LocalizedStrings.SystemAdmin_COMPLETED_MERGE_OF_0_LOGS_TO_1
          .toLocalizedString(
            new Object[] {Integer.valueOf(idx), ((outOption != null) ? outOption : "stdout")})); 
    }
  }
  
  /**
   * Returns the contents located at the end of the file as a string.
   * @throws IOException if the file can not be opened or read
   */
  public String tailFile(File file, boolean problemsOnly)
    throws IOException
  {
    byte buffer[] = new byte[128000];
    int readSize = buffer.length;
    RandomAccessFile f = new RandomAccessFile(file, "r");
    long length = f.length();
    if (length < readSize) {
      readSize = (int)length;
    }
    long seekOffset = length - readSize;
    f.seek(seekOffset);
    if (readSize != f.read(buffer, 0, readSize)) {
      throw new EOFException("Failed to read " + readSize + " bytes from " + file.getAbsolutePath());
    }
    f.close();
    // Now look for the last message header
    int msgStart = -1;
    int msgEnd = readSize;
    for (int i = readSize-1; i>=0; i--) {
      if (buffer[i] == '[' && (buffer[i+1] == 's' || buffer[i+1] == 'e' || buffer[i+1] == 'w' /* ignore all messages except severe, error, and warning to fix bug 28968 */)
          && i > 0 && (buffer[i-1] == '\n' || buffer[i-1] == '\r')) {
        msgStart = i;
        break;
      }
    }
    if (msgStart == -1) {
      if (problemsOnly) {
        return null;
      }
      // Could not find message start. Show last line instead.
      for (int i = readSize-3; i>=0; i--) {
        if (buffer[i] == '\n' || buffer[i] == '\r') {
          msgStart = (buffer[i]=='\n')? (i+1) : (i+2);
          break;
        }
      }
      if (msgStart == -1) {
        // Could not find line so show entire buffer
        msgStart = 0;
      }
    } else {
      // try to find the start of the next message and get rid of it
      for (int i=msgStart+1; i < readSize; i++) {
        if (buffer[i] == '[' && (buffer[i-1] == '\n' || buffer[i-1] == '\r')) {
          msgEnd = i;
          break;
        }
      }
    }
    for (int i=msgStart; i < msgEnd; i++) {
      if (buffer[i] == '\n' || buffer[i] == '\r') {
        buffer[i] = ' ';
      }
    }
    return new String(buffer, msgStart, msgEnd - msgStart);
  }

  protected void format(PrintWriter pw, String msg, String linePrefix,
      int initialLength) {
    final int maxWidth = 79;
    boolean firstLine = true;
    int lineLength = 0;
    final int  prefixLength = linePrefix.length();
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
        }
        else {
          idx++;
        }
      } else if (msg.charAt(idx) == ' '
                 && idx > 0 && msg.charAt(idx-1) == '.'
                 && idx < (msg.length() - 1) && msg.charAt(idx+1) == ' ') {
        // treat ".  " as a hardbreak
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
    for (int i=0; i < breakChars.length; i++) {
      if (c == breakChars[i]) {
        return true;
      }
    }
    return false;
  }
  private static int findWordBreak(String str, int fromIdx) {
    int result = str.length();
    for (int i=0; i < breakChars.length; i++) {
      int tmp = str.indexOf(breakChars[i], fromIdx+1);
      if (tmp > fromIdx && tmp < result) {
        result = tmp;
      }
    }
    return result;
  }

  private static class StatSpec implements StatArchiveReader.StatSpec {
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
        statId = cmdLineSpec.substring(dotIdx+1);
        cmdLineSpec = cmdLineSpec.substring(0, dotIdx);
      }
      int commaIdx = cmdLineSpec.indexOf(':');
      if (commaIdx != -1) {
        instanceId = cmdLineSpec.substring(0, commaIdx);
        typeId = cmdLineSpec.substring(commaIdx+1);
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
      return "StatSpec instanceId=" + this.instanceId
        + " typeId="+ this.typeId
        + " statId="+ this.statId;
    }

    public int getCombineType() {
      return this.combineType;
    }
    
    public boolean archiveMatches(File archive) {
      return true;
    }
    public boolean statMatches(String statName) {
      if (this.sp == null) {
        return true;
      } else {
        Matcher m = this.sp.matcher(statName);
        return m.matches();
      }
    }
    public boolean typeMatches(String typeName) {
      if (this.tp == null) {
        return true;
      } else {
        Matcher m = this.tp.matcher(typeName);
        return m.matches();
      }
    }
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
      result[idx] = new StatSpec((String)it.next());
      idx++;
    }
    return result;
  }
  
  private static void printStatValue(StatArchiveReader.StatValue v,
                                     long startTime, long endTime,
                                     boolean nofilter, boolean persec,
                                     boolean persample,
                                     boolean prunezeros, boolean details) {
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
      for (int i=0; i < snapshots.length; i++) {
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
   * @param directory the system directory of the system to list.
   * @param archiveNames the archive file(s) to read.
   * @param details if true the statistic descriptions will also be listed.
   * @param nofilter if true then printed stat values will all be raw unfiltered.
   * @param persec if true then printed stat values will all be the rate of change, per second, of the raw values.
   * @param persample if true then printed stat values will all be the rate of change, per sample, of the raw values.
   * @param prunezeros if true then stat values whose samples are all zero will not be printed.
   *
   * @throws UncreatedSystemException if the system <code>sysDir</code>
   *   does not exist, is not a directory, or does not contain a configuration file.
   * @throws NoSystemException if the system is not running or could not be connected to.
   * @throws IllegalArgumentException if a statSpec does not match a resource and/or statistic.
   * @throws GemFireIOException if the archive could not be read
   */
  public void statistics(File directory, List archiveNames,
                         boolean details, boolean nofilter,
                         boolean persec,
                         boolean persample,
                         boolean prunezeros,
                         boolean monitor,
                         long startTime, long endTime,
                         List cmdLineSpecs) {
    if (persec && nofilter) {
      throw new IllegalArgumentException(LocalizedStrings.SystemAdmin_THE_NOFILTER_AND_PERSEC_OPTIONS_ARE_MUTUALLY_EXCLUSIVE.toLocalizedString());
    }
    if (persec && persample) {
      throw new IllegalArgumentException(LocalizedStrings.SystemAdmin_THE_PERSAMPLE_AND_PERSEC_OPTIONS_ARE_MUTUALLY_EXCLUSIVE.toLocalizedString());
    }
    if (nofilter && persample) {
      throw new IllegalArgumentException(LocalizedStrings.SystemAdmin_THE_PERSAMPLE_AND_NOFILTER_OPTIONS_ARE_MUTUALLY_EXCLUSIVE.toLocalizedString());
    }
    StatSpec[] specs = createSpecs(cmdLineSpecs);
    if (archiveOption != null) {
      if (directory != null) {
        throw new IllegalArgumentException(LocalizedStrings.SystemAdmin_THE_ARCHIVE_AND_DIR_OPTIONS_ARE_MUTUALLY_EXCLUSIVE.toLocalizedString());
      }
      StatArchiveReader reader = null;
      boolean interrupted = false;
      try {
        reader = new StatArchiveReader((File[])archiveNames.toArray(new File[archiveNames.size()]), specs, !monitor);
        //Runtime.getRuntime().gc(); System.out.println("DEBUG: heap size=" + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
        if (specs.length == 0) {
          if (details) {
            StatArchiveReader.StatArchiveFile[] archives = reader.getArchives();
            for (int i=0; i < archives.length; i++) {
              System.out.println(archives[i].getArchiveInfo().toString());
            }
          }
        }
        do {
          if (specs.length == 0) {
            Iterator it = reader.getResourceInstList().iterator();
            while (it.hasNext()) {
              ResourceInst inst = (ResourceInst)it.next();
              StatValue values[] = inst.getStatValues();
              boolean firstTime = true;
              for (int i=0; i < values.length; i++) {
                if (values[i] != null && values[i].hasValueChanged()) {
                  if (firstTime) {
                    firstTime = false;
                    System.out.println(inst.toString());
                  }
                  printStatValue(values[i], startTime, endTime, nofilter, persec, persample, prunezeros, details);
                }
              }
            }
          } else {
            Map<CombinedResources, List<StatValue>> allSpecsMap = new HashMap<CombinedResources, List<StatValue>>();
            for (int i=0; i < specs.length; i++) {
              StatValue[] values = reader.matchSpec(specs[i]);
              if (values.length == 0) {
                if (!quiet) {
                  System.err.println(
                      LocalizedStrings.SystemAdmin_WARNING_NO_STATS_MATCHED_0
                      .toLocalizedString(specs[i].cmdLineSpec));
                }
              } else {
                Map<CombinedResources, List<StatValue>> specMap = new HashMap<CombinedResources, List<StatValue>>();
                for (StatValue v: values) {
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
                      LocalizedStrings.SystemAdmin_INFO_FOUND_0_MATCHES_FOR_1
                      .toLocalizedString(
                          new Object[] { 
                              Integer.valueOf(specMap.size()), specs[i].cmdLineSpec
                          }));
                }
                for (Map.Entry<CombinedResources, List<StatValue>> me: specMap.entrySet()) {
                  List<StatArchiveReader.StatValue> list = allSpecsMap.get(me.getKey());
                  if (list != null) {
                    list.addAll(me.getValue());
                  } else {
                    allSpecsMap.put(me.getKey(), me.getValue());
                  }
                }
              }
            }
            for (Map.Entry<CombinedResources, List<StatValue>> me: allSpecsMap.entrySet()) {
              System.out.println(me.getKey());
              for (StatValue v: me.getValue()) {
                printStatValue(v, startTime, endTime, nofilter, persec, persample, prunezeros, details);
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
        throw new GemFireIOException(LocalizedStrings.SystemAdmin_FAILED_READING_0.toLocalizedString(archiveOption), ex);
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
   * Represents a list of ResourceInst that have been combined together.
   * Note the most common case is for this class to only own a single ResourceInst.
   *
   */
  @SuppressWarnings("serial")
  private static final class CombinedResources extends ArrayList<ResourceInst> {
    public CombinedResources(StatValue v) {
      super(Arrays.asList(v.getResources()));
    }
    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      boolean first = true;
      for (ResourceInst inst: this) {
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

  public SystemAdmin() {
    // register DSFID types first; invoked explicitly so that all message type
    // initializations do not happen in first deserialization on a possibly
    // "precious" thread
    DSFIDFactory.registerTypes();
  }

  private final static String[] helpTopics = new String [] {
    "all", "overview", "commands", "options", "usage", "configuration"
  };

  protected void printHelpTopic(String topic, PrintWriter pw) {
    if (topic.equalsIgnoreCase("all")) {
      for (int i=0; i < helpTopics.length; i++) {
        if (!helpTopics[i].equals("all")) {
          pw.println("-------- " + helpTopics[i] + " --------");
          printHelpTopic(helpTopics[i], pw);
        }
      }
    } else if (topic.equalsIgnoreCase("overview")) {
      pw.println(LocalizedStrings.
        SystemAdmin_THIS_PROGRAM_ALLOWS_GEMFIRE_TO_BE_MANAGED_FROM_THE_COMMAND_LINE_IT_EXPECTS_A_COMMAND_TO_EXECUTE_SEE_THE_HELP_TOPIC_0_FOR_A_SUMMARY_OF_SUPPORTED_OPTIONS_SEE_THE_HELP_TOPIC_1_FOR_A_CONCISE_DESCRIPTION_OF_COMMAND_LINE_SYNTAX_SEE_THE_HELP_TOPIC_2_FOR_A_DESCRIPTION_OF_SYSTEM_CONFIGURATION_SEE_THE_HELP_TOPIC_3_FOR_HELP_ON_A_SPECIFIC_COMMAND_USE_THE_4_OPTION_WITH_THE_COMMAND_NAME
        .toLocalizedString( new Object[] { "commands", "options", "usage", "configuration" , "-h" }));
    } else if (topic.equalsIgnoreCase("commands")) {
      pw.println((String)usageMap.get("gemfire") + " <command> ...");
      format(pw, (String)helpMap.get("gemfire"), "  ", 0);
      for (int i=0; i < validCommands.length; i++) {
        pw.println((String)usageMap.get(validCommands[i]));
        if (helpMap.get(validCommands[i]) == null) {
          pw.println("  (help message missing for " + validCommands[i]+")");
        } else {
          format(pw, (String)helpMap.get(validCommands[i]), "  ", 0);
        }
      }
    } else if (topic.equalsIgnoreCase("options")) {
      pw.println(LocalizedStrings.
        SystemAdmin_ALL_COMMAND_LINE_OPTIONS_START_WITH_A_AND_ARE_NOT_REQUIRED_EACH_OPTION_HAS_A_DEFAULT_THAT_WILL_BE_USED_WHEN_ITS_NOT_SPECIFIED_OPTIONS_THAT_TAKE_AN_ARGUMENT_ALWAYS_USE_A_SINGLE_CHARACTER_WITH_NO_SPACES_TO_DELIMIT_WHERE_THE_OPTION_NAME_ENDS_AND_THE_ARGUMENT_BEGINS_OPTIONS_THAT_PRECEDE_THE_COMMAND_WORD_CAN_BE_USED_WITH_ANY_COMMAND_AND_ARE_ALSO_PERMITTED_TO_FOLLOW_THE_COMMAND_WORD
.toLocalizedString());
      for (int i=0; i < validOptions.length; i++) {
        pw.print(validOptions[i] + ":");
        try {
          format(pw, (String)helpMap.get(validOptions[i]), "  ", validOptions[i].length() + 1);
        } catch (RuntimeException ex) {
          System.err.println(LocalizedStrings.SystemAdmin_NO_HELP_FOR_OPTION_0.toLocalizedString(validOptions[i]));
          throw ex;
        }
      }
    } else if (topic.equalsIgnoreCase("usage")) {
      pw.println(LocalizedStrings.SystemAdmin_EXPLAINATION_OF_COMMAND_OPTIONS.toLocalizedString());
      for (int i=0; i < validCommands.length; i++) {
        pw.println(getUsageString(validCommands[i]));
      }
    }
  }

  protected void help(List args) {
    String topic = "overview";
    if (args.size() > 0) {
      topic = (String)args.get(0);
      if (!Arrays.asList(helpTopics).contains(topic.toLowerCase())) {
        System.err.println(LocalizedStrings.SystemAdmin_ERROR_INVALID_HELP_TOPIC_0.toLocalizedString(topic));
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
    result
      .append(usageMap.get("gemfire"))
      .append(' ');
    if (cmd == null || cmd.equalsIgnoreCase("gemfire")) {
      result
        .append(join(Arrays.asList(validCommands), "|"))
        .append(" ...");
    } else {
      result.append(usageMap.get(cmd.toLowerCase()));
    }
    return result.toString();
  }

  protected void usage(String cmd) {
    System.err.println(LocalizedStrings.SystemAdmin_USAGE.toLocalizedString() 
      + " " + getUsageString(cmd));
    System.exit(1);
  }

  private final static String[] validCommands = new String [] {
    "version",
    "stats",
      START_LOCATOR, "stop-locator", "status-locator", "info-locator",
    "tail-locator-log",
 "merge-logs", "encrypt-password",
    "revoke-missing-disk-store",
    "list-missing-disk-stores",
    "validate-disk-store",
    "upgrade-disk-store",
    "compact-disk-store",
    "compact-all-disk-stores",
    "modify-disk-store",
    "show-disk-store-metadata",
    "export-disk-store",
    "shut-down-all",
    "backup",
    "print-stacks",
    "help"
  };

  protected static String[] getValidCommands() {
    return validCommands.clone();
  }

  private final static String[] aliasCommands = new String [] {
    "locator-start", "locator-stop", "locator-status", "locator-info",
    "locator-tail-log",
    "logs-merge",
    "shutdown-all",
    "shutdownall",
    "compact",
    "modify",
    "validate"
  };
  private final static String[] validOptions = new String [] {
    "-address=",
    "-archive=",
    "-concurrencyLevel=",
    "-debug",
    "-remove",
    "-details",
    "-dir=",
    "-endtime=",
    "-h",
    "-help",
    "-initialCapacity=",
    "-loadFactor=",
    "-lru=",
    "-lruAction=",
    "-lruLimit=",
    "-maxOplogSize=",
    "-properties=",
    "-monitor",
    "-nofilter",
    "-persample",
    "-persec",
    "-out=",
    "-port=",
    "-prunezeros",
    "-region=",
    "-starttime=",
    "-statisticsEnabled=",
    "-peer=",
    "-server=",
    "-q",
    "-D",
    "-X",
    "-outputDir="
  };

  protected String checkCmd(String theCmd) {
    String cmd = theCmd;
    if (!Arrays.asList(validCommands).contains(cmd.toLowerCase())) {
      if (!Arrays.asList(aliasCommands).contains(cmd.toLowerCase())) {
        System.err.println(LocalizedStrings.SystemAdmin_ERROR_INVALID_COMMAND_0.toLocalizedString(cmd));
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
          throw new InternalGemFireException(LocalizedStrings.SystemAdmin_UNHANDLED_ALIAS_0.toLocalizedString(cmd));
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
                LocalizedStrings.SystemAdmin_GEMFIRE_HELP.toLocalizedString(new Object[] { join(validCommands), "-h", "-debug", "-help", "-q", "-J<vmOpt>"}));
    helpMap.put("version",
      LocalizedStrings.SystemAdmin_VERSION_HELP.toLocalizedString());
    helpMap.put("help", 
      LocalizedStrings.SystemAdmin_HELP_HELP.toLocalizedString());
    helpMap.put("stats", 
      LocalizedStrings.SystemAdmin_STATS_HELP_PART_A
        .toLocalizedString( new Object[] {
          "+", "++", ":", ".", "-details", "-nofilter", "-archive=", "-persec",
          "-persample", "-prunezeros"
      }) + "\n" +
      LocalizedStrings.SystemAdmin_STATS_HELP_PART_B
        .toLocalizedString( new Object[] {
        "-starttime", "-archive=", DateFormatter.FORMAT_STRING, "-endtime", 
      }));
    helpMap.put("encrypt-password",
      LocalizedStrings.SystemAdmin_ENCRYPTS_A_PASSWORD_FOR_USE_IN_CACHE_XML_DATA_SOURCE_CONFIGURATION.toLocalizedString());
    helpMap.put(START_LOCATOR,
      LocalizedStrings.SystemAdmin_START_LOCATOR_HELP
        .toLocalizedString(new Object[] { "-port=",  Integer.valueOf(DistributionLocator.DEFAULT_LOCATOR_PORT), "-address=", "-dir=", "-properties=", "-peer=", "-server=", "-hostname-for-clients=", "-D", "-X" })); 
    helpMap.put("stop-locator",  
      LocalizedStrings.SystemAdmin_STOP_LOCATOR_HELP
        .toLocalizedString(new Object[] { "-port=",  Integer.valueOf(DistributionLocator.DEFAULT_LOCATOR_PORT), "-address=", "-dir="})); 
    helpMap.put("status-locator", 
      LocalizedStrings.SystemAdmin_STATUS_LOCATOR_HELP
        .toLocalizedString(new Object[] { join(ManagerInfo.statusNames), "-dir="})); 
    helpMap.put("info-locator",
      LocalizedStrings.SystemAdmin_INFO_LOCATOR_HELP
        .toLocalizedString("-dir=")); 
    helpMap.put("tail-locator-log", 
      LocalizedStrings.SystemAdmin_TAIL_LOCATOR_HELP
        .toLocalizedString("-dir=")); 
    helpMap.put("merge-logs", 
      LocalizedStrings.SystemAdmin_MERGE_LOGS
        .toLocalizedString("-out")); 
    helpMap.put("validate-disk-store",
                LocalizedStrings.SystemAdmin_VALIDATE_DISK_STORE.toLocalizedString()); 
    helpMap.put("upgrade-disk-store",
        "Upgrade an offline disk store with new version format. \n"
        + "  -maxOplogSize=<long> causes the oplogs created by compaction to be no larger than the specified size in megabytes.");
    helpMap.put("compact-disk-store",
                "Compacts an offline disk store. Compaction removes all unneeded records from the persistent files.\n"
                + "  -maxOplogSize=<long> causes the oplogs created by compaction to be no larger than the specified size in megabytes."); 
    helpMap.put("compact-all-disk-stores",
                "Connects to a running system and tells its members to compact their disk stores. " +
                "This command uses the compaction threshold that each member has " +
                "configured for its disk stores. The disk store must have allow-force-compaction " +
                "set to true in order for this command to work.\n" +
                    "This command will use the gemfire.properties file to determine what distributed system to connect to.");
    helpMap.put("modify-disk-store",
                LocalizedStrings.SystemAdmin_MODIFY_DISK_STORE.toLocalizedString()); 
    helpMap.put("revoke-missing-disk-store",
                "Connects to a running system and tells its members to stop waiting for the " +
                "specified disk store to be available. Only revoke a disk store if its files " + 
                "are lost. Once a disk store is revoked its files can no longer be loaded so be " +
                "careful. Use the list-missing-disk-stores command to get descriptions of the" +
                "missing disk stores.\n" +
                "You must pass the in the unique id for the disk store to revoke. The unique id is listed in the output " +
                "of the list-missing-disk-stores command, for example a63d7d99-f8f8-4907-9eb7-cca965083dbb.\n" +
                    "This command will use the gemfire.properties file to determine what distributed system to connect to.");
    helpMap.put("list-missing-disk-stores",
                "Prints out a description of the disk stores that are currently missing from a distributed system\n\\n."
                    + "This command will use the gemfire.properties file to determine what distributed system to connect to.");
    helpMap.put("export-disk-store", 
                "Exports an offline disk store.  The persistent data is written to a binary format.\n"
                + "  -outputDir=<directory> specifies the location of the exported snapshot files.");
    helpMap.put("shut-down-all",
                "Connects to a running system and asks all its members that have a cache to close the cache and disconnect from system." +
                "The timeout parameter allows you to specify that the system should be shutdown forcibly after the time has exceeded.\n" +
                    "This command will use the gemfire.properties file to determine what distributed system to connect to.");
    helpMap.put("backup",
                "Connects to a running system and asks all its members that have persistent data " +
                "to backup their data to the specified directory. The directory specified must exist " +
                "on all members, but it can be a local directory on each machine. This command " +
                "takes care to ensure that the backup files will not be corrupted by concurrent " +
                "operations. Backing up a running system with filesystem copy is not recommended.\n" +
                    "This command will use the gemfire.properties file to determine what distributed system to connect to.");
    helpMap.put("print-stacks",
                "fetches stack dumps of all processes.  By default an attempt" +
                " is made to remove idle GemFire threads from the dump.  " +
                "Use -all-threads to include these threads in the dump.  " +
                "An optional filename may be given for storing the dumps.");
    helpMap.put("-out=", 
      LocalizedStrings.SystemAdmin_CAUSES_GEMFIRE_TO_WRITE_OUTPUT_TO_THE_SPECIFIED_FILE_THE_FILE_IS_OVERWRITTEN_IF_IT_ALREADY_EXISTS
        .toLocalizedString()); 
    helpMap.put("-debug", 
      LocalizedStrings.SystemAdmin_CAUSES_GEMFIRE_TO_PRINT_OUT_EXTRA_INFORMATION_WHEN_IT_FAILS_THIS_OPTION_IS_SUPPORTED_BY_ALL_COMMANDS
        .toLocalizedString()); 
    helpMap.put("-details", 
      LocalizedStrings.SystemAdmin_CAUSES_GEMFIRE_TO_PRINT_DETAILED_INFORMATION_WITH_THE_0_COMMAND_IT_MEANS_STATISTIC_DESCRIPTIONS
        .toLocalizedString("stats")); 
    helpMap.put("-nofilter", 
      LocalizedStrings.SystemAdmin_CAUSES_GEMFIRE_0_COMMAND_TO_PRINT_UNFILTERED_RAW_STATISTIC_VALUES_THIS_IS_THE_DEFAULT_FOR_NONCOUNTER_STATISTICS
        .toLocalizedString("stats")); 
    helpMap.put("-persec", 
      LocalizedStrings.SystemAdmin_CAUSES_GEMFIRE_0_COMMAND_TO_PRINT_THE_RATE_OF_CHANGE_PER_SECOND_FOR_STATISTIC_VALUES_THIS_IS_THE_DEFAULT_FOR_COUNTER_STATISTICS
        .toLocalizedString("stats")); 
    helpMap.put("-persample", 
      LocalizedStrings.SystemAdmin_CAUSES_GEMFIRE_0_COMMAND_TO_PRINT_THE_RATE_OF_CHANGE_PER_SAMPLE_FOR_STATISTIC_VALUES
        .toLocalizedString("stats")); 
    helpMap.put("-prunezeros", 
      LocalizedStrings.SystemAdmin_CAUSES_GEMFIRE_0_COMMAND_TO_NOT_PRINT_STATISTICS_WHOSE_VALUES_ARE_ALL_ZERO
        .toLocalizedString("stats")); 
    helpMap.put("-port=", 
      LocalizedStrings.SystemAdmin_USED_TO_SPECIFY_A_NONDEFAULT_PORT_WHEN_STARTING_OR_STOPPING_A_LOCATOR
        .toLocalizedString()); 
    helpMap.put("-address=", 
      LocalizedStrings.SystemAdmin_USED_TO_SPECIFY_A_SPECIFIC_IP_ADDRESS_TO_LISTEN_ON_WHEN_STARTING_OR_STOPPING_A_LOCATOR
        .toLocalizedString()); 
    helpMap.put("-hostname-for-clients=",
      LocalizedStrings.SystemAdmin_USED_TO_SPECIFY_A_HOST_NAME_OR_IP_ADDRESS_TO_GIVE_TO_CLIENTS_SO_THEY_CAN_CONNECT_TO_A_LOCATOR
        .toLocalizedString());
    helpMap.put("-properties=", 
      LocalizedStrings.SystemAdmin_USED_TO_SPECIFY_THE_0_FILE_TO_BE_USED_IN_CONFIGURING_THE_LOCATORS_DISTRIBUTEDSYSTEM
          .toLocalizedString(DistributionConfig.GEMFIRE_PREFIX + "properties"));
    helpMap.put("-archive=", 
      LocalizedStrings.SystemAdmin_THE_ARGUMENT_IS_THE_STATISTIC_ARCHIVE_FILE_THE_0_COMMAND_SHOULD_READ
        .toLocalizedString("stats")); 
    helpMap.put("-h", 
      LocalizedStrings.SystemAdmin_CAUSES_GEMFIRE_TO_PRINT_OUT_INFORMATION_INSTEAD_OF_PERFORMING_THE_COMMAND_THIS_OPTION_IS_SUPPORTED_BY_ALL_COMMANDS
        .toLocalizedString());
    helpMap.put("-help", helpMap.get("-h"));
    helpMap.put("-q", 
      LocalizedStrings.SystemAdmin_TURNS_ON_QUIET_MODE_THIS_OPTION_IS_SUPPORTED_BY_ALL_COMMANDS
        .toLocalizedString()); 
    helpMap.put("-starttime=", 
      LocalizedStrings.SystemAdmin_CAUSES_THE_0_COMMAND_TO_IGNORE_STATISTICS_SAMPLES_TAKEN_BEFORE_THIS_TIME_THE_ARGUMENT_FORMAT_MUST_MATCH_1
        .toLocalizedString(new Object[] {"stats", DateFormatter.FORMAT_STRING})); 
    helpMap.put("-endtime=", 
      LocalizedStrings.SystemAdmin_CAUSES_THE_0_COMMAND_TO_IGNORE_STATISTICS_SAMPLES_TAKEN_AFTER_THIS_TIME_THE_ARGUMENT_FORMAT_MUST_MATCH_1
        .toLocalizedString(new Object[] {"stats", DateFormatter.FORMAT_STRING})); 
    helpMap.put("-dir=", 
      LocalizedStrings.SystemAdmin_DIR_ARGUMENT_HELP
          .toLocalizedString(new Object[] { DistributionConfig.GEMFIRE_PREFIX + "properties",
              DistributionConfig.GEMFIRE_PREFIX + "systemDirectory", "GEMFIRE", "defaultSystem",
                "version" }));
    helpMap.put("-D", 
      LocalizedStrings.SystemAdmin_SETS_A_JAVA_SYSTEM_PROPERTY_IN_THE_LOCATOR_VM_USED_MOST_OFTEN_FOR_CONFIGURING_SSL_COMMUNICATION
        .toLocalizedString()); 
    helpMap.put("-X", 
      LocalizedStrings.SystemAdmin_SETS_A_JAVA_VM_X_SETTING_IN_THE_LOCATOR_VM_USED_MOST_OFTEN_FOR_INCREASING_THE_SIZE_OF_THE_VIRTUAL_MACHINE
        .toLocalizedString()); 
    helpMap.put("-remove", 
      LocalizedStrings.SystemAdmin_REMOVE_OPTION_HELP.toLocalizedString()); 
    helpMap.put("-maxOplogSize=",
                "Limits the size of any oplogs that are created to the specified size in megabytes.");
    helpMap.put("-lru=", 
      LocalizedStrings.SystemAdmin_LRU_OPTION_HELP.toLocalizedString()); 
    helpMap.put("-lruAction=", 
      LocalizedStrings.SystemAdmin_LRUACTION_OPTION_HELP.toLocalizedString()); 
    helpMap.put("-lruLimit=", 
      LocalizedStrings.SystemAdmin_LRULIMIT_OPTION_HELP.toLocalizedString()); 
    helpMap.put("-concurrencyLevel=", 
      LocalizedStrings.SystemAdmin_CONCURRENCYLEVEL_OPTION_HELP.toLocalizedString()); 
    helpMap.put("-initialCapacity=", 
      LocalizedStrings.SystemAdmin_INITIALCAPACITY_OPTION_HELP.toLocalizedString()); 
    helpMap.put("-loadFactor=", 
      LocalizedStrings.SystemAdmin_LOADFACTOR_OPTION_HELP.toLocalizedString()); 
    helpMap.put("-statisticsEnabled=", 
      LocalizedStrings.SystemAdmin_STATISTICSENABLED_OPTION_HELP.toLocalizedString()); 
    helpMap.put("-region=", 
      LocalizedStrings.SystemAdmin_REGION_OPTION_HELP.toLocalizedString()); 
    helpMap.put("-monitor", 
      LocalizedStrings.SystemAdmin_MONITOR_OPTION_HELP.toLocalizedString()); 
    helpMap.put("-peer=",
                "-peer=<true|false> True, the default, causes the locator to find peers for other peers. False will cause the locator to not locate peers.");
    helpMap.put("-server=",
                "-server=<true|false> True, the default, causes the locator to find servers for clients. False will cause the locator to not locate servers for clients.");
    helpMap.put("-outputDir=",
                "The directory where the disk store should be exported.");
  }

  protected final Map usageMap = new HashMap();

  protected void initUsageMap() {
    usageMap.put("gemfire", "gemfire [-debug] [-h[elp]] [-q] [-J<vmOpt>]*");
    usageMap.put("version", "version");
    usageMap.put("help", "help [" + join(helpTopics, "|") + "]");
    usageMap.put("stats", "stats ([<instanceId>][:<typeId>][.<statId>])* [-details] [-nofilter|-persec|-persample] [-prunezeros] [-starttime=<time>] [-endtime=<time>] -archive=<statFile>");
    usageMap.put(START_LOCATOR,
        "start-locator [-port=<port>] [-address=<ipAddr>] [-dir=<locatorDir>] [-properties=<gemfire.properties>] [-peer=<true|false>] [-server=<true|false>] [-hostname-for-clients=<ipAddr>] [-D<system.property>=<value>] [-X<vm-setting>]");
    usageMap.put("stop-locator", "stop-locator [-port=<port>] [-address=<ipAddr>] [-dir=<locatorDir>]");
    usageMap.put("status-locator", "status-locator [-dir=<locatorDir>]");
    usageMap.put("info-locator", "info-locator [-dir=<locatorDir>]");
    usageMap.put("tail-locator-log", "tail-locator-log [-dir=<locatorDir>]");
    usageMap.put("merge-logs", "merge-logs <logFile>+ [-out=<outFile>]");
    usageMap.put("encrypt-password", "encrypt-password <passwordString>");
    usageMap.put("validate-disk-store", "validate-disk-store <diskStoreName> <directory>+");
    usageMap.put("upgrade-disk-store", "upgrade-disk-store <diskStoreName> <directory>+ [-maxOplogSize=<int>]");
    usageMap.put("compact-disk-store", "compact-disk-store <diskStoreName> <directory>+ [-maxOplogSize=<int>]");
    usageMap.put("compact-all-disk-stores", "compact-all-disk-stores");
    usageMap.put("modify-disk-store", "modify-disk-store <diskStoreName> <directory>+ [-region=<regionName> [-remove|(-lru=<none|lru-entry-count|lru-heap-percentage|lru-memory-size>|-lruAction=<none|overflow-to-disk|local-destroy>|-lruLimit=<int>|-concurrencyLevel=<int>|-initialCapacity=<int>|-loadFactor=<float>|-statisticsEnabled=<boolean>)*]]");
    usageMap.put("list-missing-disk-stores", "list-missing-disk-stores");
    usageMap.put("export-disk-store", "export-disk-store <diskStoreName> <directory>+ [-outputDir=<directory>]");
    usageMap.put("shut-down-all", "shut-down-all [timeout_in_ms]");
    usageMap.put("backup", "backup [-baseline=<baseline directory>] <target directory>");
    usageMap.put("revoke-missing-disk-store", "revoke-missing-disk-store <disk-store-id>");
    usageMap.put("print-stacks", "print-stacks [-all-threads] [<filename>]");
  }
  // option statics
  private static boolean debug = false;
  private static boolean details = false;
  private static boolean nofilter = false;
  private static boolean persec = false;
  private static boolean persample = false;
  private static boolean prunezeros = false;
  private static boolean quiet = false;
  private static boolean help = false;
  private static boolean monitor = false;
  private static boolean showBuckets = false;
  private static long startTime = -1;
  private static long endTime = -1;
  private static String portOption = null;
  private static String addressOption = "";
  private static String regionOption = null;
  private static long maxOplogSize = -1L;
  private static String lruOption = null;
  private static String lruActionOption = null;
  private static String lruLimitOption = null;
  private static String concurrencyLevelOption = null;
  private static String initialCapacityOption = null;
  private static String loadFactorOption = null;
  private static String compressorClassNameOption = null;
  private static String statisticsEnabledOption = null;
  private static boolean remove = false;
  private static String sysDirName = null;
  private static ArrayList archiveOption = new ArrayList();
  private static String printStacksOption = null;
  private static String outOption = null;
  private static Properties propertyOption = new Properties();
  private static boolean serverOption = true;
  private static boolean peerOption = true;
  private static String gemfirePropertiesFileOption = null;
  private static ArrayList xoptions = new ArrayList();
  private static String hostnameForClientsOption = null;
  private static String baselineDir = null;                                     // Baseline directory option value for backup command
  private static String outputDir = null;

  private static Map cmdOptionsMap = new HashMap();
  static {
    cmdOptionsMap.put("gemfire", new String[] {"--help", "-h", "-help", "-debug", "-q"});
    cmdOptionsMap.put("version", new String[] {});
    cmdOptionsMap.put("help", new String[] {});
    cmdOptionsMap.put("merge-logs", new String[] {"-out="});
    cmdOptionsMap.put("stats", new String[] {"-details", "-monitor", "-nofilter", "-persec", "-persample", "-prunezeros", "-archive=", "-starttime=", "-endtime="});
    cmdOptionsMap
        .put(START_LOCATOR, new String[] { "-port=", "-dir=", "-address=", "-properties=", "-D", "-X", "-peer=", "-server=", "-hostname-for-clients=" });
    cmdOptionsMap.put("stop-locator",  new String[] {"-port=", "-dir=", "-address=", "-D"});
    cmdOptionsMap.put("status-locator",  new String[] {"-dir=", "-D"});
    cmdOptionsMap.put("info-locator",  new String[] {"-dir=", "-D"});
    cmdOptionsMap.put("tail-locator-log",  new String[] {"-dir=", "-D"});
    cmdOptionsMap.put("validate-disk-store", new String[] {});
    cmdOptionsMap.put("upgrade-disk-store", new String[] {"-maxOplogSize="});
    cmdOptionsMap.put("compact-disk-store", new String[] {"-maxOplogSize="});
    cmdOptionsMap.put("modify-disk-store", new String[] {"-region=", "-remove",
                                                         "-lru=", "-lruAction=", "-lruLimit=",
                                                         "-concurrencyLevel=",
                                                         "-initialCapacity=",
                                                         "-loadFactor=",
                                                         "-statisticsEnabled="});
    cmdOptionsMap.put("list-missing-disk-stores", new String[] {});
    cmdOptionsMap.put("compact-all-disk-stores", new String[] {});
    cmdOptionsMap.put("revoke-missing-disk-store", new String[] {});
    cmdOptionsMap.put("show-disk-store-metadata", new String[] {"-buckets"});
    cmdOptionsMap.put("export-disk-store", new String[] {"-outputDir="});
    cmdOptionsMap.put("shut-down-all", new String[] {});
    cmdOptionsMap.put("backup", new String[] {"-baseline="});
    cmdOptionsMap.put("encrypt-password", new String[] {});
    cmdOptionsMap.put("print-stacks", new String[]{"-all-threads"});
  }

  private static long parseLong(String arg) {
    try {
      return Long.parseLong(arg);
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException("Could not parse -maxOplogSize=" + arg
                                         + " because: " + ex.getMessage());
    }
  }
  
  private static long parseTime(String arg) {
    DateFormat fmt = DateFormatter.createDateFormat();
    try {
      Date d = fmt.parse(arg);
      return d.getTime();
    } catch (ParseException ex) {
      throw new IllegalArgumentException(LocalizedStrings.SystemAdmin_TIME_WAS_NOT_IN_THIS_FORMAT_0_1.toLocalizedString(new Object[] {DateFormatter.FORMAT_STRING, ex}));
    }
  }

  protected boolean matchCmdArg(String cmd, String arg) {
    String[] validArgs = (String[])cmdOptionsMap.get(cmd.toLowerCase());
    for (int i=0; i < validArgs.length; i++) {
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
            int idx = argValue.indexOf( '=' );
            String key = argValue.substring( 0, idx );
            String value = argValue.substring( idx + 1 );
            propertyOption.setProperty( key, value );
          } else if (validArgs[i].equals("-X")) {
            xoptions.add(arg);
          } else if(validArgs[i].equals("-baseline=")) {
            baselineDir=argValue;
          } else if (validArgs[i].equals("-outputDir=")) {
            outputDir = argValue;
          } else {
            throw new InternalGemFireException(LocalizedStrings.SystemAdmin_UNEXPECTED_VALID_OPTION_0.toLocalizedString(validArgs[i]));
          }
          return true;
        }
      } else if (validArgs[i].equalsIgnoreCase(arg)) {
        if (validArgs[i].equals("-h")
            || validArgs[i].toLowerCase().matches("-{0,2}help")) {
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
          throw new InternalGemFireException(LocalizedStrings.SystemAdmin_UNEXPECTED_VALID_OPTION_0.toLocalizedString(validArgs[i]));
        }
        return true;
      }
    }
    return false;
  }

  protected void printHelp(String cmd) {
    List<String> lines = format((String)helpMap.get(cmd.toLowerCase()), 80);
    for(String line : lines) {
      System.err.println(line);
    }
    usage(cmd);
  }
  
  public static List<String> format(String string, int width) {
    List<String> results = new ArrayList<String>();
    String[] realLines = string.split("\n");
    for(String line : realLines) {
      results.addAll(lineWrapOut(line, width));
      results.add("");
    }
    
    return results;
  }

  public static List<String> lineWrapOut(String string, int width) {
    Pattern pattern = Pattern.compile("(.{0," + (width -1) + "}\\S|\\S{" + (width) +",})(\n|\\s+|$)");
    
    Matcher matcher = pattern.matcher(string);
    List<String> lines = new ArrayList<String>();
    while(matcher.find()) {
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
    if (cmd.equalsIgnoreCase("stats")) {
      return false;
    }
    if (cmd.equalsIgnoreCase("merge-logs")) {
      return false;
    }
    if (cmd.equalsIgnoreCase("version")) {
      return false;
    }
    if (cmd.equalsIgnoreCase("help")) {
      return false;
    }
    return true;
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
    File prodDir= getProductDir();
    if (prodDir == null) {
      return null;
    }
    File hiddenDir = new File(prodDir.getParentFile(),"hidden");
    if (!hiddenDir.exists()) {
      hiddenDir = new File(prodDir.getCanonicalFile().getParentFile(), "hidden");
    }
    if (!hiddenDir.exists()) {
      //If we don't have a jar file (eg, when running in eclipse), look for a hidden
      //directory in same same directory as our classes.
      File libDir = findGemFireLibDir();
      File oldHiddenDir = hiddenDir; 
      if(libDir != null && libDir.exists()) {
        hiddenDir = new File(libDir, "hidden");
      } if(!hiddenDir.exists()) {
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
        String arg = (String)it.next();
        if (arg.startsWith("-")) {
          if (matchCmdArg("gemfire", arg)) {
            it.remove();
          } else {
            System.err.println(
              LocalizedStrings.SystemAdmin_ERROR_UNKNOWN_OPTION_0
                .toLocalizedString(arg));
            usage();
          }
        }
        else {
          break;
        }
      }
    } catch (IllegalArgumentException ex) {
      System.err.println(LocalizedStrings.SystemAdmin_ERROR.toLocalizedString()
        + ": " + getExceptionMessage(ex));
      if (debug) {
        ex.printStackTrace(System.err);
      }
      System.exit(1); // fix for bug 28351
    }
    if (cmdLine.size() == 0) {
      if (help) {
        printHelp("gemfire");
      } else {
        System.err.println(LocalizedStrings.SystemAdmin_ERROR_WRONG_NUMBER_OF_COMMAND_LINE_ARGS.toLocalizedString());
        usage();
      }
    }
    cmd = (String)cmdLine.remove(0);
    cmd = checkCmd(cmd);
    File sysDir = null;
//    File configFile = null;
    try {
      Iterator it = cmdLine.iterator();
      while (it.hasNext()) {
        String arg = (String)it.next();
        if (arg.startsWith("-")) {
          if (matchCmdArg(cmd, arg) || matchCmdArg("gemfire", arg)) {
            it.remove();
          } else {
            System.err.println(
              LocalizedStrings.SystemAdmin_ERROR_UNKNOWN_OPTION_0
                .toLocalizedString(arg));
            usage(cmd);
          }
        }
      }
    } catch (IllegalArgumentException ex) {
      System.err.println(LocalizedStrings.SystemAdmin_ERROR.toLocalizedString()
        + ": " + getExceptionMessage(ex));
      if (debug) {
        ex.printStackTrace(System.err);
      }
      System.exit(1); // fix for bug 28351
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
        statistics(sysDir, archiveOption, details, nofilter, persec, persample, prunezeros, monitor, startTime, endTime, cmdLine);
      } else if (cmd.equalsIgnoreCase("version")) {
        boolean optionOK = (cmdLine.size() == 0);
        if (cmdLine.size() == 1) {
          optionOK = false;

          String option = (String) cmdLine.get(0);
          if ("CREATE".equals(option) ||
              "FULL".equalsIgnoreCase(option)) {
            // CREATE and FULL are secret for internal use only
            optionOK = true;
          }
        }

        if (!optionOK) {
          System.err.println( LocalizedStrings.SystemAdmin_ERROR_UNEXPECTED_COMMAND_LINE_ARGUMENTS_0.toLocalizedString(join(cmdLine)));
          usage(cmd);
        }
        System.out.println(
          LocalizedStrings.SystemAdmin_GEMFIRE_PRODUCT_DIRECTORY_0
            .toLocalizedString(getProductDir()));

        if (cmdLine.size() == 1 && "CREATE".equals(cmdLine.get(0))) {
          GemFireVersion.createVersionFile();

        } else if (cmdLine.size() == 1 &&
                   "FULL".equalsIgnoreCase(String.valueOf(cmdLine.get(0)))) {
          GemFireVersion.print(System.out, true);

        } else {
          GemFireVersion.print(System.out, false);
        }
      } else if (cmd.equalsIgnoreCase("help")) {
        if (cmdLine.size() > 1) {
          System.err.println( LocalizedStrings.SystemAdmin_ERROR_UNEXPECTED_COMMAND_LINE_ARGUMENTS_0.toLocalizedString(join(cmdLine)));
          usage(cmd);
        }
        help(cmdLine);
      } else if (cmd.equalsIgnoreCase(START_LOCATOR)) {
        if (cmdLine.size() != 0) {
          System.err.println( LocalizedStrings.SystemAdmin_ERROR_UNEXPECTED_COMMAND_LINE_ARGUMENTS_0.toLocalizedString(join(cmdLine)));
          usage(cmd);
        }
        locatorStart(sysDir, portOption, addressOption, gemfirePropertiesFileOption, propertyOption, xoptions, peerOption, serverOption, hostnameForClientsOption);
        if (!quiet) {
          System.out.println(LocalizedStrings.SystemAdmin_LOCATOR_START_COMPLETE.toLocalizedString());
        }
      } else if (cmd.equalsIgnoreCase("stop-locator")) {
        if (cmdLine.size() != 0) {
          System.err.println( LocalizedStrings.SystemAdmin_ERROR_UNEXPECTED_COMMAND_LINE_ARGUMENTS_0.toLocalizedString(join(cmdLine)));
          usage(cmd);
        }
        locatorStop(sysDir, portOption, addressOption, propertyOption);
        if (!quiet) {
          System.out.println(LocalizedStrings.SystemAdmin_LOCATOR_STOP_COMPLETE.toLocalizedString());
        }
      } else if (cmd.equalsIgnoreCase("status-locator")) {
        if (cmdLine.size() != 0) {
          System.err.println( LocalizedStrings.SystemAdmin_ERROR_UNEXPECTED_COMMAND_LINE_ARGUMENTS_0.toLocalizedString(join(cmdLine)));
          usage(cmd);
        }
        if (!quiet) {
          System.out.println(locatorStatus(sysDir));
        }
      } else if (cmd.equalsIgnoreCase("info-locator")) {
        if (cmdLine.size() != 0) {
          System.err.println( LocalizedStrings.SystemAdmin_ERROR_UNEXPECTED_COMMAND_LINE_ARGUMENTS_0.toLocalizedString(join(cmdLine)));
          usage(cmd);
        }
        System.out.println(locatorInfo(sysDir));
      } else if (cmd.equalsIgnoreCase("tail-locator-log")) {
        if (cmdLine.size() != 0) {
          System.err.println( LocalizedStrings.SystemAdmin_ERROR_UNEXPECTED_COMMAND_LINE_ARGUMENTS_0.toLocalizedString(join(cmdLine)));
          usage(cmd);
        }
        System.out.println(locatorTailLog(sysDir));
      } else if (cmd.equalsIgnoreCase("merge-logs")) {
        if (cmdLine.size() == 0) {
          System.err.println(LocalizedStrings.SystemAdmin_ERROR_EXPECTED_AT_LEAST_ONE_LOG_FILE_TO_MERGE.toLocalizedString());
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
      } else if (cmd.equalsIgnoreCase("encrypt-password")) {
        if (cmdLine.size() != 1) {
          usage(cmd);
        }
        PasswordUtil.encrypt((String)cmdLine.get(0));
      } else if (cmd.equalsIgnoreCase("print-stacks")) {
        printStacks(cmdLine, printStacksOption != null);
      } else {
        System.err.println(
          LocalizedStrings.SystemAdmin_ERROR_UNKNOWN_COMMAND_0
            .toLocalizedString(cmd));
        usage();
      }
    } catch (InterruptedException ex) {
      System.err.println(
          LocalizedStrings.SystemAdmin_ERROR_OPERATION_0_FAILED_BECAUSE_1
            .toLocalizedString(new Object[] { cmd, getExceptionMessage(ex) }));
      if (debug) {
        ex.printStackTrace(System.err);
      }
      System.exit(1); // fix for bug 28351
    } catch (IllegalArgumentException ex) {
      System.err.println(
        LocalizedStrings.SystemAdmin_ERROR_OPERATION_0_FAILED_BECAUSE_1
          .toLocalizedString(new Object[] {cmd, getExceptionMessage(ex)}));

      if (debug) {
        ex.printStackTrace(System.err);
      }
      System.exit(1); // fix for bug 28351
    } catch (Exception ex) {
      System.err.println(
          LocalizedStrings.SystemAdmin_ERROR_OPERATION_0_FAILED_BECAUSE_1
          .toLocalizedString(new Object[] {cmd, getExceptionMessage(ex)}));
      if (debug) {
        ex.printStackTrace(System.err);
      }
      System.exit(1); // fix for bug 28351
    }
  }
}

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
package com.gemstone.gemfire.management.internal.cli;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.Map.Entry;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.cache.execute.AbstractExecution;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.util.IOUtils;
import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.functions.MembersForRegionFunction;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResultException;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;

/**
 * This class contains utility methods used by classes used to build the Command
 * Line Interface (CLI).
 *
 *
 * @since GemFire 7.0
 */
public class CliUtil {
  public static final String GFSHVM_IDENTIFIER = "gfsh";
  public static boolean isGfshVM = Boolean.getBoolean(GFSHVM_IDENTIFIER);

  public static final FileFilter JAR_FILE_FILTER = new CustomFileFilter(".jar");

  public static boolean isGfshVM() {
    return isGfshVM;
  }

  public static String cliDependenciesExist(boolean includeGfshDependencies) {
    String jarProductName = null;

    // Parser & CliCommand from Spring Shell
    jarProductName = checkLibraryByLoadingClass("org.springframework.shell.core.Parser", "Spring Shell");
    jarProductName = checkLibraryByLoadingClass("org.springframework.shell.core.annotation.CliCommand", "Spring Shell");
    if (jarProductName != null) {
      return jarProductName;
    }

    // SpringVersion from Spring Core
    jarProductName = checkLibraryByLoadingClass("org.springframework.core.SpringVersion", "Spring Core");
    if (jarProductName != null) {
      return jarProductName;
    }

    if (includeGfshDependencies) {
      // ConsoleReader from jline
      jarProductName = checkLibraryByLoadingClass("jline.console.ConsoleReader", "JLine");
      if (jarProductName != null) {
        return jarProductName;
      }
    }

    return jarProductName;
  }

  private static String checkLibraryByLoadingClass(String className, String jarProductName) {
    try {
      ClassPathLoader.getLatest().forName(className);
    } catch (ClassNotFoundException e) {
      return jarProductName;
    }

    return null;
  }

  public static Cache getCacheIfExists() {
    Cache cache;
    try {
      cache = CacheFactory.getAnyInstance();
    } catch (CacheClosedException e) {
      //ignore & return null
      cache = null;
    }

    return cache;
  }

  public static byte[][] filesToBytes(String[] fileNames) throws FileNotFoundException, IOException {
    List<byte[]> filesDataList = new ArrayList<byte[]>();

    for (int i = 0; i < fileNames.length; i++) {
      File file = new File(fileNames[i]);

      if (!file.exists()) {
        throw new FileNotFoundException("Could not find "+file.getCanonicalPath());
      }

      if (file.isDirectory()) { //TODO - Abhishek: (1) No recursive search yet. (2) Do we need to check/limit size of the files too?
        File[] childrenFiles = file.listFiles(JAR_FILE_FILTER);
        for (int j = 0; j < childrenFiles.length; j++) {
          //1. add name of the file as bytes at even index
          filesDataList.add(childrenFiles[j].getName().getBytes());
          //2. add file contents as bytes at odd index
          filesDataList.add(toByteArray(new FileInputStream(childrenFiles[j])));
        }
      } else {
        filesDataList.add(file.getName().getBytes());
        filesDataList.add(toByteArray(new FileInputStream(file)));
      }
    }

    byte[][] filesData = new byte[filesDataList.size()][];

    filesData = filesDataList.toArray(filesData);

    return filesData;
  }

  public static byte[] toByteArray(InputStream input) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    int n = 0;
    byte[] buffer = new byte[4096];
    while (-1 != (n = input.read(buffer))) {
      output.write(buffer, 0, n);
    }

    return output.toByteArray();
  }

  public static String[] bytesToNames(byte[][] fileData) {
    String[] names = new String[fileData.length / 2];
    for (int i = 0; i < fileData.length; i += 2) {
      names[i / 2] = new String(fileData[i]);
    }

    return names;
  }

  public static byte[][] bytesToData(byte[][] fileData) {
    byte[][] data = new byte[fileData.length / 2][];
    for (int i = 1; i < fileData.length; i += 2) {
      data[i / 2] = fileData[i];
    }

    return data;
  }

  public static void bytesToFiles(byte[][] fileData, String parentDirPath, boolean mkRequireddirs)
      throws FileNotFoundException, IOException, UnsupportedOperationException {
    FileOutputStream fos = null;

    File parentDir = new File(parentDirPath);
    if (mkRequireddirs && !parentDir.exists()) {
      if (!parentDir.mkdirs()) {
        throw new UnsupportedOperationException("Couldn't create required directory structure for "+parentDirPath);
      }
    }
    for (int i = 0; i < fileData.length; i++) {
      if (i%2 == 0) {
        // Expect file name as bytes at even index
        String fileName = new String(fileData[i]);
        fos = new FileOutputStream(new File(parentDir, fileName));
      } else {
        // Expect file contents as bytes at odd index
        fos.write(fileData[i]);
        fos.close();
      }
    }
  }

  

  public static boolean isValidFileName (String filePath, String extension) {
    boolean isValid = true;
    return isValid;
  }
  
  public static Set<String> getAllRegionNames() {
    Cache cache = CacheFactory.getAnyInstance();
    Set<String> regionNames = new HashSet<String>();
    Set<Region<?,?>> rootRegions = cache.rootRegions();

    Iterator<Region<?,?>> rootRegionIters = rootRegions.iterator();

    while (rootRegionIters.hasNext()) {
      Region<?,?> rootRegion = rootRegionIters.next();
      regionNames.add(rootRegion.getFullPath().substring(1));

      Set<Region<?, ?>> subRegions = rootRegion.subregions(true);
      Iterator<Region<?,?>> subRegionIters = subRegions.iterator();

      while (subRegionIters.hasNext()) {
        Region<?,?> subRegion = subRegionIters.next();
        regionNames.add(subRegion.getFullPath().substring(1));
      }
    }
    return regionNames;
  }

  public static String convertStringSetToString(Set<String> stringSet, char delimiter) {
    StringBuilder sb = new StringBuilder();
    if (stringSet != null) {
      Iterator<String> iters = stringSet.iterator();

      while (iters.hasNext()) {
        String stringValue = iters.next();
        sb.append(stringValue);
        sb.append(delimiter);
      }
    }
    return sb.toString();
  }

  public static String convertStringListToString(List<String> stringList, char delimiter) {
    StringBuilder sb = new StringBuilder();
    if (stringList != null) {
      Iterator<String> iters = stringList.iterator();

      while (iters.hasNext()) {
        String stringValue = iters.next();
        sb.append(stringValue);
        sb.append(delimiter);
      }
    }
    return sb.toString();
  }
  

        
  public static Set<DistributedMember> findAllMatchingMembers(final String groups, final String members)
      throws CommandResultException {
    
    String[] groupsArray = (groups == null ? null : groups.split(","));
    String[] membersArray = (members == null ? null : members.split(","));
    
    return findAllMatchingMembers(groupsArray, membersArray);
  }
  
  public static Set<DistributedMember> findAllMatchingMembers(final String[] groups, final String[] members) throws CommandResultException {
    Set<DistributedMember> matchingMembers = new HashSet<DistributedMember>();
    Cache cache = CacheFactory.getAnyInstance();

    if ((members != null && members.length > 0) && (groups != null && groups.length > 0)) {
      throw new CommandResultException(ResultBuilder.createUserErrorResult(CliStrings.PROVIDE_EITHER_MEMBER_OR_GROUP_MESSAGE));
    }

    if (members != null && members.length > 0) {
      for (String memberNameOrId : members) {
        DistributedMember member = getDistributedMemberByNameOrId(memberNameOrId);
        if (member != null) {
          matchingMembers.add(member);
        } else {
          throw new CommandResultException(ResultBuilder.createUserErrorResult(CliStrings.format(
              CliStrings.MEMBER_NOT_FOUND_ERROR_MESSAGE, memberNameOrId)));
        }
      }
    } else if (groups != null && groups.length > 0) {
      Set<DistributedMember> allNormalMembers = getAllNormalMembers(cache);
      for (String group : groups) {
        Set<DistributedMember> groupMembers = new HashSet<DistributedMember>();
        for (DistributedMember member : allNormalMembers) {
          if (member.getGroups().contains(group)) {
            groupMembers.add(member);
          }
        }

        if (!groupMembers.isEmpty()) {
          matchingMembers.addAll(groupMembers);

        } else {
          throw new CommandResultException(ResultBuilder.createUserErrorResult(CliStrings.format(
              CliStrings.NO_MEMBERS_IN_GROUP_ERROR_MESSAGE, group)));
        }
      }
    } else {
      matchingMembers.addAll(getAllNormalMembers(cache));
      if (matchingMembers.isEmpty()) {
        throw new CommandResultException(ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE));
      }
    }

    return matchingMembers;
  }

  public static DistributedMember getDistributedMemberByNameOrId (String memberNameOrId) {
    DistributedMember memberFound = null;

    if (memberNameOrId != null) {
      Cache cache = CacheFactory.getAnyInstance();
      Set<DistributedMember>memberSet = CliUtil.getAllMembers(cache);
      for (DistributedMember member: memberSet) {
        if (memberNameOrId.equalsIgnoreCase(member.getId()) || memberNameOrId.equals(member.getName())) {
          memberFound = member;
          break;
        }
      }
    }
    return memberFound;
  }

  public static String stackTraceAsString(Throwable e) {
    String stackAsString = "";
    if (e != null) {
      StringWriter writer = new StringWriter();
      PrintWriter pw = new PrintWriter(writer);
      e.printStackTrace(pw);
      stackAsString = writer.toString();
    }
    return stackAsString;
  }

  @SuppressWarnings("unchecked")
  public static <K> Class<K> forName(String classToLoadName, String neededFor) {
    Class<K> loadedClass = null;
    try {
      // Set Constraints
      ClassPathLoader classPathLoader = ClassPathLoader.getLatest();
      if (classToLoadName != null && !classToLoadName.isEmpty()) {
        loadedClass = (Class<K>) classPathLoader.forName(classToLoadName);
      }
    } catch (ClassNotFoundException | NoClassDefFoundError e) {
      throw new RuntimeException(CliStrings.format(CliStrings.CREATE_REGION__MSG__COULDNOT_FIND_CLASS_0_SPECIFIED_FOR_1, new Object[] {classToLoadName, neededFor}), e);
    }
    catch (ClassCastException e) {
      throw new RuntimeException(CliStrings.format(CliStrings.CREATE_REGION__MSG__CLASS_SPECIFIED_FOR_0_SPECIFIED_FOR_1_IS_NOT_OF_EXPECTED_TYPE, new Object[] {classToLoadName, neededFor}), e);
    }

    return loadedClass;
  }

  public static <K> K newInstance(Class<K> klass, String neededFor) {
    K instance = null;
    try {
      instance = klass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(CliStrings.format(CliStrings.CREATE_REGION__MSG__COULDNOT_INSTANTIATE_CLASS_0_SPECIFIED_FOR_1, new Object[] {klass, neededFor}), e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(CliStrings.format(CliStrings.CREATE_REGION__MSG__COULDNOT_ACCESS_CLASS_0_SPECIFIED_FOR_1, new Object[] {klass, neededFor}), e);
    }

    return instance;
  }

  static class CustomFileFilter implements FileFilter {
    private String extensionWithDot;

    public CustomFileFilter(String extensionWithDot) {
      this.extensionWithDot = extensionWithDot;
    }

    @Override
    public boolean accept(File pathname) {
      String name = pathname.getName();

      return name.endsWith(extensionWithDot);
    }
  }

  public static DeflaterInflaterData compressBytes(byte[] input) {
    Deflater compresser = new Deflater();
    compresser.setInput(input);
    compresser.finish();
    byte[] buffer = new byte[100];
    byte[] result = new byte[0];
    int compressedDataLength = 0;
    int totalCompressedDataLength = 0;
    do {
      byte[] newResult = new byte[result.length + buffer.length];
      System.arraycopy(result, 0, newResult, 0, result.length);

      compressedDataLength = compresser.deflate(buffer);
      totalCompressedDataLength += compressedDataLength;
//      System.out.println(compressedDataLength);
//      System.out.println("uc: b  "+buffer.length);
//      System.out.println("uc: r  "+result.length);
//      System.out.println("uc: nr "+newResult.length);
//      System.out.println();
      System.arraycopy(buffer, 0, newResult, result.length, buffer.length);
      result = newResult;
    } while (compressedDataLength != 0);
    return new DeflaterInflaterData(totalCompressedDataLength, result);
  }

  public static DeflaterInflaterData uncompressBytes(byte[] output, int compressedDataLength) throws DataFormatException {
    Inflater decompresser = new Inflater();
    decompresser.setInput(output, 0, compressedDataLength);
    byte[] buffer = new byte[512];
    byte[] result = new byte[0];
    int bytesRead;
    while (!decompresser.needsInput()) {
      bytesRead = decompresser.inflate(buffer);
      byte[] newResult = new byte[result.length + bytesRead];
      System.arraycopy(result, 0, newResult, 0, result.length);
      System.arraycopy(buffer, 0, newResult, result.length, bytesRead);
      result = newResult;
    }
//    System.out.println(new String(result));
    decompresser.end();

    return new DeflaterInflaterData(result.length, result);
  }

  public static class DeflaterInflaterData implements Serializable {
    private static final long serialVersionUID = 1104813333595216795L;

    private final int dataLength;
    private final byte[] data;
    /**
     * @param dataLength
     * @param data
     */
    public DeflaterInflaterData(int dataLength, byte[] data) {
      this.dataLength = dataLength;
      this.data = data;
    }

    public int getDataLength() {
      return dataLength;
    }

    public byte[] getData() {
      return data;
    }

    @Override
    public String toString() {
      return String.valueOf(dataLength);
    }
  }

  public static void main(String[] args) {
    try {
      byte[][] filesToBytes = filesToBytes(new String[] {"/export/abhishek1/work/aspenmm/GFTryouts/test.json"});

      System.out.println(filesToBytes[1].length);

      DeflaterInflaterData compressBytes = compressBytes(filesToBytes[1]);
      System.out.println(compressBytes);

      DeflaterInflaterData uncompressBytes = uncompressBytes(compressBytes.data, compressBytes.dataLength);
      System.out.println(uncompressBytes);

      System.out.println(new String(uncompressBytes.getData()));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (DataFormatException e) {
      e.printStackTrace();
    }
  }

  public static void main1(String[] args) {
    try {
      byte[][] fileToBytes = filesToBytes(new String[] {"../dumped/source/lib"});

      bytesToFiles(fileToBytes, "../dumped/dest/lib/", true);

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static boolean contains(Object[] array, Object object) {
    boolean contains = false;

    if (array != null && object != null) {
      contains = Arrays.asList(array).contains(object);
    }

    return contains;
  }

  /**
   * Returns a set of all the members of the distributed system excluding locators.
   */
  @SuppressWarnings("unchecked")
  public static Set<DistributedMember> getAllNormalMembers(Cache cache) {
    return new HashSet<DistributedMember>(((InternalDistributedSystem)cache.getDistributedSystem()).getDistributionManager().getNormalDistributionManagerIds());
  }

  /**
   * Returns a set of all the members of the distributed system including locators.
   */
  @SuppressWarnings("unchecked")
  public static Set<DistributedMember> getAllMembers(Cache cache) {
    return new HashSet<DistributedMember>(((InternalDistributedSystem)cache.getDistributedSystem()).getDistributionManager().getDistributionManagerIds());
  }

  @SuppressWarnings("unchecked")
  public static Set<DistributedMember> getAllMembers(InternalDistributedSystem internalDS) {
    return new HashSet<DistributedMember>(internalDS.getDistributionManager().getDistributionManagerIds());
  }

  /**
   * Returns a set of all the members of the distributed system for the given groups.
   */
  public static Set<DistributedMember> getDistributedMembersByGroup(Cache cache,  String[] groups) {
    Set<DistributedMember> groupMembers = new HashSet<DistributedMember>();
    for(String group : groups){
      groupMembers.addAll(((InternalDistributedSystem)cache.getDistributedSystem()).getDistributionManager().getGroupMembers(group));
    }
    return groupMembers;
  }

  /***
   * Executes a function with arguments on a set of members , ignores the departed members.
   * @param function Function to be executed.
   * @param args Arguments passed to the function, pass null if you wish to pass no arguments to the function.
   * @param targetMembers Set of members on which the function is to be executed.
   *
   * @return ResultCollector
   */
  public static ResultCollector<?, ?> executeFunction(final Function function, Object args , final Set<DistributedMember> targetMembers) {
    Execution execution = null;

    if (args != null) {
      execution = FunctionService.onMembers(targetMembers).withArgs(args);
    } else {
      execution = FunctionService.onMembers(targetMembers);
    }

    ((AbstractExecution) execution).setIgnoreDepartedMembers(true);
    return execution.execute(function);
  }

  /***
   * Executes a function with arguments on a member , ignores the departed member.
   * @param function Function to be executed
   * @param args Arguments passed to the function, pass null if you wish to pass no arguments to the function.
   * @param targetMember Member on which the function is to be executed.
   * @return ResultCollector
   */
  public static ResultCollector<?, ?> executeFunction(final Function function, Object args , final DistributedMember targetMember) {
    Execution execution = null;

    if (args != null) {
      execution = FunctionService.onMember(targetMember).withArgs(args);
    } else {
      execution = FunctionService.onMember(targetMember);
    }

    ((AbstractExecution) execution).setIgnoreDepartedMembers(true);
    return execution.execute(function);
  }

  /**
   * Returns a Set of DistributedMember for members that have the specified <code>region</code>.
   * <code>returnAll</code> indicates whether to return all members or only the first member we find.
   *
   * @param region region path for which members that have this region are required
   * @param cache Cache instance to use to find members
   * @param returnAll whether to return all members or only the first member we find. Returns all when <code>true</code>
   * @return a Set of DistributedMember for members that have the specified <code>region</code>.
   */
  public static Set<DistributedMember> getRegionAssociatedMembers(final String region, final Cache cache, boolean returnAll) {
    if (region == null || region.isEmpty()) {
      return null;
    }

    ManagementService managementService = ManagementService.getExistingManagementService(cache);
    DistributedSystemMXBean distributedSystemMXBean = managementService.getDistributedSystemMXBean();
    Set<DistributedMember> matchedMembers = new HashSet<DistributedMember>();

    Set<DistributedMember> allClusterMembers = new HashSet<DistributedMember>();
    allClusterMembers.addAll(cache.getMembers());
    allClusterMembers.add(cache.getDistributedSystem().getDistributedMember());

    for (DistributedMember member : allClusterMembers) {
      try {
        if (distributedSystemMXBean.fetchRegionObjectName(CliUtil.getMemberNameOrId(member), region) != null) {
          matchedMembers.add(member);
        }
      } catch (Exception e) {
        //ignore for now
      }
    }
    return matchedMembers;
  }

  public static String getMemberNameOrId(DistributedMember distributedMember) {
    String nameOrId = null;
    if (distributedMember != null) {
      nameOrId = distributedMember.getName();
      nameOrId = nameOrId != null && !nameOrId.isEmpty() ? nameOrId : distributedMember.getId();
    }
    return nameOrId ;
  }

  public static String collectionToString(Collection<?> col, int newlineAfter) {
    if (col != null) {
      StringBuilder builder = new StringBuilder();
      int lastNewlineAt = 0;

      for (Iterator<?> it = col.iterator(); it.hasNext();) {
        Object object = (Object) it.next();
        builder.append(String.valueOf(object));
        if (it.hasNext()) {
          builder.append(", ");
        }
        if (newlineAfter > 0 && (builder.length() - lastNewlineAt) / newlineAfter >= 1) {
          builder.append(GfshParser.LINE_SEPARATOR);
        }
      }
      return builder.toString();
    } else {
      return "" + null;
    }
  }

  public static <T>String arrayToString(T[] array) {
    if (array != null) {
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < array.length; i++) {
        Object object = (Object) array[i];
        builder.append(String.valueOf(object));
        if (i < array.length - 1) {
          builder.append(", ");
        }
      }
      return builder.toString();
    } else {
      return "" + null;
    }
  }

  public static String decodeWithDefaultCharSet(String urlToDecode) {
    try {
      return URLDecoder.decode(urlToDecode, Charset.defaultCharset().name());
    } catch (UnsupportedEncodingException e) {
      return urlToDecode;
    }
  }

  /**
   * Resolves file system path relative to Gfsh. If the pathname is not specified, then pathname is returned.
   * <p/>
   *
   * @param pathname
   *          a String value specifying the file system pathname to resolve.
   * @return a String specifying a path relative to Gfsh.
   */
  // Moved form LauncherLifeCycleCommands
  public static String resolvePathname(final String pathname) {
    return (StringUtils.isBlank(pathname) ? pathname : IOUtils.tryGetCanonicalPathElseGetAbsolutePath(new File(pathname)));
  }
  
  
  public static void runLessCommandAsExternalViewer(Result commandResult, boolean isError){
    
    StringBuilder sb = new StringBuilder();
    String NEW_LINE = System.getProperty("line.separator");    
    
    while (commandResult.hasNextLine()) {
      sb.append(commandResult.nextLine()).append(NEW_LINE);
    }
    
    File file = null;
    FileWriter fw = null;
    try {
      file = File.createTempFile("gfsh_output", "less");      
      fw = new FileWriter(file);
      fw.append(sb.toString());
      fw.close();
      File workingDir = file.getParentFile();
      Process p = Runtime.getRuntime().exec(
          new String[] {
              "sh",
              "-c",
              "LESSOPEN=\"|color %s\" less -SR " + file.getName()
                  + " < /dev/tty > /dev/tty " }, null, workingDir);
      p.waitFor();      
    } catch (IOException e) {
      Gfsh.printlnErr(e.getMessage());
    } catch (InterruptedException e) {
      Gfsh.printlnErr(e.getMessage());     
    } finally {
      if(file!=null)
        file.delete();
    }    
    
  }
  
    public static String getClientIdFromCacheClientProxy(CacheClientProxy p){
      if(p == null){
        return null;
      }
      StringBuffer buffer = new StringBuffer();
      buffer.append("[").append(p.getProxyID()).append(":port=").append(p.getRemotePort()).append(":primary=")
          .append(p.isPrimary()).append("]");
      return buffer.toString();
  }
 
  public static Set<DistributedMember> getMembersForeRegionViaFunction(
      Cache cache, String regionPath) {
    try {
      Set<DistributedMember> regionMembers = new HashSet<DistributedMember>();
      MembersForRegionFunction membersForRegionFunction = new MembersForRegionFunction();
      FunctionService.registerFunction(membersForRegionFunction);
      Set<DistributedMember> targetMembers = CliUtil.getAllMembers(cache);
      List<?> resultList = (List<?>) CliUtil.executeFunction(membersForRegionFunction, regionPath, targetMembers).getResult();

      for (Object object : resultList) {
        try {
          if (object instanceof Exception) {
            LogWrapper.getInstance().warning("Exception in getMembersForeRegionViaFunction " + ((Throwable) object).getMessage(), ((Throwable) object));
            continue;
          } else if (object instanceof Throwable) {
            LogWrapper.getInstance().warning( "Exception in getMembersForeRegionViaFunction "+ ((Throwable) object).getMessage(), ((Throwable) object));
            continue;
          }
          if (object != null) {
            Map<String, String> memberDetails = (Map<String, String>) object;
            Iterator<Entry<String, String>> it = memberDetails.entrySet().iterator();
            while (it.hasNext()) {
              Entry<String, String> entry = it.next();
              Set<DistributedMember> dsMems = CliUtil.getAllMembers(cache);
              for (DistributedMember mem : dsMems) {
                if (mem.getId().equals(entry.getKey())) {
                  regionMembers.add(mem);
                }
              }
            }
          }
        } catch (Exception ex) {
          LogWrapper.getInstance().warning("getMembersForeRegionViaFunction exception " + ex);
          continue;
        }
      }
      return regionMembers;
    } catch (Exception e) {
      LogWrapper.getInstance().warning("getMembersForeRegionViaFunction exception " + e);
      return null;
    }
  }
  
}

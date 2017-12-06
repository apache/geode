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
package org.apache.geode.management.internal.cli;

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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.apache.commons.lang.StringUtils;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.exceptions.UserErrorException;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.shell.Gfsh;

/**
 * This class contains utility methods used by classes used to build the Command Line Interface
 * (CLI).
 *
 * @since GemFire 7.0
 */
public class CliUtil {
  public static final FileFilter JAR_FILE_FILTER = new CustomFileFilter(".jar");

  public static String cliDependenciesExist(boolean includeGfshDependencies) {
    String jarProductName;

    // Parser & CliCommand from Spring Shell
    jarProductName =
        checkLibraryByLoadingClass("org.springframework.shell.core.Parser", "Spring Shell");
    jarProductName = checkLibraryByLoadingClass(
        "org.springframework.shell.core.annotation.CliCommand", "Spring Shell");
    if (jarProductName != null) {
      return jarProductName;
    }

    // SpringVersion from Spring Core
    jarProductName =
        checkLibraryByLoadingClass("org.springframework.core.SpringVersion", "Spring Core");
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

  public static InternalCache getCacheIfExists() {
    InternalCache cache;
    try {
      cache = getInternalCache();
    } catch (CacheClosedException e) {
      // ignore & return null
      cache = null;
    }

    return cache;
  }

  public static byte[][] filesToBytes(String[] fileNames) throws IOException {
    List<byte[]> filesDataList = new ArrayList<>();

    for (String fileName : fileNames) {
      File file = new File(fileName);

      if (!file.exists()) {
        throw new FileNotFoundException("Could not find " + file.getCanonicalPath());
      }

      if (file.isDirectory()) {
        File[] childrenFiles = file.listFiles(JAR_FILE_FILTER);
        for (File childrenFile : childrenFiles) {
          // 1. add name of the file as bytes at even index
          filesDataList.add(childrenFile.getName().getBytes());
          // 2. add file contents as bytes at odd index
          filesDataList.add(toByteArray(new FileInputStream(childrenFile)));
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
      throws IOException, UnsupportedOperationException {
    FileOutputStream fos = null;

    File parentDir = new File(parentDirPath);
    if (mkRequireddirs && !parentDir.exists()) {
      if (!parentDir.mkdirs()) {
        throw new UnsupportedOperationException(
            "Couldn't create required directory structure for " + parentDirPath);
      }
    }
    for (int i = 0; i < fileData.length; i++) {
      if (i % 2 == 0) {
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

  private static InternalCache getInternalCache() {
    return (InternalCache) CacheFactory.getAnyInstance();
  }

  public static Set<String> getAllRegionNames() {
    InternalCache cache = getInternalCache();
    Set<String> regionNames = new HashSet<>();
    Set<Region<?, ?>> rootRegions = cache.rootRegions();

    for (Region<?, ?> rootRegion : rootRegions) {
      regionNames.add(rootRegion.getFullPath().substring(1));

      Set<Region<?, ?>> subRegions = rootRegion.subregions(true);

      for (Region<?, ?> subRegion : subRegions) {
        regionNames.add(subRegion.getFullPath().substring(1));
      }
    }
    return regionNames;
  }

  public static String convertStringSetToString(Set<String> stringSet, char delimiter) {
    StringBuilder sb = new StringBuilder();
    if (stringSet != null) {

      for (String stringValue : stringSet) {
        sb.append(stringValue);
        sb.append(delimiter);
      }
    }
    return sb.toString();
  }

  public static String convertStringListToString(List<String> stringList, char delimiter) {
    StringBuilder sb = new StringBuilder();
    if (stringList != null) {

      for (String stringValue : stringList) {
        sb.append(stringValue);
        sb.append(delimiter);
      }
    }
    return sb.toString();
  }

  /**
   * Finds all Members (including both servers and locators) which belong to the given arrays of
   * groups or members.
   */
  public static Set<DistributedMember> findMembersIncludingLocators(String[] groups,
      String[] members) {
    InternalCache cache = getInternalCache();
    Set<DistributedMember> allMembers = getAllMembers(cache);

    return findMembers(allMembers, groups, members);
  }

  /**
   * Finds all Servers which belong to the given arrays of groups or members. Does not include
   * locators.
   */
  public static Set<DistributedMember> findMembers(String[] groups, String[] members) {
    InternalCache cache = getInternalCache();
    Set<DistributedMember> allNormalMembers = getAllNormalMembers(cache);

    return findMembers(allNormalMembers, groups, members);
  }

  private static Set<DistributedMember> findMembers(Set<DistributedMember> membersToConsider,
      String[] groups, String[] members) {
    if (groups == null) {
      groups = new String[] {};
    }

    if (members == null) {
      members = new String[] {};
    }

    if ((members.length > 0) && (groups.length > 0)) {
      throw new UserErrorException(CliStrings.PROVIDE_EITHER_MEMBER_OR_GROUP_MESSAGE);
    }

    if (members.length == 0 && groups.length == 0) {
      return membersToConsider;
    }

    Set<DistributedMember> matchingMembers = new HashSet<>();
    // it will either go into this loop or the following loop, not both.
    for (String memberNameOrId : members) {
      for (DistributedMember member : membersToConsider) {
        if (memberNameOrId.equalsIgnoreCase(member.getId())
            || memberNameOrId.equalsIgnoreCase(member.getName())) {
          matchingMembers.add(member);
        }
      }
    }

    for (String group : groups) {
      for (DistributedMember member : membersToConsider) {
        if (member.getGroups().contains(group)) {
          matchingMembers.add(member);
        }
      }
    }
    return matchingMembers;
  }

  public static DistributedMember getDistributedMemberByNameOrId(String memberNameOrId) {
    DistributedMember memberFound = null;

    if (memberNameOrId != null) {
      InternalCache cache = getInternalCache();
      Set<DistributedMember> memberSet = CliUtil.getAllMembers(cache);
      for (DistributedMember member : memberSet) {
        if (memberNameOrId.equalsIgnoreCase(member.getId())
            || memberNameOrId.equalsIgnoreCase(member.getName())) {
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
      throw new RuntimeException(
          CliStrings.format(CliStrings.CREATE_REGION__MSG__COULD_NOT_FIND_CLASS_0_SPECIFIED_FOR_1,
              classToLoadName, neededFor),
          e);
    } catch (ClassCastException e) {
      throw new RuntimeException(CliStrings.format(
          CliStrings.CREATE_REGION__MSG__CLASS_SPECIFIED_FOR_0_SPECIFIED_FOR_1_IS_NOT_OF_EXPECTED_TYPE,
          classToLoadName, neededFor), e);
    }

    return loadedClass;
  }

  public static <K> K newInstance(Class<K> klass, String neededFor) {
    K instance;
    try {
      instance = klass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(CliStrings.format(
          CliStrings.CREATE_REGION__MSG__COULD_NOT_INSTANTIATE_CLASS_0_SPECIFIED_FOR_1, klass,
          neededFor), e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(
          CliStrings.format(CliStrings.CREATE_REGION__MSG__COULD_NOT_ACCESS_CLASS_0_SPECIFIED_FOR_1,
              klass, neededFor),
          e);
    }

    return instance;
  }

  public static Result getFunctionResult(ResultCollector<?, ?> rc, String commandName) {
    Result result;
    List<Object> results = (List<Object>) rc.getResult();
    if (results != null) {
      Object resultObj = results.get(0);
      if (resultObj instanceof String) {
        result = ResultBuilder.createInfoResult((String) resultObj);
      } else if (resultObj instanceof Exception) {
        result = ResultBuilder.createGemFireErrorResult(((Exception) resultObj).getMessage());
      } else {
        result = ResultBuilder.createGemFireErrorResult(
            CliStrings.format(CliStrings.COMMAND_FAILURE_MESSAGE, commandName));
      }
    } else {
      result = ResultBuilder.createGemFireErrorResult(
          CliStrings.format(CliStrings.COMMAND_FAILURE_MESSAGE, commandName));
    }
    return result;
  }

  public static Set<DistributedMember> getMembersWithAsyncEventQueue(InternalCache cache,
      String queueId) {
    Set<DistributedMember> members = findMembers(null, null);
    return members.stream().filter(m -> getAsyncEventQueueIds(cache, m).contains(queueId))
        .collect(Collectors.toSet());
  }

  public static Set<String> getAsyncEventQueueIds(InternalCache cache, DistributedMember member) {
    SystemManagementService managementService =
        (SystemManagementService) ManagementService.getExistingManagementService(cache);
    return managementService.getAsyncEventQueueMBeanNames(member).stream()
        .map(x -> x.getKeyProperty("queue")).collect(Collectors.toSet());
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
      System.arraycopy(buffer, 0, newResult, result.length, buffer.length);
      result = newResult;
    } while (compressedDataLength != 0);
    return new DeflaterInflaterData(totalCompressedDataLength, result);
  }

  public static DeflaterInflaterData uncompressBytes(byte[] output, int compressedDataLength)
      throws DataFormatException {
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
    decompresser.end();

    return new DeflaterInflaterData(result.length, result);
  }

  public static class DeflaterInflaterData implements Serializable {
    private static final long serialVersionUID = 1104813333595216795L;

    private final int dataLength;
    private final byte[] data;

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

  public static boolean contains(Object[] array, Object object) {
    boolean contains = false;

    if (array != null && object != null) {
      contains = Arrays.asList(array).contains(object);
    }

    return contains;
  }

  /**
   * Returns a set of all the members of the distributed system excluding locators.
   *
   * @param cache
   */
  @SuppressWarnings("unchecked")
  public static Set<DistributedMember> getAllNormalMembers(InternalCache cache) {
    return new HashSet<DistributedMember>(cache.getInternalDistributedSystem()
        .getDistributionManager().getNormalDistributionManagerIds());
  }

  /**
   * Returns a set of all the members of the distributed system including locators.
   *
   * @param cache
   */
  @SuppressWarnings("unchecked")
  public static Set<DistributedMember> getAllMembers(InternalCache cache) {
    return getAllMembers(cache.getInternalDistributedSystem());
  }

  @SuppressWarnings("unchecked")
  public static Set<DistributedMember> getAllMembers(InternalDistributedSystem internalDS) {
    return new HashSet<DistributedMember>(
        internalDS.getDistributionManager().getDistributionManagerIds());
  }

  /***
   * Executes a function with arguments on a set of members , ignores the departed members.
   *
   * @param function Function to be executed.
   * @param args Arguments passed to the function, pass null if you wish to pass no arguments to the
   *        function.
   * @param targetMembers Set of members on which the function is to be executed.
   *
   * @return ResultCollector
   */
  public static ResultCollector<?, ?> executeFunction(final Function function, Object args,
      final Set<DistributedMember> targetMembers) {
    Execution execution;

    if (args != null) {
      execution = FunctionService.onMembers(targetMembers).setArguments(args);
    } else {
      execution = FunctionService.onMembers(targetMembers);
    }

    ((AbstractExecution) execution).setIgnoreDepartedMembers(true);
    return execution.execute(function);
  }

  /**
   * Returns a Set of DistributedMember for members that have the specified <code>region</code>.
   * <code>returnAll</code> indicates whether to return all members or only the first member we
   * find.
   *
   * @param region region path for which members that have this region are required
   * @param cache cache instance to use to find members
   * @param returnAll if true, returns all matching members, else returns only first one found.
   * @return a Set of DistributedMember for members that have the specified <code>region</code>.
   */
  public static Set<DistributedMember> getRegionAssociatedMembers(String region,
      final InternalCache cache, boolean returnAll) {
    if (region == null || region.isEmpty()) {
      return Collections.emptySet();
    }

    if (!region.startsWith(Region.SEPARATOR)) {
      region = Region.SEPARATOR + region;
    }

    DistributedRegionMXBean regionMXBean =
        ManagementService.getManagementService(cache).getDistributedRegionMXBean(region);

    if (regionMXBean == null) {
      return Collections.emptySet();
    }

    String[] regionAssociatedMemberNames = regionMXBean.getMembers();
    Set<DistributedMember> matchedMembers = new HashSet<>();
    Set<DistributedMember> allClusterMembers = new HashSet<>();
    allClusterMembers.addAll(cache.getMembers());
    allClusterMembers.add(cache.getDistributedSystem().getDistributedMember());

    for (DistributedMember member : allClusterMembers) {
      for (String regionAssociatedMemberName : regionAssociatedMemberNames) {
        String name = MBeanJMXAdapter.getMemberNameOrId(member);
        if (name.equals(regionAssociatedMemberName)) {
          matchedMembers.add(member);
          if (!returnAll) {
            return matchedMembers;
          }
        }
      }
    }
    return matchedMembers;
  }

  /**
   * this finds the member that hosts all the regions passed in.
   *
   * @param regions
   * @param cache
   * @param returnAll if true, returns all matching members, otherwise, returns only one.
   */
  public static Set<DistributedMember> getQueryRegionsAssociatedMembers(Set<String> regions,
      final InternalCache cache, boolean returnAll) {
    Set<DistributedMember> results = regions.stream()
        .map(region -> getRegionAssociatedMembers(region, cache, true)).reduce((s1, s2) -> {
          s1.retainAll(s2);
          return s1;
        }).get();

    if (returnAll || results.size() <= 1) {
      return results;
    }

    // returns a set of only one item
    return Collections.singleton(results.iterator().next());
  }

  public static String getMemberNameOrId(DistributedMember distributedMember) {
    String nameOrId = null;
    if (distributedMember != null) {
      nameOrId = distributedMember.getName();
      nameOrId = nameOrId != null && !nameOrId.isEmpty() ? nameOrId : distributedMember.getId();
    }
    return nameOrId;
  }

  public static <T> String arrayToString(T[] array) {
    if (array == null) {
      return "null";
    }
    return Arrays.stream(array).map(String::valueOf).collect(Collectors.joining(", "));
  }

  public static String decodeWithDefaultCharSet(String urlToDecode) {
    try {
      return URLDecoder.decode(urlToDecode, Charset.defaultCharset().name());
    } catch (UnsupportedEncodingException e) {
      return urlToDecode;
    }
  }

  /**
   * Resolves file system path relative to Gfsh. If the pathname is not specified, then pathname is
   * returned.
   *
   * @param pathname a String value specifying the file system pathname to resolve.
   * @return a String specifying a path relative to Gfsh.
   */
  public static String resolvePathname(final String pathname) {
    return (StringUtils.isBlank(pathname) ? pathname
        : IOUtils.tryGetCanonicalPathElseGetAbsolutePath(new File(pathname)));
  }

  public static void runLessCommandAsExternalViewer(Result commandResult, boolean isError) {
    StringBuilder sb = new StringBuilder();
    String NEW_LINE = System.getProperty("line.separator");

    while (commandResult.hasNextLine()) {
      sb.append(commandResult.nextLine()).append(NEW_LINE);
    }

    File file = null;
    FileWriter fw;
    try {
      file = File.createTempFile("gfsh_output", "less");
      fw = new FileWriter(file);
      fw.append(sb.toString());
      fw.close();
      File workingDir = file.getParentFile();
      Process p = Runtime.getRuntime().exec(
          new String[] {"sh", "-c",
              "LESSOPEN=\"|color %s\" less -SR " + file.getName() + " < /dev/tty > /dev/tty "},
          null, workingDir);
      p.waitFor();
    } catch (IOException | InterruptedException e) {
      Gfsh.printlnErr(e.getMessage());
    } finally {
      if (file != null)
        file.delete();
    }
  }

  public static String getClientIdFromCacheClientProxy(CacheClientProxy p) {
    if (p == null) {
      return null;
    }
    StringBuffer buffer = new StringBuffer();
    buffer.append("[").append(p.getProxyID()).append(":port=").append(p.getRemotePort())
        .append(":primary=").append(p.isPrimary()).append("]");
    return buffer.toString();
  }

}

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
package org.apache.geode.management.internal.util;

import static org.apache.geode.cache.Region.SEPARATOR;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.classloader.ClassPathLoader;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.exceptions.UserErrorException;
import org.apache.geode.management.internal.i18n.CliStrings;

public class ManagementUtils {

  private static final Logger logger = LogService.getLogger();

  @Immutable
  public static final FileFilter JAR_FILE_FILTER = new CustomFileFilter(".jar");

  /**
   * Returns a set of all the members of the distributed system excluding locators.
   */
  public static Set<DistributedMember> getAllNormalMembers(InternalCache cache) {
    return new HashSet<>(
        cache.getDistributionManager().getNormalDistributionManagerIds());
  }

  public static Set<DistributedMember> getAllLocators(InternalCache cache) {
    return new HashSet<>(
        cache.getDistributionManager().getLocatorDistributionManagerIds());
  }

  /**
   * Returns a set of all the members of the distributed system of a specific version excluding
   * locators.
   */
  public static Set<DistributedMember> getNormalMembersWithSameOrNewerVersion(InternalCache cache,
      KnownVersion version) {
    return getAllNormalMembers(cache).stream().filter(
        member -> ((InternalDistributedMember) member).getVersion()
            .compareTo(version) >= 0)
        .collect(Collectors.toSet());
  }

  /**
   * Returns a set of all the members of the distributed system including locators.
   */
  public static Set<DistributedMember> getAllMembers(InternalCache cache) {
    return getAllMembers(cache.getInternalDistributedSystem());
  }

  public static Set<DistributedMember> getAllMembers(InternalDistributedSystem internalDS) {
    return new HashSet<>(
        internalDS.getDistributionManager().getDistributionManagerIds());
  }

  /***
   * Executes a function with arguments on a set of members, ignoring the departed members.
   *
   * @param function Function to be executed.
   * @param args Arguments passed to the function, pass null if you wish to pass no arguments to the
   *        function.
   * @param targetMembers Set of members on which the function is to be executed.
   *
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
          MessageFormat.format(CliStrings.ERROR__MSG__COULD_NOT_FIND_CLASS_0_SPECIFIED_FOR_1,
              classToLoadName, neededFor),
          e);
    } catch (ClassCastException e) {
      throw new RuntimeException(MessageFormat.format(
          CliStrings.ERROR__MSG__CLASS_0_SPECIFIED_FOR_1_IS_NOT_OF_EXPECTED_TYPE,
          classToLoadName, neededFor), e);
    }

    return loadedClass;
  }

  public static DistributedMember getDistributedMemberByNameOrId(String memberNameOrId,
      InternalCache cache) {
    if (memberNameOrId == null) {
      return null;
    }

    Set<DistributedMember> memberSet = getAllMembers(cache);
    return memberSet.stream().filter(member -> memberNameOrId.equalsIgnoreCase(member.getId())
        || memberNameOrId.equalsIgnoreCase(member.getName())).findFirst().orElse(null);
  }

  public static Set<String> getAllRegionNames(Cache cache) {
    Set<String> regionNames = new HashSet<>();
    Set<Region<?, ?>> rootRegions = cache.rootRegions();

    for (Region<?, ?> rootRegion : rootRegions) {
      try {
        Set<Region<?, ?>> subRegions = rootRegion.subregions(true);

        for (Region<?, ?> subRegion : subRegions) {
          regionNames.add(subRegion.getFullPath().substring(1));
        }

      } catch (Exception e) {
        logger.debug("Cannot get subregions of " + rootRegion.getFullPath()
            + ". Omitting from the set of region names.", e);
        continue;
      }

      regionNames.add(rootRegion.getFullPath().substring(1));
    }

    return regionNames;
  }

  public static List<String> bytesToFiles(Byte[][] fileData, String parentDirPath)
      throws IOException, UnsupportedOperationException {
    List<String> filesPaths = new ArrayList<>();
    FileOutputStream fos = null;
    File file = null;

    File parentDir = new File(parentDirPath);
    if (!parentDir.exists() && !parentDir.mkdirs()) {
      throw new UnsupportedOperationException(
          "Couldn't create required directory structure for " + parentDirPath);
    }
    for (int i = 0; i < fileData.length; i++) {
      byte[] bytes = ArrayUtils.toPrimitive(fileData[i]);
      if (i % 2 == 0) {
        // Expect file name as bytes at even index
        String fileName = new String(bytes);
        file = new File(parentDir, fileName);
        fos = new FileOutputStream(file);
      } else {
        // Expect file contents as bytes at odd index
        fos.write(bytes);
        fos.close();
        filesPaths.add(file.getAbsolutePath());
      }
    }
    return filesPaths;
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

    if (!region.startsWith(SEPARATOR)) {
      region = SEPARATOR + region;
    }

    DistributedRegionMXBean regionMXBean =
        ManagementService.getManagementService(cache).getDistributedRegionMXBean(region);

    if (regionMXBean == null) {
      return Collections.emptySet();
    }

    String[] regionAssociatedMemberNames = regionMXBean.getMembers();
    Set<DistributedMember> matchedMembers = new HashSet<>();
    Set<DistributedMember> allClusterMembers = new HashSet<>(cache.getMembers());
    allClusterMembers.add(cache.getDistributedSystem().getDistributedMember());

    for (DistributedMember member : allClusterMembers) {
      List<String> regionAssociatedMemberNamesList = Arrays.asList(regionAssociatedMemberNames);
      String name = MBeanJMXAdapter.getMemberNameOrUniqueId(member);
      if (regionAssociatedMemberNamesList.contains(name)) {
        matchedMembers.add(member);
        if (!returnAll) {
          return matchedMembers;
        }
      }
    }
    return matchedMembers;
  }

  /**
   * this finds the member that hosts all the regions passed in.
   *
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

  public static Set<DistributedMember> findMembers(Set<DistributedMember> membersToConsider,
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

  /**
   * Finds all Members (including both servers and locators) which belong to the given arrays of
   * groups or members.
   */
  public static Set<DistributedMember> findMembersIncludingLocators(String[] groups,
      String[] members, InternalCache cache) {
    Set<DistributedMember> allMembers = getAllMembers(cache);
    return findMembers(allMembers, groups, members);
  }

  /**
   * Finds all Servers which belong to the given arrays of groups or members. Does not include
   * locators.
   */
  public static Set<DistributedMember> findMembers(String[] groups, String[] members,
      InternalCache cache) {
    Set<DistributedMember> allNormalMembers = getAllNormalMembers(cache);

    return findMembers(allNormalMembers, groups, members);
  }

  /**
   * Finds all Servers which belong to the given arrays of groups or members. Does not include
   * locators.
   */
  public static Set<DistributedMember> findMembers(String[] groups, String[] members,
      DistributionManager distributionManager) {
    Set<DistributedMember> allNormalMembers = new HashSet<>(
        distributionManager.getNormalDistributionManagerIds());

    return findMembers(allNormalMembers, groups, members);
  }

  /**
   * Even thought this is only used in a test, caller of MemberMXBean.processCommand(String, Map,
   * Byte[][]) will need to use this method to convert a fileList to Byte[][] to call that
   * deprecated API.
   *
   * Once that deprecated API is removed, we can delete this method and the tests.
   */
  public static Byte[][] filesToBytes(List<String> fileNames) throws IOException {
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

    return filesDataList.stream().map(ArrayUtils::toObject).toArray(Byte[][]::new);
  }

  public static byte[] toByteArray(InputStream input) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    int n;
    byte[] buffer = new byte[4096];
    while (-1 != (n = input.read(buffer))) {
      output.write(buffer, 0, n);
    }

    return output.toByteArray();
  }

  static class CustomFileFilter implements FileFilter {
    private final String extensionWithDot;

    public CustomFileFilter(String extensionWithDot) {
      this.extensionWithDot = extensionWithDot;
    }

    @Override
    public boolean accept(File pathname) {
      String name = pathname.getName();
      return name.endsWith(extensionWithDot);
    }
  }
}

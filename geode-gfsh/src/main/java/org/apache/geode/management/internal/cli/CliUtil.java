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

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.configuration.domain.DeclarableTypeInstantiator;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.util.ManagementUtils;

/**
 * This class contains utility methods used by classes used to build the Command Line Interface
 * (CLI).
 *
 * @since GemFire 7.0
 */
public class CliUtil {

  public static String cliDependenciesExist(boolean includeGfshDependencies) {
    // "Validate" each dependency by attempting to load an associated class
    Map<String, String> classLibraryMap = new HashMap<>();
    classLibraryMap.put("org.springframework.shell.core.Parser", "Spring Shell");
    classLibraryMap.put("org.springframework.shell.core.annotation.CliCommand", "Spring Shell");
    classLibraryMap.put("org.springframework.core.SpringVersion", "Spring Core");
    if (includeGfshDependencies) {
      classLibraryMap.put("jline.console.ConsoleReader", "JLine");
    }

    List<String> unloadableJars =
        classLibraryMap.entrySet().stream().filter(entry -> !canLoadClass(entry.getKey()))
            .map(Map.Entry::getValue).collect(Collectors.toList());
    return unloadableJars.isEmpty() ? null : String.join(",", unloadableJars);
  }

  private static boolean canLoadClass(String className) {
    try {
      ClassPathLoader.getLatest().forName(className);
    } catch (ClassNotFoundException e) {
      return false;
    }
    return true;
  }

  public static String getMemberNameOrId(DistributedMember distributedMember) {
    String nameOrId = null;
    if (distributedMember != null) {
      nameOrId = distributedMember.getName();
      nameOrId = nameOrId != null && !nameOrId.isEmpty() ? nameOrId : distributedMember.getId();
    }
    return nameOrId;
  }

  public static Set<DistributedMember> getMembersWithAsyncEventQueue(InternalCache cache,
      String queueId) {
    Set<DistributedMember> members = ManagementUtils.findMembers(null, null, cache);
    return members.stream().filter(m -> getAsyncEventQueueIds(cache, m).contains(queueId))
        .collect(Collectors.toSet());
  }

  public static Set<String> getAsyncEventQueueIds(InternalCache cache, DistributedMember member) {
    SystemManagementService managementService =
        (SystemManagementService) ManagementService.getExistingManagementService(cache);
    return managementService.getAsyncEventQueueMBeanNames(member).stream()
        .map(x -> x.getKeyProperty("queue")).collect(Collectors.toSet());
  }


  public static DeflaterInflaterData compressBytes(byte[] input) {
    Deflater compresser = new Deflater();
    compresser.setInput(input);
    compresser.finish();
    byte[] buffer = new byte[100];
    byte[] result = new byte[0];
    int compressedDataLength;
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

  public static String decodeWithDefaultCharSet(String urlToDecode) {
    try {
      return URLDecoder.decode(urlToDecode, Charset.defaultCharset().name());
    } catch (UnsupportedEncodingException e) {
      return urlToDecode;
    }
  }

  /**
   * @deprecated use {@link DeclarableTypeInstantiator}
   */
  @Deprecated
  public static <K> K newInstance(Class<K> klass, String neededFor) {
    K instance;
    try {
      instance = klass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(CliStrings.format(
          CliStrings.ERROR__MSG__COULD_NOT_INSTANTIATE_CLASS_0_SPECIFIED_FOR_1, klass,
          neededFor), e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(
          CliStrings.format(CliStrings.ERROR__MSG__COULD_NOT_ACCESS_CLASS_0_SPECIFIED_FOR_1,
              klass, neededFor),
          e);
    }

    return instance;
  }

  /**
   * Resolves file system path relative to Gfsh. If the pathname is not specified, then pathname is
   * returned.
   *
   * @param pathname a String value specifying the file system pathname to resolve.
   * @return a String specifying a path relative to Gfsh.
   */
  public static String resolvePathname(final String pathname) {
    return (isBlank(pathname) ? pathname
        : IOUtils.tryGetCanonicalPathElseGetAbsolutePath(new File(pathname)));
  }


  public static void runLessCommandAsExternalViewer(Result commandResult) {
    StringBuilder sb = new StringBuilder();
    String NEW_LINE = System.getProperty("line.separator");

    while (commandResult.hasNextLine()) {
      sb.append(commandResult.nextLine()).append(NEW_LINE);
    }
    commandResult.resetToFirstLine();

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
      Gfsh.println(e.getMessage());
    } finally {
      if (file != null)
        file.delete();
    }
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
}

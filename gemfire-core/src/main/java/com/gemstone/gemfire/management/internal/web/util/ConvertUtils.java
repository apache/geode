/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.web.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.util.IOUtils;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.web.io.MultipartFileResourceAdapter;

import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.web.multipart.MultipartFile;

/**
 * The ConvertUtils class is a support class for performing conversions used by the GemFire web application
 * and REST interface.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.CliUtil
 * @since 8.0
 */
@SuppressWarnings("unused")
public abstract class ConvertUtils {

  /**
   * Converts the 2-dimensional byte array of file data, which includes the name of the file as bytes followed by
   * the byte content of the file, for all files being transmitted by Gfsh to the GemFire Manager.
   * <p/>
   * @param fileData a 2 dimensional byte array of files names and file content.
   * @return an array of Spring Resource objects encapsulating the details (name and content) of each file being
   * transmitted by Gfsh to the GemFire Manager.
   * @see org.springframework.core.io.ByteArrayResource
   * @see org.springframework.core.io.Resource
   * @see com.gemstone.gemfire.management.internal.cli.CliUtil#bytesToData(byte[][])
   * @see com.gemstone.gemfire.management.internal.cli.CliUtil#bytesToNames(byte[][])
   */
  public static Resource[] convert(final byte[][] fileData) {
    if (fileData != null) {
      final String[] fileNames = CliUtil.bytesToNames(fileData);
      final byte[][] fileContent = CliUtil.bytesToData(fileData);

      final List<Resource> resources = new ArrayList<Resource>(fileNames.length);

      for (int index = 0; index < fileNames.length; index++) {
        final String filename = fileNames[index];
        resources.add(new ByteArrayResource(fileContent[index], String.format("Contents of JAR file (%1$s).", filename)) {
          @Override
          public String getFilename() {
            return filename;
          }
        });
      }

      return resources.toArray(new Resource[resources.size()]);
    }

    return new Resource[0];
  }

  /**
   * Converts the array of MultipartFiles into a 2-dimensional byte array containing content from each MultipartFile.
   * The 2-dimensional byte array format is used by Gfsh and the GemFire Manager to transmit file data.
   * <p/>
   * @param files an array of Spring MultipartFile objects to convert into the 2-dimensional byte array format.
   * @return a 2-dimensional byte array containing the content of each MultipartFile.
   * @throws IOException if an I/O error occurs reading the contents of a MultipartFile.
   * @see #convert(org.springframework.core.io.Resource...)
   * @see org.springframework.web.multipart.MultipartFile
   */
  public static byte[][] convert(final MultipartFile... files) throws IOException {
    if (files != null) {
      final List<Resource> resources = new ArrayList<Resource>(files.length);

      for (final MultipartFile file : files) {
        resources.add(new MultipartFileResourceAdapter(file));
      }

      return convert(resources.toArray(new Resource[resources.size()]));
    }

    return new byte[0][];
  }

  /**
   * Converts the array of Resources into a 2-dimensional byte array containing content from each Resource.
   * The 2-dimensional byte array format is used by Gfsh and the GemFire Manager to transmit file data.
   * <p/>
   * @param resources an array of Spring Resource objects to convert into the 2-dimensional byte array format.
   * @return a 2-dimensional byte array containing the content of each Resource.
   * @throws IllegalArgumentException if the filename of a Resource was not specified.
   * @throws IOException if an I/O error occurs reading the contents of a Resource!
   * @see org.springframework.core.io.Resource
   */
  public static byte[][] convert(final Resource... resources) throws IOException {
    if (resources != null) {
      final List<byte[]> fileData = new ArrayList<byte[]>(resources.length * 2);

      for (final Resource resource : resources) {
        if (StringUtils.isBlank(resource.getFilename())) {
          throw new IllegalArgumentException(String.format("The filename of Resource (%1$s) must be specified!",
            resource.getDescription()));
        }

        fileData.add(resource.getFilename().getBytes());
        fileData.add(IOUtils.toByteArray(resource.getInputStream()));
      }

      return fileData.toArray(new byte[fileData.size()][]);
    }

    return new byte[0][];
  }

}

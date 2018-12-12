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

import java.io.File;
import java.net.URL;
import java.net.URLDecoder;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.pdx.internal.unsafe.UnsafeWrapper;

/**
 * The class puts in one place the code that will determine the name of the GemFire shared library.
 * This aids debugging.
 */
public class SharedLibrary {

  /**
   * A suffix added on to distinguish between the Linux and Solaris library names since they reside
   * in the same directory.
   */
  private static final String SOLARIS_LIBRARY_SUFFIX = "_sol";

  private static final boolean is64Bit;
  private static final int referenceSize;
  private static final int objectHeaderSize;

  private static final UnsafeWrapper unsafe;
  static {
    UnsafeWrapper tmp = null;
    try {
      tmp = new UnsafeWrapper();
    } catch (RuntimeException ignore) {
    } catch (Error ignore) {
    }
    unsafe = tmp;
  }

  static {
    int sunbits = Integer.getInteger("sun.arch.data.model", 0).intValue(); // also used by JRockit
    if (sunbits == 64) {
      is64Bit = true;
    } else if (sunbits == 32) {
      is64Bit = false;
    } else {
      int ibmbits = Integer.getInteger("com.ibm.vm.bitmode", 0).intValue();
      if (ibmbits == 64) {
        is64Bit = true;
      } else if (ibmbits == 32) {
        is64Bit = false;
      } else {
        if (unsafe != null) {
          is64Bit = unsafe.getAddressSize() == 8;
        } else {
          is64Bit = false;
        }
      }
    }
    if (!is64Bit) {
      referenceSize = 4;
      objectHeaderSize = 8;
    } else {
      int scaleIndex = 0;
      int tmpReferenceSize = 0;
      int tmpObjectHeaderSize = 0;
      if (SystemUtils.isAzulJVM()) {
        tmpObjectHeaderSize = 8;
        tmpReferenceSize = 8;
      } else {
        if (unsafe != null) {
          // Use unsafe to figure out the size of an object reference since we might
          // be using compressed oops.
          // Note: as of java 8 compressed oops do not imply a compressed object header.
          // The object header is determined by UseCompressedClassPointers.
          // UseCompressedClassPointers requires UseCompressedOops
          // but UseCompressedOops does not require UseCompressedClassPointers.
          // But it seems unlikely that someone would compress their oops
          // not their class pointers.
          scaleIndex = unsafe.arrayScaleIndex(Object[].class);
          if (scaleIndex == 4) {
            // compressed oops
            tmpReferenceSize = 4;
            tmpObjectHeaderSize = 12;
          } else if (scaleIndex == 8) {
            tmpReferenceSize = 8;
            tmpObjectHeaderSize = 16;
          } else {
            System.out.println("Unexpected arrayScaleIndex " + scaleIndex
                + ". Using max heap size to estimate reference size.");
            scaleIndex = 0;
          }
        }
        if (scaleIndex == 0) {
          // If our heap is > 32G (64G on java 8) then assume large oops. Otherwise assume
          // compressed oops.
          long SMALL_OOP_BOUNDARY = 32L;
          if (org.apache.commons.lang.SystemUtils.isJavaVersionAtLeast(1.8f)) {
            SMALL_OOP_BOUNDARY = 64L;
          }
          if (Runtime.getRuntime().maxMemory() > (SMALL_OOP_BOUNDARY * 1024 * 1024 * 1024)) {
            tmpReferenceSize = 8;
            tmpObjectHeaderSize = 16;
          } else {
            tmpReferenceSize = 4;
            tmpObjectHeaderSize = 12;
          }
        }
      }
      referenceSize = tmpReferenceSize;
      objectHeaderSize = tmpObjectHeaderSize;
    }
  }

  /**
   * @return true if this process is 64bit
   * @throws RuntimeException if sun.arch.data.model doesn't fit expectations
   */
  public static boolean is64Bit() {
    return is64Bit;
  }

  /**
   * @return true if this process is running on Solaris, no effort is made to distinguish between
   *         sparc and x86, the library will simply fail to load.
   * @throws RuntimeException if sun.arch.data.model doesn't fit expectations
   */
  public static boolean isSolaris() {
    String osName = System.getProperty("os.name");
    return osName.equals("SunOS");
  }

  /**
   * Returns the os specific name of the GemFire shared library.
   */
  public static String getName() {
    StringBuffer result = new StringBuffer("gemfire");
    if (isSolaris()) {
      result.append(SOLARIS_LIBRARY_SUFFIX);
    }
    if (is64Bit()) {
      result.append("64");
    }
    if (Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "debug")) {
      result.append("_g");
    }
    return result.toString();
  }

  public static void loadLibrary(boolean debug) throws UnsatisfiedLinkError {
    String library = getName();
    try {
      URL gemfireJarURL = GemFireVersion.getJarURL();

      if (gemfireJarURL == null) {
        throw new InternalGemFireError("Unable to locate jar file.");
      }

      String gemfireJar = null;
      try {
        gemfireJar = URLDecoder.decode(gemfireJarURL.getFile(), "UTF-8");
      } catch (java.io.UnsupportedEncodingException uee) {
        // This should never happen because UTF-8 is required to be implemented
        throw new RuntimeException(uee);
      }
      int index = gemfireJar.lastIndexOf("/");
      if (index == -1) {
        throw new InternalGemFireError("Unable to parse gemfire jar path.");
      }
      String libDir = gemfireJar.substring(0, index + 1);
      File libraryPath = new File(libDir, System.mapLibraryName(library));
      if (libraryPath.exists()) {
        System.load(libraryPath.getPath());
        return;
      }
    } catch (InternalGemFireError ige) {
      /**
       * Unable to make a guess as to where the gemfire native library is based on its position
       * relative to gemfire jar.
       */
      if (debug) {
        System.out.println("Problem loading library from URL path: " + ige);
      }
    } catch (UnsatisfiedLinkError ule) {
      /**
       * Unable to load the gemfire native library in the product tree, This is very unexpected and
       * should not happen. Reattempting using System.loadLibrary
       */
      if (debug) {
        System.out.println("Problem loading library from URL path: " + ule);
      }
    }
    System.loadLibrary(library);
  }

  /**
   * Returns the size in bytes of a C pointer in this shared library, returns 4 for a 32 bit shared
   * library, and 8 for a 64 bit shared library . This method makes a native call, so you can't use
   * it to determine which library to load .
   *
   */
  public static int pointerSizeBytes() {
    return SmHelper.pointerSizeBytes();
  }

  /**
   * Accessor method for the is64Bit flag
   *
   * @return returns a boolean indicating if the 64bit native library was loaded.
   * @since GemFire 5.1
   */
  public static boolean getIs64Bit() {
    return PureJavaMode.is64Bit();
  }

  public static int getReferenceSize() {
    return referenceSize;
  }

  public static int getObjectHeaderSize() {
    return objectHeaderSize;
  }
}

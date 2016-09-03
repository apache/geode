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

import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.TreeMap;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * This class provides build and version information about GemFire.
 * It gathers this information from the resource property file
 * for this class.
 */
public class GemFireVersion {
  private static String RESOURCE_NAME = "GemFireVersion.properties";

  /** The singleton instance */
  private static VersionDescription description;

  private GemFireVersion() {
  }

  private static synchronized VersionDescription getDescription() {
    if (description == null) {
      String name = GemFireVersion.class.getPackage().getName()
          .replace('.', '/') + "/" + RESOURCE_NAME;
      description = new VersionDescription(name);
    }
    return description;
  }
  
  public static void main(String[] args) {
    System.out.println(asString());
  }

  public static String getProductName() {
    return getDescription().getProperty(VersionDescription.PRODUCT_NAME);
  }

  public static String getGemFireVersion() {
    return getDescription().getProperty(VersionDescription.GEMFIRE_VERSION);
  }

  public static String getSourceDate() {
    return getDescription().getProperty(VersionDescription.SOURCE_DATE);
  }

  public static String getSourceRepository() {
    return getDescription().getProperty(VersionDescription.SOURCE_REPOSITORY);
  }

  public static String getSourceRevision() {
    return getDescription().getProperty(VersionDescription.SOURCE_REVISION);
  }

  public static String getBuildId() {
    return getDescription().getProperty(VersionDescription.BUILD_ID);
  }

  public static String getBuildDate() {
    return getDescription().getProperty(VersionDescription.BUILD_DATE);
  }

  public static String getBuildPlatform() {
    return getDescription().getProperty(VersionDescription.BUILD_PLATFORM);
  }

  public static String getBuildJavaVersion() {
    return getDescription().getProperty(VersionDescription.BUILD_JAVA_VERSION);
  }

  public static String getGemFireJarFileName() {
    return "geode-core-" + GemFireVersion.getGemFireVersion() +".jar";
  }

  public static void print(PrintWriter pw) {
    getDescription().print(pw);
  }
  
  public static void print(PrintStream ps) {
    print(new PrintWriter(ps, true));
  }

  public static String asString() {
    StringWriter sw = new StringWriter(256);
    PrintWriter pw = new PrintWriter(sw);
    print(pw);
    pw.flush();
    return sw.toString();
  }

  /** Public method that returns the URL of the gemfire jar file */
  public static URL getJarURL() {
    java.security.CodeSource cs = 
      GemFireVersion.class.getProtectionDomain().getCodeSource();
    if (cs != null) {
      return cs.getLocation();
    }
    // fix for bug 33274 - null CodeSource from protection domain in Sybase
    URL csLoc = null;
    StringTokenizer tokenizer = new StringTokenizer(System.getProperty("java.class.path"), File.pathSeparator);
    while(tokenizer.hasMoreTokens()) {
      String jar = tokenizer.nextToken();
      if (jar.indexOf(getGemFireJarFileName()) != -1) {
        File gemfireJar = new File(jar);
        try {
          csLoc = gemfireJar.toURL();
        } catch (Exception e) {}
        break;
      }
    }
    if (csLoc != null) {
      return csLoc;
    }
    // try the boot class path to fix bug 37394
    tokenizer = new StringTokenizer(System.getProperty("sun.boot.class.path"), File.pathSeparator);
    while(tokenizer.hasMoreTokens()) {
      String jar = tokenizer.nextToken();
      if (jar.indexOf(getGemFireJarFileName()) != -1) {
        File gemfireJar = new File(jar);
        try {
          csLoc = gemfireJar.toURL();
        } catch (Exception e) {}
        break;
      }
    }
    return csLoc;
  }
  
  static class VersionDescription {
    /** Constant for the GemFire version Resource Property entry */
    static final String PRODUCT_NAME = "Product-Name";

    /** Constant for the GemFire version Resource Property entry */
    static final String GEMFIRE_VERSION = "Product-Version";

    /** Constant for the source code date Resource Property entry */
    static final String SOURCE_DATE = "Source-Date";

    /** Constant for the source code revision Resource Property entry */
    static final String SOURCE_REVISION = "Source-Revision";

    /** Constant for the source code repository Resource Property entry */
    static final String SOURCE_REPOSITORY = "Source-Repository";

    /** Constant for the build date Resource Property entry */
    static final String BUILD_DATE = "Build-Date";

    /** Constant for the build id Resource Property entry */
    static final String BUILD_ID = "Build-Id";

    /** Constant for the build Java version Resource Property entry */
    static final String BUILD_PLATFORM = "Build-Platform";

    /** Constant for the build Java version Resource Property entry */
    static final String BUILD_JAVA_VERSION = "Build-Java-Version";

    /** the version properties */
    private final Properties description;

    /** Error message to display instead of the version information */
    private final Optional<String> error;

    public VersionDescription(String name) {
      InputStream is = ClassPathLoader.getLatest().getResourceAsStream(getClass(), name);
      if (is == null) {
        error = Optional.of(LocalizedStrings.GemFireVersion_COULD_NOT_FIND_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_0.toLocalizedString(name));
        description = null;
        return;
      }

      description = new Properties();
      try {
        description.load(is);
      } catch (Exception ex) {
        error = Optional.of(LocalizedStrings.GemFireVersion_COULD_NOT_READ_PROPERTIES_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_0_BECAUSE_1.toLocalizedString(new Object[] {name, ex}));
        return;
      }

      error = validate(description);
    }

    public String getProperty(String key) {
      return error.orElse(description.getProperty(key));
    }
    
    public String getNativeCodeVersion() {
      return SmHelper.getNativeVersion();
    }
    
    void print(PrintWriter pw) {
      if (error.isPresent()) {
        pw.println(error.get());
      } else {
        for (Entry<?,?> props : new TreeMap<>(description).entrySet()) {
          pw.println(props.getKey() + ": " + props.getValue());
        }
      }

      // not stored in the description map
      pw.println("Native version: " + getNativeCodeVersion());
      printHostInfo(pw);
    }

    private void printHostInfo(PrintWriter pw)
        throws InternalGemFireError, Error, VirtualMachineError {
      try {
        StringBuffer sb = new StringBuffer(SocketCreator.getLocalHost().toString())
            .append(", ")
            .append(Runtime.getRuntime().availableProcessors()).append(" cpu(s), ")
            .append(System.getProperty("os.arch")).append(' ')
            .append(System.getProperty("os.name")).append(' ')
            .append(System.getProperty("os.version")).append(' ');
        pw.println(LocalizedStrings.GemFireVersion_RUNNING_ON_0.toLocalizedString(sb.toString()));
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
      }
    }

    private Optional<String> validate(Properties props) {
      if (props.get(PRODUCT_NAME) == null) {
        return Optional.of(LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {PRODUCT_NAME, RESOURCE_NAME}));
      }

      if (props.get(GEMFIRE_VERSION) == null) {
        return Optional.of(LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {GEMFIRE_VERSION, RESOURCE_NAME}));
      }

      if (props.get(SOURCE_DATE) == null) {
        return Optional.of(LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {SOURCE_DATE, RESOURCE_NAME}));
      }

      if (props.get(SOURCE_REVISION) == null) {
        return Optional.of(LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {SOURCE_REVISION, RESOURCE_NAME}));
      }

      if (props.get(SOURCE_REPOSITORY) == null) {
        return Optional.of(LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {SOURCE_REPOSITORY, RESOURCE_NAME}));
      }

      if (props.get(BUILD_DATE) == null) {
        return Optional.of(LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {BUILD_DATE, RESOURCE_NAME}));
      }

      if (props.get(BUILD_ID) == null) {
        return Optional.of(LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {BUILD_ID, RESOURCE_NAME}));
      }

      if (props.get(BUILD_PLATFORM) == null) {
        return Optional.of(LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {BUILD_PLATFORM, RESOURCE_NAME}));
      }

      if (props.get(BUILD_JAVA_VERSION) == null) {
        return Optional.of(LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {BUILD_JAVA_VERSION, RESOURCE_NAME}));
      }
      return Optional.empty();
    }
  }
}

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

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.net.SocketCreator;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class provides build and version information about GemFire.
 * It gathers this information from the resource property file
 * for this class.
 */
public class GemFireVersion {

  private static String RESOURCE_NAME = "GemFireVersion.properties";
  
  /** The singleton instance */
  private static GemFireVersion instance;

  /** Constant for the GemFire version Resource Property entry */
  private static final String PRODUCT_NAME = "Product-Name";

  /** Constant for the GemFire version Resource Property entry */
  private static final String GEMFIRE_VERSION = "Product-Version";

  /** Constant for the source code date Resource Property entry */
  private static final String SOURCE_DATE = "Source-Date";

  /** Constant for the source code revision Resource Property entry */
  private static final String SOURCE_REVISION = "Source-Revision";

  /** Constant for the source code repository Resource Property entry */
  private static final String SOURCE_REPOSITORY = "Source-Repository";

  /** Constant for the build date Resource Property entry */
  private static final String BUILD_DATE = "Build-Date";

  /** Constant for the build id Resource Property entry */
  private static final String BUILD_ID = "Build-Id";

  /** Constant for the build Java version Resource Property entry */
  private static final String BUILD_PLATFORM = "Build-Platform";

  /** Constant for the build Java version Resource Property entry */
  private static final String BUILD_JAVA_VERSION = "Build-Java-Version";

  ////////////////////  Instance Fields  ////////////////////

  /** Error message to display instead of the version information */
  private String error = null;

  /** The name of this product */
  private String productName;

  /** This product's version */
  private String gemfireVersion;

  /** The version of GemFire native code library */
  private String nativeVersion;

  /** The date that the source code for GemFire was last updated */  
  private String sourceDate;

  /** The revision of the source code used to build GemFire */  
  private String sourceRevision;

  /** The repository in which the source code for GemFire resides */  
  private String sourceRepository;

  /** The date on which GemFire was built */
  private String buildDate;

  /** The ID of the GemFire build */
  private String buildId;

  /** The platform on which GemFire was built */
  private String buildPlatform;

  /** The version of Java that was used to build GemFire */
  private String buildJavaVersion;

  ////////////////////  Static Methods  ////////////////////

  /**
   * Returns (or creates) the singleton instance of this class
   */
  private static GemFireVersion getInstance() {
    if (instance == null) {
      instance = new GemFireVersion();
    }

    return instance;
  }

  /**
   * Returns the name of this product
   */
  public static String getProductName() {
    GemFireVersion v = getInstance();
    if (v.error != null) {
      return v.error;

    } else {
      return v.productName;
    }
  }

  /**
   * Returns the version of GemFire being used
   */
  public static String getGemFireVersion() {
    GemFireVersion v = getInstance();
    if (v.error != null) {
      return v.error;

    } else {
      return v.gemfireVersion;
    }
  }

  private static String stripSpaces(String s) {
    StringBuffer result = new StringBuffer(s);
    while (result.charAt(0) == ' ') {
      result.deleteCharAt(0);
    }
    while (result.charAt(result.length()-1) == ' ') {
      result.deleteCharAt(result.length()-1);
    }
    return result.toString();
  }
  
  /**
   * Returns the version of GemFire native code library being used
   */
  public static String getNativeCodeVersion() {
    GemFireVersion v = getInstance();
    if (v.error != null) {
      return v.error;

    } else {
      return v.nativeVersion;
    }
  }
  public static String getJavaCodeVersion() {
    GemFireVersion v = getInstance();
    if (v.error != null) {
      return v.error;

    } else {
      StringBuffer result = new StringBuffer(80);
      result.append(GemFireVersion.getGemFireVersion())
        .append(' ')
        .append(GemFireVersion.getBuildId())
        .append(' ')
        .append(GemFireVersion.getBuildDate())
        .append(" javac ")
        .append(GemFireVersion.getBuildJavaVersion());
      return result.toString();
    }
  }

  /**
   * Returns the date of the source code from which GemFire was built
   */
  public static String getSourceDate() {
    GemFireVersion v = getInstance();
    if (v.error != null) {
      return v.error;

    } else {
      return v.sourceDate;
    }
  }

  /**
   * Returns the revision of the source code on which GemFire was
   * built.
   *
   * @since GemFire 4.0
   */
  public static String getSourceRevision() {
    GemFireVersion v = getInstance();
    if (v.error != null) {
      return v.error;

    } else {
      return v.sourceRevision;
    }
  }

  /**
   * Returns the source code repository from which GemFire was built.
   *
   * @since GemFire 4.0
   */
  public static String getSourceRepository() {
    GemFireVersion v = getInstance();
    if (v.error != null) {
      return v.error;

    } else {
      return v.sourceRepository;
    }
  }

  /**
   * Returns the date on which GemFire was built
   */
  public static String getBuildDate() {
    GemFireVersion v = getInstance();
    if (v.error != null) {
      return v.error;

    } else {
      return v.buildDate;
    }
  }

  /**
   * Returns the id of the GemFire build
   */
  public static String getBuildId() {
    GemFireVersion v = getInstance();
    if (v.error != null) {
      return v.error;

    } else {
      return v.buildId;
    }
  }

  /**
   * Returns the platform on which GemFire was built
   */
  public static String getBuildPlatform() {
    GemFireVersion v = getInstance();
    if (v.error != null) {
      return v.error;

    } else {
      return v.buildPlatform;
    }
  }

  /**
   * Returns the version of Java used to build GemFire
   */
  public static String getBuildJavaVersion() {
    GemFireVersion v = getInstance();
    if (v.error != null) {
      return v.error;

    } else {
      return v.buildJavaVersion;
    }
  }

  ////////////////////  Constructors  ////////////////////

  /**
   * Private constructor that read the resource properties
   * and extracts interesting pieces of information from it
   */
  private GemFireVersion() {
    String name =
      GemFireVersion.class.getPackage().getName().replace('.', '/');
    name = name + "/" + RESOURCE_NAME;

    InputStream is = ClassPathLoader.getLatest().getResourceAsStream(getClass(), name);
    if (is == null) {
      error = LocalizedStrings.GemFireVersion_COULD_NOT_FIND_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_0.toLocalizedString(RESOURCE_NAME);
      return;
    }
    Properties props = new Properties();
    try {
      props.load(is);
    } catch (Exception ex) {
      error = LocalizedStrings.GemFireVersion_COULD_NOT_READ_PROPERTIES_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_0_BECAUSE_1.toLocalizedString(new Object[] {RESOURCE_NAME, ex});
      return;
    }

    this.nativeVersion = SmHelper.getNativeVersion();
    this.productName = props.getProperty(PRODUCT_NAME);
    if (this.productName == null) {
      error = LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {PRODUCT_NAME, RESOURCE_NAME});
      return;
    }
    this.gemfireVersion = props.getProperty(GEMFIRE_VERSION);
    if (this.gemfireVersion == null) {
      error = LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {GEMFIRE_VERSION, RESOURCE_NAME});
      return;
    }
    this.sourceDate = props.getProperty(SOURCE_DATE);
    if (this.sourceDate == null) {
      error = LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {SOURCE_DATE, RESOURCE_NAME});
      return;
    }
    this.sourceRevision = props.getProperty(SOURCE_REVISION);
    if (this.sourceRevision == null) {
      error = LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {SOURCE_REVISION, RESOURCE_NAME});
      return;
    }
    this.sourceRepository = props.getProperty(SOURCE_REPOSITORY);
    if (this.sourceRepository == null) {
      error = LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {SOURCE_REPOSITORY, RESOURCE_NAME});
      return;
    }
    this.buildDate = props.getProperty(BUILD_DATE);
    if (this.buildDate == null) {
      error = LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {BUILD_DATE, RESOURCE_NAME});
      return;
    }
    this.buildId = props.getProperty(BUILD_ID);
    if (this.buildId == null) {
      error = LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {BUILD_ID, RESOURCE_NAME});
      return;
    }
    this.buildPlatform = props.getProperty(BUILD_PLATFORM);
    if (this.buildPlatform == null) {
      error = LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {BUILD_PLATFORM, RESOURCE_NAME});
      return;
    }
    this.buildJavaVersion = props.getProperty(BUILD_JAVA_VERSION);
    if (this.buildJavaVersion == null) {
      error = LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {BUILD_JAVA_VERSION, RESOURCE_NAME});
      return;
    }
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
  
  public static String getGemFireJarFileName() {
    return "geode-core-" + GemFireVersion.getGemFireVersion() +".jar";
  }

  private final static String VER_FILE_NAME = "GemFireVersion.properties";
  private final static String JAR_VER_NAME = "gemfire-core-jar";

    public static void createVersionFile() {
        String jarVersion = stripSpaces(GemFireVersion.getJavaCodeVersion());
        File libDir = SystemAdmin.findGemFireLibDir();
        if (libDir == null) {
          throw new RuntimeException(LocalizedStrings.GemFireVersion_COULD_NOT_DETERMINE_PRODUCT_LIB_DIRECTORY.toLocalizedString());
        }
        File versionPropFile = new File(libDir, VER_FILE_NAME);
        Properties props = new Properties();
        props.setProperty(JAR_VER_NAME, jarVersion);
        try {
          FileOutputStream out = new FileOutputStream(versionPropFile);
          props.store(out, "Expected versions for this product build as of");
          out.close();
        } catch (IOException ex) {
          throw new RuntimeException(LocalizedStrings.GemFireVersion_COULD_NOT_WRITE_0_BECAUSE_1.toLocalizedString(new Object[] {versionPropFile, ex.toString()}));
        }
        System.out.println("Created \"" + versionPropFile + "\"");
    }
  /**
   * Encodes all available version information into a string and then
   * returns that string.
   */
  public static String asString() {
    StringWriter sw = new StringWriter(256);
    PrintWriter pw = new PrintWriter(sw);
    print(pw);
    pw.flush();
    return sw.toString();
  }
    /**
     * Prints all available version information (excluding source code
     * information) to the given <code>PrintWriter</code> in a
     * standard format.
     */
    public static void print(PrintWriter pw) {
      print(pw, true);
    }

    /**
     * Prints all available version information to the given
     * <code>PrintWriter</code> in a standard format.
     *
     * @param printSourceInfo
     *        Should information about the source code be printed?
     */
    public static void print(PrintWriter pw, 
                             boolean printSourceInfo) {
        String jarVersion = stripSpaces(GemFireVersion.getJavaCodeVersion());
        pw.println("Java version:   " + jarVersion);
        String libVersion = stripSpaces(GemFireVersion.getNativeCodeVersion());
        pw.println("Native version: " + libVersion);
        File libDir = SystemAdmin.findGemFireLibDir();
        if (libDir != null) {
          File versionPropFile = new File(libDir, VER_FILE_NAME);
          if (versionPropFile.exists()) {
            try {
              Properties props = new Properties();
              FileInputStream inStream = new FileInputStream(versionPropFile);
              try {
                props.load(inStream);
              }
              finally {
                inStream.close();
              }
              String expectedJarVersion = props.getProperty(JAR_VER_NAME);
              if (expectedJarVersion != null) {
                if (!expectedJarVersion.equals(jarVersion)) {
                  pw.println(LocalizedStrings.GemFireVersion_WARNING_EXPECTED_JAVA_VERSION_0.toLocalizedString(expectedJarVersion));
                }
              }
            } catch (IOException ex) {
              pw.println(LocalizedStrings.GemFireVersion_WARNING_FAILED_TO_READ_0_BECAUSE_1.toLocalizedString(new Object[] {versionPropFile, ex}));
            }
//           } else {
//             pw.println(LocalizedStrings.GemFireVersion_WARNING_COULD_NOT_FIND_0.toLocalizedString(versionPropFile));
          }
//         } else {
//           pw.println(LocalizedStrings.GemFireVersion_WARNING_COULD_NOT_DETERMINE_THE_PRODUCTS_LIB_DIRECTORY.toLocalizedString());
        }
	
        if (printSourceInfo) {
          String sourceRevision = GemFireVersion.getSourceRevision();
          pw.println("Source revision: " + sourceRevision);

          String sourceRepository =
            GemFireVersion.getSourceRepository();
          pw.println("Source repository: " + sourceRepository);
        }

	InetAddress host = null;
	try {
	    host = SocketCreator.getLocalHost();
	} 
	catch (VirtualMachineError err) {
	   SystemFailure.initiateFailure(err);
	   // If this ever returns, rethrow the error.  We're poisoned
	   // now, so don't let this thread continue.
	   throw err;
	}
	catch (Throwable t) {
	     // Whenever you catch Error or Throwable, you must also
	     // catch VirtualMachineError (see above).  However, there is
	     // _still_ a possibility that you are dealing with a cascading
	     // error condition, so you also need to check to see if the JVM
	     // is still usable:
	     SystemFailure.checkFailure();
	}
        int cpuCount = Runtime.getRuntime().availableProcessors();
        pw.println(LocalizedStrings.GemFireVersion_RUNNING_ON_0.toLocalizedString(
                   host
                   + ", " + cpuCount + " cpu(s)"
                   + ", " + System.getProperty("os.arch")
                   + " " + System.getProperty("os.name")
                   + " " + System.getProperty("os.version")
                   ));
    }
	
    /**
     * Prints all available version information (excluding information
     * about the source code) to the given <code>PrintStream</code> in
     * a standard format.
     */
    public static void print(PrintStream ps) {
	print(ps, true);
    }

    /**
     * Prints all available version information to the given
     * <code>PrintStream</code> in a standard format.
     *
     * @param printSourceInfo
     *        Should information about the source code be printed?
     */
    public static void print(PrintStream ps,
                             boolean printSourceInfo) {
	PrintWriter pw = new PrintWriter(ps);
	print(pw, printSourceInfo);
	pw.flush();
    }
    
  ///////////////////////  Main Program  ////////////////////

//  private static final PrintStream out = System.out;
//  private static final PrintStream err = System.err;

  /**
   * Populates the gemfireVersion.properties file
   */
  public static void main(String[] args) {
      print(System.out);
  }

  private static final Pattern MAJOR_MINOR = Pattern.compile("(\\d+)\\.(\\d*)(.*)");
  private static final Pattern RELEASE = Pattern.compile("\\.(\\d*)(.*)");
  private static final Pattern MAJOR_MINOR_RELEASE = Pattern.compile("(\\d+)\\.(\\d*)\\.(\\d*)(.*)");
  
  public static int getMajorVersion(String v) {
    int majorVersion = 0;
    Matcher m = MAJOR_MINOR.matcher(v);
    if (m.matches()) {
      String digits = m.group(1);
      if (digits != null && digits.length() > 0) {
        majorVersion = Integer.decode(digits).intValue();
      }
    }
    return majorVersion;
  }

  public static int getMinorVersion(String v) {
    int minorVersion = 0;
    Matcher m = MAJOR_MINOR.matcher(v);
    if (m.matches()) {
      String digits = m.group(2);
      if (digits != null && digits.length() > 0) {
        minorVersion = Integer.decode(digits).intValue();
      }
    }
    return minorVersion;
  }

  public static int getRelease(String v) {
    int release = 0;
    Matcher m = MAJOR_MINOR.matcher(v);
    if (m.matches()) {
      String others = m.group(3);
      Matcher r = RELEASE.matcher(others);
      if (r.matches()) {
        String digits = r.group(1);
        if (digits != null && digits.length() > 0) {
          try {
            release = Integer.decode(digits).intValue();
          } catch (NumberFormatException e) {
            release = 0;
          }
        }
      }
    }
    return release;
  }
  
  public static int getBuild(String v) {
    int build = 0;
    Matcher m = MAJOR_MINOR_RELEASE.matcher(v);
    if (m.matches()) {
      String buildStr = m.group(4);
      Matcher b = RELEASE.matcher(buildStr);
      if (b.matches()) {
        String digits = b.group(1);
        if (digits != null && digits.length() > 0) {
          try {
            build = Integer.decode(digits).intValue();
          } catch (NumberFormatException e) {
            build = 0;
          }
        }
      }
    }
    return build;
  }
  
  /** 
   * Compare version's sections major, minor, release one by one
   * 
   * @return >0: v1 is newer than v2
   *          0: same
   *          <0: v1 is older than v2
   * @deprecated please use the {@link Version} class to read the version
   * of the local member and compare versions for backwards compatibility 
   * purposes. see also {@link SerializationVersions} for how to make backwards
   * compatible messages.
   */
  public static int compareVersions(String v1, String v2) {
    return compareVersions(v1, v2, true);
  }
  
  /* 
   * Compare version's sections major, minor, release one by one
   * 
   * @param v1 the first version
   * @param v2 the second version
   * @param includeBuild whether to also compare the build numbers
   * 
   * @return: >0: v1 is newer than v2
   *          0: same
   *          <0: v1 is older than v2
   */
  public static int compareVersions(String v1, String v2, boolean includeBuild) {
    int major1, minor1, release1, build1;
    int major2, minor2, release2, build2;
    
    if (v1 == null && v2 != null) return -1;
    if (v1 != null && v2 == null) return 1;
    if (v1 == null && v2 == null) return 0;
    
    major1 = getMajorVersion(v1);
    major2 = getMajorVersion(v2);
    
    minor1 = getMinorVersion(v1);
    minor2 = getMinorVersion(v2);
    
    release1 = getRelease(v1);
    release2 = getRelease(v2);

    if (major1 > major2) return 1;
    if (major1 < major2) return -1;
    
    if (minor1 > minor2) return 1;
    if (minor1 < minor2) return -1;

    if (release1 > release2) return 1;
    if (release1 < release2) return -1;
    
    if (includeBuild) {
      build1 = getBuild(v1);
      build2 = getBuild(v2);
      
      if (build1 > build2) return 1;
      if (build1 < build2) return -1;
    }

    return 0;
  }
}

/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.internal.tools.gfsh.app;

import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * This class provides build and version information about gfsh.
 * It gathers this information from the resource property file
 * for this class.
 * 
 * @author abhishek
 */
/* This class has most of the code 'copied' from GemFireVersion.java.
 * The differences are:
 * 1. RESOURCE_NAME is "GfshVersion.properties";
 * 2. Uses same error strings as GemFireVersion - only resource name/path differs
 * 3. Methods asString & print accept different argument and have slightly 
 * different behaviour. 
 */
public class GfshVersion {
  protected static String RESOURCE_NAME = "GfshVersion.properties";

  private static final Pattern MAJOR_MINOR = Pattern.compile("(\\d+)\\.(\\d*)(.*)");
  
  /** The singleton instance */
  private static GfshVersion instance;

  /** Constant for the GemFire version Resource Property entry */
  private static final String PRODUCT_NAME = "Product-Name";

  /** Constant for the GemFire version Resource Property entry */
  private static final String PRODUCT_VERSION = "Product-Version";

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
  private String gfshVersion;

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
  
  /* Just to log gfsh jar version in GemFire Server logs */  
  static {
    try {
      Cache cache = CacheFactory.getAnyInstance();
      if (cache != null && cache.isServer() && cache.getLogger() != null) {
        cache.getLogger().config("gfsh version: " + getJavaCodeVersion());
      }
    } catch (CacheClosedException e) {
      // Ignore, this is just to get handle on log writer.
    }
  }
  
  ////////////////////  Constructor  ////////////////////

  /**
   * Private constructor that read the resource properties
   * and extracts interesting pieces of information from it
   */
  private GfshVersion() {
    String gfeVersionPath  = GemFireVersion.class.getPackage().getName().replace('.', '/');    
    String gfshVersionPath = GfshVersion.class.getPackage().getName().replace('.', '/');    
    gfshVersionPath = gfshVersionPath + "/" + RESOURCE_NAME;
    
    String xtraGfshVersionPath = gfshVersionPath.substring(gfeVersionPath.length() + 1);
    
    InputStream is = ClassPathLoader.getLatest().getResourceAsStream(getClass(), gfshVersionPath);
    if (is == null) {
      error = LocalizedStrings.GemFireVersion_COULD_NOT_FIND_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_0.toLocalizedString(xtraGfshVersionPath);
      return;
    }

    Properties props = new Properties();
    try {
      props.load(is);
    } catch (Exception ex) {
      error = LocalizedStrings.GemFireVersion_COULD_NOT_READ_PROPERTIES_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_0_BECAUSE_1.toLocalizedString(new Object[] {xtraGfshVersionPath, ex});
      return;
    }
    
    this.productName = props.getProperty(PRODUCT_NAME);
    if (this.productName == null) {
      error = LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {PRODUCT_NAME, xtraGfshVersionPath});
      return;
    }
    this.gfshVersion = props.getProperty(PRODUCT_VERSION);
    if (this.gfshVersion == null) {
      error = LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {PRODUCT_VERSION, xtraGfshVersionPath});
      return;
    }
    this.sourceDate = props.getProperty(SOURCE_DATE);
    if (this.sourceDate == null) {
      error = LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {SOURCE_DATE, xtraGfshVersionPath});
      return;
    }
    this.sourceRevision = props.getProperty(SOURCE_REVISION);
    if (this.sourceRevision == null) {
      error = LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {SOURCE_REVISION, xtraGfshVersionPath});
      return;
    }
    this.sourceRepository = props.getProperty(SOURCE_REPOSITORY);
    if (this.sourceRepository == null) {
      error = LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {SOURCE_REPOSITORY, xtraGfshVersionPath});
      return;
    }
    this.buildDate = props.getProperty(BUILD_DATE);
    if (this.buildDate == null) {
      error = LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {BUILD_DATE, xtraGfshVersionPath});
      return;
    }
    this.buildId = props.getProperty(BUILD_ID);
    if (this.buildId == null) {
      error = LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {BUILD_ID, xtraGfshVersionPath});
      return;
    }
    this.buildPlatform = props.getProperty(BUILD_PLATFORM);
    if (this.buildPlatform == null) {
      error = LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {BUILD_PLATFORM, xtraGfshVersionPath});
      return;
    }
    this.buildJavaVersion = props.getProperty(BUILD_JAVA_VERSION);
    if (this.buildJavaVersion == null) {
      error = LocalizedStrings.GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1.toLocalizedString(new Object[] {BUILD_JAVA_VERSION, xtraGfshVersionPath});
      return;
    }
  }
  
  ////////////////////  Static Methods  ////////////////////

  /**
   * Returns (or creates) the singleton instance of this class
   */
  private static GfshVersion getInstance() {
    if (instance == null) {
      instance = new GfshVersion();
    }

    return instance;
  }

  /**
   * Returns the name of this product
   */
  public static String getProductName() {
    GfshVersion v = getInstance();
    if (v.error != null) {
      return v.error;

    } else {
      return v.productName;
    }
  }

  /**
   * Returns the version of GemFire being used
   */
  public static String getGfshVersion() {
    GfshVersion v = getInstance();
    if (v.error != null) {
      return v.error;

    } else {
      return v.gfshVersion;
    }
  }

  public static String getJavaCodeVersion() {
    GfshVersion v = getInstance();
    if (v.error != null) {
      return v.error;

    } else {
      StringBuilder result = new StringBuilder(80);
      result.append(GfshVersion.getGfshVersion())
        .append(' ')
        .append(GfshVersion.getBuildId())
        .append(' ')
        .append(GfshVersion.getBuildDate())
        .append(" javac ")
        .append(GfshVersion.getBuildJavaVersion());
      return result.toString();
    }
  }

  /**
   * Returns the date of the source code from which GemFire was built
   */
  public static String getSourceDate() {
    GfshVersion v = getInstance();
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
   */
  public static String getSourceRevision() {
    GfshVersion v = getInstance();
    if (v.error != null) {
      return v.error;

    } else {
      return v.sourceRevision;
    }
  }

  /**
   * Returns the source code repository from which GemFire was built.
   *
   */
  public static String getSourceRepository() {
    GfshVersion v = getInstance();
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
    GfshVersion v = getInstance();
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
    GfshVersion v = getInstance();
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
    GfshVersion v = getInstance();
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
    GfshVersion v = getInstance();
    if (v.error != null) {
      return v.error;

    } else {
      return v.buildJavaVersion;
    }
  }
  
  /**
   * Encodes all available version information into a string and then
   * returns that string.
   * 
   * @param printSourceInfo
   *        Should information about the source code be printed?
   */
  public static String asString(boolean printSourceInfo) {
    StringWriter sw = new StringWriter(256);
    PrintWriter pw = new PrintWriter(sw);
    print(pw, printSourceInfo);
    pw.flush();
    return sw.toString();
  }

  /**
   * Prints all available version information to the given
   * <code>PrintWriter</code> in a standard format.
   * 
   * @param pw
   *          writer to write version info to
   * @param printSourceInfo
   *          Should information about the source code be printed?
   */
  protected static void print(PrintWriter pw, boolean printSourceInfo) {
    String jarVersion = GfshVersion.getJavaCodeVersion().trim();
    pw.println(getProductName() + " version: " + jarVersion);

    /*
     * TODO: GemFireVersion here compares the version info read by
     * GemFireVersion and available in product/lib directory. The 'version
     * CREATE' option to create the gfsh version file is not added currently. As
     * gfsh releases could be different than GemFire release, could we still use
     * product/lib?
     */

    if (printSourceInfo) {
      String sourceRevision = GfshVersion.getSourceRevision();
      pw.println("Source revision: " + sourceRevision);

      String sourceRepository = GfshVersion.getSourceRepository();
      pw.println("Source repository: " + sourceRepository);
    }

    InetAddress host = null;
    try {
      host = SocketCreator.getLocalHost();
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

    int cpuCount = Runtime.getRuntime().availableProcessors();
    pw.println(LocalizedStrings.GemFireVersion_RUNNING_ON_0
        .toLocalizedString(host + ", " + cpuCount + " cpu(s)" + ", "
            + System.getProperty("os.arch") + " "
            + System.getProperty("os.name") + " "
            + System.getProperty("os.version")));

    pw.flush();
  }

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
  
  ///////////////////////  Main Program  ////////////////////

  /**
   * Populates the gemfireVersion.properties file
   */
  public final static void main(String args[]) {    
//    System.out.println("-------------------------------------------------");
//    
//    System.out.println(asString(false));
//    
//    System.out.println("-------------------------------------------------");
//    
    System.out.println(asString(true));    
  }
}

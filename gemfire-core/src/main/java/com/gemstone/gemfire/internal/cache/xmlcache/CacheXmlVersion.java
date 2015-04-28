/**
 * 
 */
package com.gemstone.gemfire.internal.cache.xmlcache;

import java.util.HashMap;

/**
 * {@link Enum} for Cache XML versions. Resolves issues with old String based
 * comparisons. Under the old String comparison version "8.1" was older than
 * "8_0" and "10.0" was older than "9.0".
 *
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
// TODO future - replace constants in CacheXml with this Enum completely
public enum CacheXmlVersion {
  VERSION_3_0(CacheXml.VERSION_3_0, null, CacheXml.PUBLIC_ID_3_0, CacheXml.SYSTEM_ID_3_0),
  VERSION_4_0(CacheXml.VERSION_4_0, null, CacheXml.PUBLIC_ID_4_0, CacheXml.SYSTEM_ID_4_0),
  VERSION_4_1(CacheXml.VERSION_4_1, null, CacheXml.PUBLIC_ID_4_1, CacheXml.SYSTEM_ID_4_1),
  VERSION_5_0(CacheXml.VERSION_5_0, null, CacheXml.PUBLIC_ID_5_0, CacheXml.SYSTEM_ID_5_0),
  VERSION_5_1(CacheXml.VERSION_5_1, null, CacheXml.PUBLIC_ID_5_1, CacheXml.SYSTEM_ID_5_1),
  VERSION_5_5(CacheXml.VERSION_5_5, null, CacheXml.PUBLIC_ID_5_5, CacheXml.SYSTEM_ID_5_5),
  VERSION_5_7(CacheXml.VERSION_5_7, null, CacheXml.PUBLIC_ID_5_7, CacheXml.SYSTEM_ID_5_7),
  VERSION_5_8(CacheXml.VERSION_5_8, null, CacheXml.PUBLIC_ID_5_8, CacheXml.SYSTEM_ID_5_8),
  VERSION_6_0(CacheXml.VERSION_6_0, null, CacheXml.PUBLIC_ID_6_0, CacheXml.SYSTEM_ID_6_0),
  VERSION_6_1(CacheXml.VERSION_6_1, null, CacheXml.PUBLIC_ID_6_1, CacheXml.SYSTEM_ID_6_1),
  VERSION_6_5(CacheXml.VERSION_6_5, null, CacheXml.PUBLIC_ID_6_5, CacheXml.SYSTEM_ID_6_5),
  VERSION_6_6(CacheXml.VERSION_6_6, null, CacheXml.PUBLIC_ID_6_6, CacheXml.SYSTEM_ID_6_6),
  VERSION_7_0(CacheXml.VERSION_7_0, null, CacheXml.PUBLIC_ID_7_0, CacheXml.SYSTEM_ID_7_0),
  VERSION_8_0(CacheXml.VERSION_8_0, null, CacheXml.PUBLIC_ID_8_0, CacheXml.SYSTEM_ID_8_0),
  VERSION_8_1(CacheXml.VERSION_8_1, CacheXml.SCHEMA_8_1_LOCATION, null, null);

  private static final HashMap<String, CacheXmlVersion> valuesForVersion = new HashMap<>();
  static {
    for (final CacheXmlVersion cacheXmlVersion : values()) {
      valuesForVersion.put(cacheXmlVersion.getVersion(), cacheXmlVersion);
    }
  }

  private final String version;
  private final String schemaLocation;
  private final String publicId;
  private final String systemId;

  private CacheXmlVersion(String version, String schemaLocation, String publicId, String systemId) {
    this.version = version;
    this.schemaLocation = schemaLocation;
    this.publicId = publicId;
    this.systemId = systemId;
  }

  /**
   * The version of the DTD or Schema. Prior to 8.1 this value was of the format
   * "8_0". With 8.1 and later the value is of the format "8.1". Values should
   * match constants in {@link CacheXml} like {@link CacheXml#VERSION_8_1}.
   * 
   * @return the version
   * 
   * @since 8.1
   */
  public String getVersion() {
    return version;
  }

  /**
   * The schema file location.
   * 
   * @return the schemaLocation if schema exists, otherwise null.
   * 
   * @since 8.1
   */
  public String getSchemaLocation() {
    return schemaLocation;
  }

  /**
   * The DTD public id.
   * 
   * @return the publicId if DTD exists, otherwise null.
   * @since 8.1
   */
  public String getPublicId() {
    return publicId;
  }

  /**
   * The DTD system id.
   * 
   * @return the systemId if DTD exists, otherwise null.
   * @since 8.1
   */
  public String getSystemId() {
    return systemId;
  }

  /**
   * Get {@link CacheXmlVersion} for given <code>version</code> string. Use
   * constants from {@link CacheXml} for example:
   * <code>CacheXmlVersion.valueForVersion(CacheXml.VERSION_8_1);</code>
   * 
   * @param version
   *          string to lookup.
   * @return {@link CacheXmlVersion} if exists.
   * @throws IllegalArgumentException
   *           if version does not exist.
   * @since 8.1
   */
  public static final CacheXmlVersion valueForVersion(final String version) {
    final CacheXmlVersion cacheXmlVersion = valuesForVersion.get(version);
    if (null == cacheXmlVersion) {
      throw new IllegalArgumentException("No enum constant " + CacheXmlVersion.class.getCanonicalName() + " for version " + version + ".");
    }
    return cacheXmlVersion;
  }

}

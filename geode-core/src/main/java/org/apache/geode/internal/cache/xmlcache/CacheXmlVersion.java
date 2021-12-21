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
package org.apache.geode.internal.cache.xmlcache;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.geode.annotations.Immutable;

/**
 * {@link Enum} for Cache XML versions. Resolves issues with old String based comparisons. Under the
 * old String comparison version "8.1" was older than "8_0" and "10.0" was older than "9.0".
 * <p>
 * TODO future - replace constants in CacheXml with this Enum completely
 *
 * @since GemFire 8.1
 */
public enum CacheXmlVersion {

  GEMFIRE_3_0(CacheXml.VERSION_3_0, CacheXml.PUBLIC_ID_3_0, CacheXml.SYSTEM_ID_3_0, null, null),
  GEMFIRE_4_0(CacheXml.VERSION_4_0, CacheXml.PUBLIC_ID_4_0, CacheXml.SYSTEM_ID_4_0, null, null),
  GEMFIRE_4_1(CacheXml.VERSION_4_1, CacheXml.PUBLIC_ID_4_1, CacheXml.SYSTEM_ID_4_1, null, null),
  GEMFIRE_5_0(CacheXml.VERSION_5_0, CacheXml.PUBLIC_ID_5_0, CacheXml.SYSTEM_ID_5_0, null, null),
  GEMFIRE_5_1(CacheXml.VERSION_5_1, CacheXml.PUBLIC_ID_5_1, CacheXml.SYSTEM_ID_5_1, null, null),
  GEMFIRE_5_5(CacheXml.VERSION_5_5, CacheXml.PUBLIC_ID_5_5, CacheXml.SYSTEM_ID_5_5, null, null),
  GEMFIRE_5_7(CacheXml.VERSION_5_7, CacheXml.PUBLIC_ID_5_7, CacheXml.SYSTEM_ID_5_7, null, null),
  GEMFIRE_5_8(CacheXml.VERSION_5_8, CacheXml.PUBLIC_ID_5_8, CacheXml.SYSTEM_ID_5_8, null, null),
  GEMFIRE_6_0(CacheXml.VERSION_6_0, CacheXml.PUBLIC_ID_6_0, CacheXml.SYSTEM_ID_6_0, null, null),
  GEMFIRE_6_1(CacheXml.VERSION_6_1, CacheXml.PUBLIC_ID_6_1, CacheXml.SYSTEM_ID_6_1, null, null),
  GEMFIRE_6_5(CacheXml.VERSION_6_5, CacheXml.PUBLIC_ID_6_5, CacheXml.SYSTEM_ID_6_5, null, null),
  GEMFIRE_6_6(CacheXml.VERSION_6_6, CacheXml.PUBLIC_ID_6_6, CacheXml.SYSTEM_ID_6_6, null, null),
  GEMFIRE_7_0(CacheXml.VERSION_7_0, CacheXml.PUBLIC_ID_7_0, CacheXml.SYSTEM_ID_7_0, null, null),
  GEMFIRE_8_0(CacheXml.VERSION_8_0, CacheXml.PUBLIC_ID_8_0, CacheXml.SYSTEM_ID_8_0, null, null),
  GEMFIRE_8_1(CacheXml.VERSION_8_1, null, null, CacheXml.SCHEMA_8_1_LOCATION,
      CacheXml.GEMFIRE_NAMESPACE),

  // Ordinality matters here, so keep the 1.0 version after the 8.x versions
  // Version 1.0 is the start of Geode versions. In terms of releases, Geode 1.0 > GemFire 8.x.
  GEODE_1_0(CacheXml.VERSION_1_0, null, null, CacheXml.SCHEMA_1_0_LOCATION,
      CacheXml.GEODE_NAMESPACE);

  @Immutable
  private static final Map<String, CacheXmlVersion> valuesForVersion;
  static {
    HashMap<String, CacheXmlVersion> valuesForVersionMap = new HashMap<>();
    for (final CacheXmlVersion cacheXmlVersion : values()) {
      valuesForVersionMap.put(cacheXmlVersion.getVersion(), cacheXmlVersion);
    }

    valuesForVersion = Collections.unmodifiableMap(valuesForVersionMap);
  }

  private final String version;
  private final String schemaLocation;
  private final String namespace;
  private final String publicId;
  private final String systemId;

  CacheXmlVersion(String version, String publicId, String systemId, String schemaLocation,
      String namespace) {
    this.version = version;
    this.publicId = publicId;
    this.systemId = systemId;
    this.schemaLocation = schemaLocation;
    this.namespace = namespace;
  }

  /**
   * The version of the DTD or Schema. Prior to 8.1 this value was of the format "8_0". With 8.1 and
   * later the value is of the format "8.1". Values should match constants in {@link CacheXml} like
   * {@link CacheXml#VERSION_8_1}.
   *
   * @return the version
   *
   * @since GemFire 8.1
   */
  public String getVersion() {
    return version;
  }

  /**
   * The schema file location.
   *
   * @return the schemaLocation if schema exists, otherwise null.
   *
   * @since GemFire 8.1
   */
  public String getSchemaLocation() {
    return schemaLocation;
  }

  /**
   * The namespace.
   *
   * @return the namespace if schema exists, otherwise null.
   */
  public String getNamespace() {
    return namespace;
  }

  /**
   * The DTD public id.
   *
   * @return the publicId if DTD exists, otherwise null.
   * @since GemFire 8.1
   */
  public String getPublicId() {
    return publicId;
  }

  /**
   * The DTD system id.
   *
   * @return the systemId if DTD exists, otherwise null.
   * @since GemFire 8.1
   */
  public String getSystemId() {
    return systemId;
  }

  /**
   * Get {@link CacheXmlVersion} for given <code>version</code> string. Use constants from
   * {@link CacheXml} for example:
   * <code>CacheXmlVersion.valueForVersion(CacheXml.GEMFIRE_8_1);</code>
   *
   * @param version string to lookup.
   * @return {@link CacheXmlVersion} if exists.
   * @throws IllegalArgumentException if version does not exist.
   * @since GemFire 8.1
   */
  public static CacheXmlVersion valueForVersion(final String version) {
    final CacheXmlVersion cacheXmlVersion = valuesForVersion.get(version);
    if (null == cacheXmlVersion) {
      throw new IllegalArgumentException("No enum constant "
          + CacheXmlVersion.class.getCanonicalName() + " for version " + version + ".");
    }
    return cacheXmlVersion;
  }

}

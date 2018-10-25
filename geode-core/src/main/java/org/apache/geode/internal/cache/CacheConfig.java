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
package org.apache.geode.internal.cache;

import java.util.List;

import org.apache.geode.cache.client.internal.InternalClientCache;
import org.apache.geode.internal.cache.xmlcache.CacheServerCreation;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.SecurityManager;

/**
 * This is helper class used by CacheFactory to pass the cache configuration values to cache
 * creation code.
 *
 * @since GemFire 6.6
 */
public class CacheConfig {
  public static final boolean DEFAULT_PDX_READ_SERIALIZED = false;
  public static final PdxSerializer DEFAULT_PDX_SERIALIZER = null;
  public static final String DEFAULT_PDX_DISK_STORE = null;
  public static final boolean DEFAULT_PDX_PERSISTENT = false;
  public static final boolean DEFAULT_PDX_IGNORE_UNREAD_FIELDS = false;

  private SecurityManager securityManager = null;
  private PostProcessor postProcessor = null;

  public boolean pdxReadSerialized = DEFAULT_PDX_READ_SERIALIZED;

  /**
   * cacheXMLDescription is used to reinitialize the cache after a reconnect. It overrides any
   * cache.xml filename setting in distributed system properties.
   */
  private String cacheXMLDescription = null;

  /**
   * list of cache servers to create after auto-reconnect if cluster configuration is being used
   */
  private List<CacheServerCreation> cacheServerCreation;

  /**
   * This indicates if the pdxReadSerialized value is set by user. This is used during cache xml
   * parsing. The value set by user api overrides the value set in cache.xml value.
   */
  public boolean pdxReadSerializedUserSet = false;

  public PdxSerializer pdxSerializer = DEFAULT_PDX_SERIALIZER;

  public boolean pdxSerializerUserSet = false;

  public String pdxDiskStore = DEFAULT_PDX_DISK_STORE;

  public boolean pdxDiskStoreUserSet = false;

  public boolean pdxPersistent = DEFAULT_PDX_PERSISTENT;

  public boolean pdxPersistentUserSet = false;

  public boolean pdxIgnoreUnreadFields = DEFAULT_PDX_IGNORE_UNREAD_FIELDS;
  public boolean pdxIgnoreUnreadFieldsUserSet = false;



  public boolean isPdxReadSerialized() {
    return pdxReadSerialized;
  }



  public void setPdxReadSerialized(boolean pdxReadSerialized) {
    this.pdxReadSerializedUserSet = true;
    this.pdxReadSerialized = pdxReadSerialized;
  }



  public PdxSerializer getPdxSerializer() {
    return pdxSerializer;
  }

  public SecurityManager getSecurityManager() {
    return securityManager;
  }

  public void setSecurityManager(SecurityManager securityManager) {
    this.securityManager = securityManager;
  }

  public void setPdxSerializer(PdxSerializer pdxSerializer) {
    pdxSerializerUserSet = true;
    this.pdxSerializer = pdxSerializer;
  }

  public PostProcessor getPostProcessor() {
    return postProcessor;
  }

  public void setPostProcessor(PostProcessor postProcessor) {
    this.postProcessor = postProcessor;
  }

  public String getPdxDiskStore() {
    return pdxDiskStore;
  }



  public void setPdxDiskStore(String pdxDiskStore) {
    this.pdxDiskStoreUserSet = true;
    this.pdxDiskStore = pdxDiskStore;
  }



  public boolean isPdxPersistent() {
    return pdxPersistent;
  }



  public void setPdxPersistent(boolean pdxPersistent) {
    this.pdxPersistentUserSet = true;
    this.pdxPersistent = pdxPersistent;
  }

  public boolean getPdxIgnoreUnreadFields() {
    return this.pdxIgnoreUnreadFields;
  }

  public void setPdxIgnoreUnreadFields(boolean ignore) {
    this.pdxIgnoreUnreadFields = ignore;
    this.pdxIgnoreUnreadFieldsUserSet = true;
  }


  public String getCacheXMLDescription() {
    return cacheXMLDescription;
  }



  public void setCacheXMLDescription(String cacheXMLDescription) {
    this.cacheXMLDescription = cacheXMLDescription;
  }


  public List<CacheServerCreation> getCacheServerCreation() {
    return this.cacheServerCreation;
  }


  public void setCacheServerCreation(List<CacheServerCreation> servers) {
    this.cacheServerCreation = servers;
  }

  public void validateCacheConfig(InternalClientCache cacheInstance) {
    // To fix bug 44961 only validate our attributes against the existing cache
    // if they have been explicitly set by the set.
    // So all the following "ifs" check that "*UserSet" is true.
    // If they have not then we might use a cache.xml that will specify them.
    // Since we don't have the cache.xml info here we need to only complain
    // if we are sure that we will be incompatible with the existing cache.
    if (this.pdxReadSerializedUserSet
        && this.pdxReadSerialized != cacheInstance.getPdxReadSerialized()) {
      throw new IllegalStateException(
          String.format("Existing cache has different cache configuration, it has:%s",
              "pdxReadSerialized: " + cacheInstance.getPdxReadSerialized()));
    }
    if (this.pdxDiskStoreUserSet && !equals(this.pdxDiskStore, cacheInstance.getPdxDiskStore())) {
      throw new IllegalStateException(
          String.format("Existing cache has different cache configuration, it has:%s",
              "pdxDiskStore: " + cacheInstance.getPdxDiskStore()));
    }
    if (this.pdxPersistentUserSet && this.pdxPersistent != cacheInstance.getPdxPersistent()) {
      throw new IllegalStateException(
          String.format("Existing cache has different cache configuration, it has:%s",
              "pdxPersistent: " + cacheInstance.getPdxPersistent()));
    }
    if (this.pdxIgnoreUnreadFieldsUserSet
        && this.pdxIgnoreUnreadFields != cacheInstance.getPdxIgnoreUnreadFields()) {
      throw new IllegalStateException(
          String.format("Existing cache has different cache configuration, it has:%s",

              "pdxIgnoreUnreadFields: " + cacheInstance.getPdxIgnoreUnreadFields()));
    }
    if (this.pdxSerializerUserSet
        && !samePdxSerializer(this.pdxSerializer, cacheInstance.getPdxSerializer())) {
      throw new IllegalStateException(
          String.format("Existing cache has different cache configuration, it has:%s",
              "pdxSerializer: " + cacheInstance.getPdxSerializer()));
    }
  }

  private boolean samePdxSerializer(PdxSerializer s1, PdxSerializer s2) {
    Object o1 = s1;
    Object o2 = s2;
    if (s1 instanceof ReflectionBasedAutoSerializer
        && s2 instanceof ReflectionBasedAutoSerializer) {
      // Fix for bug 44907.
      o1 = ((ReflectionBasedAutoSerializer) s1).getManager();
      o2 = ((ReflectionBasedAutoSerializer) s2).getManager();
    }
    return equals(o1, o2);
  }

  private boolean equals(Object o1, Object o2) {
    if (o1 == null) {
      return o2 == null;
    }
    if (o2 == null) {
      return false;
    }
    return o1.equals(o2);
  }



  public void setDeclarativeConfig(CacheConfig cacheConfig) {
    if (!this.pdxDiskStoreUserSet) {
      this.pdxDiskStore = cacheConfig.getPdxDiskStore();
      this.pdxDiskStoreUserSet = cacheConfig.pdxDiskStoreUserSet;
    }
    if (!this.pdxPersistentUserSet) {
      this.pdxPersistent = cacheConfig.isPdxPersistent();
      this.pdxPersistentUserSet = cacheConfig.pdxPersistentUserSet;
    }
    if (!this.pdxReadSerializedUserSet) {
      this.pdxReadSerialized = cacheConfig.isPdxReadSerialized();
      this.pdxReadSerializedUserSet = cacheConfig.pdxReadSerializedUserSet;
    }
    if (!this.pdxSerializerUserSet) {
      this.pdxSerializer = cacheConfig.getPdxSerializer();
      this.pdxSerializerUserSet = cacheConfig.pdxSerializerUserSet;
    }
    if (!this.pdxIgnoreUnreadFieldsUserSet) {
      this.pdxIgnoreUnreadFields = cacheConfig.getPdxIgnoreUnreadFields();
      this.pdxIgnoreUnreadFieldsUserSet = cacheConfig.pdxIgnoreUnreadFieldsUserSet;
    }
  }

}

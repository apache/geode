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

package org.apache.geode.management.internal.cli.functions;

import java.io.Serializable;
import java.util.List;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.management.domain.ClassName;

/**
 * This class extracts serializable components from RegionAttributes and represents them simply as
 * strings. The relevant RegionAttributes fields are nulled out so that Serialzation can occur where
 * the relevant callbacks do not have classes defined. For example when retrieving a RegionAttribute
 * from a server to a locator.
 */
public class RegionAttributesWrapper<K, V> implements Serializable {

  private static final long serialVersionUID = -5517424520268271436L;

  private RegionAttributes<K, V> regionAttributes;

  private List<ClassName<CacheListener>> cacheListenerClasses;

  private ClassName<CacheLoader> cacheLoaderClass;

  private ClassName<CacheWriter> cacheWriterClass;

  private String compressorClass;

  private String keyConstraintClass;

  private String valueConstraintClass;

  public RegionAttributesWrapper() {}

  public RegionAttributes<K, V> getRegionAttributes() {
    return regionAttributes;
  }

  public void setRegionAttributes(RegionAttributes<K, V> regionAttributes) {
    this.regionAttributes = regionAttributes;
  }

  public List<ClassName<CacheListener>> getCacheListenerClasses() {
    return cacheListenerClasses;
  }

  public void setCacheListenerClasses(List<ClassName<CacheListener>> cacheListenerClasses) {
    this.cacheListenerClasses = cacheListenerClasses;
  }

  public ClassName<CacheLoader> getCacheLoaderClass() {
    return cacheLoaderClass;
  }

  public void setCacheLoaderClass(ClassName<CacheLoader> cacheLoaderClass) {
    this.cacheLoaderClass = cacheLoaderClass;
  }

  public ClassName<CacheWriter> getCacheWriterClass() {
    return cacheWriterClass;
  }

  public void setCacheWriterClass(ClassName<CacheWriter> cacheWriterClass) {
    this.cacheWriterClass = cacheWriterClass;
  }

  public String getCompressorClass() {
    return compressorClass;
  }

  public void setCompressorClass(String compressorClass) {
    this.compressorClass = compressorClass;
  }

  public String getKeyConstraintClass() {
    return keyConstraintClass;
  }

  public void setKeyConstraintClass(String keyConstraintClass) {
    this.keyConstraintClass = keyConstraintClass;
  }

  public String getValueConstraintClass() {
    return valueConstraintClass;
  }

  public void setValueConstraintClass(String valueConstraintClass) {
    this.valueConstraintClass = valueConstraintClass;
  }
}

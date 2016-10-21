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
package org.apache.geode.admin.internal;

import org.apache.geode.admin.CacheServerConfig;
import org.apache.geode.admin.CacheVmConfig;
import org.apache.geode.internal.admin.GemFireVM;

import static org.apache.geode.distributed.ConfigurationProperties.*;

/**
 * An implementation of <code>CacheVmConfig</code>
 *
 * @since GemFire 4.0
 */
public class CacheServerConfigImpl extends ManagedEntityConfigImpl
    implements CacheVmConfig, CacheServerConfig {

  /**
   * Declarative caching XML file that is used to initialize the Cache in the cache server.
   */
  private String cacheXMLFile;

  /** Extra classpath for the cache server */
  private String classpath;

  /////////////////////// Constructors ///////////////////////

  /**
   * Creates a new <code>CacheServerConfigImpl</code> with the default configuration settings.
   */
  public CacheServerConfigImpl() {
    this.cacheXMLFile = null;
    this.classpath = null;
  }

  /**
   * Creates a new <code>CacheServerConfigImpl</code> for a running cache server.
   */
  public CacheServerConfigImpl(GemFireVM vm) {
    super(vm);

    String name = CACHE_XML_FILE;
    this.cacheXMLFile = vm.getConfig().getAttribute(name);
    this.classpath = null;
  }

  /**
   * Copy constructor
   */
  public CacheServerConfigImpl(CacheServerConfig other) {
    super(other);
    this.cacheXMLFile = other.getCacheXMLFile();
    this.classpath = other.getClassPath();
  }

  /**
   * Copy constructor
   */
  public CacheServerConfigImpl(CacheVmConfig other) {
    super(other);
    this.cacheXMLFile = other.getCacheXMLFile();
    this.classpath = other.getClassPath();
  }

  ////////////////////// Instance Methods //////////////////////

  public String getCacheXMLFile() {
    return this.cacheXMLFile;
  }

  public void setCacheXMLFile(String cacheXMLFile) {
    checkReadOnly();
    this.cacheXMLFile = cacheXMLFile;
    configChanged();
  }

  public String getClassPath() {
    return this.classpath;
  }

  public void setClassPath(String classpath) {
    checkReadOnly();
    this.classpath = classpath;
    configChanged();
  }

  @Override
  public void validate() {
    super.validate();

    // Nothing to validate really. Cache.xml file could live on
    // different file system.
  }

  /**
   * Currently, listeners are not supported on the locator config.
   */
  @Override
  protected void configChanged() {

  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    return new CacheServerConfigImpl((CacheVmConfig) this);
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append(super.toString());
    sb.append(" cacheXMLFile=");
    sb.append(this.getCacheXMLFile());
    sb.append(" classPath=");
    sb.append(this.getClassPath());

    return sb.toString();
  }

}

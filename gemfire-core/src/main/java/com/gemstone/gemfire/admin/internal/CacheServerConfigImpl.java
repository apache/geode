/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin.internal;

import com.gemstone.gemfire.admin.CacheServerConfig;
import com.gemstone.gemfire.admin.CacheVmConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.admin.GemFireVM;

/**
 * An implementation of <code>CacheVmConfig</code>
 *
 * @author David Whitlock
 * @since 4.0
 */
public class CacheServerConfigImpl extends ManagedEntityConfigImpl 
  implements CacheVmConfig, CacheServerConfig {

  /** Declarative caching XML file that is used to initialize the
   * Cache in the cache server. */
  private String cacheXMLFile;

  /** Extra classpath for the cache server */
  private String classpath;

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>CacheServerConfigImpl</code> with the default
   * configuration settings.
   */
  public CacheServerConfigImpl() {
    this.cacheXMLFile = null;
    this.classpath = null;
  }

  /**
   * Creates a new <code>CacheServerConfigImpl</code> for a running
   * cache server.
   */
  public CacheServerConfigImpl(GemFireVM vm) {
    super(vm);

    String name = DistributionConfig.CACHE_XML_FILE_NAME;
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

  //////////////////////  Instance Methods  //////////////////////

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

    // Nothing to validate really.  Cache.xml file could live on
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
    return new CacheServerConfigImpl((CacheVmConfig)this);
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

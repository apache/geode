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
package org.apache.geode.admin;

/**
 * Configuration for a GemFire cache server VM that is managed by the
 * administration API.  The VM may or may not be running.
 *
 * @see AdminDistributedSystem#addCacheVm()
 *
 * @since GemFire 5.7
 * @deprecated as of 7.0 use the <code><a href="{@docRoot}/org/apache/geode/management/package-summary.html">management</a></code> package instead
 */
public interface CacheVmConfig extends ManagedEntityConfig {
  /**
   * Returns the <code>cache.xml</code> declarative caching
   * initialization file used to configure this cache server VM.  By
   * default, a cache server VM is started without an XML file.
   */
  public String getCacheXMLFile();

  /**
   * Sets the <code>cache.xml</code> declarative caching
   * initialization file used to configure this cache server VM.
   */
  public void setCacheXMLFile(String cacheXml);

  /**
   * Returns the location(s) of user classes (such as cache loaders)
   * required by the cache server VM.
   */
  public String getClassPath();

  /**
   * Sets the location(s) of user classes (such as cache loaders)
   * required by the cache server VM.
   */
  public void setClassPath(String classpath);
}

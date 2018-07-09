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
package org.apache.geode.cache.query.internal.index;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.OQLIndexTest;

/**
 * @since GemFire 6.6.1
 */
@Category({OQLIndexTest.class})
public class NewDeclarativeIndexCreationJUnitTest {

  private static final String CACHE_XML_FILE_NAME = "cachequeryindex.xml";

  private Cache cache;
  private File cacheXmlFile;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    this.cacheXmlFile = this.temporaryFolder.newFile(CACHE_XML_FILE_NAME);
    FileUtils.copyURLToFile(getClass().getResource(CACHE_XML_FILE_NAME), this.cacheXmlFile);
    assertThat(this.cacheXmlFile).exists(); // precondition

    Properties props = new Properties();
    props.setProperty(CACHE_XML_FILE, this.cacheXmlFile.getAbsolutePath());
    props.setProperty(MCAST_PORT, "0");
    DistributedSystem ds = DistributedSystem.connect(props);
    this.cache = CacheFactory.create(ds);
  }

  @After
  public void tearDown() throws Exception {
    if (this.cache != null) {
      this.cache.close();
    }
  }

  @Test
  public void testAsynchronousIndexCreatedOnRoot_PortfoliosRegion() {
    Region root = this.cache.getRegion("/root/portfolios");
    IndexManager im = IndexUtils.getIndexManager((InternalCache) cache, root, true);
    assertThat(im.getIndexes()).isNotEmpty();

    RegionAttributes ra = root.getAttributes();
    assertThat(ra.getIndexMaintenanceSynchronous()).isFalse();
  }

  @Test
  public void testSynchronousIndexCreatedOnRoot_StringRegion() {
    Region root = this.cache.getRegion("/root/string");
    IndexManager im = IndexUtils.getIndexManager((InternalCache) cache, root, true);
    assertThat(im.getIndexes()).isNotEmpty();

    RegionAttributes ra = root.getAttributes();
    assertThat(ra.getIndexMaintenanceSynchronous()).isTrue();

    root = this.cache.getRegion("/root/string1");
    im = IndexUtils.getIndexManager((InternalCache) cache, root, true);
    assertThat(im.isIndexMaintenanceTypeSynchronous()).isTrue();
  }

  @Test
  public void testSynchronousIndexCreatedOnRootRegion() {
    Region root = this.cache.getRegion("/root");
    IndexManager im = IndexUtils.getIndexManager((InternalCache) cache, root, true);
    assertThat(im.getIndexes()).isNotEmpty();

    RegionAttributes ra = root.getAttributes();
    assertThat(ra.getIndexMaintenanceSynchronous()).isTrue();
  }


  /**
   * Index creation tests for new DTD changes for Index tag for 6.6.1 with no function/primary-key
   * tag
   */
  @Test
  public void testAsynchronousIndexCreatedOnPortfoliosRegionWithNewDTD() {
    Region root = this.cache.getRegion("/root/portfolios2");
    IndexManager im = IndexUtils.getIndexManager((InternalCache) cache, root, true);
    assertThat(im.getIndexes()).isNotEmpty();

    RegionAttributes ra = root.getAttributes();
    assertThat(ra.getIndexMaintenanceSynchronous()).isFalse();
  }

  @Test
  public void testSynchronousIndexCreatedOnStringRegionWithNewDTD() {
    Region root = this.cache.getRegion("/root/string2");
    IndexManager im = IndexUtils.getIndexManager((InternalCache) cache, root, true);;
    assertThat(im.getIndexes()).isNotEmpty();

    RegionAttributes ra = root.getAttributes();
    assertThat(ra.getIndexMaintenanceSynchronous()).isTrue();

    root = this.cache.getRegion("/root/string1");
    im = IndexUtils.getIndexManager((InternalCache) cache, root, true);
    assertThat(im.isIndexMaintenanceTypeSynchronous()).isTrue();
  }

  /**
   * TODO: move this to a different test class because it requires different setup
   */
  @Test
  public void testIndexCreationExceptionOnRegionWithNewDTD() throws Exception {
    if (this.cache != null && !this.cache.isClosed()) {
      this.cache.close();
    }

    this.cacheXmlFile = this.temporaryFolder.newFile("cachequeryindexwitherror.xml");
    FileUtils.copyURLToFile(getClass().getResource("cachequeryindexwitherror.xml"),
        this.cacheXmlFile);
    assertThat(this.cacheXmlFile).exists(); // precondition

    Properties props = new Properties();
    props.setProperty(CACHE_XML_FILE, this.cacheXmlFile.getAbsolutePath());
    props.setProperty(MCAST_PORT, "0");

    DistributedSystem ds = DistributedSystem.connect(props);

    // TODO: refactoring GemFireCacheImpl.initializeDeclarativeCache requires change here
    assertThatThrownBy(() -> CacheFactory.create(ds)).isExactlyInstanceOf(CacheXmlException.class)
        .hasCauseInstanceOf(InternalGemFireException.class);
  }
}

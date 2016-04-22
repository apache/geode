/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gemstone.gemfire.cache.lucene.internal.directory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.store.BaseDirectoryTestCase;
import org.apache.lucene.store.Directory;
import org.junit.Rule;
import org.junit.experimental.categories.Category;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.ChunkKey;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.File;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * A unit test of the RegionDirectory class that uses the Directory test
 * case from the lucene code base.
 * 
 * This test is still mocking out the underlying cache, rather than using
 * a real region.
 */
@Category(UnitTest.class)
public class RegionDirectoryJUnitTest extends BaseDirectoryTestCase {
  @Rule
  public SystemPropertiesRestoreRule restoreProps = new SystemPropertiesRestoreRule();
  
  protected Directory getDirectory(Path path) throws IOException {
    
    //This is super lame, but log4j automatically sets the system property, and the lucene
    //test asserts that no system properties have changed. Unfortunately, there is no
    //way to control the order of rules, so we can't clear this property with a rule
    //or @After method. Instead, do it in the close method of the directory.
    return new RegionDirectory(new ConcurrentHashMap<String, File>(), new ConcurrentHashMap<ChunkKey, byte[]>());
  }
}

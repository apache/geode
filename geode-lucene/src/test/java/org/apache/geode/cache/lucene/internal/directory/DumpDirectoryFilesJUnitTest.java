/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene.internal.directory;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Collections;

import org.apache.lucene.index.IndexWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.lucene.internal.InternalLuceneIndex;
import org.apache.geode.cache.lucene.internal.InternalLuceneService;
import org.apache.geode.cache.lucene.internal.filesystem.FileSystem;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.lucene.internal.repository.RepositoryManager;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class DumpDirectoryFilesJUnitTest {

  private RegionFunctionContext context;
  private final String indexName = "index";
  private final String directoryName = "directory";
  private final String bucketName = "bucket";
  private FileSystem fileSystem;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void createMocks() throws BucketNotFoundException {

    GemFireCacheImpl cache = Fakes.cache();
    context = mock(RegionFunctionContext.class);
    ResultSender sender = mock(ResultSender.class);
    Region region = mock(Region.class);
    InternalLuceneService service = mock(InternalLuceneService.class);
    InternalLuceneIndex index = mock(InternalLuceneIndex.class);
    RepositoryManager repoManager = mock(RepositoryManager.class);
    IndexRepository repo = mock(IndexRepository.class);
    IndexWriter writer = mock(IndexWriter.class);
    RegionDirectory directory = mock(RegionDirectory.class);
    fileSystem = mock(FileSystem.class);
    Region bucket = mock(Region.class);
    when(bucket.getFullPath()).thenReturn(bucketName);

    when(context.getArguments()).thenReturn(new String[] {directoryName, indexName});
    when(context.getResultSender()).thenReturn(sender);
    when(context.getDataSet()).thenReturn(region);
    when(region.getCache()).thenReturn(cache);
    when(cache.getService(any())).thenReturn(service);
    when(repoManager.getRepositories(eq(context))).thenReturn(Collections.singleton(repo));
    when(index.getRepositoryManager()).thenReturn(repoManager);
    when(index.getName()).thenReturn(indexName);
    when(service.getIndex(eq(indexName), any())).thenReturn(index);
    when(directory.getFileSystem()).thenReturn(fileSystem);
    when(writer.getDirectory()).thenReturn(directory);
    when(repo.getWriter()).thenReturn(writer);
    when(repo.getRegion()).thenReturn(bucket);
  }

  @Test
  public void shouldInvokeExportOnBuckets() throws BucketNotFoundException {
    DumpDirectoryFiles dump = new DumpDirectoryFiles();
    dump.execute(context);

    File expectedDir = new File(directoryName, indexName + "_" + bucketName);
    verify(fileSystem).export(eq(expectedDir));
  }

  @Test
  public void shouldThrowIllegalStateWhenMissingIndex() throws BucketNotFoundException {
    DumpDirectoryFiles dump = new DumpDirectoryFiles();
    when(context.getArguments()).thenReturn(new String[] {"badDirectory", "badIndex"});
    expectedException.expect(IllegalStateException.class);
    dump.execute(context);
  }

  @Test
  public void shouldThrowIllegalArgumentWhenGivenBadArguments() throws BucketNotFoundException {
    DumpDirectoryFiles dump = new DumpDirectoryFiles();
    when(context.getArguments()).thenReturn(new Object());
    expectedException.expect(IllegalArgumentException.class);
    dump.execute(context);
  }

  @Test
  public void shouldThrowIllegalArgumentWhenMissingArgument() throws BucketNotFoundException {
    DumpDirectoryFiles dump = new DumpDirectoryFiles();
    when(context.getArguments()).thenReturn(new String[] {"not enough args"});
    expectedException.expect(IllegalArgumentException.class);
    dump.execute(context);
  }


}

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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.lucene.LuceneQueryProvider;
import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.cache.lucene.LuceneServiceProvider;
import com.gemstone.gemfire.cache.lucene.internal.InternalLuceneIndex;
import com.gemstone.gemfire.cache.lucene.internal.LuceneServiceImpl;
import com.gemstone.gemfire.cache.lucene.internal.distributed.CollectorManager;
import com.gemstone.gemfire.cache.lucene.internal.distributed.LuceneFunctionContext;
import com.gemstone.gemfire.cache.lucene.internal.distributed.TopEntriesCollector;
import com.gemstone.gemfire.cache.lucene.internal.distributed.TopEntriesCollectorManager;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.FileSystem;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepositoryImpl;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexResultCollector;
import com.gemstone.gemfire.cache.lucene.internal.repository.RepositoryManager;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;
import com.gemstone.gemfire.internal.logging.LogService;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;

public class DumpDirectoryFiles implements Function, InternalEntity {
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LogService.getLogger();
  public static final String ID = DumpDirectoryFiles.class.getSimpleName();

  @Override
  public void execute(FunctionContext context) {
    RegionFunctionContext ctx = (RegionFunctionContext) context;

    if(!(context.getArguments() instanceof String[])) {
      throw new IllegalArgumentException("Arguments should be a string array");
    }
    String[] args = (String[]) context.getArguments();
    if(args.length != 2) {
      throw new IllegalArgumentException("Expected 2 arguments: exportLocation, indexName");
    }


    String exportLocation =args[0];
    String indexName =args[1];

    final Region<Object, Object> region = ctx.getDataSet();
    LuceneService service = LuceneServiceProvider.get(ctx.getDataSet().getCache());
    InternalLuceneIndex index = (InternalLuceneIndex) service.getIndex(indexName, region.getFullPath());
    if(index == null) {
      throw new IllegalStateException("Index not found for region " + region + " index " + indexName);
    }

    final RepositoryManager repoManager = index.getRepositoryManager();
    try {
      final Collection<IndexRepository> repositories = repoManager.getRepositories(ctx);
      repositories.stream().forEach(repo -> {
        final IndexWriter writer = repo.getWriter();
        RegionDirectory directory = (RegionDirectory) writer.getDirectory();
        FileSystem fs = directory.getFileSystem();

        String bucketName = index.getName() + "_" + repo.getRegion().getFullPath();
        bucketName = bucketName.replace("/", "_");
        File bucketDirectory = new File(exportLocation, bucketName);
        bucketDirectory.mkdirs();
        fs.export(bucketDirectory);
      });
      context.getResultSender().lastResult(null);
    }
    catch (BucketNotFoundException e) {
      throw new FunctionException(e);
    }
  }

  @Override public String getId() {
    return ID;
  }

  @Override
  public boolean optimizeForWrite() {
    return true;
  }
}

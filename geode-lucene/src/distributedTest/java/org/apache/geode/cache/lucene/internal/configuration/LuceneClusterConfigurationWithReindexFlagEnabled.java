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
package org.apache.geode.cache.lucene.internal.configuration;

import java.io.IOException;
import java.util.Properties;

import org.junit.After;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.LuceneTest;


// Delete this file when LuceneServiceImpl.LUCENE_REINDEX flag is removed.
@Category({LuceneTest.class})
public class LuceneClusterConfigurationWithReindexFlagEnabled
    extends LuceneClusterConfigurationDUnitTest {

  MemberVM[] servers = new MemberVM[3];

  @Override
  MemberVM createServer(int index) throws IOException {
    servers[index - 1] = super.createServer(index);
    servers[index - 1].invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = true);
    return servers[index - 1];

  }

  @After
  public void clearLuceneReindexFlag() {
    for (MemberVM server : servers) {
      if (server != null) {
        server.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = false);
      }
    }
  }

  @Override
  MemberVM createServer(Properties properties, int index) throws Exception {
    servers[index - 1] = super.createServer(properties, index);
    servers[index - 1].getVM().invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = true);
    return servers[index - 1];
  }
}

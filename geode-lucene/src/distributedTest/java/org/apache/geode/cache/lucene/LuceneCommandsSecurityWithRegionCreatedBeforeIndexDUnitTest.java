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
package org.apache.geode.cache.lucene;


import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.LuceneTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@Category({SecurityTest.class, LuceneTest.class})
@RunWith(GeodeParamsRunner.class)
public class LuceneCommandsSecurityWithRegionCreatedBeforeIndexDUnitTest
    extends LuceneCommandsSecurityDUnitTest {

  @Before
  public void setLuceneReindexFlag() {
    MemberVM server = this.locatorServer.getMember(1);
    server.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = true);
  }

  @After
  public void clearLuceneReindexFlag() {
    MemberVM server = this.locatorServer.getMember(1);
    server.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = false);
  }

  @Override
  protected void createIndexAndRegion() throws Exception {
    // Connect gfsh to locator with permissions necessary to create an index and region
    this.gfshShell.secureConnectAndVerify(this.locator.getPort(), GfshCommandRule.PortType.locator,
        "cluster,data", "cluster,data");

    // Create region
    this.gfshShell.executeAndAssertThat(getCreateRegionCommand()).statusIsSuccess();

    // Create lucene index
    this.gfshShell.executeAndAssertThat(getCreateIndexCommand()).statusIsSuccess();

    // Disconnect gfsh
    this.gfshShell.disconnect();
  }
}

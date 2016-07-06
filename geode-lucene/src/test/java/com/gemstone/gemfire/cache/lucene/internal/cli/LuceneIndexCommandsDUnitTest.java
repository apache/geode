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
package com.gemstone.gemfire.cache.lucene.internal.cli;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.cache.lucene.LuceneServiceProvider;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.CommandManager;
import com.gemstone.gemfire.management.internal.cli.commands.CliCommandTestBase;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;
import com.gemstone.gemfire.test.dunit.*;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.gemstone.gemfire.cache.lucene.test.LuceneTestUtilities.*;
import static com.gemstone.gemfire.test.dunit.Assert.*;

@Category(DistributedTest.class)
public class LuceneIndexCommandsDUnitTest extends CliCommandTestBase {

  @Test
  public void testListIndex() throws Exception {
    setupSystem();
    CommandManager.getInstance().add(LuceneIndexCommands.class.newInstance());

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    String commandString = csb.toString();
    writeToLog("Command String :\n ", commandString);
    CommandResult commandResult = executeCommand(commandString);
    String resultAsString = commandResultToString(commandResult);
    writeToLog("Result String :\n ", resultAsString);
    assertEquals(Status.OK, commandResult.getStatus());
    assertTrue(resultAsString.contains(INDEX_NAME));
  }

  private void writeToLog(String text, String resultAsString) {
    System.out.println(getTestMethodName() + " : ");
    System.out.println(resultAsString);
  }

  protected void initDataStore(SerializableRunnableIF createIndex) throws Exception {
    createIndex.run();
    getCache().createRegionFactory(RegionShortcut.PARTITION).create(REGION_NAME);
  }

  private void setupSystem() {
    disconnectAllFromDS();
    setUpJmxManagerOnVm0ThenConnect(null);

    final VM manager = Host.getHost(0).getVM(0);
    final VM vm1 = Host.getHost(0).getVM(1);

    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndex(INDEX_NAME, REGION_NAME, "text");
    };
    vm1.invoke(()->initDataStore(createIndex));
  }

}

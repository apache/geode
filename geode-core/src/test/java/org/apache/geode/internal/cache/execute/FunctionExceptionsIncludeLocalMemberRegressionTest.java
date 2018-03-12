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
package org.apache.geode.internal.cache.execute;

import static com.googlecode.catchexception.CatchException.catchException;
import static com.googlecode.catchexception.CatchException.caughtException;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FunctionServiceTest;

/**
 * TRAC #41779: InternalExecution.setWaitOnExceptionFlag(true) does not add the exception from the
 * local node to the FunctionException list of exceptions
 *
 * <p>
 * Extracted from {@link PRFunctionExecutionDUnitTest}.
 */
@Category({DistributedTest.class, FunctionServiceTest.class})
@SuppressWarnings("serial")
public class FunctionExceptionsIncludeLocalMemberRegressionTest extends CacheTestCase {

  private Execution execution;

  @Before
  public void setUp() {
    VM datastore1 = getHost(0).getVM(0);
    VM datastore2 = getHost(0).getVM(1);
    VM datastore3 = getHost(0).getVM(2);
    VM datastore4 = getHost(0).getVM(3);

    String regionName = getUniqueName();

    // create stores on all VMs including controller
    datastore1.invoke(() -> createPartitionedRegion(regionName));
    datastore2.invoke(() -> createPartitionedRegion(regionName));
    datastore3.invoke(() -> createPartitionedRegion(regionName));
    datastore4.invoke(() -> createPartitionedRegion(regionName));

    createPartitionedRegion(regionName);

    datastore1.invoke(() -> registerThrowsExceptionFunction());
    datastore2.invoke(() -> registerThrowsExceptionFunction());
    datastore3.invoke(() -> registerThrowsExceptionFunction());
    datastore4.invoke(() -> registerThrowsExceptionFunction());

    registerThrowsExceptionFunction();

    execution = FunctionService.onRegion(cache.getRegion(regionName));
    ((InternalExecution) execution).setWaitOnExceptionFlag(true);
  }

  @Test
  public void functionExceptionsIncludeLocalMember() throws Exception {
    ResultCollector resultCollector = execution.execute(ThrowsExceptionFunction.class.getName());

    catchException(resultCollector).getResult();
    assertThat((Exception) caughtException()).isInstanceOf(FunctionException.class);

    FunctionException functionException = caughtException();
    assertThat(functionException.getExceptions()).hasSize(5);
  }

  private void createPartitionedRegion(final String regionName) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setLocalMaxMemory(10);
    paf.setRedundantCopies(0);

    RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(paf.create());

    regionFactory.create(regionName);
  }

  private void registerThrowsExceptionFunction() {
    FunctionService.registerFunction(new ThrowsExceptionFunction());
  }

  /**
   * Simple function that throws NullPointerException after sending some results.
   */
  private static class ThrowsExceptionFunction implements Function {

    @Override
    public void execute(FunctionContext context) {
      // first send some results
      for (int index = 0; index < 5; ++index) {
        context.getResultSender().sendResult(index);
      }
      // then throw an exception
      throw new NullPointerException("Thrown by " + context.getMemberName());
    }

    @Override
    public String getId() {
      return getClass().getName();
    }

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }
}

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
package com.gemstone.gemfire.management.internal.cli.commands;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionInvocationTargetException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreConfigHolder;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.execute.AbstractExecution;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.functions.AlterHDFSStoreFunction;
import com.gemstone.gemfire.management.internal.cli.functions.CliFunctionResult;
import com.gemstone.gemfire.management.internal.cli.functions.CreateHDFSStoreFunction;
import com.gemstone.gemfire.management.internal.cli.functions.DescribeHDFSStoreFunction;
import com.gemstone.gemfire.management.internal.cli.functions.DestroyHDFSStoreFunction;
import com.gemstone.gemfire.management.internal.cli.functions.ListHDFSStoresFunction;
import com.gemstone.gemfire.management.internal.cli.functions.ListHDFSStoresFunction.HdfsStoreDetails;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.result.InfoResultData;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.management.internal.cli.util.HDFSStoreNotFoundException;
import com.gemstone.gemfire.management.internal.cli.util.MemberNotFoundException;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * The HDFSStoreCommandsJUnitTest class is a test suite of test cases testing
 * the contract and functionality of the HDFSStoreCommands class implementing
 * commands in the GemFire shell (gfsh) that access and modify hdfs stores in
 * GemFire. </p>
 * 
 * @author Namrata Thanvi
 * @see com.gemstone.gemfire.management.internal.cli.commands.HDFSStoreCommands
 * @see com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreConfigHolder
 * @see com.gemstone.gemfire.management.internal.cli.functions.DescribeHDFSStoreFunction
 * @see org.jmock.Expectations
 * @see org.jmock.Mockery
 * @see org.jmock.lib.legacy.ClassImposteriser
 * @see org.junit.Assert
 * @see org.junit.Test
 */
@Category({IntegrationTest.class, HoplogTest.class})
public class HDFSStoreCommandsJUnitTest {

  private Mockery mockContext;

  @Before
  public void setUp() {
    mockContext = new Mockery() {
      {
        setImposteriser(ClassImposteriser.INSTANCE);
      }
    };
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  @Test
  public void testGetHDFSStoreDescription() {
    final String hdfsStoreName = "mockHdfsStore";
    final String memberId = "mockMember";
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");
    final ResultCollector mockResultCollector = mockContext.mock(ResultCollector.class, "ResultCollector");

    final HDFSStoreConfigHolder expectedHdfsStoreConfigHolder = createMockHDFSStoreConfigHolder(mockContext, "hdfsStoreName",
        "hdfs://localhost:9000", "testDir", 1024, 20, .25f, null, 40, 40, null, false, 0, 2048, true, true, true, 40,
        40, 40, 800);

    mockContext.checking(new Expectations() {
      {
        oneOf(mockMember).getName();
        will(returnValue(null));
        oneOf(mockMember).getId();
        will(returnValue(memberId));
        oneOf(mockFunctionExecutor).withArgs(with(equal(hdfsStoreName)));
        will(returnValue(mockFunctionExecutor));
        oneOf(mockFunctionExecutor).execute(with(aNonNull(DescribeHDFSStoreFunction.class)));
        will(returnValue(mockResultCollector));
        oneOf(mockResultCollector).getResult();
        will(returnValue(Arrays.asList(expectedHdfsStoreConfigHolder)));
      }
    });

    final HDFSStoreCommands commands = new TestHDFSStoreCommands(mockCache, mockMember, mockFunctionExecutor);

    final HDFSStoreConfigHolder actualHdfsStoreConfigHolder = commands.getHDFSStoreDescription(memberId, hdfsStoreName);

    assertNotNull(actualHdfsStoreConfigHolder);
    assertEquals(expectedHdfsStoreConfigHolder, actualHdfsStoreConfigHolder);
  }

  @Test(expected = MemberNotFoundException.class)
  public void testGetHDFSStoreDescriptionThrowsMemberNotFoundException() {
    final String hdfsStoreName = "mockHdfsStore";
    final String memberId = "mockMember";
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    mockContext.checking(new Expectations() {
      {
        oneOf(mockMember).getName();
        will(returnValue(null));
        oneOf(mockMember).getId();
        will(returnValue("testMember"));
      }
    });

    final HDFSStoreCommands commands = new TestHDFSStoreCommands(mockCache, mockMember, null);

    try {
      commands.getHDFSStoreDescription(memberId, hdfsStoreName);
    } catch (MemberNotFoundException expected) {
      assertEquals(CliStrings.format(CliStrings.MEMBER_NOT_FOUND_ERROR_MESSAGE, memberId), expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = HDFSStoreNotFoundException.class)
  public void testGetHDFSStoreDescriptionThrowsResourceNotFoundException() {
    final String hdfsStoreName = "mockHdfsStore";
    final String memberId = "mockMember";

    final Cache mockCache = mockContext.mock(Cache.class, "Cache");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");

    mockContext.checking(new Expectations() {
      {
        oneOf(mockMember).getName();
        will(returnValue(null));
        oneOf(mockMember).getId();
        will(returnValue(memberId));
        oneOf(mockFunctionExecutor).withArgs(with(equal(hdfsStoreName)));
        will(returnValue(mockFunctionExecutor));
        oneOf(mockFunctionExecutor).execute(with(aNonNull(DescribeHDFSStoreFunction.class)));
        will(throwException(new HDFSStoreNotFoundException("expected")));
      }
    });

    final HDFSStoreCommands commands = new TestHDFSStoreCommands(mockCache, mockMember, mockFunctionExecutor);

    try {
      commands.getHDFSStoreDescription(memberId, hdfsStoreName);
    } catch (HDFSStoreNotFoundException expected) {
      assertEquals("expected", expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = RuntimeException.class)
  public void testGetHDFSStoreDescriptionThrowsRuntimeException() {
    final String hdfsStoreName = "mockHdfsStore";
    final String memberId = "mockMember";

    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");

    mockContext.checking(new Expectations() {
      {
        oneOf(mockMember).getName();
        will(returnValue(null));
        oneOf(mockMember).getId();
        will(returnValue(memberId));
        oneOf(mockFunctionExecutor).withArgs(with(equal(hdfsStoreName)));
        will(returnValue(mockFunctionExecutor));
        oneOf(mockFunctionExecutor).execute(with(aNonNull(DescribeHDFSStoreFunction.class)));
        will(throwException(new RuntimeException("expected")));
      }
    });

    final HDFSStoreCommands commands = new TestHDFSStoreCommands(mockCache, mockMember, mockFunctionExecutor);

    try {
      commands.getHDFSStoreDescription(memberId, hdfsStoreName);
    } catch (RuntimeException expected) {
      assertEquals("expected", expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = RuntimeException.class)
  public void testGetHDFSStoreDescriptionWithInvalidFunctionResultReturnType() {
    final String hdfsStoreName = "mockHDFSStore";
    final String memberId = "mockMember";

    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");

    final ResultCollector mockResultCollector = mockContext.mock(ResultCollector.class, "ResultCollector");

    mockContext.checking(new Expectations() {
      {
        oneOf(mockMember).getName();
        will(returnValue(null));
        oneOf(mockMember).getId();
        will(returnValue(memberId));
        oneOf(mockFunctionExecutor).withArgs(with(equal(hdfsStoreName)));
        will(returnValue(mockFunctionExecutor));
        oneOf(mockFunctionExecutor).execute(with(aNonNull(DescribeHDFSStoreFunction.class)));
        will(returnValue(mockResultCollector));
        oneOf(mockResultCollector).getResult();
        will(returnValue(Arrays.asList(new Object())));
      }
    });

    final HDFSStoreCommands commands = new TestHDFSStoreCommands(mockCache, mockMember, mockFunctionExecutor);

    try {
      commands.getHDFSStoreDescription(memberId, hdfsStoreName);
    } catch (RuntimeException expected) {
      assertEquals(CliStrings.format(CliStrings.UNEXPECTED_RETURN_TYPE_EXECUTING_COMMAND_ERROR_MESSAGE, Object.class
          .getName(), CliStrings.DESCRIBE_HDFS_STORE), expected.getMessage());
      assertNull(expected.getCause());
      throw expected;
    }
  }

  @Test
  public void testGetHDFSStoreListing() {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final DistributedMember mockDistributedMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final AbstractExecution mockFunctionExecutor = mockContext.mock(AbstractExecution.class, "Function Executor");

    final ResultCollector mockResultCollector = mockContext.mock(ResultCollector.class, "ResultCollector");

    final HDFSStoreConfigHolder expectedHdfsStoreConfigHolderOne = createMockHDFSStoreConfigHolder(mockContext, "hdfsStoreName1",
        "hdfs://localhost:9000", "testDir", 1024, 20, .25f, null, 40, 40, null, false, 0, 2048, true, true, true, 40,
        40, 40, 800);
    final HDFSStoreConfigHolder expectedHdfsStoreConfigHolderTwo = createMockHDFSStoreConfigHolder(mockContext, "hdfsStoreName2",
        "hdfs://localhost:9000", "testDir", 1024, 20, .25f, null, 40, 40, null, false, 0, 2048, true, true, true, 40,
        40, 40, 800);
    final HDFSStoreConfigHolder expectedHdfsStoreConfigHolderThree = createMockHDFSStoreConfigHolder(mockContext, "hdfsStoreName3",
        "hdfs://localhost:9000", "testDir", 1024, 20, .25f, null, 40, 40, null, false, 0, 2048, true, true, true, 40,
        40, 40, 800);
 
    
    HdfsStoreDetails d1=new HdfsStoreDetails(expectedHdfsStoreConfigHolderOne.getName(), "member1", "member1");
    HdfsStoreDetails d2=new HdfsStoreDetails(expectedHdfsStoreConfigHolderTwo.getName(), "member2", "member2");
    HdfsStoreDetails d3=new HdfsStoreDetails(expectedHdfsStoreConfigHolderThree.getName(), "member3", "member3");
    
    final Set<HdfsStoreDetails> expectedHdfsStores = new HashSet<HdfsStoreDetails>();
    expectedHdfsStores.add( d1);
    expectedHdfsStores.add(d2 );    
    expectedHdfsStores.add(d3);

    final List<Object> results = new ArrayList<Object>();
    results.add(expectedHdfsStores);
    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionExecutor).setIgnoreDepartedMembers(with(equal(true)));
        oneOf(mockFunctionExecutor).execute(with(aNonNull(ListHDFSStoresFunction.class)));
        will(returnValue(mockResultCollector));
        oneOf(mockResultCollector).getResult();
        will(returnValue(results));
      }
    });

    final HDFSStoreCommands commands = new TestHDFSStoreCommands(mockCache, mockDistributedMember, mockFunctionExecutor);

    final List<?> actualHdfsStores = commands.getHdfsStoreListing(commands.getNormalMembers(mockCache));

    Assert.assertNotNull(actualHdfsStores);   
    Assert.assertTrue(actualHdfsStores.contains(d1));
    Assert.assertTrue(actualHdfsStores.contains(d2));
    Assert.assertTrue(actualHdfsStores.contains(d3));
  }

  @Test(expected = RuntimeException.class)
  public void testGetHDFSStoreListThrowsRuntimeException() {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");
    final DistributedMember mockDistributedMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");

    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionExecutor).execute(with(aNonNull(ListHDFSStoresFunction.class)));
        will(throwException(new RuntimeException("expected")));
      }
    });

    final HDFSStoreCommands commands = new TestHDFSStoreCommands(mockCache, mockDistributedMember, mockFunctionExecutor);

    try {
      commands.getHdfsStoreListing(commands.getNormalMembers(mockCache));
    } catch (RuntimeException expected) {
      assertEquals("expected", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testGetHDFSStoreListReturnsFunctionInvocationTargetExceptionInResults() {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");
    final DistributedMember mockDistributedMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final AbstractExecution mockFunctionExecutor = mockContext.mock(AbstractExecution.class, "Function Executor");
    final ResultCollector mockResultCollector = mockContext.mock(ResultCollector.class, "ResultCollector");

    final HDFSStoreConfigHolder expectedHdfsStoreConfigHolder = createMockHDFSStoreConfigHolder(mockContext, "hdfsStoreName",
        "hdfs://localhost:9000", "testDir", 1024, 20, .25f, null, 40, 40, null, false, 0, 2048, true, true, true, 40,
        40, 40, 800);

    final List<HdfsStoreDetails> expectedHdfsStores = Arrays.asList(new HdfsStoreDetails(
        expectedHdfsStoreConfigHolder.getName(), "member1", "member1"));

    final List<Object> results = new ArrayList<Object>();

    results.add(expectedHdfsStores);
    results.add(new FunctionInvocationTargetException("expected"));

    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionExecutor).setIgnoreDepartedMembers(with(equal(true)));
        oneOf(mockFunctionExecutor).execute(with(aNonNull(ListHDFSStoresFunction.class)));
        will(returnValue(mockResultCollector));
        oneOf(mockResultCollector).getResult();
        will(returnValue(results));
      }
    });

    final HDFSStoreCommands commands = new TestHDFSStoreCommands(mockCache, mockDistributedMember, mockFunctionExecutor);

    final List<HdfsStoreDetails> actualHdfsStores = commands.getHdfsStoreListing(commands
        .getNormalMembers(mockCache));

  }

  @Test
  public void testGetCreatedHDFSStore() throws JSONException {
    final String hdfsStoreName = "mockHdfsStore";
    final String memberId = "mockMember";
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");
    final ResultCollector mockResultCollector = mockContext.mock(ResultCollector.class, "ResultCollector");
    XmlEntity xml = null;
    final CliFunctionResult cliResult = new CliFunctionResult(memberId, xml, "Success");
    // Need to fix the return value of this function
    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionExecutor).withArgs(with(aNonNull(HDFSStoreConfigHolder.class)));
        will(returnValue(mockFunctionExecutor));
        oneOf(mockFunctionExecutor).execute(with(aNonNull(CreateHDFSStoreFunction.class)));
        will(returnValue(mockResultCollector));
        oneOf(mockResultCollector).getResult();
        will(returnValue(Arrays.asList(cliResult)));
      }
    });

    final HDFSStoreCommands commands = new TestHDFSStoreCommands(mockCache, mockMember, mockFunctionExecutor);

    final Result result = commands.getCreatedHdfsStore(null, hdfsStoreName, "hdfs://localhost:9000", "test", null, 20,
        20, true, true, 100, 10000, "testStore", true, 10, true, .23F, 10, 10, 10, 10, 10);

    assertNotNull(result);
    assertEquals(Status.OK, result.getStatus());
    TabularResultData resultData = (TabularResultData)((CommandResult)result).getResultData();
    GfJsonObject jsonObject = resultData.getGfJsonObject().getJSONObject("content");
    assertNotNull(jsonObject.get("Member"));
    assertNotNull(jsonObject.get("Result"));

    assertEquals(memberId, (((JSONArray)jsonObject.get("Member")).get(0)));
    assertEquals("Success", (((JSONArray)jsonObject.get("Result")).get(0)));
  }

  @Test
  public void testGetCreatedHDFSStoreWithThrowable() throws JSONException {
    final String hdfsStoreName = "mockHdfsStore";
    final String memberId = "mockMember";
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");
    final ResultCollector mockResultCollector = mockContext.mock(ResultCollector.class, "ResultCollector");
    RuntimeException exception = new RuntimeException("Test Exception");

    final CliFunctionResult cliResult = new CliFunctionResult(memberId, exception, null);
    // Need to fix the return value of this function
    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionExecutor).withArgs(with(aNonNull(HDFSStoreConfigHolder.class)));
        will(returnValue(mockFunctionExecutor));
        oneOf(mockFunctionExecutor).execute(with(aNonNull(CreateHDFSStoreFunction.class)));
        will(returnValue(mockResultCollector));
        oneOf(mockResultCollector).getResult();
        will(returnValue(Arrays.asList(cliResult)));
      }
    });

    final HDFSStoreCommands commands = new TestHDFSStoreCommands(mockCache, mockMember, mockFunctionExecutor);

    final Result result = commands.getCreatedHdfsStore(null, hdfsStoreName, "hdfs://localhost:9000", "test", null, 20,
        20, true, true, 100, 10000, "testStore", true, 10, true, .23F, 10, 10, 10, 10, 10);

    assertNotNull(result);
    assertEquals(Status.ERROR, result.getStatus());

    TabularResultData resultData = (TabularResultData)((CommandResult)result).getResultData();
    GfJsonObject jsonObject = resultData.getGfJsonObject().getJSONObject("content");
    assertNotNull(jsonObject.get("Member"));
    assertNotNull(jsonObject.get("Result"));
    assertEquals(memberId, (((JSONArray)jsonObject.get("Member")).get(0)));
    assertEquals("ERROR: " + exception.getClass().getName() + ": " + exception.getMessage(), (((JSONArray)jsonObject
        .get("Result")).get(0)));
  }

  @Test
  public void testGetCreatedHDFSStoreWithCacheClosedException() throws JSONException {
    final String hdfsStoreName = "mockHdfsStore";
    final String memberId = "mockMember";
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");
    final ResultCollector mockResultCollector = mockContext.mock(ResultCollector.class, "ResultCollector");

    final CliFunctionResult cliResult = new CliFunctionResult(memberId, false, null);
    // Need to fix the return value of this function
    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionExecutor).withArgs(with(aNonNull(HDFSStoreConfigHolder.class)));
        will(returnValue(mockFunctionExecutor));
        oneOf(mockFunctionExecutor).execute(with(aNonNull(CreateHDFSStoreFunction.class)));
        will(returnValue(mockResultCollector));
        oneOf(mockResultCollector).getResult();
        will(returnValue(Arrays.asList(cliResult)));
      }
    });

    final HDFSStoreCommands commands = new TestHDFSStoreCommands(mockCache, mockMember, mockFunctionExecutor);

    final Result result = commands.getCreatedHdfsStore(null, hdfsStoreName, "hdfs://localhost:9000", "test", null, 20,
        20, true, true, 100, 10000, "testStore", true, 10, true, .23F, 10, 10, 10, 10, 10);

    assertNotNull(result);
    InfoResultData resultData = (InfoResultData)((CommandResult)result).getResultData();
    GfJsonObject jsonObject = resultData.getGfJsonObject().getJSONObject("content");
    assertNotNull(jsonObject.get("message"));

    assertEquals("Unable to create hdfs store:" + hdfsStoreName, (((JSONArray)jsonObject.get("message")).get(0)));
  }

  @Test
  public void testGetAlteredHDFSStore() throws JSONException {
    final String hdfsStoreName = "mockHdfsStore";
    final String memberId = "mockMember";
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");
    final ResultCollector mockResultCollector = mockContext.mock(ResultCollector.class, "ResultCollector");
    XmlEntity xml = null;
    final CliFunctionResult cliResult = new CliFunctionResult(memberId, xml, "Success");
    // Need to fix the return value of this function
    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionExecutor).withArgs(with(aNonNull(HDFSStoreConfigHolder.class)));
        will(returnValue(mockFunctionExecutor));
        oneOf(mockFunctionExecutor).execute(with(aNonNull(AlterHDFSStoreFunction.class)));
        will(returnValue(mockResultCollector));
        oneOf(mockResultCollector).getResult();
        will(returnValue(Arrays.asList(cliResult)));
      }
    });

    final HDFSStoreCommands commands = new TestHDFSStoreCommands(mockCache, mockMember, mockFunctionExecutor);

    final Result result = commands.getAlteredHDFSStore(null, hdfsStoreName, 100, 100, true, 100, true, 100, 100, 100,
        100, 100);

    assertNotNull(result);
    assertEquals(Status.OK, result.getStatus());
    TabularResultData resultData = (TabularResultData)((CommandResult)result).getResultData();
    GfJsonObject jsonObject = resultData.getGfJsonObject().getJSONObject("content");
    assertNotNull(jsonObject.get("Member"));
    assertNotNull(jsonObject.get("Result"));

    assertEquals(memberId, (((JSONArray)jsonObject.get("Member")).get(0)));
    assertEquals("Success", (((JSONArray)jsonObject.get("Result")).get(0)));
  }

  @Test
  public void testGetAlteredHDFSStoreWithThrowable() throws JSONException {
    final String hdfsStoreName = "mockHdfsStore";
    final String memberId = "mockMember";
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");
    final ResultCollector mockResultCollector = mockContext.mock(ResultCollector.class, "ResultCollector");
    RuntimeException exception = new RuntimeException("Test Exception");
    final CliFunctionResult cliResult = new CliFunctionResult(memberId, exception, "Success");
    // Need to fix the return value of this function
    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionExecutor).withArgs(with(aNonNull(HDFSStoreConfigHolder.class)));
        will(returnValue(mockFunctionExecutor));
        oneOf(mockFunctionExecutor).execute(with(aNonNull(AlterHDFSStoreFunction.class)));
        will(returnValue(mockResultCollector));
        oneOf(mockResultCollector).getResult();
        will(returnValue(Arrays.asList(cliResult)));
      }
    });

    final HDFSStoreCommands commands = new TestHDFSStoreCommands(mockCache, mockMember, mockFunctionExecutor);

    final Result result = commands.getAlteredHDFSStore(null, hdfsStoreName, 100, 100, true, 100, true, 100, 100, 100,
        100, 100);

    assertNotNull(result);
    assertEquals(Status.ERROR, result.getStatus());
    TabularResultData resultData = (TabularResultData)((CommandResult)result).getResultData();
    GfJsonObject jsonObject = resultData.getGfJsonObject().getJSONObject("content");
    assertNotNull(jsonObject.get("Member"));
    assertNotNull(jsonObject.get("Result"));

    assertEquals(memberId, (((JSONArray)jsonObject.get("Member")).get(0)));
    assertEquals("ERROR: " + exception.getClass().getName() + ": " + exception.getMessage(), (((JSONArray)jsonObject
        .get("Result")).get(0)));
  }

  @Test
  public void testGetAlteredHDFSStoreWithCacheClosedException() throws JSONException {
    final String hdfsStoreName = "mockHdfsStore";
    final String memberId = "mockMember";
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");
    final ResultCollector mockResultCollector = mockContext.mock(ResultCollector.class, "ResultCollector");
    final CliFunctionResult cliResult = new CliFunctionResult(memberId, false, null);
    // Need to fix the return value of this function
    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionExecutor).withArgs(with(aNonNull(HDFSStoreConfigHolder.class)));
        will(returnValue(mockFunctionExecutor));
        oneOf(mockFunctionExecutor).execute(with(aNonNull(AlterHDFSStoreFunction.class)));
        will(returnValue(mockResultCollector));
        oneOf(mockResultCollector).getResult();
        will(returnValue(Arrays.asList(cliResult)));
      }
    });

    final HDFSStoreCommands commands = new TestHDFSStoreCommands(mockCache, mockMember, mockFunctionExecutor);

    final Result result = commands.getAlteredHDFSStore(null, hdfsStoreName, 100, 100, true, 100, true, 100, 100, 100,
        100, 100);

    assertNotNull(result);
    TabularResultData resultData = (TabularResultData)((CommandResult)result).getResultData();
    JSONObject jsonObject = (JSONObject)resultData.getGfJsonObject().get("content");
    assertEquals(0, jsonObject.length());
  }

  @Test
  public void testDestroyStore() throws JSONException {
    final String hdfsStoreName = "mockHdfsStore";
    final String memberId = "mockMember";
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");
    final ResultCollector mockResultCollector = mockContext.mock(ResultCollector.class, "ResultCollector");
    XmlEntity xml = null;
    final CliFunctionResult cliResult = new CliFunctionResult(memberId, xml, "Success");
    // Need to fix the return value of this function
    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionExecutor).withArgs(hdfsStoreName);
        will(returnValue(mockFunctionExecutor));
        oneOf(mockFunctionExecutor).execute(with(aNonNull(DestroyHDFSStoreFunction.class)));
        will(returnValue(mockResultCollector));
        oneOf(mockResultCollector).getResult();
        will(returnValue(Arrays.asList(cliResult)));
      }
    });

    final HDFSStoreCommands commands = new TestHDFSStoreCommands(mockCache, mockMember, mockFunctionExecutor);

    final Result result = commands.destroyStore(hdfsStoreName, null);

    assertNotNull(result);
    assertEquals(Status.OK, result.getStatus());
    TabularResultData resultData = (TabularResultData)((CommandResult)result).getResultData();
    GfJsonObject jsonObject = resultData.getGfJsonObject().getJSONObject("content");
    assertNotNull(jsonObject.get("Member"));
    assertNotNull(jsonObject.get("Result"));

    assertEquals(memberId, (((JSONArray)jsonObject.get("Member")).get(0)));
    assertEquals("Success", (((JSONArray)jsonObject.get("Result")).get(0)));
  }

  @Test
  public void testDestroyStoreWithThrowable() throws JSONException {
    final String hdfsStoreName = "mockHdfsStore";
    final String memberId = "mockMember";
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");
    final ResultCollector mockResultCollector = mockContext.mock(ResultCollector.class, "ResultCollector");
    RuntimeException exception = new RuntimeException("Test Exception");
    final CliFunctionResult cliResult = new CliFunctionResult(memberId, exception, "Success");
    // Need to fix the return value of this function
    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionExecutor).withArgs(hdfsStoreName);
        will(returnValue(mockFunctionExecutor));
        oneOf(mockFunctionExecutor).execute(with(aNonNull(DestroyHDFSStoreFunction.class)));
        will(returnValue(mockResultCollector));
        oneOf(mockResultCollector).getResult();
        will(returnValue(Arrays.asList(cliResult)));
      }
    });

    final HDFSStoreCommands commands = new TestHDFSStoreCommands(mockCache, mockMember, mockFunctionExecutor);

    final Result result = commands.destroyHdfstore(hdfsStoreName, null);

    assertNotNull(result);
    assertEquals(Status.ERROR, result.getStatus());
    TabularResultData resultData = (TabularResultData)((CommandResult)result).getResultData();
    GfJsonObject jsonObject = resultData.getGfJsonObject().getJSONObject("content");
    assertNotNull(jsonObject.get("Member"));
    assertNotNull(jsonObject.get("Result"));

    assertEquals(memberId, (((JSONArray)jsonObject.get("Member")).get(0)));
    assertEquals("ERROR: " + exception.getClass().getName() + ": " + exception.getMessage(), (((JSONArray)jsonObject
        .get("Result")).get(0)));
  }

  @Test
  public void testDestroyStoreWithCacheClosedException() throws JSONException {
    final String hdfsStoreName = "mockHdfsStore";
    final String memberId = "mockMember";
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");
    final ResultCollector mockResultCollector = mockContext.mock(ResultCollector.class, "ResultCollector");
    final CliFunctionResult cliResult = new CliFunctionResult(memberId, false, null);
    // Need to fix the return value of this function
    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionExecutor).withArgs(hdfsStoreName);
        will(returnValue(mockFunctionExecutor));
        oneOf(mockFunctionExecutor).execute(with(aNonNull(DestroyHDFSStoreFunction.class)));
        will(returnValue(mockResultCollector));
        oneOf(mockResultCollector).getResult();
        will(returnValue(Arrays.asList(cliResult)));
      }
    });

    final HDFSStoreCommands commands = new TestHDFSStoreCommands(mockCache, mockMember, mockFunctionExecutor);

    final Result result = commands.destroyHdfstore(hdfsStoreName, null);

    assertNotNull(result);

    assertNotNull(result);
    InfoResultData resultData = (InfoResultData)((CommandResult)result).getResultData();
    GfJsonObject jsonObject = resultData.getGfJsonObject().getJSONObject("content");
    assertNotNull(jsonObject.get("message"));

    assertEquals("No matching hdfs stores found.", (((JSONArray)jsonObject.get("message")).get(0)));
  }

  public static HDFSStoreConfigHolder createMockHDFSStoreConfigHolder(Mockery mockContext, final String storeName, final String namenode,
      final String homeDir, final int maxFileSize, final int fileRolloverInterval, final float blockCachesize,
      final String clientConfigFile, final int batchSize, final int batchInterval, final String diskStoreName,
      final boolean syncDiskwrite, final int dispatcherThreads, final int maxMemory, final boolean bufferPersistent,
      final boolean minorCompact, final boolean majorCompact, final int majorCompactionInterval,
      final int majorCompactionThreads, final int minorCompactionThreads, final int purgeInterval) {

    HDFSStoreConfigHolder mockHdfsStore = mockContext.mock(HDFSStoreConfigHolder.class, "HDFSStoreConfigHolder_"
        + storeName);

    createMockStore(mockContext, mockHdfsStore, storeName, namenode, homeDir, maxFileSize, fileRolloverInterval,
        minorCompact, minorCompactionThreads, majorCompact, majorCompactionThreads, majorCompactionInterval,
        purgeInterval, blockCachesize, clientConfigFile, batchSize,
        batchInterval, diskStoreName, syncDiskwrite, dispatcherThreads, maxMemory, bufferPersistent);
    return mockHdfsStore;

  }

  public static void createMockStore(Mockery mockContext, final HDFSStore mockStore, final String storeName,
      final String namenode, final String homeDir, final int maxFileSize, final int fileRolloverInterval,
      final boolean minorCompact, final int minorCompactionThreads, final boolean majorCompact,
      final int majorCompactionThreads, final int majorCompactionInterval, final int purgeInterval,
      final float blockCachesize, final String clientConfigFile, final int batchSize, final int batchInterval,
      final String diskStoreName, final boolean syncDiskwrite, final int dispatcherThreads, final int maxMemory,
      final boolean bufferPersistent) {

    mockContext.checking(new Expectations() {
      {
        allowing(mockStore).getName();
        will(returnValue(storeName));
        allowing(mockStore).getNameNodeURL();
        will(returnValue(namenode));
        allowing(mockStore).getHomeDir();
        will(returnValue(homeDir));
        allowing(mockStore).getWriteOnlyFileRolloverSize();
        will(returnValue(maxFileSize));
        allowing(mockStore).getWriteOnlyFileRolloverInterval();
        will(returnValue(fileRolloverInterval));
        allowing(mockStore).getMinorCompaction();
        will(returnValue(minorCompact));
        allowing(mockStore).getMajorCompaction();
        will(returnValue(majorCompact));
        allowing(mockStore).getMajorCompactionInterval();
        will(returnValue(majorCompactionInterval));
        allowing(mockStore).getMajorCompactionThreads();
        will(returnValue(majorCompactionThreads));
        allowing(mockStore).getMinorCompactionThreads();
        will(returnValue(minorCompactionThreads));
        allowing(mockStore).getPurgeInterval();
        will(returnValue(purgeInterval));
        allowing(mockStore).getInputFileCountMax();
        will(returnValue(10));
        allowing(mockStore).getInputFileSizeMax();
        will(returnValue(1024));
        allowing(mockStore).getInputFileCountMin();
        will(returnValue(2));
        allowing(mockStore).getBlockCacheSize();
        will(returnValue(blockCachesize));
        allowing(mockStore).getHDFSClientConfigFile();
        will(returnValue(clientConfigFile));

        allowing(mockStore).getBatchSize();
        will(returnValue(batchSize));
        allowing(mockStore).getBatchInterval();
        will(returnValue(batchInterval));
        allowing(mockStore).getDiskStoreName();
        will(returnValue(diskStoreName));
        allowing(mockStore).getSynchronousDiskWrite();
        will(returnValue(syncDiskwrite));
        allowing(mockStore).getBufferPersistent();
        will(returnValue(bufferPersistent));
        allowing(mockStore).getDispatcherThreads();
        will(returnValue(dispatcherThreads));
        allowing(mockStore).getMaxMemory();
        will(returnValue(maxMemory));
      }
    });
  }

  protected static class TestHDFSStoreCommands extends HDFSStoreCommands {

    private final Cache cache;

    private final DistributedMember distributedMember;

    private final Execution functionExecutor;

    public TestHDFSStoreCommands(final Cache cache, final DistributedMember distributedMember,
        final Execution functionExecutor) {
      assert cache != null: "The Cache cannot be null!";
      this.cache = cache;
      this.distributedMember = distributedMember;
      this.functionExecutor = functionExecutor;
    }

    @Override
    protected Cache getCache() {
      return this.cache;
    }

    @Override
    protected Set<DistributedMember> getMembers(final Cache cache) {
      assertSame(getCache(), cache);
      return Collections.singleton(this.distributedMember);
    }

    @Override
    protected Execution getMembersFunctionExecutor(final Set<DistributedMember> members) {
      Assert.assertNotNull(members);
      return this.functionExecutor;
    }

    @Override
    protected Set<DistributedMember> getNormalMembers(final Cache cache) {
      assertSame(getCache(), cache);
      return Collections.singleton(this.distributedMember);
    }

    @Override
    protected Set<DistributedMember> getGroupMembers(String[] groups) {
      Set<DistributedMember> dm = new HashSet<DistributedMember>();
      dm.add(distributedMember);
      return dm;

    }
  }

}

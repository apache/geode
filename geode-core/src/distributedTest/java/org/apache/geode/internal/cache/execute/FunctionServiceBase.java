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

import static org.apache.geode.test.dunit.Wait.pause;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

/*
 * Base class for tests of FunctionService that are agnostic to the type of Execution that they are
 * running on. The goal is to completely cover all common behavior of sending results and sending
 * exceptions here and have them run with all topologies in child classes.
 */
public abstract class FunctionServiceBase extends JUnit4CacheTestCase {

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  protected transient CustomCollector customCollector;

  @Before
  public void createCollector() {
    customCollector = new CustomCollector();
  }

  /**
   * Return the execution used to execute functions for this test. Subclasses should override this
   * to provide a specific execution, for example onMember.
   */
  public abstract Execution getExecution();

  /**
   * Return the number of members the function is expected to execute on
   */
  public abstract int numberOfExecutions();


  @Override
  public Properties getDistributedSystemProperties() {
    Properties result = super.getDistributedSystemProperties();
    result.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.internal.cache.execute.**;org.apache.geode.test.dunit.**");
    return result;
  }



  @Test
  public void functionContextGetCacheIsNotNullAndOpen() {
    ResultCollector rc = getExecution().execute((context) -> {
      Cache cache = context.getCache();
      assertNotNull(cache);
      assertFalse(cache.isClosed());
      context.getResultSender().lastResult("done");
    });
    List<String> results = (List<String>) rc.getResult();
    assertEquals(numberOfExecutions(), results.size());
    results.stream().forEach(element -> assertEquals("done", element));
  }

  @Test
  public void defaultCollectorReturnsSingleResult() {
    ResultCollector rc = getExecution().execute((context) -> {
      context.getResultSender().lastResult("done");
    });
    List<String> results = (List<String>) rc.getResult();
    assertEquals(numberOfExecutions(), results.size());
    results.stream().forEach(element -> assertEquals("done", element));
  }

  @Test()
  public void defaultCollectorReturnsAllIntermediateResults() {
    ResultCollector rc = getExecution().execute((context) -> {
      context.getResultSender().sendResult("one");
      context.getResultSender().lastResult("two");
    });
    final List<String> result = (List<String>) rc.getResult();
    assertEquals(numberOfExecutions(), result.stream().filter(s -> s.equals("one")).count());
    assertEquals(numberOfExecutions(), result.stream().filter(s -> s.equals("two")).count());
  }

  @Test()
  public void defaultCollectorThrowsExceptionAfterFunctionThrowsIllegalState() {
    IgnoredException.addIgnoredException("java.lang.IllegalStateException");
    // GEODE-1762 - clients throw from execute, but peers throw from rc.getResult
    thrown.expect(FunctionException.class);
    // GEODE-1762 - clients wrap cause in a ServerOperationException
    // thrown.expectCause(isA(IllegalStateException.class));
    ResultCollector rc = getExecution().execute((context) -> {
      throw new IllegalStateException();
    });
    final Object result = rc.getResult();
  }

  @Test()
  public void defaultCollectorThrowsExceptionAfterFunctionThrowsFunctionException() {
    // GEODE-1762 - clients throw from execute, but peers throw from rc.getResult
    thrown.expect(FunctionException.class);
    ResultCollector rc = getExecution().execute((context) -> {
      throw new FunctionException();
    });
    final Object result = rc.getResult();
  }

  /**
   * Tests what happens if a function returns an exception as a result. This is kind a weird, but it
   * seems that the default collector will just throw it as an exception
   */
  @Test()
  public void defaultCollectorThrowsExceptionAfterFunctionReturnsIllegalStateException() {
    // GEODE-1762 - clients throw from execute, but peers throw from rc.getResult
    // GEODE-1762 - clients throw a ServerOperationException
    thrown.expect(Exception.class);
    // thrown.expect(FunctionException.class);
    // thrown.expectCause(isA(IllegalStateException.class));
    ResultCollector rc = getExecution().execute((context) -> {
      context.getResultSender().lastResult(new IllegalStateException());
    });
    final Object result = rc.getResult();
  }

  @Test
  public void defaultCollectorThrowsExceptionAfterFunctionReturnsFunctionException() {
    // GEODE-1762 - clients throw from execute, but peers throw from rc.getResult
    thrown.expect(FunctionException.class);
    thrown.expectCause(is((Throwable) null));
    ResultCollector rc = getExecution().execute((context) -> {
      context.getResultSender().lastResult(new FunctionException());
    });
    final Object result = rc.getResult();
  }

  @Test
  public void defaultCollectorThrowsExceptionAfterFunctionReturnsIllegalStateExceptionAsIntermediateResult() {
    // GEODE-1762 - clients throw from execute, but peers throw from rc.getResult
    // GEODE-1762 - client throws a ServerOperationException
    thrown.expect(Exception.class);
    // thrown.expect(FunctionException.class);
    // thrown.expectCause(isA(IllegalStateException.class));
    ResultCollector rc = getExecution().execute((context) -> {
      context.getResultSender().sendResult(new IllegalStateException());
      context.getResultSender().lastResult("done");
    });
    final Object result = rc.getResult();
  }

  @Test
  public void defaultCollectorThrowsExceptionAfterFunctionReturnsFunctionExceptionAsIntermediateResult() {
    // GEODE-1762 - clients throw from execute, but peers throw from rc.getResult
    thrown.expect(FunctionException.class);
    thrown.expectCause(is((Throwable) null));
    ResultCollector rc = getExecution().execute((context) -> {
      context.getResultSender().sendResult(new FunctionException());
      context.getResultSender().lastResult("done");
    });
    final Object result = rc.getResult();
  }

  @Test
  public void defaultCollectorReturnsResultOfSendException() {
    ResultCollector rc = getExecution().execute((context) -> {
      context.getResultSender().sendException(new IllegalStateException());
    });
    final List<Object> result = (List<Object>) rc.getResult();
    assertEquals(numberOfExecutions(), result.size());
    result.stream()
        .forEach(element -> assertEquals(IllegalStateException.class, element.getClass()));
  }

  @Test
  public void defaultCollectorReturnsResultOfSendFunctionException() {
    ResultCollector rc = getExecution().execute((context) -> {
      context.getResultSender().sendException(new FunctionException());
    });
    final List<Object> result = (List<Object>) rc.getResult();
    assertEquals(numberOfExecutions(), result.size());
    result.stream().forEach(element -> assertEquals(FunctionException.class, element.getClass()));
  }

  @Test
  public void customCollectorDoesNotSeeExceptionFunctionThrowsIllegalState() {
    // GEODE-1762 - clients throw from execute, but peers throw from rc.getResult
    IgnoredException.addIgnoredException("java.lang.IllegalStateException");
    try {
      ResultCollector rc = getExecution().withCollector(customCollector).execute((context) -> {
        throw new IllegalStateException();
      });
      rc.getResult();
      fail("should have received an exception");
    } catch (FunctionException expected) {
    }

    Assert.assertEquals(0, customCollector.getResult().size());
  }

  @Test
  public void customCollectorDoesNotSeeExceptionFunctionThrowsFunctionException() {
    // GEODE-1762 - clients throw from execute, but peers throw from rc.getResult
    try {
      ResultCollector rc = getExecution().withCollector(customCollector).execute((context) -> {
        throw new FunctionException();
      });
      rc.getResult();
      fail("should have received an exception");
    } catch (FunctionException expected) {
    }

    Assert.assertEquals(0, customCollector.getResult().size());
  }

  @Test
  public void customCollectorDoesNotSeeExceptionAfterFunctionReturnsIllegalStateException() {
    // GEODE-1762 - clients throw from execute, but peers throw from rc.getResult
    try {
      ResultCollector rc = getExecution().execute((context) -> {
        context.getResultSender().lastResult(new IllegalStateException());
      });
      rc.getResult();
      fail("should have received an exception");
      // GEODE-1762 - clients throw a ServerOperationException
    } catch (Exception expected) {
    }
    Assert.assertEquals(0, customCollector.getResult().size());
  }

  @Test
  public void customCollectorDoesNotSeeExceptionAfterFunctionReturnsIllegalStateExceptionAsIntermediateResult() {
    // GEODE-1762 - clients throw from execute, but peers throw from rc.getResult
    try {
      ResultCollector rc = getExecution().execute((context) -> {
        context.getResultSender().sendResult(new IllegalStateException());
        context.getResultSender().lastResult("done");
      });
      rc.getResult();
      fail("should have received an exception");
      // GEODE-1762 - clients throw a ServerOperationException
    } catch (Exception expected) {
    }
    Assert.assertEquals(0, customCollector.getResult().size());
  }

  @Test
  public void customCollectorReturnsResultOfSendException() {
    ResultCollector rc = getExecution().withCollector(customCollector).execute((context) -> {
      context.getResultSender().sendException(new IllegalStateException());
    });
    final List<Object> result = (List<Object>) rc.getResult();
    assertEquals(numberOfExecutions(), result.size());
    result.stream()
        .forEach(element -> assertEquals(IllegalStateException.class, element.getClass()));
    assertEquals(result, customCollector.getResult());
  }

  @Test
  public void customCollectorReturnsResultOfSendFunctionException() {
    ResultCollector rc = getExecution().withCollector(customCollector).execute((context) -> {
      context.getResultSender().sendException(new FunctionException());
    });
    final List<Object> result = (List<Object>) rc.getResult();
    assertEquals(numberOfExecutions(), result.size());
    result.stream().forEach(element -> assertEquals(FunctionException.class, element.getClass()));
    assertEquals(result, customCollector.getResult());
  }

  /**
   * Test that a custom result collector will still receive all partial results from other members
   * when one member fails
   */
  @Test
  public void nonHAFunctionResultCollectorIsPassedPartialResultsAfterCloseCache() {
    List<InternalDistributedMember> members = getAllMembers();

    InternalDistributedMember firstMember = members.iterator().next();

    // Execute a function which will close the cache on one member.
    try {
      ResultCollector rc = getExecution().withCollector(customCollector)
          .execute(new CacheClosingNonHAFunction(firstMember));
      rc.getResult();
      fail("Should have thrown an exception");
    } catch (Exception expected) {
      // do nothing
    }
    members.remove(firstMember);
    assertEquals(members, customCollector.getResult());
    assertEquals(numberOfExecutions() - 1, customCollector.getResult().size());
  }

  /**
   * Test the a result collector will timeout using the timeout provided
   */
  @Test
  public void resultCollectorHonorsFunctionTimeout() throws InterruptedException {
    Function sleepingFunction = context -> {
      try {
        long endTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!context.getCache().isClosed() && System.nanoTime() < endTime) {
          Thread.sleep(10);
        }
      } catch (InterruptedException e) {
        // exit
      }
      context.getResultSender().sendResult("FAILED");
    };

    ResultCollector collector = getExecution().execute(sleepingFunction);
    thrown.expect(FunctionException.class);
    collector.getResult(1, TimeUnit.SECONDS);
  }

  protected List<InternalDistributedMember> getAllMembers() {
    // Get a list of all of the members
    ResultCollector rs = getExecution().execute(functionContext -> {
      functionContext.getResultSender()
          .lastResult(InternalDistributedSystem.getAnyInstance().getDistributedMember());
    });
    return (List<InternalDistributedMember>) rs.getResult();
  }

  public static class CustomCollector implements ResultCollector<Object, List<Object>> {
    private final ArrayList<Object> results = new ArrayList<>();

    @Override
    public List<Object> getResult() throws FunctionException {
      return results;
    }

    @Override
    public List<Object> getResult(final long timeout, final TimeUnit unit)
        throws FunctionException, InterruptedException {
      return results;
    }

    @Override
    public void addResult(final DistributedMember memberID, final Object resultOfSingleExecution) {
      results.add(resultOfSingleExecution);
    }

    @Override
    public void endResults() {}

    @Override
    public void clearResults() {
      results.clear();
    }
  }

  /**
   * A function which will close the cache if the given member matches the member executing this
   * function
   */
  public static class CacheClosingNonHAFunction implements Function, DataSerializable {

    private InternalDistributedMember member;

    public CacheClosingNonHAFunction() {} // for serialization

    public CacheClosingNonHAFunction(final InternalDistributedMember member) {
      this.member = member;
    }

    @Override
    public void execute(FunctionContext context) {
      final InternalDistributedMember myId =
          InternalDistributedSystem.getAnyInstance().getDistributedMember();
      if (myId.equals(member)) {
        CacheFactory.getAnyInstance().close();
        throw new CacheClosedException();
      }
      pause(1000);
      context.getResultSender().lastResult(myId);
    }

    @Override
    public boolean isHA() {
      return false;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeObject(member, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      member = DataSerializer.readObject(in);
    }
  }

}

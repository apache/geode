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
package org.apache.geode.cache30;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRule;

/**
 * This class tests various search load and write scenarios for distributed regions
 */

public class SearchAndLoadDUnitTest extends JUnit4CacheTestCase {

  private static boolean loaderInvoked;
  private static boolean remoteLoaderInvoked;
  private static int remoteLoaderInvokedCount;
  private static boolean netWriteInvoked;
  private static boolean operationWasCreate;
  private static boolean originWasRemote;
  private static int writerInvocationCount;

  private static final CountDownLatch readyForExceptionLatch = new CountDownLatch(1);
  private static final CountDownLatch loaderInvokedLatch = new CountDownLatch(1);
  private VM vm0;
  private VM vm1;
  private VM vm2;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Before
  public void setup() {
    vm0 = VM.getVM(0);
    vm1 = VM.getVM(1);
    vm2 = VM.getVM(2);
  }


  /**
   * Returns region attributes for a <code>GLOBAL</code> region
   */
  protected <K, V> RegionAttributes<K, V> getRegionAttributes() {
    AttributesFactory<K, V> factory = new AttributesFactory<>();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    return factory.create();
  }

  @Test
  public void testNetSearch() throws CacheException {
    final String name = getUniqueName() + "-ACK";
    final String objectName = "NetSearchKey";
    final Integer value = 440;

    vm0.invoke("Create ACK Region", new SerializableRunnable() {
      @Override
      public void run() {
        try {
          RegionFactory<Object, Object> factory = getCache().createRegionFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setStatisticsEnabled(true);
          Region<Object, Object> region = createRegion(name, factory);
          region.create(objectName, null);
        } catch (CacheException ex) {
          fail("While creating ACK region", ex);
        }
      }
    });

    vm1.invoke("Create ACK Region", new SerializableRunnable() {
      @Override
      public void run() {
        try {

          RegionFactory<Object, Object> factory = getCache().createRegionFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setStatisticsEnabled(true);
          Region<Object, Object> region = createRegion(name, factory);
          region.put(objectName, value);
        } catch (CacheException ex) {
          fail("While creating ACK region", ex);
        }
      }
    });

    vm2.invoke("Create ACK Region", new SerializableRunnable() {
      @Override
      public void run() {
        try {
          RegionFactory<Object, Object> factory = getCache().createRegionFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setStatisticsEnabled(true);
          Region<Object, Object> region = createRegion(name, factory);
          region.create(objectName, null);
        } catch (CacheException ex) {
          fail("While creating ACK region", ex);
        }
      }
    });

    vm0.invoke("Get a value", new SerializableRunnable() {
      @Override
      public void run() {
        try {
          Object result = getRootRegion().getSubregion(name).get(objectName);
          assertThat(value).isEqualTo(result);
        } catch (CacheLoaderException | TimeoutException cle) {
          fail("While Get a value", cle);
        }
      }
    });
  }


  /**
   * This test is for a bug in which a cache loader threw an exception that caused the wrong value
   * to be put in a Future in nonTxnFindObject. This in turn caused a concurrent search for the
   * object to not invoke the loader a second time.
   * <p>
   * VM0 is used to create a cache and a region having a loader that simulates the conditions that
   * caused the bug. One async thread then does a get() which invokes the loader. Another async
   * thread does a get() which reaches nonTxnFindObject and blocks waiting for the first thread's
   * load to complete. The loader then throws an exception that is sent back to the first thread.
   * The second thread should then cause the loader to be invoked again, and this time the loader
   * will return a value. Both threads then validate that they received the expected result.
   */
  @Test
  public void testConcurrentLoad() throws Throwable {
    final String name = getUniqueName() + "Region";
    final String objectName = "theKey";
    final Integer value = 44;
    final String exceptionString = "causing first cache-load to fail";

    remoteLoaderInvoked = false;
    loaderInvoked = false;

    vm0.invoke("create region " + name + " in vm0", new CacheSerializableRunnable() {
      @Override
      public void run2() {
        remoteLoaderInvoked = false;
        loaderInvoked = false;
        RegionFactory<Object, Object> factory = getCache().createRegionFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setConcurrencyChecksEnabled(true);
        factory.setCacheLoader(new CacheLoader<Object, Object>() {
          boolean firstInvocation = true;

          @Override
          public synchronized Object load(LoaderHelper helper) {
            System.out.println("invoked cache loader for " + helper.getKey());
            loaderInvoked = true;
            loaderInvokedLatch.countDown();
            if (firstInvocation) {
              firstInvocation = false;
              try {
                // wait for both threads to be ready for the exception to be thrown
                System.out.println("waiting for vm0t2 to be ready before throwing exception");
                readyForExceptionLatch.await(30, TimeUnit.SECONDS);
                // give the second thread time to get into loader code
                Thread.sleep(5000);
              } catch (InterruptedException e) {
                fail("interrupted");
              }
              System.out.println("throwing exception");
              throw new RuntimeException(exceptionString);
            }
            System.out.println("returning value=" + value);
            return value;
          }

          @Override
          public void close() {}
        });

        Region<Object, Object> region = createRegion(name, factory);
        region.create(objectName, null);
        IgnoredException.addIgnoredException(exceptionString);
      }
    });

    AsyncInvocation async1 = null;
    try {
      async1 = vm0.invokeAsync("Concurrently invoke the remote loader on the same key - t1",
          new CacheSerializableRunnable() {
            @Override
            public void run2() {
              Region<Object, Object> region = getCache().getRegion("root" + SEPARATOR + name);

              logger.info("t1 is invoking get(" + objectName + ")");
              try {
                logger.info("t1 retrieved value " + region.get(objectName));
                fail("first load should have triggered an exception");
              } catch (RuntimeException e) {
                if (!e.getMessage().contains(exceptionString)) {
                  throw e;
                }
              }
            }
          });

      vm0.invoke(
          "Concurrently invoke the loader on the same key - t2", new CacheSerializableRunnable() {
            @Override
            public void run2() {
              final Region<Object, Object> region = getCache().getRegion("root" + SEPARATOR + name);
              final Object[] valueHolder = new Object[1];

              // wait for vm1 to cause the loader to be invoked
              logger.info("t2 is waiting for loader to be invoked by t1");
              try {
                loaderInvokedLatch.await(30, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                fail("interrupted");
              }
              await().until(() -> loaderInvoked);

              Thread t = new Thread("invoke get()") {
                @Override
                public void run() {
                  try {
                    valueHolder[0] = region.get(objectName);
                  } catch (RuntimeException e) {
                    valueHolder[0] = e;
                  }
                }
              };

              t.setDaemon(true);
              t.start();
              try {
                // let the thread get to the point of blocking on vm1's Future
                // in LocalRegion.nonTxnFindObject()
                Thread.sleep(5000);
              } catch (InterruptedException e) {
                fail("interrupted");
              }

              readyForExceptionLatch.countDown();
              try {
                t.join(30000);
              } catch (InterruptedException e) {
                fail("interrupted");
              }
              if (t.isAlive()) {
                t.interrupt();
                fail("get() operation blocked for too long - test needs some work");
              }

              logger.info("t2 is invoking get(" + objectName + ")");
              Object value = valueHolder[0];
              if (value instanceof RuntimeException) {
                if (((Exception) value).getMessage().contains(exceptionString)) {
                  fail("second load should not have thrown an exception");
                } else {
                  throw (RuntimeException) value;
                }
              } else {
                logger.info("t2 retrieved value " + value);
                assertThat(value).isNotNull();
              }
            }
          });
    } finally {
      if (async1 != null) {
        async1.get();
      }
    }
  }

  @Test
  public void testNetLoadNoLoaders() throws CacheException {
    final String name = getUniqueName() + "-ACK";
    final String objectName = "B";
    SerializableRunnable create = new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        RegionFactory<Object, Object> factory = getCache().createRegionFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        createRegion(name, factory);
      }
    };

    vm0.invoke("Create Region", create);
    vm1.invoke("Create Region", create);

    vm0.invoke("Get with No Loaders defined", new SerializableRunnable() {
      @Override
      public void run() {
        try {
          Object result = getRootRegion().getSubregion(name).get(objectName);
          assertThat(result).isNull();
        } catch (CacheLoaderException | TimeoutException cle) {
          fail("While getting value for ACK region", cle);
        }
      }
    });
  }

  @Test
  public void testNetLoad() throws CacheException {
    disconnectAllFromDS();
    final String name = getUniqueName() + "-ACK";
    final String objectName = "B";
    final Integer value = 43;
    loaderInvoked = false;
    remoteLoaderInvoked = false;
    vm0.invoke("Create ACK Region", () -> {
      try {
        loaderInvoked = false;
        RegionFactory<Object, Object> factory = getCache().createRegionFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);

        Region<Object, Object> region = createRegion(name, factory);
        region.create(objectName, null);

      } catch (CacheException ex) {
        fail("While creating ACK region", ex);
      }
    });

    vm1.invoke("Create ACK Region", () -> {
      try {
        RegionFactory<Object, Object> factory = getCache().createRegionFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setCacheLoader(new CacheLoader<Object, Object>() {
          @Override
          public Object load(LoaderHelper helper) {
            remoteLoaderInvoked = true;
            return value;
          }

          @Override
          public void close() {

          }
        });
        createRegion(name, factory);
      } catch (CacheException ex) {
        fail("While creating ACK region", ex);
      }
    });

    vm0.invoke("Get a value from remote loader", () -> {
      for (int i = 0; i < 1; i++) {
        try {
          Object result = getRootRegion().getSubregion(name).get(objectName);
          assertThat(value).isEqualTo(result);
          assertThat(loaderInvoked).isEqualTo(Boolean.FALSE);

        } catch (CacheLoaderException | TimeoutException cle) {
          fail("While getting value for ACK region", cle);

        }
      }
    });
  }

  /**
   * Confirm that a netLoad that returns null will NOT allow other netLoad methods to be called.
   */
  @Test
  public void testEmptyNetLoad() throws CacheException {
    disconnectAllFromDS();

    final String name = getUniqueName() + "-ACK";
    final String objectName = "B";
    loaderInvoked = false;
    remoteLoaderInvoked = false;
    remoteLoaderInvokedCount = 0;
    vm0.invoke("Create ACK Region", () -> {
      loaderInvoked = false;
      remoteLoaderInvoked = false;
      remoteLoaderInvokedCount = 0;
      try {
        RegionFactory<Object, Object> factory = getCache().createRegionFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);

        Region<Object, Object> region = createRegion(name, factory);
        region.create(objectName, null);
      } catch (CacheException ex) {
        fail("While creating ACK region", ex);
      }
    });

    SerializableRunnable installLoader = new SerializableRunnable() {
      @Override
      public void run() {
        loaderInvoked = false;
        remoteLoaderInvoked = false;
        remoteLoaderInvokedCount = 0;
        try {
          RegionFactory<Object, Object> factory = getCache().createRegionFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setCacheLoader(new CacheLoader<Object, Object>() {
            @Override
            public Object load(LoaderHelper helper) {
              remoteLoaderInvoked = true;
              remoteLoaderInvokedCount++;
              return null;
            }

            @Override
            public void close() {

            }
          });
          createRegion(name, factory);
        } catch (CacheException ex) {
          fail("While creating ACK region", ex);
        }
      }
    };

    vm1.invoke("Create ACK Region", installLoader);
    vm2.invoke("Create ACK Region", installLoader);
    vm0.invoke("Get a value from remote loader", () -> {
      for (int i = 0; i < 1; i++) {
        try {
          Object result = getRootRegion().getSubregion(name).get(objectName);
          assertThat(result).isNull();
          assertThat(loaderInvoked).isFalse();

        } catch (CacheLoaderException | TimeoutException cle) {
          fail("While getting value for ACK region", cle);
        }
      }
    });

    // we only invoke one netLoad loader even when they return null.
    boolean xor = vmRemoteLoaderInvoked(vm1) ^ vmRemoteLoaderInvoked(vm2);
    assertThat(xor).describedAs(
        "vm1=" + vmRemoteLoaderInvoked(vm1) + " vm2=" + vmRemoteLoaderInvoked(vm2) + " vm1Count="
            + vmRemoteLoaderInvokedCount(vm1) + " vm2Count=" + vmRemoteLoaderInvokedCount(vm2))
        .isTrue();
    int total = vmRemoteLoaderInvokedCount(vm1) + vmRemoteLoaderInvokedCount(vm2);
    assertThat(total)
        .describedAs(
            "vm1=" + vmRemoteLoaderInvokedCount(vm1) + " vm2=" + vmRemoteLoaderInvokedCount(vm2))
        .isEqualTo(1);
  }

  private static boolean vmRemoteLoaderInvoked(VM vm) {
    return vm.invoke(SearchAndLoadDUnitTest::fetchRemoteLoaderInvoked);
  }

  private static int vmRemoteLoaderInvokedCount(VM vm) {
    return vm.invoke(SearchAndLoadDUnitTest::fetchRemoteLoaderInvokedCount);
  }

  private static Boolean fetchRemoteLoaderInvoked() {
    return remoteLoaderInvoked;
  }

  private static Integer fetchRemoteLoaderInvokedCount() {
    return remoteLoaderInvokedCount;
  }

  @Test
  public void testLocalLoad() throws CacheException {
    final String name = getUniqueName() + "-ACK";
    final String objectName = "C";
    final Integer value = 44;
    remoteLoaderInvoked = false;
    loaderInvoked = false;
    vm0.invoke("Create ACK Region", () -> {
      remoteLoaderInvoked = false;
      loaderInvoked = false;
      try {
        RegionFactory<Object, Object> factory = getCache().createRegionFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setCacheLoader(new CacheLoader<Object, Object>() {
          @Override
          public Object load(LoaderHelper helper) {
            loaderInvoked = true;
            return value;
          }

          @Override
          public void close() {}
        });

        Region<Object, Object> region = createRegion(name, factory);
        region.create(objectName, null);

      } catch (CacheException ex) {
        fail("While creating ACK region", ex);
      }
    });

    vm1.invoke("Create ACK Region", () -> {
      remoteLoaderInvoked = false;
      loaderInvoked = false;
      try {
        RegionFactory<Object, Object> factory = getCache().createRegionFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setCacheLoader(new CacheLoader<Object, Object>() {
          @Override
          public Object load(LoaderHelper helper) {
            remoteLoaderInvoked = true;
            return value;
          }

          @Override
          public void close() {}
        });

        createRegion(name, factory);
      } catch (CacheException ex) {
        fail("While creating ACK region", ex);
      }
    });

    vm0.invoke("Get a value from local loader", () -> {
      try {
        Object result = getRootRegion().getSubregion(name).get(objectName);
        assertThat(value).isEqualTo(result);
        assertThat(loaderInvoked).isEqualTo(Boolean.TRUE);
        assertThat(remoteLoaderInvoked).isEqualTo(Boolean.FALSE);

      } catch (CacheLoaderException | TimeoutException ignored) {
      }
    });
  }

  @Test
  public void testNetWrite() throws CacheException {
    final String name = getUniqueName() + "-ACK";
    final String objectName = "Gemfire7";
    final Integer value = 483;

    vm0.invoke("Create ACK Region with cacheWriter", () -> {
      netWriteInvoked = false;
      try {
        RegionFactory<Object, Object> factory = getCache().createRegionFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setCacheWriter(new CacheWriter<Object, Object>() {
          @Override
          public void beforeCreate(EntryEvent e) throws CacheWriterException {
            netWriteInvoked = true;
          }

          @Override
          public void beforeUpdate(EntryEvent e) throws CacheWriterException {
            netWriteInvoked = true;
          }

          @Override
          public void beforeDestroy(EntryEvent e) throws CacheWriterException {}

          @Override
          public void beforeRegionDestroy(RegionEvent e) throws CacheWriterException {}

          @Override
          public void beforeRegionClear(RegionEvent e) throws CacheWriterException {}

          @Override
          public void close() {}
        });

        createRegion(name, factory);

      } catch (CacheException ex) {
        fail("While creating ACK region", ex);
      }
    });

    vm1.invoke("Create ACK Region", () -> {
      loaderInvoked = false;
      remoteLoaderInvoked = false;
      netWriteInvoked = false;
      try {
        RegionFactory<Object, Object> factory = getCache().createRegionFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        createRegion(name, factory);
      } catch (CacheException ex) {
        fail("While creating ACK region", ex);
      }
    });

    vm1.invoke("Do a put operation resulting in cache writer notification in other vm",
        () -> {
          try {
            getRootRegion().getSubregion(name).put(objectName, value);
            try {
              Object result = getRootRegion().getSubregion(name).get(objectName);
              assertThat(result).isEqualTo(value);
            } catch (CacheLoaderException | TimeoutException ignored) {
            }
          } catch (CacheWriterException | TimeoutException ignored) {

          }
        });

    vm0.invoke("ensure that cache writer was invoked", new SerializableRunnable() {
      @Override
      public void run() {
        assertThat(netWriteInvoked).describedAs("expected cache writer to be invoked").isTrue();
      }
    });
  }

  @Test
  public void testOneHopNetWrite() throws CacheException {
    final String name = getUniqueName() + "Region";
    final String objectName = "Object7";
    final Integer value = 483;
    final Integer updateValue = 484;

    vm0.invoke("Create replicated region with cacheWriter", () -> {
      netWriteInvoked = false;
      operationWasCreate = false;
      originWasRemote = false;
      writerInvocationCount = 0;
      try {
        RegionFactory<Object, Object> factory = getCache().createRegionFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        factory.setCacheWriter(new CacheWriter<Object, Object>() {
          @Override
          public void beforeCreate(EntryEvent e) throws CacheWriterException {
            logger
                .info("cache writer beforeCreate invoked for " + e);
            netWriteInvoked = true;
            operationWasCreate = true;
            originWasRemote = e.isOriginRemote();
            writerInvocationCount++;
          }

          @Override
          public void beforeUpdate(EntryEvent e) throws CacheWriterException {
            logger
                .info("cache writer beforeUpdate invoked for " + e);
            netWriteInvoked = true;
            operationWasCreate = false;
            originWasRemote = e.isOriginRemote();
            writerInvocationCount++;
          }

          @Override
          public void beforeDestroy(EntryEvent e) throws CacheWriterException {}

          @Override
          public void beforeRegionDestroy(RegionEvent e) throws CacheWriterException {}

          @Override
          public void beforeRegionClear(RegionEvent e) throws CacheWriterException {}

          @Override
          public void close() {}
        });

        createRegion(name, factory);

      } catch (CacheException ex) {
        fail("While creating replicated region", ex);
      }
    });

    vm1.invoke("Create empty Region", () -> {
      try {
        RegionFactory<Object, Object> factory = getCache().createRegionFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setDataPolicy(DataPolicy.EMPTY);
        createRegion(name, factory);
      } catch (CacheException ex) {
        fail("While creating empty region", ex);
      }
    });

    vm1.invoke("do a put that should be proxied in the other vm and invoke its cache writer",
        () -> {
          try {
            getRootRegion().getSubregion(name).put(objectName, value);
          } catch (CacheWriterException | TimeoutException ignored) {
          }
        });

    vm0.invoke("ensure that cache writer was invoked with correct settings in event",
        () -> {
          assertThat(netWriteInvoked).describedAs("expected cache writer to be invoked").isTrue();
          assertThat(originWasRemote).describedAs("expected originRemote to be true").isTrue();
          assertThat(operationWasCreate).describedAs("expected event to be create").isTrue();
          assertThat(writerInvocationCount)
              .describedAs("expected only one cache writer invocation").isEqualTo(1);
          // set flags for the next test - updating the same key
          netWriteInvoked = false;
          writerInvocationCount = 0;
        });

    vm1.invoke("do an update that should be proxied in the other vm and invoke its cache writer",
        () -> {
          try {
            getRootRegion().getSubregion(name).put(objectName, updateValue);
          } catch (CacheWriterException | TimeoutException ignored) {
          }
        });

    vm0.invoke("ensure that cache writer was invoked with correct settings in event",
        () -> {
          assertThat(netWriteInvoked).describedAs("expected cache writer to be invoked").isTrue();
          assertThat(originWasRemote).describedAs("expected originRemote to be true").isTrue();
          assertThat(operationWasCreate).describedAs("expected event to be create").isTrue();
          assertThat(writerInvocationCount)
              .describedAs("expected only one cache writer invocation").isEqualTo(1);
        });
  }


  /**
   * same as the previous test but the cache writer is in a third, non-replicated, vm
   */
  @Test
  public void testOneHopNetWriteRemoteWriter() throws CacheException {
    final String name = getUniqueName() + "Region";
    final String objectName = "Object7";
    final Integer value = 483;
    final Integer updateValue = 484;

    vm0.invoke("Create replicate Region", () -> {
      try {
        RegionFactory<Object, Object> factory = getCache().createRegionFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        createRegion(name, factory);
      } catch (CacheException ex) {
        fail("While creating empty region", ex);
      }
    });

    vm1.invoke("Create empty Region", () -> {
      try {
        RegionFactory<Object, Object> factory = getCache().createRegionFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setDataPolicy(DataPolicy.EMPTY);
        createRegion(name, factory);
      } catch (CacheException ex) {
        fail("While creating empty region", ex);
      }
    });

    vm2.invoke("Create replicated region with cacheWriter", () -> {
      netWriteInvoked = false;
      operationWasCreate = false;
      originWasRemote = false;
      writerInvocationCount = 0;
      try {
        RegionFactory<Object, Object> factory = getCache().createRegionFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setDataPolicy(DataPolicy.EMPTY);
        factory.setCacheWriter(new CacheWriter<Object, Object>() {
          @Override
          public void beforeCreate(EntryEvent e) throws CacheWriterException {
            logger.info("cache writer beforeCreate invoked for " + e);
            netWriteInvoked = true;
            operationWasCreate = true;
            originWasRemote = e.isOriginRemote();
            writerInvocationCount++;
          }

          @Override
          public void beforeUpdate(EntryEvent e) throws CacheWriterException {
            logger.info("cache writer beforeUpdate invoked for " + e);
            netWriteInvoked = true;
            operationWasCreate = false;
            originWasRemote = e.isOriginRemote();
            writerInvocationCount++;
          }

          @Override
          public void beforeDestroy(EntryEvent e) throws CacheWriterException {}

          @Override
          public void beforeRegionDestroy(RegionEvent e) throws CacheWriterException {}

          @Override
          public void beforeRegionClear(RegionEvent e) throws CacheWriterException {}

          @Override
          public void close() {}
        });

        createRegion(name, factory);

      } catch (CacheException ex) {
        fail("While creating replicated region", ex);
      }
    });

    vm1.invoke("do a put that should be proxied in the other vm and invoke its cache writer",
        () -> {
          try {
            getRootRegion().getSubregion(name).put(objectName, value);
          } catch (CacheWriterException | TimeoutException ignored) {
          }
        });

    vm2.invoke("ensure that cache writer was invoked with correct settings in event",
        () -> {
          assertThat(netWriteInvoked).describedAs("expected cache writer to be invoked").isTrue();
          assertThat(originWasRemote).describedAs("expected originRemote to be true").isTrue();
          assertThat(operationWasCreate).describedAs("expected event to be create").isTrue();
          assertThat(writerInvocationCount)
              .describedAs("expected only one cache writer invocation").isEqualTo(1);
          // set flags for the next test - updating the same key
          netWriteInvoked = false;
          writerInvocationCount = 0;
        });

    vm1.invoke("do an update that should be proxied in the other vm and invoke its cache writer",
        () -> {
          try {
            getRootRegion().getSubregion(name).put(objectName, updateValue);
          } catch (CacheWriterException | TimeoutException ignored) {
          }
        });

    vm2.invoke("ensure that cache writer was invoked with correct settings in event",
        () -> {
          assertThat(netWriteInvoked).describedAs("expected cache writer to be invoked").isTrue();
          assertThat(originWasRemote).describedAs("expected originRemote to be true").isTrue();
          assertThat(operationWasCreate).describedAs("expected event to be create").isTrue();
          assertThat(writerInvocationCount)
              .describedAs("expected only one cache writer invocation").isEqualTo(1);
        });
  }
}

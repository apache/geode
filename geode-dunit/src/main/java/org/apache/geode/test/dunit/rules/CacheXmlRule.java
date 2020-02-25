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
package org.apache.geode.test.dunit.rules;

import static org.apache.geode.test.dunit.VM.DEFAULT_VM_COUNT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.rules.TemporaryFolder;

import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

/**
 * Used to generate a cache.xml. Basically just a {@code CacheCreation} with a few more methods
 * implemented.
 */
@SuppressWarnings("serial")
public class CacheXmlRule extends AbstractDistributedRule {

  private static final String BEFORE = "before";
  private static final String AFTER = "after";

  private static final CacheXmlCreation DUMMY = mock(CacheXmlCreation.class);
  private static final AtomicReference<ICacheXmlCreation> DELEGATE = new AtomicReference<>(DUMMY);
  private static final SerializableRunnableIF NO_OP = mock(SerializableRunnableIF.class);

  private final AtomicReference<SerializableRunnableIF> createCache = new AtomicReference<>(NO_OP);

  private final SerializableTemporaryFolder temporaryFolder;

  public CacheXmlRule() {
    this(DEFAULT_VM_COUNT);
  }

  public CacheXmlRule(int vmCount) {
    this(vmCount, new SerializableTemporaryFolder());
  }

  private CacheXmlRule(int vmCount, SerializableTemporaryFolder temporaryFolder) {
    super(vmCount);
    this.temporaryFolder = temporaryFolder;
  }

  public CacheXmlRule cacheBuilder(SerializableRunnableIF createCache) {
    this.createCache.set(createCache);
    return this;
  }

  public void beginCacheXml() {
    DELEGATE.get().beginCacheXml();
  }

  public void finishCacheXml(String name) {
    DELEGATE.get().finishCacheXml(name);
  }

  public void finishCacheXml(File root, String name, boolean useSchema, String xmlVersion)
      throws IOException {
    DELEGATE.get().finishCacheXml(root, name, useSchema, xmlVersion);
  }

  public InternalCache getCache() {
    InternalCache cache = DELEGATE.get().getCache();
    assertThat(cache).isInstanceOf(CacheXmlCreation.class);
    return cache;
  }

  @Override
  public void before() throws Exception {
    Method method = TemporaryFolder.class.getDeclaredMethod(BEFORE);
    method.setAccessible(true);
    method.invoke(temporaryFolder);

    invoker().invokeInEveryVMAndController(() -> invokeBefore());
  }

  @Override
  public void after() {
    invoker().invokeInEveryVMAndController(() -> invokeAfter());

    try {
      Method method = TemporaryFolder.class.getDeclaredMethod(AFTER);
      method.setAccessible(true);
      method.invoke(temporaryFolder);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void afterCreateVM(VM vm) {
    vm.invoke(() -> invokeBefore());
  }

  @Override
  protected void afterBounceVM(VM vm) {
    vm.invoke(() -> invokeBefore());
  }

  private void invokeBefore() throws Exception {
    DELEGATE.set(new CacheXmlCreation(createCache.get(), temporaryFolder));
  }

  private void invokeAfter() {
    DELEGATE.set(DUMMY);
  }

  private interface ICacheXmlCreation {
    void beginCacheXml();

    void finishCacheXml(String name);

    void finishCacheXml(File root, String name, boolean useSchema, String xmlVersion)
        throws IOException;

    InternalCache getCache();

    void close();
  }

  private static class CacheXmlCreation extends CacheCreation implements ICacheXmlCreation {

    private final AtomicBoolean closed = new AtomicBoolean();

    private final SerializableRunnableIF createCache;
    private final SerializableTemporaryFolder temporaryFolder;

    CacheXmlCreation(SerializableRunnableIF createCache,
        SerializableTemporaryFolder temporaryFolder) {
      this.createCache = createCache;
      this.temporaryFolder = temporaryFolder;
    }

    /**
     * Sets this test up with a {@code CacheCreation} as its cache. Any existing cache is closed.
     * Whoever calls this must also call {@code finishCacheXml}.
     */
    @Override
    public void beginCacheXml() {
      // nothing
    }

    /**
     * Finish what {@code beginCacheXml} started. It does this be generating a cache.xml file and
     * then creating a real cache using that cache.xml.
     */
    @Override
    public void finishCacheXml(String name) {
      try {
        File file = temporaryFolder.newFile(name + "-cache.xml");
        PrintWriter printWriter = new PrintWriter(new FileWriter(file), true);
        CacheXmlGenerator.generate(getCache(), printWriter);
        printWriter.close();
        close();
        GemFireCacheImpl.testCacheXml = file;
        createCache.run();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      } finally {
        GemFireCacheImpl.testCacheXml = null;
      }
    }

    /**
     * Finish what {@code beginCacheXml} started. It does this be generating a cache.xml file and
     * then creating a real cache using that cache.xml.
     */
    @Override
    public void finishCacheXml(File root, String name, boolean useSchema, String xmlVersion)
        throws IOException {
      File dir = new File(root, "XML_" + xmlVersion);
      dir.mkdirs();
      File file = new File(dir, name + ".xml");
      PrintWriter printWriter = new PrintWriter(new FileWriter(file), true);
      CacheXmlGenerator.generate(getCache(), printWriter, useSchema, xmlVersion);
      printWriter.close();
      close();
      GemFireCacheImpl.testCacheXml = file;
      try {
        createCache.run();
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        GemFireCacheImpl.testCacheXml = null;
      }
    }

    @Override
    public InternalCache getCache() {
      return this;
    }

    @Override
    public void close() {
      closed.set(true);
    }

    @Override
    public boolean isClosed() {
      return closed.get();
    }
  }
}

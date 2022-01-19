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
package org.apache.geode.internal.cache.extension;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.internal.cache.xmlcache.XmlGenerator;
import org.apache.geode.test.fake.Fakes;

/**
 * Unit tests for {@link SimpleExtensionPoint}.
 *
 * @since GemFire 8.1
 */
public class SimpleExtensionPointJUnitTest {

  /**
   * Test method for {@link SimpleExtensionPoint#SimpleExtensionPoint(Extensible, Object)} .
   */
  @Test
  public void testSimpleExtensionPoint() {
    final MockImpl m = new MockImpl();
    assertSame(m.extensionPoint.extensible, m.extensionPoint.target);
    assertNotNull(m.extensionPoint.extensions);
    assertNotNull(m.extensionPoint.iterable);
  }

  /**
   * Test method for {@link SimpleExtensionPoint#getExtensions()} .
   */
  @Test
  public void testGetExtensions() {
    final MockImpl m = new MockImpl();

    assertEquals(0, m.extensionPoint.extensions.size());
    assertTrue(!m.extensionPoint.iterable.iterator().hasNext());

    final Iterable<Extension<MockInterface>> extensions = m.getExtensionPoint().getExtensions();
    assertNotNull(extensions);

    // extensions should be empty
    final Iterator<Extension<MockInterface>> iterator = extensions.iterator();
    assertTrue(!iterator.hasNext());
    try {
      iterator.next();
      fail("Expected NoSuchElementException.");
    } catch (NoSuchElementException e) {
      // ignore
    }
  }

  /**
   * Test method for {@link SimpleExtensionPoint#addExtension(Extension)} .
   */
  @Test
  public void testAddExtension() {
    final MockImpl m = new MockImpl();
    final MockExtension extension = new MockExtension();

    m.getExtensionPoint().addExtension(extension);
    assertEquals(1, m.extensionPoint.extensions.size());

    final Iterable<Extension<MockInterface>> extensions = m.getExtensionPoint().getExtensions();
    assertNotNull(extensions);
    final Iterator<Extension<MockInterface>> iterator = extensions.iterator();

    // first and only entry should be our extension.
    final Extension<MockInterface> actual = iterator.next();
    assertSame(extension, actual);

    // should only be one extension in the iterator.
    try {
      iterator.next();
      fail("Expected NoSuchElementException.");
    } catch (NoSuchElementException e) {
      // ignore
    }
  }

  /**
   * Test method for {@link SimpleExtensionPoint#removeExtension(Extension)} .
   */
  @Test
  public void testRemoveExtension() {
    final MockImpl m = new MockImpl();
    final MockExtension extension = new MockExtension();
    m.getExtensionPoint().addExtension(extension);

    final Iterable<Extension<MockInterface>> extensions = m.getExtensionPoint().getExtensions();
    assertNotNull(extensions);

    final Iterator<Extension<MockInterface>> i = extensions.iterator();

    // first and only entry should be our extension.
    final Extension<MockInterface> actual = i.next();
    assertSame(extension, actual);

    // should not be able to remove it via iterator.
    try {
      i.remove();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // ignore
    }

    m.getExtensionPoint().removeExtension(extension);
    assertEquals(0, m.extensionPoint.extensions.size());

    // extensions should be empty
    final Iterable<Extension<MockInterface>> extensionsRemoved =
        m.getExtensionPoint().getExtensions();
    try {
      extensionsRemoved.iterator().next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      // ignore
    }
  }

  /**
   * Test method for {@link SimpleExtensionPoint#getTarget()} .
   */
  @Test
  public void testGetTarget() {
    final MockImpl m = new MockImpl();
    final MockInterface a = m.getExtensionPoint().getTarget();

    assertSame(m, a);
  }

  /**
   * Test method for {@link SimpleExtensionPoint#fireCreate(Extensible)} .
   */
  @Test
  public void testFireCreate() {
    final MockImpl m = new MockImpl();
    final AtomicInteger counter = new AtomicInteger(0);
    final MockExtension extension = new MockExtension() {
      @Override
      public void onCreate(Extensible<MockInterface> source, Extensible<MockInterface> target) {
        counter.incrementAndGet();
      }
    };

    counter.set(0);
    m.getExtensionPoint().addExtension(extension);
    // fire with itself as the target
    m.extensionPoint.fireCreate(m);
    assertEquals(1, counter.get());

    counter.set(0);
    m.getExtensionPoint().removeExtension(extension);
    // fire with itself as the target
    m.extensionPoint.fireCreate(m);
    assertEquals(0, counter.get());
  }

  /**
   * Test method for {@link SimpleExtensionPoint#beforeCreate(Cache)} .
   */
  @Test
  public void testBeforeCreate() {
    final MockImpl m = new MockImpl();
    final Cache c = Fakes.cache();
    final AtomicInteger counter = new AtomicInteger(0);
    final MockExtension extension = new MockExtension() {
      @Override
      public void beforeCreate(Extensible<MockInterface> source, Cache cache) {
        counter.incrementAndGet();
      }
    };

    counter.set(0);
    m.getExtensionPoint().addExtension(extension);
    // Verify beforeCreate is invoked when the extension is added
    m.extensionPoint.beforeCreate(c);
    assertEquals(1, counter.get());

    counter.set(0);
    m.getExtensionPoint().removeExtension(extension);
    // Verify beforeCreate is not invoked when the extension is removed
    m.extensionPoint.beforeCreate(c);
    assertEquals(0, counter.get());
  }

  private interface MockInterface {
    void method1();
  }

  private static class MockImpl implements MockInterface, Extensible<MockInterface> {

    private final SimpleExtensionPoint<MockInterface> extensionPoint =
        new SimpleExtensionPoint<>(this, this);

    @Override
    public ExtensionPoint<MockInterface> getExtensionPoint() {
      return extensionPoint;
    }

    @Override
    public void method1() {}

  }

  private static class MockExtension implements Extension<MockInterface> {

    @Override
    public XmlGenerator<MockInterface> getXmlGenerator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void beforeCreate(Extensible<MockInterface> source, Cache cache) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void onCreate(Extensible<MockInterface> source, Extensible<MockInterface> target) {
      throw new UnsupportedOperationException();
    }
  }
}

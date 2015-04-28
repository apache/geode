/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.extension;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.cache.xmlcache.XmlGenerator;
import com.gemstone.junit.UnitTest;

/**
 * Unit tests for {@link SimpleExtensionPoint}.
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
@Category(UnitTest.class)
public class SimpleExtensionPointJUnitTest {

  /**
   * Test method for
   * {@link SimpleExtensionPoint#SimpleExtensionPoint(Extensible, Object)} .
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
    final Iterable<Extension<MockInterface>> extensionsRemoved = m.getExtensionPoint().getExtensions();
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

  private interface MockInterface {
    public void method1();
  }

  private class MockImpl implements MockInterface, Extensible<MockInterface> {

    private SimpleExtensionPoint<MockInterface> extensionPoint = new SimpleExtensionPoint<SimpleExtensionPointJUnitTest.MockInterface>(this, this);

    @Override
    public ExtensionPoint<MockInterface> getExtensionPoint() {
      return extensionPoint;
    }

    @Override
    public void method1() {
    }

  }

  private class MockExtension implements Extension<MockInterface> {

    @Override
    public XmlGenerator<MockInterface> getXmlGenerator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void onCreate(Extensible<MockInterface> source, Extensible<MockInterface> target) {
      throw new UnsupportedOperationException();
    }

  }
}

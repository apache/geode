package com.gemstone.gemfire.internal.logging.log4j;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.i18n.StringIdImpl;
import com.gemstone.gemfire.test.junit.categories.UnitTest;
import com.gemstone.org.jgroups.util.StringId;

/**
 * Tests for LocalizedMessage which bridges our StringId LocalizedStrings for 
 * Log4J2 usage.
 * 
 * @author Kirk Lund
 */
@Category(UnitTest.class)
public class LocalizedMessageJUnitTest {

  @Test
  public void testZeroParams() {
    final StringId stringId = new StringIdImpl(100, "This is a message for testZeroParams");
    final LocalizedMessage message = LocalizedMessage.create(stringId);
    assertNull(message.getParameters());
  }
  
  @Test
  public void testEmptyParams() {
    final StringId stringId = new StringIdImpl(100, "This is a message for testEmptyParams");
    final LocalizedMessage message = LocalizedMessage.create(stringId, new Object[] {});
    final Object[] object = message.getParameters();
    assertNotNull(object);
    assertEquals(0, object.length);
  }
  
  @Test
  public void testGetThrowable() {
    final Throwable t = new Throwable();
    final StringId stringId = new StringIdImpl(100, "This is a message for testGetThrowable");
    final LocalizedMessage message = LocalizedMessage.create(stringId, t);
    assertNotNull(message.getThrowable());
    assertEquals(t, message.getThrowable());
    assertTrue(t == message.getThrowable());
  }
}

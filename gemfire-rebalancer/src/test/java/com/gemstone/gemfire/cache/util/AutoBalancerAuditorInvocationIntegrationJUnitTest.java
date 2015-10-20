package com.gemstone.gemfire.cache.util;

import static org.junit.Assert.*;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.util.AutoBalancer.OOBAuditor;
import com.gemstone.gemfire.cache.util.AutoBalancer.TimeProvider;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * IntegrationTest for AuditorInvocation in AutoBalancer. 
 * 
 * <p>AutoBalancer should:<br>
 * 1) be refactored to extract out all inner-classes and inner-interfaces<br>
 * 2) have constructor changed to accept every collaborator as an argument<br>
 * 3) then this test can correctly use mocking without any real threads to wait on
 * 
 * <p>Extracted from AutoBalancerJUnitTest
 */
@Category(IntegrationTest.class)
public class AutoBalancerAuditorInvocationIntegrationJUnitTest {

  Mockery mockContext;

  @Before
  public void setupMock() {
    mockContext = new Mockery() {
      {
        setImposteriser(ClassImposteriser.INSTANCE);
        setThreadingPolicy(new Synchroniser());
      }
    };
  }

  @After
  public void validateMock() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  @Test
  public void testAuditorInvocation() throws InterruptedException {
    int count = 0;

    final OOBAuditor mockAuditor = mockContext.mock(OOBAuditor.class);
    final TimeProvider mockClock = mockContext.mock(TimeProvider.class);
    mockContext.checking(new Expectations() {
      {
        oneOf(mockAuditor).init(with(any(Properties.class)));
        exactly(2).of(mockAuditor).execute();
        allowing(mockClock).currentTimeMillis();
        will(returnValue(950L));
      }
    });

    Properties props = AutoBalancerJUnitTest.getBasicConfig();

    assertEquals(0, count);
    AutoBalancer autoR = new AutoBalancer();
    autoR.setOOBAuditor(mockAuditor);
    autoR.setTimeProvider(mockClock);

    // the trigger should get invoked after 50 milliseconds
    autoR.init(props);
    
    // TODO: this sleep should NOT be here -- use Awaitility to await a condition instead or use mocking to avoid this altogether
    TimeUnit.MILLISECONDS.sleep(120); // removal causes failure in validateMock
  }
}

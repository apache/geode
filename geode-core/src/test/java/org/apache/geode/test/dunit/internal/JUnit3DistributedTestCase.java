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
package com.gemstone.gemfire.test.dunit.internal;

import java.io.Serializable;
import java.util.Properties;

import junit.framework.TestCase;
import org.apache.logging.log4j.Logger;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

/**
 * This class is the superclass of all distributed tests using JUnit 3.
 */
@Category(DistributedTest.class)
public abstract class JUnit3DistributedTestCase extends TestCase implements DistributedTestFixture, Serializable {

  private static final Logger logger = LogService.getLogger();

  private final JUnit4DistributedTestCase delegate = new JUnit4DistributedTestCase(this) {};

  /**
   * Constructs a new distributed test. All JUnit 3 test classes need to have a
   * String-arg constructor.
   */
  public JUnit3DistributedTestCase(final String name) {
    super(name);
    JUnit4DistributedTestCase.initializeDistributedTestCase();
  }

  //---------------------------------------------------------------------------
  // methods for tests
  //---------------------------------------------------------------------------

  /**
   * @deprecated Please override {@link #getDistributedSystemProperties()} instead.
   */
  @Deprecated
  public final void setSystem(final Properties props, final DistributedSystem ds) { // TODO: override getDistributedSystemProperties and then delete
    delegate.setSystem(props, ds);
  }

  /**
   * Returns this VM's connection to the distributed system.  If necessary, the
   * connection will be lazily created using the given {@code Properties}.
   *
   * <p>Do not override this method. Override {@link #getDistributedSystemProperties()}
   * instead.
   *
   * <p>Note: "final" was removed so that WANTestBase can override this method.
   * This was part of the xd offheap merge.
   *
   * @since GemFire 3.0
   */
  public final InternalDistributedSystem getSystem(final Properties props) {
    return delegate.getSystem(props);
  }

  /**
   * Returns this VM's connection to the distributed system.  If necessary, the
   * connection will be lazily created using the {@code Properties} returned by
   * {@link #getDistributedSystemProperties()}.
   *
   * <p>Do not override this method. Override {@link #getDistributedSystemProperties()}
   * instead.
   *
   * @see #getSystem(Properties)
   *
   * @since GemFire 3.0
   */
  public final InternalDistributedSystem getSystem() {
    return delegate.getSystem();
  }

  public final InternalDistributedSystem basicGetSystem() {
    return delegate.basicGetSystem();
  }

  public final void nullSystem() { // TODO: delete
    delegate.nullSystem();
  }

  public static final InternalDistributedSystem getSystemStatic() {
    return JUnit4DistributedTestCase.getSystemStatic();
  }

  /**
   * Returns a loner distributed system that isn't connected to other vms.
   *
   * @since GemFire 6.5
   */
  public final InternalDistributedSystem getLonerSystem() {
    return delegate.getLonerSystem();
  }

  /**
   * Returns whether or this VM is connected to a {@link DistributedSystem}.
   */
  public final boolean isConnectedToDS() {
    return delegate.isConnectedToDS();
  }

  /**
   * Returns a {@code Properties} object used to configure a connection to a
   * {@link DistributedSystem}. Unless overridden, this method will return an
   * empty {@code Properties} object.
   *
   * @since GemFire 3.0
   */
  public Properties getDistributedSystemProperties() {
    return delegate.defaultGetDistributedSystemProperties();
  }

  public static final void disconnectAllFromDS() {
    JUnit4DistributedTestCase.disconnectAllFromDS();
  }

  /**
   * Disconnects this VM from the distributed system
   */
  public static final void disconnectFromDS() {
    JUnit4DistributedTestCase.disconnectFromDS();
  }

  //---------------------------------------------------------------------------
  // name methods
  //---------------------------------------------------------------------------

  public static final String getTestMethodName() {
    return JUnit4DistributedTestCase.getTestMethodName();
  }

  /**
   * Returns a unique name for this test method.  It is based on the
   * name of the class as well as the name of the method.
   */
  public final String getUniqueName() {
    return delegate.getUniqueName();
  }

  //---------------------------------------------------------------------------
  // setup methods
  //---------------------------------------------------------------------------

  /**
   * Sets up the DistributedTestCase.
   * <p>
   * Do not override this method. Override {@link #preSetUp()} with work that
   * needs to occur before setUp() or override {@link #postSetUp()} with work
   * that needs to occur after setUp().
   */
  @Override
  public final void setUp() throws Exception {
    delegate.setUp();
  }

  /**
   * {@code preSetUp()} is invoked before {@link JUnit4DistributedTestCase#setUpDistributedTestCase()}.
   *
   * <p>Override this as needed. Default implementation is empty.
   */
  public void preSetUp() throws Exception {
  }

  /**
   * {@code postSetUp()} is invoked after {@link JUnit4DistributedTestCase#setUpDistributedTestCase()}.
   *
   * <p>Override this as needed. Default implementation is empty.
   */
  public void postSetUp() throws Exception {
  }

  //---------------------------------------------------------------------------
  // teardown methods
  //---------------------------------------------------------------------------

  /**
   * Tears down the DistributedTestCase.
   *
   * <p>Do not override this method. Override {@link #preTearDown()} with work that
   * needs to occur before tearDown() or override {@link #postTearDown()} with work
   * that needs to occur after tearDown().
   */
  @Override
  public final void tearDown() throws Exception {
    delegate.tearDown();
  }

  /**
   * {@code preTearDown()} is invoked before {@link JUnit4DistributedTestCase#tearDownDistributedTestCase()}.
   *
   * <p>Override this as needed. Default implementation is empty.
   */
  public void preTearDown() throws Exception {
  }

  /**
   * {@code postTearDown()} is invoked after {@link JUnit4DistributedTestCase#tearDownDistributedTestCase()}.
   *
   * <p>Override this as needed. Default implementation is empty.
   */
  public void postTearDown() throws Exception {
  }

  /**
   * {@code preTearDownAssertions()} is invoked before any tear down methods
   * have been invoked. If this method throws anything, tear down methods will
   * still be invoked.
   *
   * <p>Override this as needed. Default implementation is empty.
   */
  public void preTearDownAssertions() throws Exception {
  }

  /**
   * {@code postTearDownAssertions()} is invoked after all tear down methods
   * have completed. This method will not be invoked if
   * {@code preTearDownAssertions()} throws.
   *
   * <p>Override this as needed. Default implementation is empty.
   */
  public void postTearDownAssertions() throws Exception {
  }

  protected static final void destroyRegions(final Cache cache) { // TODO: this should move to CacheTestCase
    JUnit4DistributedTestCase.destroyRegions(cache);
  }
}

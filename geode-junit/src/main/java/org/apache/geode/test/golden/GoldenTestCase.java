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
package org.apache.geode.test.golden;

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.junit.After;
import org.junit.Before;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.test.process.ProcessWrapper;

/**
 * Test framework for launching processes and comparing output to expected golden output.
 *
 * @since GemFire 4.1.1
 */
public abstract class GoldenTestCase {

  /** Use to enable debug output in the JUnit process */
  protected static final String DEBUG_PROPERTY = "golden.test.DEBUG";
  protected static final boolean DEBUG = Boolean.getBoolean(DEBUG_PROPERTY);

  /** The log4j2 config used within the spawned process */
  private static final String LOG4J2_CONFIG_URL_STRING =
      GoldenTestCase.class.getResource("log4j2-test.xml").toString();
  private static final String[] JVM_ARGS = new String[] {
      "-D" + ConfigurationFactory.CONFIGURATION_FILE_PROPERTY + "=" + LOG4J2_CONFIG_URL_STRING};

  private final List<ProcessWrapper> processes = new ArrayList<ProcessWrapper>();

  @Before
  public final void setUpGoldenTest() throws Exception {
    subSetUp();
  }

  @After
  public final void tearDownGoldenTest() throws Exception {
    try {
      for (ProcessWrapper process : this.processes) {
        process.destroy();
        printProcessOutput(process, true);
      }
    } finally {
      this.processes.clear();
    }
    subTearDown();
  }

  /**
   * Override this for additional set up.
   *
   */
  public void subSetUp() throws Exception {
    // override me
  }

  /**
   * Override this for additional tear down after destroying all processes and printing output.
   *
   */
  public void subTearDown() throws Exception {
    // override me
  }

  protected final ProcessWrapper createProcessWrapper(
      final ProcessWrapper.Builder processWrapperBuilder, final Class<?> main) {
    final ProcessWrapper processWrapper =
        processWrapperBuilder.jvmArguments(JVM_ARGS).mainClass(main).build();
    this.processes.add(processWrapper);
    return processWrapper;
  }

  /**
   * Creates and returns a new GoldenComparator instance. Default implementation is
   * RegexGoldenComparator. Override if you need a different implementation such as
   * StringGoldenComparator.
   */
  protected GoldenComparator createGoldenComparator() {
    return new RegexGoldenComparator(expectedProblemLines());
  }

  /**
   * Returns an array of expected problem strings. Without overriding this, any line with warning,
   * error or severe will cause the test to fail. By default, null is returned.
   *
   * (see PartitionedRegionTest which expects a WARNING log message)
   */
  protected String[] expectedProblemLines() {
    return null;
  }

  protected void assertOutputMatchesGoldenFile(final String actualOutput,
      final String goldenFileName) throws IOException {
    GoldenComparator comparator = createGoldenComparator();
    comparator.assertOutputMatchesGoldenFile(actualOutput, goldenFileName);
  }

  protected final void assertOutputMatchesGoldenFile(final ProcessWrapper process,
      final String goldenFileName) throws IOException {
    GoldenComparator comparator = createGoldenComparator();
    comparator.assertOutputMatchesGoldenFile(process.getOutput(), goldenFileName);
  }

  protected final Properties createProperties() {
    Properties properties = new Properties();
    properties.setProperty(DistributionConfig.GEMFIRE_PREFIX + MCAST_PORT, "0");
    properties.setProperty(DistributionConfig.GEMFIRE_PREFIX + LOG_LEVEL, "warning");
    properties.setProperty("file.encoding", "UTF-8");
    return editProperties(properties);
  }

  /**
   * Override this to modify the properties that were created by createProperties().
   */
  protected Properties editProperties(final Properties properties) {
    return properties;
  }

  protected final void outputLine(final String string) {
    System.out.println(string);
  }

  protected final void printProcessOutput(final ProcessWrapper process,
      final boolean ignoreStopped) {
    debug(process.getOutput(ignoreStopped), "OUTPUT");
  }

  protected static void debug(final String output, final String title) {
    debug("------------------ BEGIN " + title + " ------------------");
    debug(output);
    debug("------------------- END " + title + " -------------------");
  }

  protected static void debug(final String string) {
    if (DEBUG) {
      System.out.println(string);
    }
  }
}

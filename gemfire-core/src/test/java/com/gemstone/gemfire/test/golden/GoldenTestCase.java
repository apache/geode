package com.gemstone.gemfire.test.golden;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.apache.logging.log4j.core.config.ConfigurationFactory;

import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.test.process.ProcessWrapper;

import junit.framework.TestCase;

/**
 * The abstract superclass of tests that need to process output from the
 * quickstart examples.
 *
 * @author Kirk Lund
 * @since 4.1.1
 */
public abstract class GoldenTestCase extends TestCase {
  protected static final Marker GOLDEN_TEST = MarkerManager.getMarker("GOLDEN_TEST");

  protected final Logger logger = LogService.getLogger();
  
  private final int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
  private final List<ProcessWrapper> processes = new ArrayList<ProcessWrapper>();
  
  static {
    final URL configUrl = GoldenTestCase.class.getResource("log4j2-test.xml");
    if (configUrl != null) {
      System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY, configUrl.toString());
    }
  }
  
  private final static String[] jvmArgs = new String[] {
    "-D"+ConfigurationFactory.CONFIGURATION_FILE_PROPERTY+"="+System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY)
  };
  
  public GoldenTestCase(String name) {
    super(name);
  }

  @Override
  public final void setUp() throws Exception {
    super.setUp();
    subSetUp();
  }
  
  @Override
  public final void tearDown() throws Exception {
    super.tearDown();
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
   * @throws Exception
   */
  public void subSetUp() throws Exception {
    // override me
  }
  
  /**
   * Override this for additional tear down after destroying all processes and
   * printing output.
   * 
   * @throws Exception
   */
  public void subTearDown() throws Exception {
    // override me
  }
  
  protected final ProcessWrapper createProcessWrapper(Class<?> main) {
    final ProcessWrapper processWrapper = new ProcessWrapper.Builder().jvmArgs(jvmArgs).main(main).build();
    this.processes.add(processWrapper);
    return processWrapper;
  }
  
  protected final ProcessWrapper createProcessWrapper(Class<?> main, String[] mainArgs) {
    final ProcessWrapper processWrapper = new ProcessWrapper.Builder().jvmArgs(jvmArgs).main(main).mainArgs(mainArgs).build();
    this.processes.add(processWrapper);
    return processWrapper;
  }
  
  protected final ProcessWrapper createProcessWrapper(Class<?> main, String[] mainArgs, boolean useMainLauncher) {
    final ProcessWrapper processWrapper = new ProcessWrapper.Builder().jvmArgs(jvmArgs).main(main).mainArgs(mainArgs).useMainLauncher(useMainLauncher).build();
    this.processes.add(processWrapper);
    return processWrapper;
  }

  /** 
   * Creates and returns a new GoldenComparator instance. Default implementation
   * is RegexGoldenComparator. Override if you need a different implementation
   * such as StringGoldenComparator.
   */
  protected GoldenComparator createGoldenComparator() {
    return new RegexGoldenComparator(expectedProblemLines());
  }
  
  /**
   * Returns an array of expected problem strings. Without overriding this,
   * any line with warning, error or severe will cause the test to fail. By
   * default, null is returned.
   * 
   *(see PartitionedRegionTest which expects a WARNING log message) 
   */
  protected String[] expectedProblemLines() {
    return null;
  }
  
  protected void assertOutputMatchesGoldenFile(String actualOutput, String goldenFileName) throws IOException {
    GoldenComparator comparator = createGoldenComparator();
    comparator.assertOutputMatchesGoldenFile(actualOutput, goldenFileName);
  }

  protected final void assertOutputMatchesGoldenFile(ProcessWrapper process, String goldenFileName) throws IOException {
    GoldenComparator comparator = createGoldenComparator();
    comparator.assertOutputMatchesGoldenFile(process.getOutput(), goldenFileName);
  }

  protected final Properties createProperties() {
    Properties properties = new Properties();
    properties.setProperty("gemfire.start-locator", "localhost[" + String.valueOf(this.locatorPort) + "]");
    properties.setProperty("gemfire.log-level", "warning");
    properties.setProperty("file.encoding", "UTF-8");
    return editProperties(properties);
  }
  
  /**
   * Override this to modify the properties that were created by createProperties().
   */
  protected Properties editProperties(final Properties properties) {
    return properties;
  }
  
  protected final int getMcastPort() {
    return this.locatorPort;
  }
  
  // TODO: get rid of this to tighten up tests
  protected final void sleep(long millis) throws InterruptedException {
    Thread.sleep(millis);
  }
  
  protected final void printProcessOutput(ProcessWrapper process) {
    innerPrintOutput(process.getOutput(), "OUTPUT");
  }
  
  protected final void printProcessOutput(ProcessWrapper process, boolean ignoreStopped) {
    innerPrintOutput(process.getOutput(ignoreStopped), "OUTPUT");
  }
  
  protected final void printProcessOutput(ProcessWrapper process, String banner) {
    innerPrintOutput(process.getOutput(), banner);
  }
  
  protected final void innerPrintOutput(String output, String title) {
    System.out.println("------------------ BEGIN " + title + " ------------------");
    System.out.println(output);
    System.out.println("------------------- END " + title + " -------------------");
  }
}

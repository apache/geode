package com.gemstone.gemfire.test.process;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Integration tests for ProcessWrapper.
 * 
 * @author Kirk Lund
 */
@Category(IntegrationTest.class)
public class ProcessWrapperJUnitTest {

  private static final String OUTPUT_OF_MAIN = "Executing ProcessWrapperJUnitTest main";
  private ProcessWrapper process;
  
  @After
  public void after() {
    if (this.process != null) {
      this.process.destroy();
    }
  }
  
  @Test
  public void testClassPath() throws Exception {
    final String classPath = System.getProperty("java.class.path");
    assertTrue("Classpath is missing log4j-api: " + classPath, classPath.toLowerCase().contains("log4j-api"));
    assertTrue("Classpath is missing log4j-core: " + classPath, classPath.toLowerCase().contains("log4j-core"));
    assertTrue("Classpath is missing fastutil: " + classPath, classPath.toLowerCase().contains("fastutil"));
  
    this.process = new ProcessWrapper.Builder().mainClass(getClass()).build();
    this.process.execute();
    this.process.waitFor();
    
    assertTrue("Output is wrong: " + process.getOutput(), process.getOutput().contains(OUTPUT_OF_MAIN));
  }
  
  @Test
  public void testInvokeWithNullArgs() throws Exception {
    this.process = new ProcessWrapper.Builder().mainClass(getClass()).build();
    this.process.execute();
    this.process.waitFor();
    assertTrue(process.getOutput().contains(OUTPUT_OF_MAIN));
  }

  public static void main(String... args) throws Exception {
    Class.forName(org.apache.logging.log4j.LogManager.class.getName());
    Class.forName(com.gemstone.gemfire.internal.logging.LogService.class.getName());
    System.out.println(OUTPUT_OF_MAIN);
  }
}

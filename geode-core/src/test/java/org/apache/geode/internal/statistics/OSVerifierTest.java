package org.apache.geode.internal.statistics;

import org.apache.geode.InternalGemFireException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class OSVerifierTest {

private static String currentOsName;
  
  @Rule
  public ExpectedException exceptionGrabber = ExpectedException.none();

  @BeforeClass
  public static void saveOsName() {
    currentOsName=System.getProperty("os.name");    
  }
  
  @After
  public void restoreOsName() {
    System.setProperty("os.name", currentOsName);
  }
  
  @Test
  public void givenLinuxOs_thenOSVerifierObjectCanBeBuilt() {
    System.setProperty("os.name","Linux");
    new OSVerifier(); 
  }
  
  @Test
  public void givenNonLinuxOs_thenOSVerifierObjectCanBeBuilt() {
    System.setProperty("os.name","NonLinux");
    exceptionGrabber.expect(InternalGemFireException.class);
    new OSVerifier();
  }
  
}

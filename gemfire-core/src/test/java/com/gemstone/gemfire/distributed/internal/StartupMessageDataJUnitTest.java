/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

import java.io.DataInput;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.ByteArrayData;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;
import com.gemstone.junit.UnitTest;

import junit.framework.TestCase;

/**
 * Tests {@link StartupMessageData}.
 * 
 * @author Kirk Lund
 * @since 7.0
 */
@Category(UnitTest.class)
public class StartupMessageDataJUnitTest extends TestCase {

  public StartupMessageDataJUnitTest(String name) {
    super(name);
  }
  
  public void testSupportedVersion() throws Exception {
    try {
      @SuppressWarnings("unused")
      StartupMessageData data = new StartupMessageData(
          null, StartupMessageData.SUPPORTED_VERSION);
      fail("Supported version should have thrown NPE for null DataInput.");
    } catch (NullPointerException expected) {
      // passed
    }
  }

  // compatibility with 6.6.2 is no longer needed.  If this is needed
  // in the future we need to change to using the new versioned data
  // streams
//  public void testUnsupportedVersion() throws Exception {
//    try {
//      @SuppressWarnings("unused")
//      StartupMessageData data = new StartupMessageData(
//          null, "6.6.2");
//      // passed
//    } catch (NullPointerException e) {
//      fail("Unsupported version should simply ignore null DataInput.");
//    }
//  }

  public void testWriteHostedLocatorsWithEmpty() throws Exception {
    Collection<String> hostedLocators = new ArrayList<String>();
    StartupMessageData data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);
    assertTrue(data.getOptionalFields().isEmpty());
  }

  public void testWriteHostedLocatorsWithNull() throws Exception {
    Collection<String> hostedLocators = null;
    StartupMessageData data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);
    assertTrue(data.getOptionalFields().isEmpty());
  }

  public void testWriteHostedLocatorsWithOne() throws Exception {
    String locatorString = createOneLocatorString();
    
    List<String> hostedLocators = new ArrayList<String>();
    hostedLocators.add(locatorString);
    
    StartupMessageData data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);
    assertEquals(1, data.getOptionalFields().size());
    assertEquals(locatorString, data.getOptionalFields().get(StartupMessageData.HOSTED_LOCATORS));
  }

  public void testWriteHostedLocatorsWithThree() throws Exception {
    String[] locatorStrings = createManyLocatorStrings(3);
    List<String> hostedLocators = new ArrayList<String>();
    for (int i = 0; i < 3; i++) {
      hostedLocators.add(locatorStrings[i]); 
    }
    
    StartupMessageData data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);
    assertEquals(1, data.getOptionalFields().size());
    
    String hostedLocatorsField = 
        data.getOptionalFields().getProperty(StartupMessageData.HOSTED_LOCATORS);
    
    StringTokenizer st = new StringTokenizer(
        hostedLocatorsField, StartupMessageData.COMMA_DELIMITER);
    for (int i = 0; st.hasMoreTokens(); i++) { 
      assertEquals(locatorStrings[i], st.nextToken());
    }
  }
  
  public void testReadHostedLocatorsWithThree() throws Exception {
    // set up the data
    String[] locatorStrings = createManyLocatorStrings(3);
    List<String> hostedLocators = new ArrayList<String>();
    for (int i = 0; i < 3; i++) {
      hostedLocators.add(locatorStrings[i]); 
    }
    
    StartupMessageData data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);
    assertEquals(1, data.getOptionalFields().size());
    
    // test readHostedLocators
    int i = 0;
    Collection<String> readLocatorStrings = data.readHostedLocators();
    assertEquals(3, readLocatorStrings.size());
    for (String readLocatorString : readLocatorStrings) { 
      assertEquals(locatorStrings[i], readLocatorString);
      i++;
    }
  }
  
  public void testToDataWithEmptyHostedLocators() throws Exception {
    Collection<String> hostedLocators = new ArrayList<String>();
    StartupMessageData data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);
    
    ByteArrayData testStream = new ByteArrayData();
    assertTrue(testStream.isEmpty());

    DataOutputStream out = testStream.getDataOutput();
    data.toData(out);
    assertTrue(testStream.size() > 0);
    
    DataInput in = testStream.getDataInput();
    Properties props = (Properties) DataSerializer.readObject(in);
    assertNull(props);
  }

  public void testToDataWithNullHostedLocators() throws Exception {
    Collection<String> hostedLocators = null;
    StartupMessageData data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);
    
    ByteArrayData testStream = new ByteArrayData();
    assertTrue(testStream.isEmpty());

    DataOutputStream out = testStream.getDataOutput();
    data.toData(out);
    assertTrue(testStream.size() > 0);
    
    DataInput in = testStream.getDataInput();
    Properties props = (Properties) DataSerializer.readObject(in);
    assertNull(props);
  }
  
  public void testToDataWithOneHostedLocator() throws Exception {
    String locatorString = createOneLocatorString();
    
    List<String> hostedLocators = new ArrayList<String>();
    hostedLocators.add(locatorString);

    StartupMessageData data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);
    
    ByteArrayData testStream = new ByteArrayData();
    assertTrue(testStream.isEmpty());

    DataOutputStream out = testStream.getDataOutput();
    data.toData(out);
    assertTrue(testStream.size() > 0);
    
    DataInput in = testStream.getDataInput();
    Properties props = (Properties) DataSerializer.readObject(in);
    assertNotNull(props);
    
    String hostedLocatorsString = props.getProperty(StartupMessageData.HOSTED_LOCATORS);
    assertNotNull(hostedLocatorsString);
    assertEquals(locatorString, hostedLocatorsString);
  }

  public void testToDataWithThreeHostedLocators() throws Exception {
    String[] locatorStrings = createManyLocatorStrings(3);
    List<String> hostedLocators = new ArrayList<String>();
    for (int i = 0; i < 3; i++) {
      hostedLocators.add(locatorStrings[i]); 
    }

    StartupMessageData data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);
    
    ByteArrayData testStream = new ByteArrayData();
    assertTrue(testStream.isEmpty());

    DataOutputStream out = testStream.getDataOutput();
    data.toData(out);
    assertTrue(testStream.size() > 0);
    
    DataInput in = testStream.getDataInput();
    Properties props = (Properties) DataSerializer.readObject(in);
    assertNotNull(props);
    
    String hostedLocatorsString = props.getProperty(
        StartupMessageData.HOSTED_LOCATORS);
    assertNotNull(hostedLocatorsString);

    Collection<String> actualLocatorStrings = new ArrayList<String>(1);
    StringTokenizer st = new StringTokenizer(hostedLocatorsString, StartupMessageData.COMMA_DELIMITER);
    while (st.hasMoreTokens()) {
      actualLocatorStrings.add(st.nextToken());
    }
    assertEquals(3, actualLocatorStrings.size());

    int i = 0;
    for (String actualLocatorString : actualLocatorStrings) { 
      assertEquals(locatorStrings[i], actualLocatorString);
      i++;
    }
  }

  public void testNullHostedLocator() throws Exception {
    String locatorString = null;
    DataInput in = getDataInputWithOneHostedLocator(locatorString);
    StartupMessageData dataToRead = new StartupMessageData(
        in, StartupMessageData.SUPPORTED_VERSION);
    Collection<String> readHostedLocators = dataToRead.readHostedLocators();
    assertNull(readHostedLocators);
  }
  
  public void testEmptyHostedLocator() throws Exception {
    String locatorString = "";
    DataInput in = getDataInputWithOneHostedLocator(locatorString);
    StartupMessageData dataToRead = new StartupMessageData(
        in, StartupMessageData.SUPPORTED_VERSION);
    Collection<String> readHostedLocators = dataToRead.readHostedLocators();
    assertNull(readHostedLocators);
  }
  
  public void testOneHostedLocator() throws Exception {
    String locatorString = createOneLocatorString();
    DataInput in = getDataInputWithOneHostedLocator(locatorString);
    StartupMessageData dataToRead = new StartupMessageData(
        in, StartupMessageData.SUPPORTED_VERSION);
    Collection<String> readHostedLocators = dataToRead.readHostedLocators();
    assertNotNull(readHostedLocators);
    assertEquals(1, readHostedLocators.size());
    assertEquals(locatorString, readHostedLocators.iterator().next());
  }
  
  private String createOneLocatorString() throws Exception {
    DistributionLocatorId locatorId = new DistributionLocatorId(
        SocketCreator.getLocalHost(), 
        445566, 
        "111.222.333.444", 
        null);
    String locatorString = locatorId.marshal();
    assertEquals("" + locatorId.getHost().getHostAddress() 
        + ":111.222.333.444[445566]", locatorString);
    return locatorString;
  }
  
  private String[] createManyLocatorStrings(int n) throws Exception {
    String[] locatorStrings = new String[3];
    for (int i = 0; i < 3; i++) {
      int j = i + 1;
      int k = j + 1;
      int l = k + 1;
      DistributionLocatorId locatorId = new DistributionLocatorId(
          SocketCreator.getLocalHost(), 
          445566, 
          ""+i+""+i+""+i+"."+j+""+j+""+j+"."+k+""+k+""+k+"."+l+""+l+""+l, 
          null);
      locatorStrings[i] = locatorId.marshal();
    }
    return locatorStrings;
  }
  
  private DataInput getDataInputWithOneHostedLocator(String locatorString) throws Exception {
    List<String> hostedLocators = new ArrayList<String>();
    if (locatorString != null) {
      hostedLocators.add(locatorString);
    }
    
    StartupMessageData dataToWrite = new StartupMessageData();
    dataToWrite.writeHostedLocators(hostedLocators);
    
    ByteArrayData testStream = new ByteArrayData();
    assertTrue(testStream.isEmpty());

    DataOutputStream out = testStream.getDataOutput();
    dataToWrite.toData(out);
    assertTrue(testStream.size() > 0);
    
    DataInput in = testStream.getDataInput();
    assertNotNull(in);
    return in;
  }
}

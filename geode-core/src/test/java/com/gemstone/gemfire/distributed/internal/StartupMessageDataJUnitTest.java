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
package com.gemstone.gemfire.distributed.internal;

import static org.junit.Assert.*;

import java.io.DataInput;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.net.SocketCreator;
import com.gemstone.gemfire.internal.ByteArrayData;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Tests {@link StartupMessageData}.
 * 
 * @since GemFire 7.0
 */
@Category(UnitTest.class)
public class StartupMessageDataJUnitTest {

  @Test
  public void testWriteHostedLocatorsWithEmpty() throws Exception {
    Collection<String> hostedLocators = new ArrayList<String>();
    StartupMessageData data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);
    assertTrue(data.getOptionalFields().isEmpty());
  }

  @Test
  public void testWriteHostedLocatorsWithNull() throws Exception {
    Collection<String> hostedLocators = null;
    StartupMessageData data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);
    assertTrue(data.getOptionalFields().isEmpty());
  }

  @Test
  public void testWriteHostedLocatorsWithOne() throws Exception {
    String locatorString = createOneLocatorString();
    
    List<String> hostedLocators = new ArrayList<String>();
    hostedLocators.add(locatorString);
    
    StartupMessageData data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);
    assertEquals(1, data.getOptionalFields().size());
    assertEquals(locatorString, data.getOptionalFields().get(StartupMessageData.HOSTED_LOCATORS));
  }

  @Test
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

  @Test
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

  @Test
  public void testToDataWithEmptyHostedLocators() throws Exception {
    Collection<String> hostedLocators = new ArrayList<String>();
    StartupMessageData data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);
    
    ByteArrayData testStream = new ByteArrayData();
    assertTrue(testStream.isEmpty());

    DataOutputStream out = testStream.getDataOutput();
    data.writeTo(out);
    assertTrue(testStream.size() > 0);
    
    DataInput in = testStream.getDataInput();
    Properties props = (Properties) DataSerializer.readObject(in);
    assertNull(props);
  }

  @Test
  public void testToDataWithNullHostedLocators() throws Exception {
    Collection<String> hostedLocators = null;
    StartupMessageData data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);
    
    ByteArrayData testStream = new ByteArrayData();
    assertTrue(testStream.isEmpty());

    DataOutputStream out = testStream.getDataOutput();
    data.writeTo(out);
    assertTrue(testStream.size() > 0);
    
    DataInput in = testStream.getDataInput();
    Properties props = (Properties) DataSerializer.readObject(in);
    assertNull(props);
  }

  @Test
  public void testToDataWithOneHostedLocator() throws Exception {
    String locatorString = createOneLocatorString();
    
    List<String> hostedLocators = new ArrayList<String>();
    hostedLocators.add(locatorString);

    StartupMessageData data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);
    
    ByteArrayData testStream = new ByteArrayData();
    assertTrue(testStream.isEmpty());

    DataOutputStream out = testStream.getDataOutput();
    data.writeTo(out);
    assertTrue(testStream.size() > 0);
    
    DataInput in = testStream.getDataInput();
    Properties props = (Properties) DataSerializer.readObject(in);
    assertNotNull(props);
    
    String hostedLocatorsString = props.getProperty(StartupMessageData.HOSTED_LOCATORS);
    assertNotNull(hostedLocatorsString);
    assertEquals(locatorString, hostedLocatorsString);
  }

  @Test
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
    data.writeTo(out);
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

  @Test
  public void testNullHostedLocator() throws Exception {
    String locatorString = null;
    DataInput in = getDataInputWithOneHostedLocator(locatorString);
    StartupMessageData dataToRead = new StartupMessageData();
    dataToRead.readFrom(in);
    Collection<String> readHostedLocators = dataToRead.readHostedLocators();
    assertNull(readHostedLocators);
  }

  @Test
  public void testEmptyHostedLocator() throws Exception {
    String locatorString = "";
    DataInput in = getDataInputWithOneHostedLocator(locatorString);
    StartupMessageData dataToRead = new StartupMessageData();
    dataToRead.readFrom(in);
    Collection<String> readHostedLocators = dataToRead.readHostedLocators();
    assertNull(readHostedLocators);
  }

  @Test
  public void testOneHostedLocator() throws Exception {
    String locatorString = createOneLocatorString();
    DataInput in = getDataInputWithOneHostedLocator(locatorString);
    StartupMessageData dataToRead = new StartupMessageData();
    dataToRead.readFrom(in);
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
    dataToWrite.writeTo(out);
    assertTrue(testStream.size() > 0);
    
    DataInput in = testStream.getDataInput();
    assertNotNull(in);
    return in;
  }
}

/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.StringTokenizer;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.GemFireVersion;

/**
 * Provides optional data fields as properties for StartupMessage and 
 * StartupResponseMessage. This is handled by serializing and deserializing
 * a new Properties instance if and only if the member version is 6.6.3 or
 * greater. New fields can be added to the Properties without breaking
 * backwards compatibility. All new fields added should be written to allow
 * for version compatibility.
 * 
 * @author Kirk Lund
 * @since 7.0
 */
class StartupMessageData {

  static final String SUPPORTED_VERSION = "6.6.3";
  static final String HOSTED_LOCATORS = "HL";
  static final String COMMA_DELIMITER = ",";
  static final String MCAST_PORT = "MP";
  static final String MCAST_HOST_ADDRESS = "MHA";
  static final String IS_SHARED_CONFIG_ENABLED = "ISCE";
  
  private final Properties optionalFields;
  
  /**
   * Constructs a new instance with empty Properties. After construction
   * the instance should optionally invoke one or more "write" methods such 
   * as {@link #writeHostedLocators(Collection)} before invoking {@link 
   * #toData(DataOutput)} to marshal the Properties into the DataOutput
   * (onto the wire).
   */
  StartupMessageData() {
    this.optionalFields = new Properties();
  }
  
  /**
   * Constructs a new instance which deserializes any optional fields it
   * finds in the DataInput (from the wire) if the specified version is
   * greater than or equal to the minimum {@link #SUPPORTED_VERSION}.
   * 
   * @throws ClassNotFoundException
   * @throws IOException
   */
  StartupMessageData(DataInput in, String version) throws ClassNotFoundException, IOException {
    // [bruce] commenting this out for the GemFireXD Cheetah 1.0 release.  It should be removed
    // from the GemFire Cedar branch as well.  Future versioning work should use the
    // version ordinal found in the DataInput passed to fromData methods.
//    if (GemFireVersion.compareVersions(version, StartupMessageData.SUPPORTED_VERSION) >= 0) {
      this.optionalFields = (Properties) DataSerializer.readObject(in);
//    } else {
//      this.optionalFields = null;
//    }
  }

  /**
   * Check for the optional field {@link #HOSTED_LOCATORS} and return the
   * value or null.
   */
  Collection<String> readHostedLocators() {
    if (this.optionalFields == null || this.optionalFields.isEmpty()) {
      return null;
    }
    Collection<String> hostedLocators = null;
    String hostedLocatorsString = this.optionalFields.getProperty(HOSTED_LOCATORS);
    if (hostedLocatorsString != null && !hostedLocatorsString.isEmpty()) {
      StringTokenizer st = new StringTokenizer(hostedLocatorsString, COMMA_DELIMITER);
      hostedLocators = new ArrayList<String>();
      while (st.hasMoreTokens()) {
        String locatorString = st.nextToken();
        if (locatorString != null && !locatorString.isEmpty()) {
          hostedLocators.add(locatorString);
        }
      }
      if (hostedLocators.isEmpty()) {
        hostedLocators = null;
      }
    }
    return hostedLocators;
  }
  
//  /**
//   * Check for the optional field {@link #HOSTED_LOCATORS_WITH_SHARED_CONFIGURATION} and return the
//   * value or null.
//   */
//  Collection<String> readHostedLocatorsWithSharedConfiguration() {
//    if (this.optionalFields == null || this.optionalFields.isEmpty()) {
//      return null;
//    }
//    Collection<String> hostedLocatorsWithSharedConfiguration = null;
//    String hostedLocatorsString = this.optionalFields.getProperty(HOSTED_LOCATORS_WITH_SHARED_CONFIGURATION);
//    if (hostedLocatorsString != null && !hostedLocatorsString.isEmpty()) {
//      StringTokenizer st = new StringTokenizer(hostedLocatorsString, COMMA_DELIMITER);
//      hostedLocatorsWithSharedConfiguration = new ArrayList<String>();
//      while (st.hasMoreTokens()) {
//        String locatorString = st.nextToken();
//        if (locatorString != null && !locatorString.isEmpty()) {
//          hostedLocatorsWithSharedConfiguration.add(locatorString);
//        }
//      }
//      if (hostedLocatorsWithSharedConfiguration.isEmpty()) {
//        hostedLocatorsWithSharedConfiguration = null;
//      }
//    }
//    return hostedLocatorsWithSharedConfiguration;
//  }
  
  boolean readIsSharedConfigurationEnabled() {
    if (this.optionalFields == null || this.optionalFields.isEmpty()) {
      return false;
    }
    return Boolean.parseBoolean((this.optionalFields.getProperty(IS_SHARED_CONFIG_ENABLED)));
  }

  /**
   * Write the value for the optional field {@link #HOSTED_LOCATORS}.
   */
  void writeHostedLocators(Collection<String> hostedLocators) {
    if (this.optionalFields == null) {
      return;
    }
    if (hostedLocators != null && !hostedLocators.isEmpty()) {
      String hostedLocatorsString = asCommaDelimitedString(hostedLocators);
      if (hostedLocatorsString != null && !hostedLocatorsString.isEmpty()) {
        this.optionalFields.setProperty(HOSTED_LOCATORS, hostedLocatorsString);
      }
    }
  }
  
//  void writeHostedLocatorsWithSharedConfiguration(Collection<String> hostedLocatorsWithSharedConfiguration) {
//    if (this.optionalFields == null) {
//      return;
//    }
//    if (hostedLocatorsWithSharedConfiguration != null && !hostedLocatorsWithSharedConfiguration.isEmpty()) {
//      String hostedLocatorsString = asCommaDelimitedString(hostedLocatorsWithSharedConfiguration);
//      if (hostedLocatorsString != null && !hostedLocatorsString.isEmpty()) {
//        this.optionalFields.setProperty(HOSTED_LOCATORS_WITH_SHARED_CONFIGURATION, hostedLocatorsString);
//      }
//    }
//  }
//  
  void writeIsSharedConfigurationEnabled(boolean isSharedConfigurationEnabled) {
    if (this.optionalFields == null) {
      return;
    }
    this.optionalFields.setProperty(IS_SHARED_CONFIG_ENABLED, Boolean.toString(isSharedConfigurationEnabled));
  }
  
  int readMcastPort() {
    int result = 0;
    if (this.optionalFields != null) {
      String resultString = this.optionalFields.getProperty(MCAST_PORT);
      if (resultString != null && !resultString.isEmpty()) {
        result = Integer.parseInt(resultString);
      }
    }
    return result;
  }
  
  void writeMcastPort(int mcastPort) {
    if (this.optionalFields != null) {
      if (mcastPort != 0) {
        this.optionalFields.setProperty(MCAST_PORT, Integer.toString(mcastPort));
      }
    }
  }
  
  String readMcastHostAddress() {
    String result = null;
    if (this.optionalFields != null) {
      result = this.optionalFields.getProperty(MCAST_HOST_ADDRESS);
    }
    return result;
  }
  
  void writeMcastHostAddress(String addr) {
    if (this.optionalFields != null) {
      if (addr != null && !addr.isEmpty()) {
        this.optionalFields.setProperty(MCAST_HOST_ADDRESS, addr);
      }
    }
  }

  /**
   * Writes all optional fields to the DataOutput or null for minimal
   * wire footprint.
   * 
   * @throws IOException
   */
  void toData(DataOutput out) throws IOException {
    if (this.optionalFields.isEmpty()) {
      DataSerializer.writeObject(null, out);
    } else {
      DataSerializer.writeObject(this.optionalFields, out);
    }
  }
  
  /**
   * Returns {@link #optionalFields} for testing.
   */
  Properties getOptionalFields() {
    return this.optionalFields;
  }
  
  /**
   * Marshals a collection of strings to a single comma-delimited string.
   * Returns null if collection is null or empty.
   */
  public static String asCommaDelimitedString(Collection<String> strings) {
    StringBuilder sb = new StringBuilder();
    for (String string : strings) {
      if (sb.length() > 0) {
        sb.append(COMMA_DELIMITER);
      }
      sb.append(string);
    }
    return sb.toString();
  }
}

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

package com.gemstone.gemfire.internal;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.TreeSet;

import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.UnmodifiableException;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.FlowControlParams;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.net.SocketCreator;

/**
 * Provides an implementation of the {@link Config} interface
 * that implements functionality that all {@link Config} implementations
 * can share.
 */
public abstract class AbstractConfig implements Config {

  /**
   * Returns the string to use as the exception message when an attempt
   * is made to set an unmodifiable attribute.
   */
  protected String _getUnmodifiableMsg(String attName) {
    return LocalizedStrings.AbstractConfig_THE_0_CONFIGURATION_ATTRIBUTE_CAN_NOT_BE_MODIFIED.toLocalizedString(attName);
  }

  /**
   * Returns a map that contains attribute descriptions
   */
  protected abstract Map getAttDescMap();

  protected abstract Map<String, ConfigSource> getAttSourceMap();

  public final static String sourceHeader = "PropertiesSourceHeader";

  /**
   * Set to true if most of the attributes can be modified.
   * Set to false if most of the attributes are read only.
   */
  protected boolean _modifiableDefault() {
    return false;
  }

  /**
   * Use {@link #toLoggerString()} instead. If you need to override this in a
   * subclass, be careful not to expose any private data or security related
   * values. Fixing bug #48155 by not exposing all values.
   */
  @Override
  public final String toString() {
    return getClass().getName() + "@" + Integer.toHexString(hashCode());
  }

  @Override
  public String toLoggerString() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    printSourceSection(ConfigSource.runtime(), pw);
    printSourceSection(ConfigSource.sysprop(), pw);
    printSourceSection(ConfigSource.api(), pw);
    for (ConfigSource fileSource : getFileSources()) {
      printSourceSection(fileSource, pw);
    }
    printSourceSection(ConfigSource.xml(), pw);
    printSourceSection(ConfigSource.launcher(), pw); // fix for bug 46653
    printSourceSection(null, pw);
    pw.close();
    return sw.toString();
  }

  /***
   * Gets the Map of GemFire properties and values from a given ConfigSource
   * @param source
   *
   * @return map of GemFire properties and values
   */
  public Map<String, String> getConfigPropsFromSource(ConfigSource source) {
    Map<String, String> configProps = new HashMap<String, String>();
    String[] validAttributeNames = getAttributeNames();
    Map<String, ConfigSource> sm = getAttSourceMap();

    for (int i = 0; i < validAttributeNames.length; i++) {
      String attName = validAttributeNames[i];
      if (source == null) {
        if (sm.get(attName) != null) {
          continue;
        }
      } else if (!source.equals(sm.get(attName))) {
        continue;
      }
      configProps.put(attName, this.getAttribute(attName));
    }
    return configProps;
  }

  /****
   * Gets all the GemFire properties defined using file(s)
   * @return Map of GemFire properties and values set using property files
   */
  public Map<String, String> getConfigPropsDefinedUsingFiles() {
    Map<String, String> configProps = new HashMap<String, String>();
    for (ConfigSource fileSource : getFileSources()) {
      configProps.putAll(getConfigPropsFromSource(fileSource));
    }
    return configProps;
  }

  private List<ConfigSource> getFileSources() {
    ArrayList<ConfigSource> result = new ArrayList<ConfigSource>();
    for (ConfigSource cs : getAttSourceMap().values()) {
      if (cs.getType() == ConfigSource.Type.FILE || cs.getType() == ConfigSource.Type.SECURE_FILE) {
        if (!result.contains(cs)) {
          result.add(cs);
        }
      }
    }
    return result;
  }

  private void printSourceSection(ConfigSource source, PrintWriter pw) {
    String[] validAttributeNames = getAttributeNames();
    boolean sourceFound = false;
    Map<String, ConfigSource> sm = getAttSourceMap();
    boolean secureSource = false;
    if (source != null && source.getType() == ConfigSource.Type.SECURE_FILE) {
      secureSource = true;
    }
    for (int i = 0; i < validAttributeNames.length; i++) {
      String attName = validAttributeNames[i];
      if (source == null) {
        if (sm.get(attName) != null) {
          continue;
        }
      } else if (!source.equals(sm.get(attName))) {
        continue;
      }
      if (!sourceFound) {
        sourceFound = true;
        if (source == null) {
          pw.println("### GemFire Properties using default values ###");
        } else {
          pw.println("### GemFire Properties defined with " + source.getDescription() + " ###");
        }
      }
      // hide the shiro-init configuration for now. Remove after we can allow customer to specify shiro.ini file
      if (attName.equals(SECURITY_SHIRO_INIT)) {
        continue;
      }
      pw.print(attName);
      pw.print('=');
      if (source == null // always show defaults
          || (!secureSource // show no values from a secure source
              && okToDisplayPropertyValue(attName))) {
        pw.println(getAttribute(attName));
      } else {
        pw.println("********");
      }
    }
  }

  private boolean okToDisplayPropertyValue(String attName) {
    if (attName.startsWith(SECURITY_PREFIX)) {
      return false;
    }
    if (attName.startsWith(DistributionConfig.SSL_SYSTEM_PROPS_NAME)) {
      return false;
    }
    if (attName.startsWith(DistributionConfig.SYS_PROP_NAME)) {
      return false;
    }
    if (attName.toLowerCase().contains("password") /* bug 45381 */) {
      return false;
    }
    return true;
  }

  /**
   * This class was added to fix bug 39382.
   * It does this be overriding "keys" which is used by the store0
   * implementation of Properties.
   */
  protected static class SortedProperties extends Properties {

    private static final long serialVersionUID = 7156507110684631135L;

    @Override
    public Enumeration keys() {
      // the TreeSet gets the sorting we desire but is only safe
      // because the keys in this context are always String which is Comparable
      return Collections.enumeration(new TreeSet(keySet()));
    }
  }

  public boolean isDeprecated(String attName) {
    return false;
  }

  public Properties toProperties() {
    Properties result = new SortedProperties();
    String[] attNames = getAttributeNames();
    for (int i = 0; i < attNames.length; i++) {
      if (isDeprecated(attNames[i])) {
        continue;
      }
      result.setProperty(attNames[i], getAttribute(attNames[i]));
    }
    return result;
  }

  public void toFile(File f) throws IOException {
    FileOutputStream out = new FileOutputStream(f);
    try {
      toProperties().store(out, null);
    } finally {
      out.close();
    }
  }

  public boolean sameAs(Config other) {
    if (this == other) {
      return true;
    }
    if (other == null) {
      return false;
    }
    if (!this.getClass().equals(other.getClass())) {
      return false;
    }
    String[] validAttributeNames = getAttributeNames();
    for (int i = 0; i < validAttributeNames.length; i++) {
      String attName = validAttributeNames[i];
      if (this.isDeprecated(attName)) {
        // since toProperties skips isDeprecated sameAs
        // needs to also skip them.
        // See GEODE-405.
        continue;
      }
      Object thisAtt = this.getAttributeObject(attName);
      Object otherAtt = other.getAttributeObject(attName);
      if (thisAtt == otherAtt) {
        continue;
      } else if (thisAtt == null) {
        return false;
      } else if (thisAtt.getClass().isArray()) {
        int thisLength = Array.getLength(thisAtt);
        int otherLength = Array.getLength(otherAtt);
        if (thisLength != otherLength) {
          return false;
        }
        for (int j = 0; j < thisLength; j++) {
          Object thisArrObj = Array.get(thisAtt, j);
          Object otherArrObj = Array.get(otherAtt, j);
          if (thisArrObj == otherArrObj) {
            continue;
          } else if (thisArrObj == null) {
            return false;
          } else if (!thisArrObj.equals(otherArrObj)) {
            return false;
          }
        }
      } else if (!thisAtt.equals(otherAtt)) {
        return false;
      }
    }
    return true;
  }

  protected void checkAttributeName(String attName) {
    String[] validAttNames = getAttributeNames();
    if (!Arrays.asList(validAttNames).contains(attName.toLowerCase())) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractConfig_UNKNOWN_CONFIGURATION_ATTRIBUTE_NAME_0_VALID_ATTRIBUTE_NAMES_ARE_1.toLocalizedString(new Object[] {
        attName,
        SystemAdmin.join(validAttNames)
      }));
    }
  }

  public String getAttribute(String attName) {
    Object result = getAttributeObject(attName);
    if (result instanceof String) {
      return (String) result;
    }

    if (attName.equalsIgnoreCase(MEMBERSHIP_PORT_RANGE)) {
      int[] value = (int[]) result;
      return "" + value[0] + "-" + value[1];
    }

    if (result.getClass().isArray()) {
      return SystemAdmin.join((Object[]) result);
    }

    if (result instanceof InetAddress) {
      InetAddress addr = (InetAddress) result;
      String addrName = null;
      if (addr.isMulticastAddress() || !SocketCreator.resolve_dns) {
        addrName = addr.getHostAddress(); // on Windows getHostName on mcast addrs takes ~5 seconds
      } else {
        addrName = SocketCreator.getHostName(addr);
      }
      return addrName;
    }

    return result.toString();
  }

  public ConfigSource getAttributeSource(String attName) {
    return getAttSourceMap().get(attName);
  }

  public void setAttribute(String attName, String attValue, ConfigSource source) {
    Object attObjectValue;
    Class valueType = getAttributeType(attName);
    try {
      if (valueType.equals(String.class)) {
        attObjectValue = attValue;
      } else if (valueType.equals(String[].class)) {
        attObjectValue = commaDelimitedStringToStringArray(attValue);
      } else if (valueType.equals(Integer.class)) {
        attObjectValue = Integer.valueOf(attValue);
      } else if (valueType.equals(Long.class)) {
        attObjectValue = Long.valueOf(attValue);
      } else if (valueType.equals(Boolean.class)) {
        attObjectValue = Boolean.valueOf(attValue);
      } else if (valueType.equals(File.class)) {
        attObjectValue = new File(attValue);
      } else if (valueType.equals(int[].class)) {
        int[] value = new int[2];
        int minus = attValue.indexOf('-');
        if (minus <= 0) {
          throw new IllegalArgumentException("expected a setting in the form X-Y but found no dash for attribute " + attName);
        }
        value[0] = Integer.valueOf(attValue.substring(0, minus)).intValue();
        value[1] = Integer.valueOf(attValue.substring(minus + 1)).intValue();
        attObjectValue = value;
      } else if (valueType.equals(InetAddress.class)) {
        try {
          attObjectValue = InetAddress.getByName(attValue);
        } catch (UnknownHostException ex) {
          throw new IllegalArgumentException(LocalizedStrings.AbstractConfig_0_VALUE_1_MUST_BE_A_VALID_HOST_NAME_2.toLocalizedString(new Object[] {
            attName,
            attValue,
            ex.toString()
          }));
        }
      } else if (valueType.equals(String[].class)) {
        if (attValue == null || attValue.length() == 0) {
          attObjectValue = null;
        } else {
          String trimAttName = trimAttributeName(attName);
          throw new UnmodifiableException(LocalizedStrings.AbstractConfig_THE_0_CONFIGURATION_ATTRIBUTE_CAN_NOT_BE_SET_FROM_THE_COMMAND_LINE_SET_1_FOR_EACH_INDIVIDUAL_PARAMETER_INSTEAD
            .toLocalizedString(new Object[] { attName, trimAttName }));
        }
      } else if (valueType.equals(FlowControlParams.class)) {
        String values[] = attValue.split(",");
        if (values.length != 3) {
          throw new IllegalArgumentException(LocalizedStrings.AbstractConfig_0_VALUE_1_MUST_HAVE_THREE_ELEMENTS_SEPARATED_BY_COMMAS.toLocalizedString(new Object[] {
            attName,
            attValue
          }));
        }
        int credits = 0;
        float thresh = (float) 0.0;
        int waittime = 0;
        try {
          credits = Integer.parseInt(values[0].trim());
          thresh = Float.valueOf(values[1].trim()).floatValue();
          waittime = Integer.parseInt(values[2].trim());
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(LocalizedStrings.AbstractConfig_0_VALUE_1_MUST_BE_COMPOSED_OF_AN_INTEGER_A_FLOAT_AND_AN_INTEGER.toLocalizedString(new Object[] {
            attName,
            attValue
          }));
        }
        attObjectValue = new FlowControlParams(credits, thresh, waittime);
      } else {
        throw new InternalGemFireException(LocalizedStrings.AbstractConfig_UNHANDLED_ATTRIBUTE_TYPE_0_FOR_1.toLocalizedString(new Object[] {
          valueType,
          attName
        }));
      }
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractConfig_0_VALUE_1_MUST_BE_A_NUMBER.toLocalizedString(new Object[] { attName, attValue }));
    }
    setAttributeObject(attName, attObjectValue, source);
  }

  private String[] commaDelimitedStringToStringArray(final String tokenizeString) {
    StringTokenizer stringTokenizer = new StringTokenizer(tokenizeString, ",");
    String[] strings = new String[stringTokenizer.countTokens()];
    for (int i = 0; i < strings.length; i++) {
      strings[i] = stringTokenizer.nextToken();
    }
    return strings;
  }

  /**
   * Removes the last character of the input string and returns the trimmed name
   */
  protected static String trimAttributeName(String attName) {
    return attName.substring(0, attName.length() - 1);
  }

  public String getAttributeDescription(String attName) {
    checkAttributeName(attName);
    if (!getAttDescMap().containsKey(attName)) {
      throw new InternalGemFireException(LocalizedStrings.AbstractConfig_UNHANDLED_ATTRIBUTE_NAME_0.toLocalizedString(attName));
    }
    return (String) getAttDescMap().get(attName);
  }
}

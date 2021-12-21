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
package org.apache.geode.management.internal.configuration.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactoryConfigurationError;

import org.apache.commons.lang3.StringUtils;
import org.xml.sax.SAXException;

import org.apache.geode.DataSerializer;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.utils.XmlUtils;

public class ConfigurationResponse implements DataSerializableFixedID {

  private Map<String, Configuration> requestedConfiguration = new HashMap<>();
  private Map<String, Set<String>> jarNames = new HashMap<>();
  private boolean failedToGetSharedConfig = false;

  // This is set to the member from which this object was received
  private transient DistributedMember member;

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.CONFIGURATION_RESPONSE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    DataSerializer.writeHashMap(requestedConfiguration, out);
    DataSerializer.writeHashMap(jarNames, out);
    DataSerializer.writeBoolean(Boolean.valueOf(failedToGetSharedConfig), out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    requestedConfiguration = DataSerializer.readHashMap(in);
    jarNames = DataSerializer.readHashMap(in);
    failedToGetSharedConfig = DataSerializer.readBoolean(in);
  }

  public Map<String, Configuration> getRequestedConfiguration() {
    return requestedConfiguration;
  }

  public void addConfiguration(Configuration configuration) {
    if (configuration != null) {
      requestedConfiguration.put(configuration.getConfigName(), configuration);
    }
  }

  public String toString() {
    StringBuffer sb = new StringBuffer();
    Set<String> configNames = requestedConfiguration.keySet();
    for (String configName : configNames) {
      sb.append("\n" + requestedConfiguration.get(configName));
    }
    return sb.toString();
  }

  public String describeConfig() {
    StringBuffer sb = new StringBuffer();
    if (requestedConfiguration.isEmpty()) {
      sb.append("Received an empty shared configuration");
    } else {
      Set<Entry<String, Configuration>> entries = requestedConfiguration.entrySet();
      Iterator<Entry<String, Configuration>> iter = entries.iterator();

      while (iter.hasNext()) {
        Entry<String, Configuration> entry = iter.next();
        String configType = entry.getKey();
        Configuration config = entry.getValue();

        if (config != null) {
          sb.append("\n***************************************************************");
          sb.append("\nConfiguration for  '" + configType + "'");
          sb.append("\n\nJar files to be deployed:");

          Set<String> jarNames = config.getJarNames();
          if (jarNames.size() == 0) {
            sb.append("\n  None");
          } else {
            jarNames.forEach(c -> sb.append("\n  " + c));
          }

          try {
            String cacheXmlContent = config.getCacheXmlContent();
            if (StringUtils.isNotBlank(cacheXmlContent)) {
              sb.append("\n" + XmlUtils.prettyXml(cacheXmlContent));
            }
          } catch (IOException | TransformerFactoryConfigurationError | TransformerException
              | SAXException | ParserConfigurationException e) {
            throw new InternalGemFireError(e);
          }
        }

      }
    }
    return sb.toString();
  }

  public void addJar(String group, Set<String> jarNames) {
    this.jarNames.put(group, new HashSet<>(jarNames));
  }

  public Map<String, Set<String>> getJarNames() {
    return jarNames;
  }

  public DistributedMember getMember() {
    return member;
  }

  public void setMember(DistributedMember member) {
    this.member = member;
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }
}

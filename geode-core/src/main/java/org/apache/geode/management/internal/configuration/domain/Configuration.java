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
package org.apache.geode.management.internal.configuration.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.management.internal.configuration.utils.XmlUtils;

/**
 * Domain object for all the configuration related data.
 *
 */
public class Configuration implements DataSerializable {

  private static final long serialVersionUID = 1L;
  private String configName;
  private String cacheXmlContent;
  private String cacheXmlFileName;
  private String propertiesFileName;
  private Properties gemfireProperties;
  Set<String> jarNames;

  // Public no arg constructor required for Deserializable
  public Configuration() {

  }

  public Configuration(Configuration that) {
    this.configName = that.configName;
    this.cacheXmlContent = that.cacheXmlContent;
    this.cacheXmlFileName = that.cacheXmlFileName;
    this.propertiesFileName = that.propertiesFileName;
    this.gemfireProperties = new Properties();
    this.gemfireProperties.putAll(that.gemfireProperties);
    this.jarNames = new HashSet<>(that.jarNames);
  }

  public Configuration(String configName) {
    this.configName = configName;
    this.cacheXmlFileName = configName + ".xml";
    this.propertiesFileName = configName + ".properties";
    this.gemfireProperties = new Properties();
    this.jarNames = new HashSet<String>();
  }

  public String getCacheXmlContent() {
    return cacheXmlContent;
  }

  public void setCacheXmlContent(String cacheXmlContent) {
    this.cacheXmlContent = cacheXmlContent;
  }

  public void setCacheXmlFile(File cacheXmlFile)
      throws TransformerException, ParserConfigurationException, IOException, SAXException {
    if (cacheXmlFile.length() == 0) {
      this.cacheXmlContent = "";
    } else {
      Document doc = XmlUtils.getDocumentBuilder().parse(cacheXmlFile);
      this.cacheXmlContent = XmlUtils.elementToString(doc);
    }
  }

  public void setPropertiesFile(File propertiesFile) throws IOException {
    if (!propertiesFile.exists())
      return;

    FileInputStream fis = null;
    try {
      fis = new FileInputStream(propertiesFile);
      this.gemfireProperties.load(fis);
    } finally {
      if (fis != null) {
        fis.close();
      }
    }
  }

  public String getCacheXmlFileName() {
    return cacheXmlFileName;
  }

  public Properties getGemfireProperties() {
    return gemfireProperties;
  }

  public void setGemfireProperties(Properties gemfireProperties) {
    this.gemfireProperties = gemfireProperties;
  }

  public void addGemfireProperties(Properties gemfireProperties) {
    this.gemfireProperties.putAll(gemfireProperties);
  }

  public String getConfigName() {
    return configName;
  }

  public String getPropertiesFileName() {
    return propertiesFileName;
  }

  public void addJarNames(Set<String> jarNames) {
    this.jarNames.addAll(jarNames);
  }

  public void addJarNames(String[] jarNames) {
    if (jarNames == null)
      return;

    this.jarNames.addAll(Stream.of(jarNames).collect(Collectors.toSet()));
  }


  public void removeJarNames(String[] jarNames) {
    if (jarNames != null) {
      this.jarNames.removeAll(Stream.of(jarNames).collect(Collectors.toSet()));
    } else {
      this.jarNames.clear();
    }
  }

  public Set<String> getJarNames() {
    return this.jarNames;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(configName, out);
    DataSerializer.writeString(cacheXmlFileName, out);
    DataSerializer.writeString(cacheXmlContent, out);
    DataSerializer.writeString(propertiesFileName, out);
    DataSerializer.writeProperties(gemfireProperties, out);
    DataSerializer.writeHashSet((HashSet<?>) jarNames, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.configName = DataSerializer.readString(in);
    this.cacheXmlFileName = DataSerializer.readString(in);
    this.cacheXmlContent = DataSerializer.readString(in);
    this.propertiesFileName = DataSerializer.readString(in);
    this.gemfireProperties = DataSerializer.readProperties(in);
    this.jarNames = DataSerializer.readHashSet(in);
  }


  @Override
  public String toString() {
    return "Configuration [configName=" + configName + ", cacheXmlContent=" + cacheXmlContent
        + ", cacheXmlFileName=" + cacheXmlFileName + ", propertiesFileName=" + propertiesFileName
        + ", gemfireProperties=" + gemfireProperties + ", jarNames=" + jarNames + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((cacheXmlContent == null) ? 0 : cacheXmlContent.hashCode());
    result = prime * result + ((cacheXmlFileName == null) ? 0 : cacheXmlFileName.hashCode());
    result = prime * result + ((configName == null) ? 0 : configName.hashCode());
    result = prime * result + ((gemfireProperties == null) ? 0 : gemfireProperties.hashCode());
    result = prime * result + ((jarNames == null) ? 0 : jarNames.hashCode());
    result = prime * result + ((propertiesFileName == null) ? 0 : propertiesFileName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof Configuration))
      return false;
    Configuration other = (Configuration) obj;
    if (cacheXmlContent == null) {
      if (other.cacheXmlContent != null)
        return false;
    } else if (!cacheXmlContent.equals(other.cacheXmlContent))
      return false;
    if (cacheXmlFileName == null) {
      if (other.cacheXmlFileName != null)
        return false;
    } else if (!cacheXmlFileName.equals(other.cacheXmlFileName))
      return false;
    if (configName == null) {
      if (other.configName != null)
        return false;
    } else if (!configName.equals(other.configName))
      return false;
    if (gemfireProperties == null) {
      if (other.gemfireProperties != null)
        return false;
    } else if (!gemfireProperties.equals(other.gemfireProperties))
      return false;
    if (jarNames == null) {
      if (other.jarNames != null)
        return false;
    } else if (!jarNames.equals(other.jarNames))
      return false;
    if (propertiesFileName == null) {
      if (other.propertiesFileName != null)
        return false;
    } else if (!propertiesFileName.equals(other.propertiesFileName))
      return false;
    return true;
  }

}

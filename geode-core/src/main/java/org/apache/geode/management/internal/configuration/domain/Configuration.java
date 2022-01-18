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


import static org.apache.geode.management.internal.utils.JarFileUtils.getArtifactId;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.VersionedDataSerializable;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.serialization.Versioning;
import org.apache.geode.internal.serialization.VersioningIO;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.internal.configuration.utils.XmlUtils;

/**
 * Domain object for all the configuration related data.
 *
 */
public class Configuration implements VersionedDataSerializable {
  private static final long serialVersionUID = 1L;
  private String configName;
  private String cacheXmlContent;
  private String cacheXmlFileName;
  private String propertiesFileName;
  private Properties gemfireProperties;
  private final Map<String, Deployment> deployments = new HashMap<>();

  // Public no arg constructor required for Deserializable
  public Configuration() {

  }

  public Configuration(Configuration that) {
    configName = that.configName;
    cacheXmlContent = that.cacheXmlContent;
    cacheXmlFileName = that.cacheXmlFileName;
    propertiesFileName = that.propertiesFileName;
    gemfireProperties = new Properties();
    gemfireProperties.putAll(that.gemfireProperties);
    deployments.putAll(that.deployments);
  }

  public Configuration(String configName) {
    this.configName = configName;
    cacheXmlFileName = configName + ".xml";
    propertiesFileName = configName + ".properties";
    gemfireProperties = new Properties();
  }

  public String getCacheXmlContent() {
    return cacheXmlContent;
  }

  public void setCacheXmlContent(String cacheXmlContent) {
    this.cacheXmlContent = cacheXmlContent;
  }

  public void setCacheXmlFile(File cacheXmlFile) throws IOException {
    if (cacheXmlFile.length() == 0) {
      cacheXmlContent = "";
    } else {
      try {
        Document doc = XmlUtils.getDocumentBuilder().parse(cacheXmlFile);
        cacheXmlContent = XmlUtils.elementToString(doc);
      } catch (SAXException | TransformerException | ParserConfigurationException e) {
        throw new IOException("Unable to parse existing cluster configuration from file: "
            + cacheXmlFile.getAbsolutePath(), e);
      }
    }
  }

  public void setPropertiesFile(File propertiesFile) throws IOException {
    if (!propertiesFile.exists()) {
      return;
    }

    try (FileInputStream fis = new FileInputStream(propertiesFile)) {
      gemfireProperties.load(fis);
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

  public void putDeployment(Deployment deployment) {
    String artifactId = getArtifactId(deployment.getFileName());
    deployments.values().removeIf(d -> getArtifactId(d.getFileName()).equals(artifactId));
    deployments.put(deployment.getId(), deployment);
  }

  public Collection<Deployment> getDeployments() {
    return deployments.values();
  }

  public void removeDeployments(Collection<String> jarNames) {
    if (jarNames == null) {
      deployments.clear();
    } else {
      for (String jarName : jarNames) {
        deployments.remove(jarName);
      }
    }
  }

  public Set<String> getJarNames() {
    return deployments.keySet();
  }

  public void toDataPre_GEODE_1_12_0_0(DataOutput out) throws IOException {
    DataSerializer.writeString(configName, out);
    DataSerializer.writeString(cacheXmlFileName, out);
    DataSerializer.writeString(cacheXmlContent, out);
    DataSerializer.writeString(propertiesFileName, out);
    DataSerializer.writeProperties(gemfireProperties, out);
    HashSet<String> jarNames = new HashSet<>(deployments.keySet());
    DataSerializer.writeHashSet(jarNames, out);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(configName, out);
    DataSerializer.writeString(cacheXmlFileName, out);
    DataSerializer.writeString(cacheXmlContent, out);
    DataSerializer.writeString(propertiesFileName, out);
    DataSerializer.writeProperties(gemfireProperties, out);
    // As of 1.12, it writes a jarNames HashSet to the stream, so that pre 1.12.0 members can
    // read the jarName, and will now also write the deployment map for the post 1.12.0 members.
    HashSet<String> jarNames = new HashSet<>(deployments.keySet());
    DataSerializer.writeHashSet(jarNames, out);
    // As of 1.12, this class starting writing the current version
    VersioningIO.writeOrdinal(out, KnownVersion.getCurrentVersion().ordinal(), true);
    DataSerializer.writeHashMap(deployments, out);
  }

  public void fromDataPre_GEODE_1_12_0_0(DataInput in) throws IOException, ClassNotFoundException {
    configName = DataSerializer.readString(in);
    cacheXmlFileName = DataSerializer.readString(in);
    cacheXmlContent = DataSerializer.readString(in);
    propertiesFileName = DataSerializer.readString(in);
    gemfireProperties = DataSerializer.readProperties(in);
    HashSet<String> jarNames = DataSerializer.readHashSet(in);
    jarNames.stream()
        .map(x -> new Deployment(x, null, null))
        .forEach(deployment -> deployments.put(deployment.getFileName(),
            deployment));
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    configName = DataSerializer.readString(in);
    cacheXmlFileName = DataSerializer.readString(in);
    cacheXmlContent = DataSerializer.readString(in);
    propertiesFileName = DataSerializer.readString(in);
    gemfireProperties = DataSerializer.readProperties(in);
    HashSet<String> jarNames = DataSerializer.readHashSet(in);
    try {
      // version of the data we are reading (1.12 or later)
      final Version version = Versioning.getVersion(VersioningIO.readOrdinal(in));
      if (version.isNotOlderThan(KnownVersion.GEODE_1_12_0)) {
        deployments.putAll(DataSerializer.readHashMap(in));
      }
    } catch (EOFException ex) {
      if (jarNames != null) {
        // we are reading pre 1.12 data. So add each jar name to deployments
        jarNames.stream()
            .map(x -> new Deployment(x, null, null))
            .forEach(
                deployment -> deployments.put(deployment.getFileName(), deployment));
      }
    }
  }

  @Override
  public String toString() {
    return "Configuration{" +
        "configName='" + configName + '\'' +
        ", cacheXmlContent='" + cacheXmlContent + '\'' +
        ", cacheXmlFileName='" + cacheXmlFileName + '\'' +
        ", propertiesFileName='" + propertiesFileName + '\'' +
        ", gemfireProperties=" + gemfireProperties +
        ", deployments=" + deployments +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Configuration that = (Configuration) o;
    return Objects.equals(configName, that.configName) &&
        Objects.equals(cacheXmlContent, that.cacheXmlContent) &&
        Objects.equals(cacheXmlFileName, that.cacheXmlFileName) &&
        Objects.equals(propertiesFileName, that.propertiesFileName) &&
        Objects.equals(gemfireProperties, that.gemfireProperties) &&
        Objects.equals(deployments, that.deployments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(configName, cacheXmlContent, cacheXmlFileName, propertiesFileName,
        gemfireProperties, deployments);
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return new KnownVersion[] {KnownVersion.GEODE_1_12_0};
  }
}

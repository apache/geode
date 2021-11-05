/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.deployment.internal.modules.xml.generator;

import java.io.File;
import java.nio.file.Path;
import java.util.Collection;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class GeodeJBossXmlGenerator {

  private static final String NAME = "name";
  private static final String PATH = "path";
  private static final String OPTIONAL = "optional";
  private static final String SERVICES = "services";
  private static final String EXPORT = "export";
  private static final String MODULE = "module";
  private static final String IMPORT = "import";
  private static final String DEPENDENCIES = "dependencies";
  private static final String RESOURCE_ROOT = "resource-root";
  private static final String RESOURCES = "resources";

  private final DocumentBuilderFactory documentBuilderFactory =
      DocumentBuilderFactory.newInstance();

  public void generate(Path outputRoot, String moduleName, String moduleVersion,
      Collection<String> resourceRoots, Collection<String> dependencies,
      boolean exportDependencies) {

    File xmlFile = createModuleDescriptorFile(outputRoot, moduleName, moduleVersion);

    try {
      DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
      Document document = documentBuilder.newDocument();

      Element moduleElement = addModuleElement(document, moduleName, moduleVersion);

      addResourceRoots(document, moduleElement, resourceRoots);

      addModuleDependencies(document, moduleElement, dependencies, exportDependencies);

      // write xml to file
      writeDocumentToFile(xmlFile, document);
    } catch (ParserConfigurationException | TransformerException e) {
      throw new RuntimeException("Module descriptor could not be created", e);
    }
  }

  private void writeDocumentToFile(File xmlFile, Document document) throws TransformerException {
    DOMSource domSource = new DOMSource(document);
    TransformerFactory transformerFactory = TransformerFactory.newInstance();
    Transformer transformer = transformerFactory.newTransformer();
    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
    transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
    StreamResult streamResult = new StreamResult(xmlFile);
    transformer.transform(domSource, streamResult);
  }

  private Element addModuleElement(Document document, String moduleName, String moduleVersion) {
    Element moduleElement = document.createElement("module");
    moduleElement.setAttribute("xmlns", "urn:jboss:module:1.9");
    String versionedName = moduleVersion == null ? moduleName : moduleName + ":" + moduleVersion;
    moduleElement.setAttribute(NAME, versionedName);
    document.appendChild(moduleElement);
    return moduleElement;
  }

  private void addResourceRoots(Document document,
      Element moduleElement, Collection<String> resourcePaths) {
    Element resourcesParentElement = document.createElement(RESOURCES);
    moduleElement.appendChild(resourcesParentElement);

    for (String resourcePath : resourcePaths) {
      Element resourceElement = document.createElement(RESOURCE_ROOT);
      resourceElement.setAttribute(PATH, resourcePath);
      resourcesParentElement.appendChild(resourceElement);
    }
  }

  private void addModuleDependencies(Document document, Element moduleElement,
      Collection<String> dependencies, boolean exportDependencies) {
    Element dependenciesParentElement = document.createElement(DEPENDENCIES);
    moduleElement.appendChild(dependenciesParentElement);
    String services = exportDependencies ? EXPORT : IMPORT;
    for (String dependency : dependencies) {
      Element dependencyElement = document.createElement(MODULE);
      dependencyElement.setAttribute(NAME, dependency);
      dependencyElement.setAttribute(OPTIONAL, "false");
      dependencyElement.setAttribute(SERVICES, services);
      dependencyElement.setAttribute(EXPORT, Boolean.toString(exportDependencies));
      dependenciesParentElement.appendChild(dependencyElement);
    }
  }

  private File createModuleDescriptorFile(Path outputRoot, String moduleName,
      String moduleVersion) {
    if (moduleVersion == null) {
      moduleVersion = "main";
    }
    Path moduleDirectory = outputRoot.resolve(moduleName).resolve(moduleVersion);
    if (!moduleDirectory.toFile().exists()) {
      if (!moduleDirectory.toFile().mkdirs()) {
        throw new RuntimeException("Could not create directory: " + moduleDirectory);
      }
    }

    return moduleDirectory.resolve("module.xml").toFile();
  }
}

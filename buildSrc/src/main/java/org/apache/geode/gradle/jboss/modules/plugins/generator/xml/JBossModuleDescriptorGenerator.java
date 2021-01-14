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
package org.apache.geode.gradle.jboss.modules.plugins.generator.xml;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.lang3.StringUtils;
import org.gradle.internal.Pair;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import org.apache.geode.gradle.jboss.modules.plugins.generator.ModuleDescriptorGenerator;
import org.apache.geode.gradle.jboss.modules.plugins.generator.domain.ModuleDependency;

public class JBossModuleDescriptorGenerator implements ModuleDescriptorGenerator {

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
  private static final String EXPORTS = "exports";

  private final DocumentBuilderFactory documentBuilderFactory =
      DocumentBuilderFactory.newInstance();

  @Override
  public void generate(Path outputRoot, String moduleName, String moduleVersion,
      Set<String> resourceRoots, List<ModuleDependency> moduleDependencies) {
    generate(outputRoot, moduleName, moduleVersion, resourceRoots, moduleDependencies, null,
        Collections.EMPTY_LIST, Collections.EMPTY_LIST,Collections.EMPTY_LIST);
  }

  @Override
  public void generate(Path outputRoot, String moduleName, String moduleVersion,
      Set<String> resourceRoots,
      List<ModuleDependency> moduleDependencies, String mainClass, List libraryPackagesToExport,
      List customPackagesToExport, List customPackagesToExclude) {
    File xmlFile = createModuleDescriptorFile(outputRoot, moduleName, moduleVersion);

    try {
      DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
      Document document = documentBuilder.newDocument();

      // add module element
      Element moduleElement =
          createElement(document, "module", Pair.of("xmlns", "urn:jboss:module:1.9"),
              Pair.of(NAME, moduleName + ":" + moduleVersion));
      document.appendChild(moduleElement);
      // add resources
      addResourceRoots(document, new LinkedList<>(resourceRoots), moduleElement);
      // add main class
      if (!StringUtils.isEmpty(mainClass)) {
        moduleElement
            .appendChild(createElement(document, "main-class", Pair.of(NAME, mainClass)));
      }

      // add module dependencies
      addModuleDependencies(document, moduleDependencies, moduleElement);

      // If extension add export filter on project files
      Element exportElement = document.createElement(EXPORTS);
      moduleElement.appendChild(exportElement);
//      addPackageExportFilters(libraryPackagesToExport, exportElement, true);
      addPackageExportFilters(customPackagesToExport, exportElement, false);
//      addExcludeFilterToPackageExportFilters(exportElement,
//          (!libraryPackagesToExport.isEmpty() || !customPackagesToExport.isEmpty()));

      // write xml to file
      writeDocumentToFile(xmlFile, document);
    } catch (ParserConfigurationException | TransformerException e) {
      throw new RuntimeException("Module Definition could not be created", e);
    }
  }

  private void addExcludeFilterToPackageExportFilters(Element moduleElement,
      boolean shouldAddElement) {
    if (shouldAddElement) {
      Document document = moduleElement.getOwnerDocument();
      Element excludeSetElement = document.createElement("exclude");
      excludeSetElement.setAttribute("path", "*");
      moduleElement.appendChild(excludeSetElement);
    }
  }

  private void addPackageExportFilters(List<String> packagesToExport, Element moduleElement,
      boolean usePathSets) {
    Document document = moduleElement.getOwnerDocument();
    if (usePathSets) {
      Element includeElement = document.createElement("include-set");
      for (String export : packagesToExport) {
        Element pathElement = document.createElement("path");
        pathElement.setAttribute("name", export);
        includeElement.appendChild(pathElement);
      }
      moduleElement.appendChild(includeElement);
    } else {
      for (String export : packagesToExport) {
        Element includeElement = document.createElement("include");
        includeElement.setAttribute("path", export);
        moduleElement.appendChild(includeElement);
      }
    }
  }

  @Override
  public void generateAlias(Path outputRoot, String moduleName, String moduleVersion) {
    File xmlFile = createModuleDescriptorFile(outputRoot, moduleName, "main");

    DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    try {
      DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
      Document document = documentBuilder.newDocument();

      // add module element
      Element moduleElement =
          createElement(document, "module-alias", Pair.of("xmlns", "urn:jboss:module:1.9"),
              Pair.of(NAME, moduleName), Pair.of("target-name", moduleName + ":" + moduleVersion));
      document.appendChild(moduleElement);

      // write xml to file
      writeDocumentToFile(xmlFile, document);
    } catch (ParserConfigurationException | TransformerException e) {
      throw new RuntimeException("Module Alias Definition could not be created", e);
    }
  }

  private File createModuleDescriptorFile(Path outputRoot, String moduleName,
      String moduleVersion) {
    Path parentPath = outputRoot.resolve(moduleName).resolve(moduleVersion);
    File xmlFile = parentPath.resolve("module.xml").toFile();
    if (!xmlFile.exists()) {
      try {
        parentPath.toFile().mkdirs();
        xmlFile.createNewFile();
        return xmlFile;
      } catch (IOException e) {
        throw new RuntimeException("File could not be created", e);
      }
    } else {
      return xmlFile;
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

  private void addResourceRoots(Document document, List<String> resourceRoots,
      Element moduleElement) {
    Element resourcesElement = createElement(document, RESOURCES);
    moduleElement.appendChild(resourcesElement);

    Collections.sort(resourceRoots);
    for (String resource : resourceRoots) {
      resourcesElement
          .appendChild(createElement(document, RESOURCE_ROOT, Pair.of(PATH, resource)));
    }
  }

  @SafeVarargs
  private final Element createElement(Document document, String name,
      Pair<String, String>... attributes) {
    Element element = document.createElement(name);
    for (Pair<String, String> attribute : attributes) {
      element.setAttribute(attribute.left, attribute.right);
    }
    return element;
  }

  private void addModuleDependencies(Document document, List<ModuleDependency> dependencies,
      Element moduleElement) {
    Element dependenciesElement = document.createElement(DEPENDENCIES);
    moduleElement.appendChild(dependenciesElement);

    dependencies.sort(Comparator.comparing(ModuleDependency::getName));
    for (ModuleDependency dependency : dependencies) {
      String services = dependency.isExport() ? EXPORT : IMPORT;
      Element exportedDependencyElement = createElement(document, MODULE,
          Pair.of(NAME, dependency.getName()),
          Pair.of(OPTIONAL, Boolean.toString(dependency.isOptional())),
          Pair.of(SERVICES, services),
          Pair.of(EXPORT, Boolean.toString(dependency.isExport())));

      dependenciesElement.appendChild(exportedDependencyElement);
    }
  }
}

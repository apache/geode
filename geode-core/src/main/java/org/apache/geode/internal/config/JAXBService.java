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

package org.apache.geode.internal.config;

import java.io.StringReader;
import java.io.StringWriter;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import com.sun.xml.internal.bind.marshaller.NamespacePrefixMapper;
import org.apache.commons.lang.StringUtils;
import org.xml.sax.SAXException;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.internal.ClassPathLoader;

public class JAXBService {
  public static String CACHE_SCHEMA =
      "http://geode.apache.org/schema/cache http://geode.apache.org/schema/cache/cache-1.0.xsd";
  Map<Class, String> classAndSchema = new HashMap<>();

  // Set<Class> bindClasses = new HashSet<>();
  Marshaller marshaller;
  Unmarshaller unmarshaller;

  // the default service will handle the cache.xsd validation and set's the cache schema location
  public JAXBService() {
    registerBindClassWithSchema(CacheConfig.class, CACHE_SCHEMA);
    // find the local Cache-1.0.xsd
    URL local_cache_xsd = ClassPathLoader.getLatest()
        .getResource("META-INF/schemas/geode.apache.org/schema/cache/cache-1.0.xsd");
    validateWith(local_cache_xsd);
  }

  public void validateWith(URL url) {
    SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema schema = null;
    try {
      schema = factory.newSchema(url);
    } catch (SAXException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    marshaller.setSchema(schema);
  }

  /**
   *
   * @param c e.g CacheConfig.class
   * @param nameSpaceAndSchemaLocation e.g "http://geode.apache.org/schema/cache
   *        http://geode.apache.org/schema/cache/cache-1.0.xsd"
   */
  public void registerBindClassWithSchema(Class c, String nameSpaceAndSchemaLocation) {
    // if this class is not in the map yet
    if (!classAndSchema.keySet().contains(c)) {
      classAndSchema.put(c, nameSpaceAndSchemaLocation);
      try {
        Set<Class> bindClasses = classAndSchema.keySet();
        JAXBContext jaxbContext =
            JAXBContext.newInstance(bindClasses.toArray(new Class[bindClasses.size()]));
        marshaller = jaxbContext.createMarshaller();
        unmarshaller = jaxbContext.createUnmarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.setProperty("com.sun.xml.internal.bind.namespacePrefixMapper",
            new CacheNamespaceMapper());
        updateSchema();
      } catch (Exception e) {
        throw new RuntimeException(e.getMessage(), e);
      }
      return;
    }

    // if this class is in the map already and we are simply adding/updating schema
    String oldSchema = classAndSchema.get(c);
    if (nameSpaceAndSchemaLocation == null) {
      return;
    }

    if (!nameSpaceAndSchemaLocation.equals(oldSchema)) {
      classAndSchema.put(c, nameSpaceAndSchemaLocation);
      updateSchema();
    }
  }

  void updateSchema() {
    try {
      String schemas = classAndSchema.values().stream().filter(Objects::nonNull)
          .collect(Collectors.joining(" "));
      marshaller.setProperty(Marshaller.JAXB_SCHEMA_LOCATION, schemas);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  public String marshall(Object object) {
    StringWriter sw = new StringWriter();
    try {
      marshaller.marshal(object, sw);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    return sw.toString();
  }

  public <T> T unMarshall(String xml) {
    try {
      return (T) unmarshaller.unmarshal(new StringReader(xml));
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  public void registerBindClasses(Class clazz) {
    registerBindClassWithSchema(clazz, null);
  }

  private class CacheNamespaceMapper extends NamespacePrefixMapper {
    private static final String CACHE_PREFIX = "";
    private static final String CACHE_URI = "http://geode.apache.org/schema/cache";
    private static final String XSI_URI = "http://www.w3.org/2001/XMLSchema-instance";

    @Override
    public String getPreferredPrefix(String namespaceUri, String suggestion,
        boolean requirePrefix) {
      if (CACHE_URI.equals(namespaceUri)) {
        return CACHE_PREFIX;
      }

      if (StringUtils.isEmpty(namespaceUri) || XSI_URI.equals(namespaceUri)) {
        return suggestion;
      }
      int lastSlash = namespaceUri.lastIndexOf("/");
      if (lastSlash > 0) {
        return namespaceUri.substring(lastSlash + 1).toLowerCase();
      }
      return suggestion;
    }

    public String[] getPreDeclaredNamespaceUris() {
      return new String[] {CACHE_URI};
    }
  }
}

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
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.commons.lang.StringUtils;
import org.xml.sax.SAXException;

import org.apache.geode.cache.configuration.XSDRootElement;
import org.apache.geode.internal.ClassPathLoader;

public class JAXBService {
  Marshaller marshaller;
  Unmarshaller unmarshaller;

  public JAXBService(Class<?>... xsdRootClasses) {
    try {
      Set<Class> bindClasses = Arrays.stream(xsdRootClasses).collect(Collectors.toSet());
      JAXBContext jaxbContext =
          JAXBContext.newInstance(bindClasses.toArray(new Class[bindClasses.size()]));
      marshaller = jaxbContext.createMarshaller();
      unmarshaller = jaxbContext.createUnmarshaller();
      marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

      String schemas = Arrays.stream(xsdRootClasses).map(c -> {
        XSDRootElement element = c.getAnnotation(XSDRootElement.class);
        if (element != null && StringUtils.isNotEmpty(element.namespace())
            && StringUtils.isNotEmpty(element.schemaLocation())) {
          return (element.namespace() + " " + element.schemaLocation());
        }
        return "";
      }).collect(Collectors.joining(" "));

      marshaller.setProperty(Marshaller.JAXB_SCHEMA_LOCATION, schemas.trim());
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
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

  public void validateWithLocalCacheXSD() {
    // find the local Cache-1.0.xsd
    URL local_cache_xsd = ClassPathLoader.getLatest()
        .getResource("META-INF/schemas/geode.apache.org/schema/cache/cache-1.0.xsd");
    validateWith(local_cache_xsd);
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
}

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
package org.apache.geode.connectors.jdbc.internal.xml;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.SerializationException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMappingExistsException;
import org.apache.geode.connectors.jdbc.internal.TableMetaDataManager;
import org.apache.geode.connectors.jdbc.internal.TableMetaDataView;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.internal.cache.extension.Extension;
import org.apache.geode.internal.cache.extension.ExtensionPoint;
import org.apache.geode.internal.cache.xmlcache.XmlGenerator;
import org.apache.geode.internal.classloader.ClassPathLoader;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.pdx.internal.PdxOutputStream;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.PdxWriterImpl;
import org.apache.geode.pdx.internal.TypeRegistry;

public class RegionMappingConfiguration implements Extension<Region<?, ?>> {

  private final RegionMapping mapping;

  public RegionMappingConfiguration(RegionMapping mapping) {
    this.mapping = mapping;
  }

  @Override
  public XmlGenerator<Region<?, ?>> getXmlGenerator() {
    return null;
  }

  @Override
  public void beforeCreate(Extensible<Region<?, ?>> source, Cache cache) {
    // nothing
  }

  @Override
  public void onCreate(Extensible<Region<?, ?>> source, Extensible<Region<?, ?>> target) {
    final ExtensionPoint<Region<?, ?>> extensionPoint = target.getExtensionPoint();
    final Region<?, ?> region = extensionPoint.getTarget();
    InternalCache internalCache = (InternalCache) region.getRegionService();
    JdbcConnectorService service = internalCache.getService(JdbcConnectorService.class);
    if (mapping.getFieldMappings().isEmpty()) {
      Class<?> pdxClazz = loadPdxClass(mapping.getPdxName());
      PdxType pdxType = getPdxTypeForClass(internalCache, pdxClazz);

      List<FieldMapping> fieldMappings = createDefaultFieldMapping(service, pdxType);
      fieldMappings.forEach(fieldMapping -> {
        mapping.addFieldMapping(fieldMapping);
      });
    }
    service.validateMapping(mapping);
    createRegionMapping(service, mapping);
  }

  private void createRegionMapping(JdbcConnectorService service,
      RegionMapping regionMapping) {
    try {
      service.createRegionMapping(regionMapping);
    } catch (RegionMappingExistsException e) {
      throw new InternalGemFireException(e);
    }
  }

  List<FieldMapping> createDefaultFieldMapping(JdbcConnectorService service,
      PdxType pdxType) {
    DataSource dataSource = getDataSource(mapping.getDataSourceName());
    if (dataSource == null) {
      throw new JdbcConnectorException("No datasource \"" + mapping.getDataSourceName()
          + "\" found when creating default field mapping");
    }
    TableMetaDataManager manager = getTableMetaDataManager();
    try (Connection connection = dataSource.getConnection()) {
      TableMetaDataView tableMetaData = manager.getTableMetaDataView(connection, mapping);
      return service.createFieldMappingUsingPdx(pdxType, tableMetaData);
    } catch (SQLException e) {
      throw JdbcConnectorException.createException(e);
    }
  }

  protected PdxType getPdxTypeForClass(Cache cache, Class<?> clazz) {
    InternalCache internalCache = (InternalCache) cache;
    TypeRegistry typeRegistry = internalCache.getPdxRegistry();

    PdxType result = typeRegistry.getExistingTypeForClass(clazz);
    if (result != null) {
      return result;
    }
    return generatePdxTypeForClass(internalCache, typeRegistry, clazz);
  }

  /**
   * Generates and returns a PdxType for the given class.
   * The generated PdxType is also stored in the TypeRegistry.
   *
   * @param cache used to generate pdx type
   * @param clazz the class to generate a PdxType for
   * @return the generated PdxType
   * @throws JdbcConnectorException if a PdxType can not be generated
   */
  PdxType generatePdxTypeForClass(InternalCache cache, TypeRegistry typeRegistry,
      Class<?> clazz) {
    Object object = createInstance(clazz);
    try {
      cache.registerPdxMetaData(object);
    } catch (SerializationException ex) {
      String className = clazz.getName();
      ReflectionBasedAutoSerializer serializer =
          getReflectionBasedAutoSerializer("\\Q" + className + "\\E");
      PdxWriter writer = createPdxWriter(typeRegistry, object);
      boolean result = serializer.toData(object, writer);
      if (!result) {
        throw new JdbcConnectorException(
            "Could not generate a PdxType using the ReflectionBasedAutoSerializer for the class  "
                + clazz.getName() + " after failing to register pdx metadata due to "
                + ex.getMessage() + ". Check the server log for details.");
      }
    }
    // serialization will leave the type in the registry
    return typeRegistry.getExistingTypeForClass(clazz);
  }

  private Object createInstance(Class<?> clazz) {
    try {
      Constructor<?> ctor = clazz.getConstructor();
      return ctor.newInstance(new Object[] {});
    } catch (NoSuchMethodException | SecurityException | InstantiationException
        | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
      throw new JdbcConnectorException(
          "Could not generate a PdxType for the class " + clazz.getName()
              + " because it did not have a public zero arg constructor. Details: " + ex);
    }
  }

  private Class<?> loadPdxClass(String className) {
    try {
      return ClassPathLoader.getLatest().forName(className);
    } catch (ClassNotFoundException ex) {
      throw new JdbcConnectorException(
          "The pdx class \"" + className + "\" could not be loaded because: " + ex);
    }
  }

  // unit test mocks this method
  DataSource getDataSource(String dataSourceName) {
    return JNDIInvoker.getDataSource(dataSourceName);
  }

  // unit test mocks this method
  ReflectionBasedAutoSerializer getReflectionBasedAutoSerializer(String className) {
    return new ReflectionBasedAutoSerializer(className);
  }

  // unit test mocks this method
  PdxWriter createPdxWriter(TypeRegistry typeRegistry, Object object) {
    return new PdxWriterImpl(typeRegistry, object, new PdxOutputStream());
  }

  // unit test mocks this method
  TableMetaDataManager getTableMetaDataManager() {
    return new TableMetaDataManager();
  }
}

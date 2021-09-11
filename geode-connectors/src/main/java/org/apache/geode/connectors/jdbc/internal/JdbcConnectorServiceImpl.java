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
package org.apache.geode.connectors.jdbc.internal;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.connectors.jdbc.JdbcWriter;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.beans.CacheServiceMBeanBase;
import org.apache.geode.pdx.internal.PdxField;
import org.apache.geode.pdx.internal.PdxType;

@Experimental
public class JdbcConnectorServiceImpl implements JdbcConnectorService {

  private static final Logger logger = LogService.getLogger();
  private final Map<String, RegionMapping> mappingsByRegion =
      new ConcurrentHashMap<>();

  @Override
  public Set<RegionMapping> getRegionMappings() {
    Set<RegionMapping> regionMappings = new HashSet<>();
    regionMappings.addAll(mappingsByRegion.values());
    return regionMappings;
  }

  @Override
  public void createRegionMapping(RegionMapping mapping)
      throws RegionMappingExistsException {
    RegionMapping existing =
        mappingsByRegion.putIfAbsent(mapping.getRegionName(), mapping);
    if (existing != null) {
      throw new RegionMappingExistsException(
          "JDBC mapping for region " + mapping.getRegionName() + " exists");
    }
  }

  @Override
  public void replaceRegionMapping(RegionMapping alteredMapping)
      throws RegionMappingNotFoundException {
    RegionMapping existingMapping =
        mappingsByRegion.get(alteredMapping.getRegionName());
    if (existingMapping == null) {
      throw new RegionMappingNotFoundException(
          "JDBC mapping for the region " + alteredMapping.getRegionName() + " was not found");
    }

    mappingsByRegion.put(existingMapping.getRegionName(), alteredMapping);
  }

  @Override
  public boolean isMappingSynchronous(String regionName, Cache cache) {
    Region<?, ?> region = cache.getRegion(regionName);
    if (region == null) {
      throw new IllegalStateException("Region for mapping could not be found.");
    }

    // If our region has a Jdbc Writer set as the cache writer then we know it is syncronous
    return region.getAttributes().getCacheWriter() != null
        && region.getAttributes().getCacheWriter() instanceof JdbcWriter;
  }

  @Override
  public void validateMapping(RegionMapping regionMapping) {

    DataSource dataSource = getDataSource(regionMapping.getDataSourceName());
    if (dataSource == null) {
      throw new JdbcConnectorException("No datasource \"" + regionMapping.getDataSourceName()
          + "\" found when creating mapping \"" + regionMapping.getRegionName() + "\"");
    }
    validateMapping(regionMapping, dataSource);
  }

  @Override
  public void validateMapping(RegionMapping regionMapping, DataSource dataSource) {
    TableMetaDataView metaDataView = getTableMetaDataView(regionMapping, dataSource);
    boolean foundDifference = false;

    if (regionMapping.getFieldMappings().size() != metaDataView.getColumnNames().size()) {
      foundDifference = true;
    } else {
      for (FieldMapping fieldMapping : regionMapping.getFieldMappings()) {
        String jdbcName = fieldMapping.getJdbcName();
        if (!metaDataView.getColumnNames().contains(jdbcName)) {
          foundDifference = true;
          break;
        }
        if (!metaDataView.getColumnDataType(jdbcName).getName()
            .equals(fieldMapping.getJdbcType())) {
          foundDifference = true;
          break;
        }
        if (metaDataView.isColumnNullable(jdbcName) != fieldMapping.isJdbcNullable()) {
          foundDifference = true;
          break;
        }
      }
    }

    if (!foundDifference) {
      if (!regionMapping.getSpecifiedIds()
          && !regionMapping.getIds().equals(String.join(",", metaDataView.getKeyColumnNames()))) {
        foundDifference = true;
      }
    }

    if (foundDifference) {
      StringBuilder sb = new StringBuilder();
      sb.append(
          "Error detected when comparing mapping for region \"" + regionMapping.getRegionName()
              + "\" with table definition: \n");

      if (!regionMapping.getSpecifiedIds()) {
        sb.append("\nId fields in Field Mappings: " + regionMapping.getIds());
        sb.append(
            "\nId fields in Table MetaData: " + String.join(",", metaDataView.getKeyColumnNames()));
      }

      sb.append("\n\nDefinition from Field Mappings (" + regionMapping.getFieldMappings().size()
          + " field mappings found):");

      for (FieldMapping fieldMapping : regionMapping.getFieldMappings()) {
        sb.append("\n" + fieldMapping.getJdbcName() + " - " + fieldMapping.getJdbcType());
      }

      sb.append("\n\nDefinition from Table Metadata (" + metaDataView.getColumnNames().size()
          + " columns found):");

      for (String name : metaDataView.getColumnNames()) {
        sb.append("\n" + name + " - " + metaDataView.getColumnDataType(name));
      }

      sb.append("\n\nDestroy and recreate the JDBC mapping for \"" + regionMapping.getRegionName()
          + "\" to resolve this error.");

      logger.error(sb.toString());

      throw new JdbcConnectorException("Jdbc mapping for \"" + regionMapping.getRegionName()
          + "\" does not match table definition, check logs for more details.");
    }
  }


  @Override
  public RegionMapping getMappingForRegion(String regionName) {
    return mappingsByRegion.get(regionName);
  }

  @Override
  public void destroyRegionMapping(String regionName) {
    mappingsByRegion.remove(regionName);
  }

  @Override
  public Class<? extends CacheService> getInterface() {
    return JdbcConnectorService.class;
  }

  @Override
  public CacheServiceMBeanBase getMBean() {
    return null;
  }


  // The following helper method is to allow for proper mocking in unit tests
  DataSource getDataSource(String dataSourceName) {
    return JNDIInvoker.getDataSource(dataSourceName);
  }

  // The following helper method is to allow for proper mocking in unit tests
  TableMetaDataManager getTableMetaDataManager() {
    return new TableMetaDataManager();
  }

  private TableMetaDataView getTableMetaDataView(RegionMapping regionMapping,
      DataSource dataSource) {
    TableMetaDataManager manager = getTableMetaDataManager();
    try (Connection connection = dataSource.getConnection()) {
      return manager.getTableMetaDataView(connection, regionMapping);
    } catch (SQLException ex) {
      throw JdbcConnectorException
          .createException("Exception thrown while connecting to datasource \""
              + regionMapping.getDataSourceName() + "\": ", ex);
    }
  }

  @Override
  public List<FieldMapping> createFieldMappingUsingPdx(PdxType pdxType,
      TableMetaDataView tableMetaDataView) {

    // TODO the table name returned in tableMetaData may be different than
    // the table name specified on the command line at this point.
    // Do we want to update the region mapping to hold the "real" table name
    List<FieldMapping> fieldMappings = new ArrayList<>();
    Set<String> columnNames = tableMetaDataView.getColumnNames();
    if (columnNames.size() != pdxType.getFieldCount()) {
      throw new JdbcConnectorException(
          "The table and pdx class must have the same number of columns/fields. But the table has "
              + columnNames.size()
              + " columns and the pdx class has " + pdxType.getFieldCount() + " fields.");
    }
    List<PdxField> pdxFields = pdxType.getFields();
    for (String jdbcName : columnNames) {
      boolean isNullable = tableMetaDataView.isColumnNullable(jdbcName);
      JDBCType jdbcType = tableMetaDataView.getColumnDataType(jdbcName);
      FieldMapping fieldMapping =
          createFieldMapping(jdbcName, jdbcType.getName(), isNullable, pdxFields);
      fieldMappings.add(fieldMapping);
    }
    return fieldMappings;
  }

  private FieldMapping createFieldMapping(String jdbcName, String jdbcType, boolean jdbcNullable,
      List<PdxField> pdxFields) {
    String pdxName = null;
    String pdxType = null;
    for (PdxField pdxField : pdxFields) {
      if (pdxField.getFieldName().equals(jdbcName)) {
        pdxName = pdxField.getFieldName();
        pdxType = pdxField.getFieldType().name();
        break;
      }
    }
    if (pdxName == null) {
      // look for one inexact match
      for (PdxField pdxField : pdxFields) {
        if (pdxField.getFieldName().equalsIgnoreCase(jdbcName)) {
          if (pdxName != null) {
            throw new JdbcConnectorException(
                "More than one PDX field name matched the column name \"" + jdbcName + "\"");
          }
          pdxName = pdxField.getFieldName();
          pdxType = pdxField.getFieldType().name();
        }
      }
    }
    if (pdxName == null) {
      throw new JdbcConnectorException(
          "No PDX field name matched the column name \"" + jdbcName + "\"");
    }
    return new FieldMapping(pdxName, pdxType, jdbcName, jdbcType, jdbcNullable);
  }
}

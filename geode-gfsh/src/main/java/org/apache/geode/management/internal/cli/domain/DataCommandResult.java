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
package org.apache.geode.management.internal.cli.domain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.StructImpl;
import org.apache.geode.cache.query.internal.Undefined;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.result.model.DataResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;


/**
 * Domain object used for Data Commands Functions
 */
public class DataCommandResult implements Serializable {

  private static final long serialVersionUID = 2601227194108110936L;

  public static final Logger logger = LogService.getLogger();
  public static final String DATA_INFO_SECTION = "data-info";
  public static final String QUERY_SECTION = "query";
  public static final String LOCATION_SECTION = "location";
  private String command;
  private Object putResult;
  private Object getResult;
  private List<SelectResultRow> selectResult;
  private String queryTraceString;

  public static final String RESULT_FLAG = "Result";
  public static final String NUM_ROWS = "Rows";

  public static final String MISSING_VALUE = "<NULL>";

  // Aggregated Data.
  private List<KeyInfo> locateEntryLocations;
  private KeyInfo locateEntryResult;
  private boolean hasResultForAggregation;

  private Object removeResult;
  private Object inputKey;
  private Object inputValue;
  private Object inputQuery;
  private Throwable error;
  private String errorString;
  private String infoString;
  private String keyClass;
  private String valueClass;
  private int limit;
  private boolean operationCompletedSuccessfully; // used for validation purposes.


  public static final String NEW_LINE = GfshParser.LINE_SEPARATOR;

  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (isGet()) {
      sb.append(" Type  : Get").append(NEW_LINE);
      sb.append(" Key  : ").append(inputKey).append(NEW_LINE);
      if (getResult != null) {
        sb.append(" ReturnValue Class : ").append(getResult.getClass()).append(NEW_LINE);
      }
      sb.append(" ReturnValue : ").append(getResult).append(NEW_LINE);
    } else if (isPut()) {
      sb.append(" Type  : Put");
      sb.append(" Key  : ").append(inputKey).append(NEW_LINE);
      if (putResult != null) {
        sb.append(" ReturnValue Class : ").append(putResult.getClass()).append(NEW_LINE);
      }
      sb.append(" ReturnValue  : ").append(putResult).append(NEW_LINE);
      sb.append(" Value  : ").append(inputValue).append(NEW_LINE);
    } else if (isRemove()) {
      sb.append(" Type  : Remove");
      sb.append(" Key  : ").append(inputKey).append(NEW_LINE);
      if (removeResult != null) {
        sb.append(" ReturnValue Class : ").append(removeResult.getClass()).append(NEW_LINE);
      }
      sb.append(" ReturnValue  : ").append(removeResult).append(NEW_LINE);
    } else if (isLocateEntry()) {
      sb.append(" Type  : Locate Entry");
      sb.append(" Key  : ").append(inputKey).append(NEW_LINE);
      // Assume here that this is aggregated result
      sb.append(" Results  : ").append(locateEntryResult).append(NEW_LINE);
      sb.append(" Locations  : ").append(locateEntryLocations).append(NEW_LINE);
    }
    if (errorString != null) {
      sb.append(" ERROR ").append(errorString);
    }
    return sb.toString();
  }

  public boolean isGet() {
    return CliStrings.GET.equals(command);
  }

  public boolean isPut() {
    return CliStrings.PUT.equals(command);
  }

  public boolean isRemove() {
    return CliStrings.REMOVE.equals(command);
  }


  public boolean isLocateEntry() {
    return CliStrings.LOCATE_ENTRY.equals(command);
  }

  public boolean isSelect() {
    return CliStrings.QUERY.equals(command);
  }

  public List<SelectResultRow> getSelectResult() {
    return selectResult;
  }


  public static DataCommandResult createGetResult(Object inputKey, Object value, Throwable error,
      String errorString, boolean flag) {
    DataCommandResult result = new DataCommandResult();
    result.command = CliStrings.GET;
    result.inputKey = inputKey;
    result.getResult = value;
    result.error = error;
    result.errorString = errorString;
    result.operationCompletedSuccessfully = flag;
    return result;
  }

  public static DataCommandResult createGetInfoResult(Object inputKey, Object value,
      Throwable error, String infoString, boolean flag) {
    DataCommandResult result = new DataCommandResult();
    result.command = CliStrings.GET;
    result.inputKey = inputKey;
    result.getResult = value;
    result.error = error;
    result.infoString = infoString;
    result.operationCompletedSuccessfully = flag;
    return result;
  }

  public static DataCommandResult createLocateEntryResult(Object inputKey, KeyInfo locationResult,
      Throwable error, String errorString, boolean flag) {
    DataCommandResult result = new DataCommandResult();
    result.command = CliStrings.LOCATE_ENTRY;
    result.inputKey = inputKey;

    if (flag) {
      result.hasResultForAggregation = true;
    }

    result.locateEntryResult = locationResult;

    result.error = error;
    result.errorString = errorString;
    result.operationCompletedSuccessfully = flag;
    return result;
  }

  public static DataCommandResult createLocateEntryInfoResult(Object inputKey,
      KeyInfo locationResult, Throwable error, String infoString, boolean flag) {
    DataCommandResult result = new DataCommandResult();
    result.command = CliStrings.LOCATE_ENTRY;
    result.inputKey = inputKey;

    if (flag) {
      result.hasResultForAggregation = true;
    }

    result.locateEntryResult = locationResult;

    result.error = error;
    result.infoString = infoString;
    result.operationCompletedSuccessfully = flag;
    return result;
  }

  public static DataCommandResult createPutResult(Object inputKey, Object value, Throwable error,
      String errorString, boolean flag) {
    DataCommandResult result = new DataCommandResult();
    result.command = CliStrings.PUT;
    result.inputKey = inputKey;
    result.putResult = value;
    result.error = error;
    result.errorString = errorString;
    result.operationCompletedSuccessfully = flag;
    return result;
  }

  public static DataCommandResult createPutInfoResult(Object inputKey, Object value,
      Throwable error, String infoString, boolean flag) {
    DataCommandResult result = new DataCommandResult();
    result.command = CliStrings.PUT;
    result.inputKey = inputKey;
    result.putResult = value;
    result.error = error;
    result.infoString = infoString;
    return result;
  }

  public static DataCommandResult createRemoveResult(Object inputKey, Object value, Throwable error,
      String errorString, boolean flag) {
    DataCommandResult result = new DataCommandResult();
    result.command = CliStrings.REMOVE;
    result.inputKey = inputKey;
    result.removeResult = value;
    result.error = error;
    result.errorString = errorString;
    result.operationCompletedSuccessfully = flag;
    return result;
  }

  public static DataCommandResult createRemoveInfoResult(Object inputKey, Object value,
      Throwable error, String infoString, boolean flag) {
    DataCommandResult result = new DataCommandResult();
    result.command = CliStrings.REMOVE;
    result.inputKey = inputKey;
    result.removeResult = value;
    result.error = error;
    result.infoString = infoString;
    result.operationCompletedSuccessfully = flag;
    return result;
  }

  public static DataCommandResult createSelectResult(Object inputQuery, List<SelectResultRow> value,
      String queryTraceString, Throwable error, String errorString, boolean flag) {
    DataCommandResult result = new DataCommandResult();
    result.command = CliStrings.QUERY;
    result.inputQuery = inputQuery;
    result.queryTraceString = queryTraceString;
    result.selectResult = value;
    result.error = error;
    result.errorString = errorString;
    result.operationCompletedSuccessfully = flag;
    return result;
  }

  public static DataCommandResult createSelectInfoResult(Object inputQuery,
      List<SelectResultRow> value, int limit, Throwable error, String infoString, boolean flag) {
    DataCommandResult result = new DataCommandResult();
    result.command = CliStrings.QUERY;
    result.inputQuery = inputQuery;
    result.limit = limit;
    result.selectResult = value;
    result.error = error;
    result.infoString = infoString;
    result.operationCompletedSuccessfully = flag;
    return result;
  }


  public String getCommand() {
    return command;
  }

  public void setCommand(String command) {
    this.command = command;
  }

  public Object getPutResult() {
    return putResult;
  }

  public void setPutResult(Object putResult) {
    this.putResult = putResult;
  }

  public Object getGetResult() {
    return getResult;
  }

  public void setGetResult(Object getResult) {
    this.getResult = getResult;
  }

  public Object getRemoveResult() {
    return removeResult;
  }

  public void setRemoveResult(Object removeResult) {
    this.removeResult = removeResult;
  }

  public Object getInputKey() {
    return inputKey;
  }

  public void setInputKey(Object inputKey) {
    this.inputKey = inputKey;
  }

  public Object getInputValue() {
    return inputValue;
  }

  public void setInputValue(Object inputValue) {
    this.inputValue = inputValue;
  }

  public Throwable getErorr() {
    return error;
  }

  public void setErorr(Throwable erorr) {
    this.error = erorr;
  }

  public String getErrorString() {
    return errorString;
  }

  public void setErrorString(String errorString) {
    this.errorString = errorString;
  }

  public String getInfoString() {
    return infoString;
  }

  public void setInfoString(String infoString) {
    this.infoString = infoString;
  }

  public String getKeyClass() {
    return keyClass;
  }

  public void setKeyClass(String keyClass) {
    this.keyClass = keyClass;
  }

  public String getValueClass() {
    return valueClass;
  }

  public void setValueClass(String valueClass) {
    this.valueClass = valueClass;
  }

  public List<KeyInfo> getLocateEntryLocations() {
    return locateEntryLocations;
  }

  public ResultModel toResultModel() {
    if (StringUtils.isEmpty(keyClass)) {
      keyClass = "java.lang.String";
    }

    if (StringUtils.isEmpty(valueClass)) {
      valueClass = "java.lang.String";
    }

    ResultModel result = new ResultModel();
    DataResultModel data = result.addData(DATA_INFO_SECTION);

    if (errorString != null) {
      data.addData("Message", errorString);
      data.addData(RESULT_FLAG, operationCompletedSuccessfully);
      return result;
    }

    data.addData(RESULT_FLAG, operationCompletedSuccessfully);
    if (infoString != null) {
      data.addData("Message", infoString);
    }

    if (isGet()) {
      toResultModel_isGet(data);
    } else if (isLocateEntry()) {
      toResultModel_isLocate(result, data);
    } else if (isPut()) {
      toResultModel_isPut(data);
    } else if (isRemove()) {
      toResultModel_isRemove(data);
    }

    return result;
  }


  private void toResultModel_isGet(DataResultModel data) {
    data.addData("Key Class", getKeyClass());
    data.addData("Key", inputKey);
    data.addData("Value Class", getValueClass());
    data.addData("Value", getResult != null ? getResult.toString() : "null");
  }

  private void toResultModel_isLocate(ResultModel result, DataResultModel data) {

    data.addData("Key Class", getKeyClass());
    data.addData("Key", inputKey);

    if (locateEntryLocations != null) {
      TabularResultModel locationTable = result.addTable(LOCATION_SECTION);

      int totalLocations = 0;

      for (KeyInfo info : locateEntryLocations) {
        List<Object[]> locations = info.getLocations();

        if (locations != null) {
          if (locations.size() == 1) {
            Object array[] = locations.get(0);
            boolean found = (Boolean) array[1];
            if (found) {
              totalLocations++;
              boolean primary = (Boolean) array[3];
              String bucketId = (String) array[4];
              locationTable.accumulate("MemberName", info.getMemberName());
              locationTable.accumulate("MemberId", info.getMemberId());
              if (bucketId != null) {// PR
                if (primary) {
                  locationTable.accumulate("Primary", "*Primary PR*");
                } else {
                  locationTable.accumulate("Primary", "No");
                }
                locationTable.accumulate("BucketId", bucketId);
              }
            }
          } else {
            for (Object[] array : locations) {
              String regionPath = (String) array[0];
              boolean found = (Boolean) array[1];
              if (found) {
                totalLocations++;
                boolean primary = (Boolean) array[3];
                String bucketId = (String) array[4];
                locationTable.accumulate("MemberName", info.getMemberName());
                locationTable.accumulate("MemberId", info.getMemberId());
                locationTable.accumulate("RegionPath", regionPath);
                if (bucketId != null) {// PR
                  if (primary) {
                    locationTable.accumulate("Primary", "*Primary PR*");
                  } else {
                    locationTable.accumulate("Primary", "No");
                  }
                  locationTable.accumulate("BucketId", bucketId);
                }
              }
            }
          }
        }
      }
      data.addData("Locations Found", totalLocations);
    } else {
      data.addData("Location Info ", "Could not find location information");
    }
  }

  private void toResultModel_isPut(DataResultModel data) {
    data.addData("Key Class", getKeyClass());
    data.addData("Key", inputKey);
    data.addData("Value Class", getValueClass());
    data.addData("Old Value", putResult != null ? putResult.toString() : "null");
  }

  private void toResultModel_isRemove(DataResultModel data) {
    if (inputKey != null) {// avoids printing key when remove ALL is called
      data.addData("Key Class", getKeyClass());
      data.addData("Key", inputKey);
    }
  }

  /**
   * This method returns result when flag interactive=false i.e. Command returns result in one go
   * and does not goes through steps waiting for user input. Method returns CompositeResultData
   * instead of Result as Command Step is required to add NEXT_STEP information to guide
   * executionStrategy to route it through final step.
   */
  public ResultModel toSelectCommandResult() {
    ResultModel result = new ResultModel();
    DataResultModel data = result.addData(DATA_INFO_SECTION);
    if (!operationCompletedSuccessfully) {
      result.setStatus(Result.Status.ERROR);
      data.addData(RESULT_FLAG, operationCompletedSuccessfully);
      if (errorString != null) {
        data.addData("Message", errorString);
      } else if (infoString != null) {
        data.addData("Message", infoString);
      }
      return result;
    } else {
      TabularResultModel table = result.addTable(DataCommandResult.QUERY_SECTION);
      data.addData(RESULT_FLAG, operationCompletedSuccessfully);
      if (infoString != null) {
        data.addData("Message", infoString);
      }
      if (inputQuery != null) {
        if (this.limit > 0) {
          data.addData("Limit", this.limit);
        }
        if (this.selectResult != null) {
          data.addData(NUM_ROWS, this.selectResult.size());
          if (this.queryTraceString != null) {
            data.addData("Query Trace", this.queryTraceString);
          }
          buildTable(table, 0, selectResult.size());
        }
      }
      return result;
    }
  }

  private void buildTable(TabularResultModel table, int startCount, int endCount) {
    int lastRowExclusive = Math.min(selectResult.size(), endCount + 1);
    List<SelectResultRow> paginatedRows = selectResult.subList(startCount, lastRowExclusive);

    // First find all the possible columns - not a Set because we want them ordered consistently
    List<String> possibleColumns = new ArrayList<>();
    for (SelectResultRow row : paginatedRows) {
      for (String column : row.getColumnValues().keySet()) {
        if (!possibleColumns.contains(column)) {
          possibleColumns.add(column);
        }
      }
    }

    for (SelectResultRow row : paginatedRows) {
      Map<String, String> columnValues = row.getColumnValues();
      for (String column : possibleColumns) {
        table.accumulate(column,
            columnValues.getOrDefault(column, DataCommandResult.MISSING_VALUE));
      }
    }
  }

  public Object getInputQuery() {
    return inputQuery;
  }

  public void setInputQuery(Object inputQuery) {
    this.inputQuery = inputQuery;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public static class KeyInfo implements Serializable {

    private String memberId;
    private String memberName;
    private String host;
    private int pid;

    // Indexes : regionName = 0, found=1, value=2 primary=3 bucketId=4
    private ArrayList<Object[]> locations = null;

    public void addLocation(Object[] locationArray) {
      if (this.locations == null) {
        locations = new ArrayList<>();
      }

      locations.add(locationArray);
    }

    public List<Object[]> getLocations() {
      return locations;
    }

    public String getMemberId() {
      return memberId;
    }

    public void setMemberId(String memberId) {
      this.memberId = memberId;
    }

    public String getMemberName() {
      return memberName;
    }

    public void setMemberName(String memberName) {
      this.memberName = memberName;
    }

    public String getHost() {
      return host;
    }

    public void setHost(String host) {
      this.host = host;
    }

    public int getPid() {
      return pid;
    }

    public void setPid(int pid) {
      this.pid = pid;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{ Member : ").append(host).append("(").append(memberId).append(") , ");
      for (Object[] array : locations) {
        boolean primary = (Boolean) array[3];
        String bucketId = (String) array[4];
        sb.append(" [ Primary : ").append(primary).append(" , BucketId : ").append(bucketId)
            .append(" ]");
      }
      sb.append(" }");
      return sb.toString();
    }

    public boolean hasLocation() {
      if (locations == null) {
        return false;
      } else {
        for (Object[] array : locations) {
          boolean found = (Boolean) array[1];
          if (found) {
            return true;
          }
        }
      }
      return false;
    }
  }

  public static final int ROW_TYPE_STRUCT_RESULT = 100;
  public static final int ROW_TYPE_BEAN = 200;
  public static final int ROW_TYPE_PRIMITIVE = 300;

  public static class SelectResultRow implements Serializable {
    private static final long serialVersionUID = 1L;
    private int type;
    private final Map<String, String> columnValues;

    public SelectResultRow(int type, Object value) {
      this.type = type;
      this.columnValues = createColumnValues(value);
    }

    public Map<String, String> getColumnValues() {
      return columnValues;
    }

    private Map<String, String> createColumnValues(Object value) {
      Map<String, String> result = new LinkedHashMap<>();

      if (value == null || MISSING_VALUE.equals(value)) {
        result.put("Value", MISSING_VALUE);
      } else if (type == ROW_TYPE_PRIMITIVE) {
        result.put(RESULT_FLAG, value.toString());
      } else if (value instanceof Undefined) {
        result.put("Value", "UNDEFINED");
      } else {
        resolveObjectToColumns(result, value);
      }

      return result;
    }

    private void resolveObjectToColumns(Map<String, String> columnData, Object value) {
      if (value instanceof PdxInstance) {
        resolvePdxToColumns(columnData, (PdxInstance) value);
      } else if (value instanceof Struct) {
        resolveStructToColumns(columnData, (StructImpl) value);
      } else if (value instanceof UUID) {
        columnData.put("Result", valueToJson(value));
      } else {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.valueToTree(value);

        node.fieldNames().forEachRemaining(field -> {
          try {
            columnData.put(field, mapper.writeValueAsString(node.get(field)));
          } catch (JsonProcessingException e) {
            columnData.put(field, e.getMessage());
          }
        });
      }
    }

    private void resolvePdxToColumns(Map<String, String> columnData, PdxInstance pdx) {
      for (String field : pdx.getFieldNames()) {
        columnData.put(field, valueToJson(pdx.getField(field)));
      }
    }

    private void resolveStructToColumns(Map<String, String> columnData, StructImpl struct) {
      for (String field : struct.getFieldNames()) {
        columnData.put(field, valueToJson(struct.get(field)));
      }
    }

    private String valueToJson(Object value) {
      if (value == null) {
        return "null";
      }

      if (value instanceof String) {
        return (String) value;
      }

      if (value instanceof PdxInstance) {
        return JSONFormatter.toJSON((PdxInstance) value);
      }

      ObjectMapper mapper = new ObjectMapper();
      try {
        return mapper.writeValueAsString(value);
      } catch (JsonProcessingException jex) {
        return jex.getMessage();
      }
    }
  }

  public void aggregate(DataCommandResult result) {
    /* Right now only called for LocateEntry */
    if (!isLocateEntry()) {
      return;
    }

    if (this.locateEntryLocations == null) {
      locateEntryLocations = new ArrayList<>();
    }

    if (result == null) {// self-transform result from single to aggregate when numMember==1
      if (this.locateEntryResult != null) {
        locateEntryLocations.add(locateEntryResult);
      }
      return;
    }

    if (result.errorString != null && !result.errorString.equals(errorString)) {
      // append errorString only if differs
      errorString = result.errorString + " " + errorString;
    }

    // append message only when it differs for negative results
    if (!operationCompletedSuccessfully && result.infoString != null
        && !result.infoString.equals(infoString)) {
      infoString = result.infoString;
    }

    if (result.hasResultForAggregation) {
      this.operationCompletedSuccessfully = true;
      infoString = result.infoString;
      if (result.locateEntryResult != null) {
        locateEntryLocations.add(result.locateEntryResult);
      }
    }
  }

}

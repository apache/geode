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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.CompositeResultData.SectionResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.cli.util.JsonUtil;


/**
 * Domain object used for Data Commands Functions
 */
public class DataCommandResult implements Serializable {
  private static Logger logger = LogManager.getLogger();

  private static final long serialVersionUID = 1L;
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

  public void setLocateEntryLocations(List<KeyInfo> locateEntryLocations) {
    this.locateEntryLocations = locateEntryLocations;
  }

  public Result toCommandResult() {

    if (StringUtils.isEmpty(keyClass)) {
      keyClass = "java.lang.String";
    }

    if (StringUtils.isEmpty(valueClass)) {
      valueClass = "java.lang.String";
    }

    if (errorString != null) {
      CompositeResultData data = ResultBuilder.createCompositeResultData();
      SectionResultData section = data.addSection();
      section.addData("Message", errorString);
      section.addData(RESULT_FLAG, operationCompletedSuccessfully);
      return ResultBuilder.buildResult(data);
    }

    CompositeResultData data = ResultBuilder.createCompositeResultData();
    SectionResultData section = data.addSection();
    TabularResultData table = section.addTable();

    section.addData(RESULT_FLAG, operationCompletedSuccessfully);
    if (infoString != null) {
      section.addData("Message", infoString);
    }

    if (isGet()) {
      toCommandResult_isGet(section, table);
    } else if (isLocateEntry()) {
      toCommandResult_isLocate(section, table);
    } else if (isPut()) {
      toCommandResult_isPut(section, table);
    } else if (isRemove()) {
      toCommandResult_isRemove(section, table);
    }
    return ResultBuilder.buildResult(data);
  }

  private void toCommandResult_isGet(SectionResultData section, TabularResultData table) {
    section.addData("Key Class", getKeyClass());
    if (!isDeclaredPrimitive(keyClass)) {
      addJSONStringToTable(table, inputKey);
    } else {
      section.addData("Key", inputKey);
    }

    section.addData("Value Class", getValueClass());
    if (!isDeclaredPrimitive(valueClass)) {
      addJSONStringToTable(table, getResult);
    } else {
      section.addData("Value", getResult);
    }
  }

  private void toCommandResult_isLocate(SectionResultData section, TabularResultData table) {

    section.addData("Key Class", getKeyClass());
    if (!isDeclaredPrimitive(keyClass)) {
      addJSONStringToTable(table, inputKey);
    } else {
      section.addData("Key", inputKey);
    }

    if (locateEntryLocations != null) {
      TabularResultData locationTable = section.addTable();

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
      section.addData("Locations Found", totalLocations);
    } else {
      section.addData("Location Info ", "Could not find location information");
    }
  }

  private void toCommandResult_isPut(SectionResultData section, TabularResultData table) {
    section.addData("Key Class", getKeyClass());

    if (!isDeclaredPrimitive(keyClass)) {
      addJSONStringToTable(table, inputKey);
    } else {
      section.addData("Key", inputKey);
    }

    section.addData("Value Class", getValueClass());
    if (!isDeclaredPrimitive(valueClass)) {
      addJSONStringToTable(table, putResult);
    } else {
      section.addData("Old Value", putResult);
    }

  }

  private void toCommandResult_isRemove(SectionResultData section, TabularResultData table) {
    if (inputKey != null) {// avoids printing key when remove ALL is called
      section.addData("Key Class", getKeyClass());
      if (!isDeclaredPrimitive(keyClass)) {
        addJSONStringToTable(table, inputKey);
      } else {
        section.addData("Key", inputKey);
      }
    }
  }

  /**
   * This method returns result when flag interactive=false i.e. Command returns result in one go
   * and does not goes through steps waiting for user input. Method returns CompositeResultData
   * instead of Result as Command Step is required to add NEXT_STEP information to guide
   * executionStrategy to route it through final step.
   */
  public CompositeResultData toSelectCommandResult() {
    if (errorString != null) {
      CompositeResultData data = ResultBuilder.createCompositeResultData();
      SectionResultData section = data.addSection();
      section.addData("Message", errorString);
      section.addData(RESULT_FLAG, operationCompletedSuccessfully);
      return data;
    } else {
      CompositeResultData data = ResultBuilder.createCompositeResultData();
      SectionResultData section = data.addSection();
      TabularResultData table = section.addTable();
      section.addData(RESULT_FLAG, operationCompletedSuccessfully);
      if (infoString != null) {
        section.addData("Message", infoString);
      }
      if (inputQuery != null) {
        if (this.limit > 0) {
          section.addData("Limit", this.limit);
        }
        if (this.selectResult != null) {
          section.addData(NUM_ROWS, this.selectResult.size());
          if (this.queryTraceString != null) {
            section.addData("Query Trace", this.queryTraceString);
          }
          buildTable(table, 0, selectResult.size());
        }
      }
      return data;
    }
  }

  private int buildTable(TabularResultData table, int startCount, int endCount) {
    // Three steps:
    // 1a. Convert each row object to a Json object.
    // 1b. Build a list of keys that are used for each object
    // 2. Pad MISSING_VALUE into Json objects for those data that are missing any particular key
    // 3. Build the table from these Json objects.

    // 1.
    int lastRowExclusive = Math.min(selectResult.size(), endCount + 1);
    List<SelectResultRow> paginatedRows = selectResult.subList(startCount, lastRowExclusive);

    List<GfJsonObject> tableRows = new ArrayList<>();
    List<GfJsonObject> rowsWithRealJsonObjects = new ArrayList<>();
    Set<String> columns = new HashSet<>();

    for (SelectResultRow row : paginatedRows) {
      GfJsonObject object = new GfJsonObject();
      try {
        if (row.value == null || MISSING_VALUE.equals(row.value)) {
          object.put("Value", MISSING_VALUE);
        } else if (row.type == ROW_TYPE_PRIMITIVE) {
          object.put(RESULT_FLAG, row.value);
        } else {
          object = buildGfJsonFromRawObject(row.value);
          rowsWithRealJsonObjects.add(object);
          object.keys().forEachRemaining(columns::add);
        }
        tableRows.add(object);
      } catch (GfJsonException e) {
        JSONObject errJson =
            new JSONObject().put("Value", "Error getting bean properties " + e.getMessage());
        tableRows.add(new GfJsonObject(errJson, false));
      }
    }

    // 2.
    for (GfJsonObject tableRow : rowsWithRealJsonObjects) {
      for (String key : columns) {
        if (!tableRow.has(key)) {
          try {
            tableRow.put(key, MISSING_VALUE);
          } catch (GfJsonException e) {
            logger.warn("Ignored GfJsonException:", e);
          }
        }
      }
    }

    // 3.
    for (GfJsonObject jsonObject : tableRows) {
      addJSONObjectToTable(table, jsonObject);
    }

    return paginatedRows.size();
  }


  private boolean isDeclaredPrimitive(String keyClass2) {
    try {
      Class klass = ClassPathLoader.getLatest().forName(keyClass2);
      return JsonUtil.isPrimitiveOrWrapper(klass);
    } catch (ClassNotFoundException e) {
      return false;
    }
  }


  private Object getDomainValue(Object value) {
    if (value instanceof String) {
      String str = (String) value;
      if (str.contains("{") && str.contains("}")) {// small filter to see if its json string
        try {
          JSONObject json = new JSONObject(str);
          return json.get("type-class");
        } catch (Exception e) {
          return str;
        }
      } else {
        return str;
      }
    }
    return value;
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

  public static class KeyInfo implements /* Data */ Serializable {

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

    // @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeString(memberId, out);
      DataSerializer.writeString(memberName, out);
      DataSerializer.writeString(host, out);
      DataSerializer.writePrimitiveInt(pid, out);
      DataSerializer.writeArrayList(locations, out);


    }

    // @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      memberId = DataSerializer.readString(in);
      memberName = DataSerializer.readString(in);
      host = DataSerializer.readString(in);
      pid = DataSerializer.readPrimitiveInt(in);
      locations = DataSerializer.readArrayList(in);
    }
  }

  public static final int ROW_TYPE_STRUCT_RESULT = 100;
  public static final int ROW_TYPE_BEAN = 200;
  public static final int ROW_TYPE_PRIMITIVE = 300;

  public static class SelectResultRow implements Serializable {
    private static final long serialVersionUID = 1L;
    private int type;
    private Object value;

    public SelectResultRow(int type, Object value) {
      this.type = type;
      this.value = value;
    }

    public int getType() {
      return type;
    }

    public void setType(int type) {
      this.type = type;
    }

    public Object getValue() {
      return value;
    }

    public void setValue(Object value) {
      this.value = value;
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


  private void addJSONObjectToTable(TabularResultData table, GfJsonObject object) {
    Iterator<String> keys;

    keys = object.keys();
    while (keys.hasNext()) {
      String k = keys.next();
      // filter out meta-field type-class used to identify java class of json object
      if (!"type-class".equals(k)) {
        Object value = object.get(k);

        if (value != null) {
          table.accumulate(k, getDomainValue(value));
        }
      }
    }
  }

  private GfJsonObject buildGfJsonFromRawObject(Object object) throws GfJsonException {
    GfJsonObject jsonObject;
    if (String.class.equals(object.getClass())) {
      jsonObject = new GfJsonObject(sanitizeJsonString((String) object));
    } else {
      jsonObject = new GfJsonObject(object, true);
    }

    return jsonObject;
  }

  private String sanitizeJsonString(String s) {
    // InputString in JSON Form but with round brackets
    String newString = s.replaceAll("'", "\"");
    if (newString.charAt(0) == '(') {
      int len = newString.length();
      newString = "{" + newString.substring(1, len - 1) + "}";
    }
    return newString;
  }

  private void addJSONStringToTable(TabularResultData table, Object object) {
    if (object == null || MISSING_VALUE.equals(object)) {
      table.accumulate("Value", MISSING_VALUE);
    } else {
      try {
        GfJsonObject jsonObject = buildGfJsonFromRawObject(object);
        addJSONObjectToTable(table, jsonObject);
      } catch (Exception e) {
        table.accumulate("Value", "Error getting bean properties " + e.getMessage());
      }
    }
  }


  // @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(command, out);
    out.writeUTF(command);
    DataSerializer.writeObject(putResult, out);
    DataSerializer.writeObject(getResult, out);
    DataSerializer.writeObject(locateEntryResult, out);
    DataSerializer.writeArrayList((ArrayList<?>) locateEntryLocations, out);
    DataSerializer.writeBoolean(hasResultForAggregation, out);
    DataSerializer.writeObject(removeResult, out);
    DataSerializer.writeObject(inputKey, out);
    DataSerializer.writeObject(inputValue, out);
    DataSerializer.writeObject(error, out);
    DataSerializer.writeString(errorString, out);
    DataSerializer.writeString(infoString, out);
    DataSerializer.writeString(keyClass, out);
    DataSerializer.writeString(valueClass, out);
    DataSerializer.writeBoolean(operationCompletedSuccessfully, out);
  }

  // @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    command = DataSerializer.readString(in);
    putResult = DataSerializer.readObject(in);
    getResult = DataSerializer.readObject(in);
    locateEntryLocations = DataSerializer.readArrayList(in);
    locateEntryResult = DataSerializer.readObject(in);
    hasResultForAggregation = DataSerializer.readBoolean(in);
    removeResult = DataSerializer.readObject(in);
    inputKey = DataSerializer.readObject(in);
    inputValue = DataSerializer.readObject(in);
    error = DataSerializer.readObject(in);
    errorString = DataSerializer.readString(in);
    infoString = DataSerializer.readString(in);
    keyClass = DataSerializer.readString(in);
    valueClass = DataSerializer.readString(in);
    operationCompletedSuccessfully = DataSerializer.readBoolean(in);
  }

}

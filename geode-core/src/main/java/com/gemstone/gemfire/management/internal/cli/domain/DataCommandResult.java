/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.cli.domain;

import static com.gemstone.gemfire.management.internal.cli.multistep.CLIMultiStepHelper.createBannerResult;
import static com.gemstone.gemfire.management.internal.cli.multistep.CLIMultiStepHelper.createPageResult;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.GfshParser;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonArray;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData;
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData.SectionResultData;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.management.internal.cli.util.JsonUtil;
import org.json.JSONObject;


/**
 * Domain object used for Data Commands Functions
 * 
 * TODO : Implement DataSerializable
 *
 */
public class DataCommandResult implements /*Data*/ Serializable{

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private String command;
  private Object putResult;
  private Object getResult;
  private List<SelectResultRow> selectResult;
  private String queryTraceString;
  
  public static final String QUERY_PAGE_START ="startCount";
  public static final String QUERY_PAGE_END ="endCount";
  public static final String QUERY_TRACE ="Query Trace";
  
  public static final String RESULT_FLAG = "Result";
  public static final String NUM_ROWS = "Rows";

  //Aggreagated Data.
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
  private boolean operationCompletedSuccessfully; //used for validation purposes.
  
  
  public static final String NEW_LINE = GfshParser.LINE_SEPARATOR;
  
  public String toString(){
    StringBuilder sb = new StringBuilder();
    if(isGet()){
      sb.append(" Type  : Get").append(NEW_LINE);
      sb.append(" Key  : ").append(inputKey).append(NEW_LINE);
      if(getResult!=null)
        sb.append(" ReturnValue Class : ").append(getResult.getClass()).append(NEW_LINE);
      sb.append(" ReturnValue : ").append(getResult).append(NEW_LINE);
    }else if(isPut()){
      sb.append(" Type  : Put");
      sb.append(" Key  : ").append(inputKey).append(NEW_LINE);
      if(putResult!=null)
        sb.append(" ReturnValue Class : ").append(putResult.getClass()).append(NEW_LINE);
      sb.append(" ReturnValue  : ").append(putResult).append(NEW_LINE);
      sb.append(" Value  : ").append(inputValue).append(NEW_LINE);      
    }else if(isRemove()){
      sb.append(" Type  : Remove");
      sb.append(" Key  : ").append(inputKey).append(NEW_LINE);
      if(removeResult!=null)
        sb.append(" ReturnValue Class : ").append(removeResult.getClass()).append(NEW_LINE);
      sb.append(" ReturnValue  : ").append(removeResult).append(NEW_LINE);      
    }else if(isLocateEntry()){
      sb.append(" Type  : Locate Entry" );
      sb.append(" Key  : ").append(inputKey).append(NEW_LINE);
      //Assume here that this is aggregated result
      sb.append(" Results  : ").append(locateEntryResult).append(NEW_LINE);
      sb.append(" Locations  : ").append(locateEntryLocations).append(NEW_LINE);
    }
    if(errorString!=null)
      sb.append(" ERROR ").append(errorString);
    return sb.toString();
  }
  
  public boolean isGet(){
    if(CliStrings.GET.equals(command))
      return true;
    else return false;
  }
  
  public boolean isPut(){
    if(CliStrings.PUT.equals(command))
      return true;
    else return false;
  }
  
  public boolean isRemove(){
    if(CliStrings.REMOVE.equals(command))
      return true;
    else return false;
  }

  
  public boolean isLocateEntry(){
    if(CliStrings.LOCATE_ENTRY.equals(command))
      return true;
    else return false;
  }
  
  public boolean isSelect(){
    if(CliStrings.QUERY.equals(command))
      return true;
    else return false;
  }
  
  public List<SelectResultRow> getSelectResult() {
    return selectResult;
  }
  
  
  public static DataCommandResult createGetResult(Object inputKey, Object value, Throwable error, String errorString, boolean flag){
    DataCommandResult result = new DataCommandResult();
    result.command = CliStrings.GET;
    result.inputKey = inputKey;
    result.getResult = value;
    result.error = error;
    result.errorString = errorString;    
    result.operationCompletedSuccessfully = flag;
    return result;
  }
  
  public static DataCommandResult createGetInfoResult(Object inputKey, Object value, Throwable error, String infoString, boolean flag){
    DataCommandResult result = new DataCommandResult();
    result.command = CliStrings.GET;
    result.inputKey = inputKey;
    result.getResult = value;
    result.error = error;
    result.infoString = infoString;
    result.operationCompletedSuccessfully = flag;
    return result;
  }
  
  public static DataCommandResult createLocateEntryResult(Object inputKey, KeyInfo locationResult, Throwable error, String errorString, boolean flag){
    DataCommandResult result = new DataCommandResult();
    result.command = CliStrings.LOCATE_ENTRY;
    result.inputKey = inputKey;
    
    if(flag){
      result.hasResultForAggregation = true;
    }
    
    result.locateEntryResult = locationResult;
    
    result.error = error;
    result.errorString = errorString;    
    result.operationCompletedSuccessfully = flag;
    return result;
  }
  
  public static DataCommandResult createLocateEntryInfoResult(Object inputKey, KeyInfo locationResult, Throwable error, String infoString, boolean flag){
    DataCommandResult result = new DataCommandResult();
    result.command = CliStrings.LOCATE_ENTRY;
    result.inputKey = inputKey;
    
    if(flag){
      result.hasResultForAggregation = true;      
    }
    
    result.locateEntryResult = locationResult;
    
    result.error = error;
    result.infoString = infoString;
    result.operationCompletedSuccessfully = flag;
    return result;
  }
  
  public static DataCommandResult createPutResult(Object inputKey, Object value, Throwable error, String errorString, boolean flag){
    DataCommandResult result = new DataCommandResult();
    result.command = CliStrings.PUT;
    result.inputKey = inputKey;
    result.putResult = value;
    result.error = error;
    result.errorString = errorString;
    result.operationCompletedSuccessfully = flag;
    return result;
  }
  
  public static DataCommandResult createPutInfoResult(Object inputKey, Object value, Throwable error, String infoString, boolean flag){
    DataCommandResult result = new DataCommandResult();
    result.command = CliStrings.PUT;
    result.inputKey = inputKey;
    result.putResult = value;
    result.error = error;
    result.infoString = infoString;
    return result;
  }
  
  public static DataCommandResult createRemoveResult(Object inputKey, Object value, Throwable error, String errorString, boolean flag){
    DataCommandResult result = new DataCommandResult();
    result.command = CliStrings.REMOVE;
    result.inputKey = inputKey;
    result.removeResult = value;
    result.error = error;
    result.errorString = errorString;
    result.operationCompletedSuccessfully = flag;
    return result;
  }
  
  public static DataCommandResult createRemoveInfoResult(Object inputKey, Object value, Throwable error, String infoString, boolean flag){
    DataCommandResult result = new DataCommandResult();
    result.command = CliStrings.REMOVE;
    result.inputKey = inputKey;
    result.removeResult = value;
    result.error = error;
    result.infoString = infoString;
    result.operationCompletedSuccessfully = flag;
    return result;
  }
  
  public static DataCommandResult createSelectResult(Object inputQuery, List<SelectResultRow> value,String queryTraceString, Throwable error, String errorString, boolean flag){
    DataCommandResult result = new DataCommandResult();
    result.command = CliStrings.QUERY;
    result.inputQuery = inputQuery;
    //result.limit = limit;
    result.queryTraceString = queryTraceString;
    result.selectResult = value;
    result.error = error;
    result.errorString = errorString;
    result.operationCompletedSuccessfully = flag;
    return result;
  }
  
  public static DataCommandResult createSelectInfoResult(Object inputQuery, List<SelectResultRow> value,int limit, Throwable error, String infoString, boolean flag){
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
    
    if(keyClass==null || keyClass.isEmpty())
      keyClass = "java.lang.String";
    
    if(valueClass==null || valueClass.isEmpty())
      valueClass = "java.lang.String";
    
    if(errorString!=null){
      //return ResultBuilder.createGemFireErrorResult(errorString);
      CompositeResultData data = ResultBuilder.createCompositeResultData();
      SectionResultData section = data.addSection();
      section.addData("Message", errorString);
      section.addData(RESULT_FLAG, operationCompletedSuccessfully);
      return ResultBuilder.buildResult(data);
    }
    else{
      CompositeResultData data = ResultBuilder.createCompositeResultData();
      SectionResultData section = data.addSection();
      TabularResultData table = section.addTable();
      
      section.addData(RESULT_FLAG, operationCompletedSuccessfully);
      if(infoString!=null)
        section.addData("Message", infoString);
      
      if(isGet()){
        
       section.addData("Key Class", getKeyClass());
       if(!isDeclaredPrimitive(keyClass))
         addJSONStringToTable(table,inputKey);
       else
         section.addData("Key", inputKey);
      
       section.addData("Value Class", getValueClass());
       if(!isDeclaredPrimitive(valueClass))
           addJSONStringToTable(table,getResult);
       else
          section.addData("Value", getResult);
        
        
      }else if(isLocateEntry()){
        
        section.addData("Key Class", getKeyClass());
        if(!isDeclaredPrimitive(keyClass))
          addJSONStringToTable(table,inputKey);
        else
          section.addData("Key", inputKey);
        
        if(locateEntryLocations!=null){
          TabularResultData locationTable = section.addTable();
          
          int totalLocations = 0;
          
          for(KeyInfo info : locateEntryLocations){
            List<Object[]> locations = info.getLocations();
            
            if(locations!=null){
              if(locations.size()==1){
                Object array[] = locations.get(0);
                //String regionPath = (String)array[0];
                boolean found = (Boolean)array[1];
                if(found){
                  totalLocations++;
                  boolean primary = (Boolean)array[3];
                  String bucketId = (String)array[4];
                  locationTable.accumulate("MemberName", info.getMemberName());
                  locationTable.accumulate("MemberId", info.getMemberId());
                  if(bucketId!=null){//PR
                    if(primary)
                      locationTable.accumulate("Primary", "*Primary PR*");
                    else
                      locationTable.accumulate("Primary", "No");
                    locationTable.accumulate("BucketId",bucketId);
                  }
                }
              }else{                
                for(Object[] array : locations){                  
                  String regionPath = (String)array[0];
                  boolean found = (Boolean)array[1];
                  if(found){
                    totalLocations++;
                    boolean primary = (Boolean)array[3];        
                    String bucketId = (String)array[4];  
                    locationTable.accumulate("MemberName", info.getMemberName());
                    locationTable.accumulate("MemberId", info.getMemberId());
                    locationTable.accumulate("RegionPath", regionPath);
                    if(bucketId!=null){//PR
                      if(primary)
                        locationTable.accumulate("Primary", "*Primary PR*");
                      else
                        locationTable.accumulate("Primary", "No");
                      locationTable.accumulate("BucketId",bucketId);
                    }
                  }
                }
              }
            }
          }          
          section.addData("Locations Found", totalLocations);          
        }else{
          section.addData("Location Info ", "Could not find location information");
        }         
         
       }
      else if(isPut()){
        section.addData("Key Class", getKeyClass());
        
        if(!isDeclaredPrimitive(keyClass)){          
          addJSONStringToTable(table,inputKey);
        }
        else
         section.addData("Key", inputKey);
        
        section.addData("Value Class", getValueClass());
        if(!isDeclaredPrimitive(valueClass)){           
           addJSONStringToTable(table,putResult);
        }
        else
          section.addData("Old Value", putResult);         
         
      }else if(isRemove()){
        if(inputKey!=null){//avoids printing key when remove ALL is called
            section.addData("Key Class", getKeyClass());
            if(!isDeclaredPrimitive(keyClass))
              addJSONStringToTable(table,inputKey);
            else
              section.addData("Key", inputKey);         
        }        
         /*if(valueClass!=null && !valueClass.isEmpty()){
           section.addData("Value Class", getValueClass());
           addJSONStringToTable(table,removeResult);
         }else
           section.addData("Value", removeResult);*/
      } else if (isSelect()) {
        //its moved to its separate method
      }       
    return ResultBuilder.buildResult(data);     
    }    
  }
  
  /**
   * This method returns result when flag interactive=false i.e. Command returns result in one go
   * and does not goes through steps waiting for user input. Method returns CompositeResultData
   * instead of Result as Command Step is required to add NEXT_STEP information to guide executionStragey
   * to route it through final step.
   */
  public CompositeResultData toSelectCommandResult() {
    if(errorString!=null){
      //return ResultBuilder.createGemFireErrorResult(errorString);
      CompositeResultData data = ResultBuilder.createCompositeResultData();
      SectionResultData section = data.addSection();
      section.addData("Message", errorString);
      section.addData(RESULT_FLAG, operationCompletedSuccessfully);
      return data;
    }
    else{
      CompositeResultData data = ResultBuilder.createCompositeResultData();
      SectionResultData section = data.addSection();
      TabularResultData table = section.addTable();
      section.addData(RESULT_FLAG, operationCompletedSuccessfully);
      if(infoString!=null){
        section.addData("Message", infoString);
      }
      if (inputQuery != null) {
        if (this.limit != -1) {
          section.addData("Limit", this.limit);
        }
        if (this.selectResult != null) {
          section.addData(NUM_ROWS, this.selectResult.size());
          if(this.queryTraceString!=null)
            section.addData("Query Trace", this.queryTraceString);
          buildTable(table, 0, selectResult.size());
        }
      }
      return data;
    }
  }
  
  /**
   * This method returns a "Page" as dictated by arguments startCount and endCount.
   * Returned result is not standard CommandResult and its consumed by Display Step
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public Result pageResult(int startCount, int endCount, String step) {
    List<String> fields = new ArrayList<String>();
    List values = new ArrayList<String>();
    fields.add(RESULT_FLAG);values.add(operationCompletedSuccessfully);
    fields.add(QUERY_PAGE_START);values.add(startCount);
    fields.add(QUERY_PAGE_END);values.add(endCount);
    if (errorString != null) {
      fields.add("Message");values.add(errorString);
      return createBannerResult(fields, values, step);
    } else {
      
      if (infoString != null) {
        fields.add("Message");values.add(infoString);
      }

      if (selectResult != null) {
        try {
          TabularResultData table = ResultBuilder.createTabularResultData();
          String[] headers = null;
          Object[][] rows = null;
          int rowCount = buildTable(table, startCount, endCount);
          GfJsonArray array = table.getHeaders();
          headers = new String[array.size()];
          rows = new Object[rowCount][array.size()];
          for (int i = 0; i < array.size(); i++) {
            headers[i] = (String) array.get(i);
            List<String> list = table.retrieveAllValues(headers[i]);
            for (int j = 0; j < list.size(); j++) {
              rows[j][i] = list.get(j);
            }
          }
          fields.add(NUM_ROWS);
          values.add((selectResult == null) ? 0 : selectResult.size());
          if(queryTraceString!=null){
            fields.add(QUERY_TRACE);values.add(queryTraceString);
          }
          return createPageResult(fields, values, step, headers, rows);
        } catch (GfJsonException e) {
          String[] headers = new String[] { "Error" };
          Object[][] rows = { { e.getMessage() } };
          String fieldsArray[] = { QUERY_PAGE_START, QUERY_PAGE_END };
          Object valuesArray[] = { startCount, endCount};
          return createPageResult(fieldsArray, valuesArray, step, headers, rows);
        }
      } else
        return createBannerResult(fields, values, step);
    }
  }
  
  private int buildTable(TabularResultData table,int startCount, int endCount){
    int rowCount=0;
    //Introspect first using tabular data
    for (int i = startCount; i <= endCount; i++) {      
      if(i >= selectResult.size())
        break;
      else rowCount++;
      
      SelectResultRow row = selectResult.get(i);
      switch (row.type) {
      case ROW_TYPE_BEAN:
        addJSONStringToTable(table, row.value);
        break;
      case ROW_TYPE_STRUCT_RESULT:
        addJSONStringToTable(table, row.value);
        break;
      case ROW_TYPE_PRIMITIVE:
        table.accumulate(RESULT_FLAG, row.value);
        break;
      }
    }
    return rowCount;
  } 

  private boolean isDeclaredPrimitive(String keyClass2) {
    try{
      Class klass =  ClassPathLoader.getLatest().forName(keyClass2);
      return JsonUtil.isPrimitiveOrWrapper(klass);
    }catch(ClassNotFoundException e){
      return false;
    }    
  }

  private void addJSONStringToTable(TabularResultData table, Object object) {    
    if(object==null || "<NULL>".equals(object)){
      table.accumulate("Value", "<NULL>");
    }
    else{
      try {
        Class klass = object.getClass();
        GfJsonObject jsonObject = null;
        if (String.class.equals(klass)) {
          // InputString in JSON Form but with round brackets          
          String json = (String) object;
          String newString = json.replaceAll("'", "\"");
          if (newString.charAt(0) == '(') {
            int len = newString.length();
            StringBuilder sb = new StringBuilder();
            sb.append("{").append(newString.substring(1, len - 1)).append("}");
            newString = sb.toString();
          }         
          jsonObject = new GfJsonObject(newString);          
        } else {
          jsonObject = new GfJsonObject(object, true);
        }
  
        Iterator<String> keys = jsonObject.keys();
        while (keys.hasNext()) {
          String k = keys.next();
          //filter out meta-field type-class used to identify java class of json obbject
          if(!"type-class".equals(k)){
            Object value = jsonObject.get(k);
            if (value != null){                         
                table.accumulate(k, getDomainValue(value));              
            }
          }
        }
      } catch (Exception e) {        
        table.accumulate("Value", "Error getting bean properties " + e.getMessage());
      }
    }
  }
  
  
  private Object getDomainValue(Object value) {
    if(value instanceof String){
      String str = (String) value;
      if(str.contains("{") && str.contains("}")){// small filter to see if its json string
        try {
          JSONObject json = new JSONObject(str);
          return json.get("type-class");
        } catch (Exception e) {
          return str;
        }
      }else return str;
    }
    return value;
  }

  public Object getInputQuery() {
    return inputQuery;
  }

  public void setInputQuery(Object inputQuery) {
    this.inputQuery = inputQuery;
  }


  public static class KeyInfo implements /*Data*/ Serializable{
    
    private String memberId;
    private String memberName;
    private String host;
    private int pid;
    
    //Indexes : regionName = 0, found=1, value=2 primary=3 bucketId=4
    private ArrayList<Object[]> locations = null;   
    
    public void addLocation(Object[] locationArray){
      if(this.locations==null)
        locations = new ArrayList<Object[]>();
        
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
   
    public String toString(){
      StringBuilder sb = new StringBuilder();
      sb.append("{ Member : ").append(host).append("(").append(memberId).append(") , ");
      for(Object[] array : locations){
        boolean primary = (Boolean)array[3];        
        String bucketId = (String)array[4];                
        sb.append(" [ Primary : ").append(primary).append(" , BucketId : ").append(bucketId).append(" ]");
      }
      sb.append(" }");
      return sb.toString();
    }

    public boolean hasLocation() {
      if(locations==null)
        return false;
      else{
        for(Object[] array:locations){
          boolean found = (Boolean)array[1];
          if(found)
            return true;
        }
      }
    return false;
   }

    //@Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeString(memberId,out);
      DataSerializer.writeString(memberName,out);
      DataSerializer.writeString(host,out);
      DataSerializer.writePrimitiveInt(pid, out);
      DataSerializer.writeArrayList(locations, out);      
      
      
    }

    //@Override
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
    if(isLocateEntry()){
      /*Right now only called for LocateEntry*/
      
      if(this.locateEntryLocations==null){
        locateEntryLocations = new ArrayList<KeyInfo>();
      }
      
      if(result==null){//self-transform result from single to aggregate when numMember==1
        if(this.locateEntryResult!=null){          
          locateEntryLocations.add(locateEntryResult);
          //TODO : Decide whether to show value or not this.getResult = locateEntryResult.getValue();
        }        
        return;
      }
      
      if(result.errorString!=null && !result.errorString.equals(errorString)){
        //append errorString only if differs
        String newString = result.errorString  + " " + errorString;
        errorString = newString;
      }
      
      //append messsage only when it differs for negative results
      if (!operationCompletedSuccessfully && result.infoString != null 
          && !result.infoString.equals(infoString) ) {
        infoString = result.infoString;
      }
      
      if(result.hasResultForAggregation /*&& result.errorString==null*/){
        this.operationCompletedSuccessfully = true;//override this result.operationCompletedSuccessfully
        infoString = result.infoString;
        if(result.locateEntryResult!=null)
          locateEntryLocations.add(result.locateEntryResult);        
      }
    }
  }

  //@Override
  public void toData(DataOutput out) throws IOException {
      DataSerializer.writeString(command, out);
      out.writeUTF(command);
      DataSerializer.writeObject(putResult,out);
      DataSerializer.writeObject(getResult,out);
      DataSerializer.writeObject(locateEntryResult, out);
      DataSerializer.writeArrayList((ArrayList<?>) locateEntryLocations, out);
      DataSerializer.writeBoolean(hasResultForAggregation, out);
      DataSerializer.writeObject(removeResult,out);
      DataSerializer.writeObject(inputKey,out);
      DataSerializer.writeObject(inputValue,out);
      DataSerializer.writeObject(error, out);
      DataSerializer.writeString(errorString, out);
      DataSerializer.writeString(infoString, out);
      DataSerializer.writeString(keyClass, out);
      DataSerializer.writeString(valueClass, out);
      DataSerializer.writeBoolean(operationCompletedSuccessfully, out);
  }

  //@Override
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



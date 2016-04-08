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
package com.gemstone.gemfire.pdx;

import java.io.IOException;
import java.math.BigInteger;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonParser.NumberType;
import com.fasterxml.jackson.core.JsonToken;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.internal.json.PdxInstanceHelper;
import com.gemstone.gemfire.pdx.internal.json.PdxListHelper;
import com.gemstone.gemfire.pdx.internal.json.PdxToJSON;


/**
 * PdxFormatter class has static methods to convert JSON document into {@link PdxInstance}
 * and methods to convert back {@link PdxInstance} into JSON Document.
 * 
 * Using this, application can put json document in gemfire cache. Application can define indexes 
 * on PdxInsatnce and then query those using OQL. Query will return the PdxInstances as results,
 * that needs to convert back into JSON document. 
 * 
 * This uses Jackson parser to parse the json document. Parser treats values in json document as 
 * number(byte, short, int, long..), string, array, object, 'true', 'false' or 'null'. Which
 * further treated as corresponding java types in PdxInstance
 * 
 * JSON objects are converted into PdxInstance
 * JSON arrays are converted into List.
 *  
 */

public class JSONFormatter {
  
  public static final String JSON_CLASSNAME = "__GEMFIRE_JSON";
  
  enum states {NONE, ObJECT_START,  FIELD_NAME, INNER_OBJECT_FOUND, SCALER_FOUND, LIST_FOUND, OBJECT_ENDS};
  
  private JSONFormatter() {
  }
  
  /**
   * To create PdxInstance from JSON string
   * 
   * @return the PdxInstance.
   * @throws JSONFormatterException if unable to parse JSON doucment
   */
  public static PdxInstance fromJSON(String jsonString) {
    JsonParser jp = null;
    try {
      jp = new JsonFactory().createParser(jsonString);
      enableJSONParserFeature(jp);
      return new JSONFormatter().getPdxInstance(jp, states.NONE, null).getPdxInstance();
    } catch (JsonParseException jpe) {
      throw new JSONFormatterException("Could not parse JSON document " , jpe);
    } catch (IOException e) {
      throw new JSONFormatterException("Could not parse JSON document: " + jp.getCurrentLocation(), e);
    } catch(Exception e) {
      throw new JSONFormatterException("Could not parse JSON document: " + jp.getCurrentLocation(), e);
    }  
  }
  
  /**
   * To create PdxInstance from JSON string
   * 
   * @return the PdxInstance.
   * @throws JSONFormatterException if unable to parse JSON doucment
   */
  public static PdxInstance fromJSON(byte[] jsonByteArray) {
    JsonParser jp = null;
    try {
      jp = new JsonFactory().createParser(jsonByteArray);
      enableJSONParserFeature(jp);
      return new JSONFormatter().getPdxInstance(jp, states.NONE, null).getPdxInstance();
    }  catch (JsonParseException jpe) {
      throw new JSONFormatterException("Could not parse JSON document " , jpe);
    } catch (IOException e) {
      throw new JSONFormatterException("Could not parse JSON document: " + jp.getCurrentLocation(), e);
    } catch(Exception e) {
      throw new JSONFormatterException("Could not parse JSON document: " + jp.getCurrentLocation(), e);
    } 
  }
  
  private static void enableJSONParserFeature(JsonParser jp) {
    jp.enable(Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER);
    jp.enable(Feature.ALLOW_UNQUOTED_FIELD_NAMES);
  }
  
  /**
   *To create JSON string from PdxInstance
   * 
   * @return the JSON string.
   * @throws JSONFormatterException if unable to create JSON doucment from PdxInstance
   */
  public static String toJSON(PdxInstance pdxInstance) {
    try {
      PdxToJSON pj = new PdxToJSON(pdxInstance);
      return pj.getJSON();
    } catch (Exception e) {
      throw new JSONFormatterException("Could not create JSON document from PdxInstance ", e);
    }    
  }
  
  /**
   *To create JSON byte array from PdxInstance
   * 
   * @return the JSON byte array.
   * @throws JSONFormatterException if unable to create JSON doucment from PdxInstance
   */
  public static byte[] toJSONByteArray(PdxInstance pdxInstance) {
    try {
      PdxToJSON pj = new PdxToJSON(pdxInstance);
      return pj.getJSONByteArray();
    } catch (Exception e) {
      throw new JSONFormatterException("Could not create JSON document from PdxInstance ", e);
    }    
  }
  
  private PdxInstanceHelper getPdxInstance(JsonParser jp, states currentState, PdxInstanceHelper currentPdxInstance) throws JsonParseException, IOException {
    String currentFieldName = null;
    if(currentState == states.ObJECT_START && currentPdxInstance == null)
      currentPdxInstance = new PdxInstanceHelper(null, null);//from getlist
    while(true)
    {
      JsonToken nt = jp.nextToken();
     
      if(nt == null)
      {
        return currentPdxInstance;
      }
       switch(nt)
      {
        case START_OBJECT:
        {
          objectStarts(currentState);
          currentState = states.ObJECT_START;
          //need to create new PdxInstance
          // root object will not name, so create classname lazily from all members.
          // child object will have name; but create this as well lazily from all members
          PdxInstanceHelper tmp = new PdxInstanceHelper(currentFieldName , currentPdxInstance);
          currentPdxInstance = tmp;
          break;
        }
        case END_OBJECT:
        {
          //pdxinstnce ends
          objectEnds(currentState);
          currentState = states.OBJECT_ENDS;
          currentPdxInstance.endObjectField("endobject");
          if(currentPdxInstance.getParent() == null)
            return currentPdxInstance;// inner pdxinstance in list
          PdxInstanceHelper tmp = currentPdxInstance; 
          currentPdxInstance = currentPdxInstance.getParent();
          currentPdxInstance.addObjectField(tmp.getPdxFieldName(), tmp.getPdxInstance());          
          break;
        }
        case FIELD_NAME:
        {
          fieldFound(currentState);
          //field name(object name, value may be object, string, array number etc)
          if(currentState == states.ObJECT_START)
            currentPdxInstance.setPdxFieldName(currentFieldName);
          
          currentFieldName = jp.getText();// not a object name
          currentState = states.FIELD_NAME;
          break;
        }
        case NOT_AVAILABLE :
        {
          throw new IllegalStateException("NOT_AVAILABLE token found");
          //break;
        }
        case START_ARRAY :
        {
          //need to create array; fieldname may be there; will it case it not there
          arrayStarts(currentState);
          PdxListHelper list = getList(jp, currentState, null);
          currentPdxInstance.addListField(currentFieldName, list);
          currentState = states.LIST_FOUND;
          currentFieldName = null;          
          break;
        }
        case END_ARRAY :
        {
          //array is end
          throw new IllegalStateException("END_ARRAY token found in getPdxInstance while current state is " + currentState);
        }
        case VALUE_EMBEDDED_OBJECT :
        {
          throw new IllegalStateException("VALUE_EMBEDDED_OBJECT token found in getPdxInstance while current state is " + currentState);
        }
        case VALUE_FALSE :
        {
          //write boolen
          boolFound(currentState);
          currentState = states.SCALER_FOUND;
          currentPdxInstance.addBooleanField(currentFieldName, jp.getValueAsBoolean());
          currentFieldName = null;
          break;
        }
        case VALUE_NULL :
        {
          //write null
          nullfound(currentState);
          currentState = states.SCALER_FOUND;
          currentPdxInstance.addNullField(currentFieldName);
          currentFieldName = null;
          break;
        }
        case VALUE_NUMBER_FLOAT:
        {
          //write double/float
          doublefound(currentState);
          currentState = states.SCALER_FOUND;
          //currentPdxInstance.addDoubleField(currentFieldName, jp.getDoubleValue());
          setNumberField(jp, currentPdxInstance, currentFieldName);
          currentFieldName = null;
          break;
        }
        case VALUE_NUMBER_INT:
        {
         //write int
          doublefound(currentState);
          currentState = states.SCALER_FOUND;
          //currentPdxInstance.addIntField(currentFieldName, jp.getIntValue());
          setNumberField(jp, currentPdxInstance, currentFieldName);
          currentFieldName = null;
          break;
        }
        case VALUE_STRING:
        {
          //write string
          doublefound(currentState);
          currentState = states.SCALER_FOUND;
          currentPdxInstance.addStringField(currentFieldName, new String(jp.getText()));
          currentFieldName = null;
          break;
        }
        case VALUE_TRUE:
        {
          //write bool
          boolFound(currentState);
          currentState = states.SCALER_FOUND;
          currentPdxInstance.addBooleanField(currentFieldName, jp.getValueAsBoolean());
          currentFieldName = null;
          break;
        }
        default:
        {
          throw new IllegalStateException("Token not handled " + nt); 
        }
      }
    }
  }   
  
  private void setNumberField(JsonParser jp, PdxInstanceHelper pih, String fieldName) throws IOException {
    try{
      NumberType nt = jp.getNumberType();
      
      switch(nt) {
      case BIG_DECIMAL:
        pih.addBigDecimalField(fieldName, jp.getDecimalValue());
        break;
      case BIG_INTEGER: {
        BigInteger bi = jp.getBigIntegerValue();
        pih.addBigIntegerField(fieldName, bi);
      }
        break;
      case DOUBLE:
        pih.addDoubleField(fieldName, jp.getDoubleValue());
        break;
      case FLOAT:
        pih.addFloatField(fieldName, jp.getFloatValue());
        break;
      case INT: {
        int val = jp.getIntValue();
        if (val > Short.MAX_VALUE || val < Short.MIN_VALUE) {
          pih.addIntField(fieldName, val);
        } else if (val > Byte.MAX_VALUE || val < Byte.MIN_VALUE) {
          pih.addShortField(fieldName, (short)val);
        } else {
          pih.addByteField(fieldName, (byte)val);
        }
      }
        break;
      case LONG:
        pih.addLongField(fieldName, jp.getLongValue());
        break;
        default:
          throw new IllegalStateException("setNumberField:unknow number type " + nt);
      }      
    }catch(JsonParseException jpe) {
      throw jpe;
    } catch (IOException e) {
      throw e;
    }
  }
  
  private void setNumberField(JsonParser jp, PdxListHelper pih) throws IOException {
    try{
      NumberType nt = jp.getNumberType();
      
      switch(nt) {
      case BIG_DECIMAL:
        pih.addBigDecimalField(jp.getDecimalValue());
        break;
      case BIG_INTEGER: {
        BigInteger bi = jp.getBigIntegerValue();
        pih.addBigIntegerField(bi);
      }
        break;
      case DOUBLE:
        pih.addDoubleField(jp.getDoubleValue());
        break;
      case FLOAT:
        pih.addFloatField(jp.getFloatValue());
        break;
      case INT: {
        int val = jp.getIntValue();
        if (val > Short.MAX_VALUE || val < Short.MIN_VALUE) {
          pih.addIntField(val);
        } else if (val > Byte.MAX_VALUE || val < Byte.MIN_VALUE) {
          pih.addShortField((short)val);
        } else {
          pih.addByteField((byte)val);
        }
      }
        break;
      case LONG:
        pih.addLongField(jp.getLongValue());
        break;
        default:
          throw new IllegalStateException("setNumberField:unknow number type " + nt);
      }      
    }catch(JsonParseException jpe) {
      throw jpe;
    } catch (IOException e) {
      throw e;
    }
  }
  
  private PdxListHelper getList(JsonParser jp, states currentState, PdxListHelper currentPdxList) throws JsonParseException, IOException {
    String currentFieldName = null;
    currentPdxList = new PdxListHelper(currentPdxList, null);
    while(true)
    {
      JsonToken nt = jp.nextToken();
     
      if(nt == null)
      {
        return currentPdxList;
      }
       switch(nt)
      {
        case START_OBJECT:
        {
          objectStarts(currentState);
          currentState = states.ObJECT_START;
          //need to create new PdxInstance
          // root object will not name, so create classname lazily from all members.
          // child object will have name; but create this as well lazily from all members
          PdxInstanceHelper tmp = getPdxInstance(jp, currentState, null);
          currentPdxList.addObjectField(currentFieldName, tmp);
          currentState = states.OBJECT_ENDS;
          break;
        }
        case END_OBJECT:
        {
          //pdxinstnce ends
          throw new IllegalStateException("getList got token END_OBJECT while current state is " + currentState);
        }
        case FIELD_NAME:
        {
          throw new IllegalStateException("getList got token FIELD_NAME while current state is " + currentState);
        }
        case NOT_AVAILABLE :
        {
          throw new IllegalStateException("NOT_AVAILABLE token found in getList current state is " + currentState);
          //break;
        }
        case START_ARRAY :
        {
          //need to create array; fieldname may be there; will it case it not there
          arrayStarts(currentState);
          PdxListHelper tmp = currentPdxList.addListField();          
          currentPdxList = tmp;
          currentState = states.LIST_FOUND;
          break;
        }
        case END_ARRAY :
        {
          //array is end
          arrayEnds(currentState);
          if(currentPdxList.getParent() == null)
            return currentPdxList;
          currentPdxList = currentPdxList.getParent(); 
          currentState = states.LIST_FOUND;
          break;
        }
        case VALUE_EMBEDDED_OBJECT :
        {
          throw new IllegalStateException("VALUE_EMBEDDED_OBJECT token found");
        }
        case VALUE_FALSE :
        {
          //write boolen
          boolFound(currentState);
          currentState = states.SCALER_FOUND;
          currentPdxList.addBooleanField(jp.getBooleanValue());
          break;
        }
        case VALUE_NULL :
        {
          //write null
          nullfound(currentState);
          currentState = states.SCALER_FOUND;
          currentPdxList.addNullField(null);          
          break;
        }
        case VALUE_NUMBER_FLOAT:
        {
          //write double/float
          doublefound(currentState);
          currentState = states.SCALER_FOUND;
          //currentPdxList.addDoubleField(jp.getDoubleValue());
          setNumberField(jp,currentPdxList);
          break;
        }
        case VALUE_NUMBER_INT:
        {
         //write int
          doublefound(currentState);
          currentState = states.SCALER_FOUND;
         // currentPdxList.addIntField(jp.getIntValue());
          setNumberField(jp,currentPdxList);
          break;
        }
        case VALUE_STRING:
        {
          //write string
          stringFound(currentState);
          currentState = states.SCALER_FOUND;
          currentPdxList.addStringField(jp.getText());
          currentFieldName = null;
          break;
        }
        case VALUE_TRUE:
        {
          //write bool
          boolFound(currentState);
          currentState = states.SCALER_FOUND;
          currentPdxList.addBooleanField(jp.getBooleanValue());
          break;
        }
        default:
        {
          throw new IllegalStateException("Token not handled in getlist" + nt); 
        }
      }
    }
  }
  
  private boolean objectStarts(states currentState)
  {
    switch(currentState)
    {
    case NONE:
    case FIELD_NAME:
    case OBJECT_ENDS://in list
    case SCALER_FOUND://inlist
      return true;
      default:
        throw new IllegalStateException("Object start called when state is " +currentState);
        
    }
  }
  
  private boolean objectEnds(states currentState)
  {
    switch(currentState)
    {
    case ObJECT_START: //when empty object on field
    case SCALER_FOUND:
    case LIST_FOUND:
    case OBJECT_ENDS://inner object closes
      return true;
      default:
        throw new IllegalStateException("Object ends called when state is " +currentState);
        
    }
  }
  
  private boolean arrayStarts(states currentState)
  {
    switch(currentState)
    {
    case SCALER_FOUND:
    case FIELD_NAME:
    case LIST_FOUND:
      return true;
      default:
        throw new IllegalStateException("Array start called when state is " +currentState);
        
    }
  }
  //enum states {NONE, ObJECT_START,  FIELD_NAME, INNER_OBJECT_FOUND, SCALER_FOUND, LIST_FOUND, OBJECT_ENDS};
  private boolean arrayEnds(states currentState)
  {
    switch(currentState)
    {
    case FIELD_NAME://when empty array
    case SCALER_FOUND:
    case LIST_FOUND:
    case OBJECT_ENDS:
      return true;
      default:
        throw new IllegalStateException("Array ends called when state is " +currentState);
        
    }   
  }
  
  private boolean stringFound(states currentState)
  {
    switch(currentState)
    {
    case FIELD_NAME:
    case SCALER_FOUND:
    case LIST_FOUND:
    case OBJECT_ENDS:
      return true;
      default:
        throw new IllegalStateException("stringFound called when state is " +currentState);
        
    }
  }
  
  private boolean intFound(states currentState)
  {
    switch(currentState)
    {
    case FIELD_NAME:
    case SCALER_FOUND:
    case LIST_FOUND:
    case OBJECT_ENDS:
      return true;
      default:
        throw new IllegalStateException("intFound called when state is " +currentState);
        
    }
  }
  
  private boolean boolFound(states currentState)
  {
    switch(currentState)
    {
    case FIELD_NAME:
    case SCALER_FOUND:
    case LIST_FOUND:
    case OBJECT_ENDS:
      return true;
      default:
        throw new IllegalStateException("boolFound called when state is " +currentState);
        
    }
  }
  
  private boolean doublefound(states currentState)
  {
    switch(currentState)
    {
    case FIELD_NAME:
    case SCALER_FOUND:
    case LIST_FOUND:
    case OBJECT_ENDS:
      return true;
      default:
        throw new IllegalStateException("doubleFound called when state is " +currentState);
        
    }
  }
  
  private boolean fieldFound(states currentState)
  {
    switch(currentState)
    {
    case ObJECT_START:
    case SCALER_FOUND:
    case LIST_FOUND:
    case OBJECT_ENDS:
      return true;
      default:
        throw new IllegalStateException("fieldFound called when state is " +currentState);
        
    }
  }
  
  private boolean nullfound(states currentState)
  {
    switch(currentState)
    {
    case FIELD_NAME:
    case SCALER_FOUND:
    case LIST_FOUND:
    case OBJECT_ENDS:
      return true;
      default:
        throw new IllegalStateException("nullFound called when state is " +currentState);
        
    }
  }
}

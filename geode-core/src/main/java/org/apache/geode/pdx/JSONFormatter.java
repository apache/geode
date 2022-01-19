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
package org.apache.geode.pdx;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonParser.NumberType;
import com.fasterxml.jackson.core.JsonToken;

import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.client.internal.ProxyCache;
import org.apache.geode.cache.client.internal.UserAttributes;
import org.apache.geode.pdx.internal.json.JSONToPdxMapper;
import org.apache.geode.pdx.internal.json.PdxInstanceHelper;
import org.apache.geode.pdx.internal.json.PdxInstanceSortedHelper;
import org.apache.geode.pdx.internal.json.PdxListHelper;
import org.apache.geode.pdx.internal.json.PdxToJSON;
import org.apache.geode.util.internal.GeodeGlossary;


/**
 * <p>
 * JSONFormatter has a static method {@link JSONFormatter#fromJSON(String, String...)} to convert a
 * JSON document into a {@link PdxInstance} and a static method
 * {@link JSONFormatter#toJSON(PdxInstance)} to convert a {@link PdxInstance} into a JSON Document.
 * </p>
 * <p>
 * Using these methods an applications may convert a JSON document into a PdxInstance for storing in
 * the cache. Indexes can then be defined on the PdxInstances so that queries can be performed using
 * OQL. Queries will return PdxInstances and these can be turned back back into JSON documents using
 * JSONFormatter.
 * </p>
 * <p>
 * JSONFormatter treats values in a json document as number(byte, short, int, long..), string,
 * array, object, 'true', 'false' or 'null'. These correspond to the following java types:
 * </p>
 *
 * <table>
 * <th>JSON</th>
 * <th>Java</th>
 * <tr>
 * <td>object</td>
 * <td>{@link PdxInstance}</td>
 * </tr>
 * <tr>
 * <td>arrays</td>
 * <td>{@link java.util.LinkedList}</td>
 * </tr>
 * <tr>
 * <td>BigDecimal</td>
 * <td>{@link BigDecimal}</td>
 * </tr>
 * <tr>
 * <td>BigInterger</td>
 * <td>{@link BigInteger}</td>
 * </tr>
 * <tr>
 * <td>Double</td>
 * <td>double</td>
 * </tr>
 * <tr>
 * <td>float</td>
 * <td>float</td>
 * </tr>
 * <tr>
 * <td>boolean</td>
 * <td>boolean</td>
 * </tr>
 * <tr>
 * <td>Integer</td>
 * <td>byte, short or int</td>
 * </tr>
 * <tr>
 * <td>null</td>
 * <td>null</td>
 * </tr>
 * </table>
 */

public class JSONFormatter {

  public static final String JSON_CLASSNAME = "__GEMFIRE_JSON";

  /***
   * By setting "gemfire.sort-json-field-names" to true, enables serialization of JSON field in JSON
   * document to be sorted. That can help to reduce the number of pdx typeId generation if different
   * JSON documents have same fields in the different order.
   */
  public static final String SORT_JSON_FIELD_NAMES_PROPERTY =
      GeodeGlossary.GEMFIRE_PREFIX + "pdx.mapper.sort-json-field-names";

  enum states {
    NONE, OBJECT_START, FIELD_NAME, SCALAR_FOUND, LIST_FOUND, LIST_ENDS, OBJECT_ENDS
  }

  private RegionService regionService;

  public JSONFormatter() {}

  public JSONFormatter(RegionService regionService) {
    this.regionService = regionService;
  }

  public RegionService getRegionService() {
    return regionService;
  }

  /**
   * Converts a String JSON document into a PdxInstance
   *
   * @param jsonString The JSON String to convert to PDX
   * @throws JSONFormatterException if unable to create the PdxInstance
   * @return The PdxInstance
   */
  public static PdxInstance fromJSON(String jsonString) {
    return new JSONFormatter().toPdxInstance(jsonString);
  }

  /**
   * Converts a String JSON document into a PdxInstance
   *
   * @param jsonString The JSON String to convert to PDX
   * @param identityFields Any desired identity fields on the JSON object to be used for equals and
   *        hashCode computations
   * @throws JSONFormatterException if unable to create the PdxInstance
   * @return The PdxInstance
   */
  public static PdxInstance fromJSON(String jsonString, String... identityFields) {
    return new JSONFormatter().toPdxInstance(jsonString, identityFields);
  }

  /**
   * Converts a Byte Array JSON document into a PdxInstance
   *
   * @param jsonByteArray The JSON Object as a byte array to convert to PDX
   * @throws JSONFormatterException if unable to create the PdxInstance
   * @return The PdxInstance
   */
  public static PdxInstance fromJSON(byte[] jsonByteArray) {
    return new JSONFormatter().toPdxInstance(jsonByteArray);
  }

  /**
   * Converts a Byte Array JSON document into a PdxInstance
   *
   * @param jsonByteArray The JSON Object as a byte array to convert to PDX
   * @param identityFields Any desired identity fields on the JSON object to be used for equals and
   *        hashCode computations
   * @throws JSONFormatterException if unable to create the PdxInstance
   * @return The PdxInstance
   */
  public static PdxInstance fromJSON(byte[] jsonByteArray, String... identityFields) {
    return new JSONFormatter().toPdxInstance(jsonByteArray, identityFields);
  }

  /**
   * Converts a JSON document (String or Byte Array) into a PdxInstance
   *
   * @param json The JSON document (String or Byte Array) to convert to PDX
   * @param identityFields Any desired identity fields on the JSON object to be used for equals and
   *        hashCode computations
   * @return The PdxInstance
   * @throws JSONFormatterException if unable to create the PdxInstance
   */
  public PdxInstance toPdxInstance(Object json, String... identityFields) {
    if (regionService != null && regionService instanceof ProxyCache) {
      ProxyCache proxyCache = (ProxyCache) regionService;
      UserAttributes.userAttributes.set(proxyCache.getUserAttributes());
    }
    JsonParser jp = null;
    try {
      if (json instanceof String) {
        jp = new JsonFactory().createParser((String) json);

      } else if (json instanceof byte[]) {
        jp = new JsonFactory().createParser((byte[]) json);
      } else {
        throw new JSONFormatterException("Could not parse the " + json.getClass() + " type");
      }
      enableJSONParserFeature(jp);
      return getPdxInstance(jp, states.NONE, null, identityFields).getPdxInstance();
    } catch (JsonParseException jpe) {
      throw new JSONFormatterException("Could not parse JSON document ", jpe);
    } catch (IOException e) {
      throw new JSONFormatterException("Could not parse JSON document: " + jp.getCurrentLocation(),
          e);
    } catch (Exception e) {
      throw new JSONFormatterException("Could not parse JSON document: " + jp.getCurrentLocation(),
          e);
    } finally {
      UserAttributes.userAttributes.set(null);
    }
  }

  private void enableJSONParserFeature(JsonParser jp) {
    jp.enable(Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER);
    jp.enable(Feature.ALLOW_UNQUOTED_FIELD_NAMES);
  }

  /**
   * Converts a PdxInstance into a JSON document in String form
   *
   * @param pdxInstance the JSON string.
   * @return the JSON string
   *         JSONFormatterException if unable to create the JSON document
   */
  public static String toJSON(PdxInstance pdxInstance) {
    return new JSONFormatter().fromPdxInstance(pdxInstance);
  }

  /**
   * Converts a PdxInstance into a JSON document
   *
   * @param pdxInstance The PdxInstance to convert
   * @return the JSON string
   * @throws JSONFormatterException if unable to create the JSON document
   */
  public String fromPdxInstance(PdxInstance pdxInstance) {
    try {
      PdxToJSON pj = new PdxToJSON(pdxInstance);
      return pj.getJSON();
    } catch (Exception e) {
      throw new JSONFormatterException("Could not create JSON document from PdxInstance", e);
    }
  }

  /**
   * Converts a PdxInstance into a JSON document in byte-array form
   *
   * @param pdxInstance The PdxInstance to convert
   * @return the JSON byte array
   * @throws JSONFormatterException if unable to create the JSON document
   */
  public static byte[] toJSONByteArray(PdxInstance pdxInstance) {
    return new JSONFormatter().toJsonByteArrayFromPdxInstance(pdxInstance);
  }

  /**
   * Converts a PdxInstance into a JSON document in byte-array form
   *
   * @param pdxInstance The PdxInstance to convert
   * @return the JSON byte array
   * @throws JSONFormatterException if unable to create the JSON document
   */
  public byte[] toJsonByteArrayFromPdxInstance(PdxInstance pdxInstance) {
    try {
      PdxToJSON pj = new PdxToJSON(pdxInstance);
      return pj.getJSONByteArray();
    } catch (Exception e) {
      throw new JSONFormatterException("Could not create JSON document from PdxInstance", e);
    }
  }

  private JSONToPdxMapper createJSONToPdxMapper(String className, JSONToPdxMapper parent,
      String... identityFields) {
    if (Boolean.getBoolean(SORT_JSON_FIELD_NAMES_PROPERTY)) {
      return new PdxInstanceSortedHelper(className, parent, identityFields);
    } else {
      return new PdxInstanceHelper(className, parent, identityFields);
    }
  }

  private JSONToPdxMapper getPdxInstance(JsonParser jp, states currentState,
      JSONToPdxMapper currentPdxInstance, String... identityFields) throws IOException {
    String currentFieldName = null;
    if (currentState == states.OBJECT_START && currentPdxInstance == null) {
      currentPdxInstance = createJSONToPdxMapper(null, null, identityFields);// from getlist
    }
    while (true) {
      JsonToken nt = jp.nextToken();

      if (nt == null) {
        return currentPdxInstance;
      }
      switch (nt) {
        case START_OBJECT: {
          objectStarts(currentState);
          currentState = states.OBJECT_START;
          // need to create new PdxInstance
          // root object will not name, so create classname lazily from all members.
          // child object will have name; but create this as well lazily from all members
          JSONToPdxMapper tmp =
              createJSONToPdxMapper(currentFieldName, currentPdxInstance, identityFields);
          currentPdxInstance = tmp;
          break;
        }
        case END_OBJECT: {
          // pdxinstnce ends
          objectEnds(currentState);
          currentState = states.OBJECT_ENDS;
          currentPdxInstance.endObjectField("endobject");
          if (currentPdxInstance.getParent() == null) {
            return currentPdxInstance;// inner pdxinstance in list
          }
          JSONToPdxMapper tmp = currentPdxInstance;
          currentPdxInstance = currentPdxInstance.getParent();
          currentPdxInstance.addObjectField(tmp.getPdxFieldName(), tmp.getPdxInstance());
          break;
        }
        case FIELD_NAME: {
          fieldFound(currentState);
          // field name(object name, value may be object, string, array number etc)
          if (currentState == states.OBJECT_START) {
            currentPdxInstance.setPdxFieldName(currentFieldName);
          }

          currentFieldName = jp.getText();// not a object name
          currentState = states.FIELD_NAME;
          break;
        }
        case NOT_AVAILABLE: {
          throw new IllegalStateException("NOT_AVAILABLE token found");
          // break;
        }
        case START_ARRAY: {
          // need to create array; fieldname may be there; will it case it not there
          arrayStarts(currentState);
          currentState = states.LIST_FOUND;
          PdxListHelper list = getList(jp, currentState, null);
          currentPdxInstance.addListField(currentFieldName, list);
          currentState = states.LIST_ENDS;
          currentFieldName = null;
          break;
        }
        case END_ARRAY: {
          // array is end
          throw new IllegalStateException(
              "END_ARRAY token found in getPdxInstance while current state is " + currentState);
        }
        case VALUE_EMBEDDED_OBJECT: {
          throw new IllegalStateException(
              "VALUE_EMBEDDED_OBJECT token found in getPdxInstance while current state is "
                  + currentState);
        }
        case VALUE_FALSE: {
          // write boolen
          boolFound(currentState);
          currentState = states.SCALAR_FOUND;
          currentPdxInstance.addBooleanField(currentFieldName, jp.getValueAsBoolean());
          currentFieldName = null;
          break;
        }
        case VALUE_NULL: {
          // write null
          nullFound(currentState);
          currentState = states.SCALAR_FOUND;
          currentPdxInstance.addNullField(currentFieldName);
          currentFieldName = null;
          break;
        }
        case VALUE_NUMBER_FLOAT: {
          // write double/float
          doubleFound(currentState);
          currentState = states.SCALAR_FOUND;
          // currentPdxInstance.addDoubleField(currentFieldName, jp.getDoubleValue());
          setNumberField(jp, currentPdxInstance, currentFieldName);
          currentFieldName = null;
          break;
        }
        case VALUE_NUMBER_INT: {
          // write int
          intFound(currentState);
          currentState = states.SCALAR_FOUND;
          // currentPdxInstance.addIntField(currentFieldName, jp.getIntValue());
          setNumberField(jp, currentPdxInstance, currentFieldName);
          currentFieldName = null;
          break;
        }
        case VALUE_STRING: {
          // write string
          stringFound(currentState);
          currentState = states.SCALAR_FOUND;
          currentPdxInstance.addObjectField(currentFieldName, jp.getText());
          currentFieldName = null;
          break;
        }
        case VALUE_TRUE: {
          // write bool
          boolFound(currentState);
          currentState = states.SCALAR_FOUND;
          currentPdxInstance.addBooleanField(currentFieldName, jp.getValueAsBoolean());
          currentFieldName = null;
          break;
        }
        default: {
          throw new IllegalStateException("Token not handled " + nt);
        }
      }
    }
  }

  private void setNumberField(JsonParser jp, JSONToPdxMapper pih, String fieldName)
      throws IOException {
    NumberType nt = jp.getNumberType();

    switch (nt) {
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
          pih.addShortField(fieldName, (short) val);
        } else {
          pih.addByteField(fieldName, (byte) val);
        }
      }
        break;
      case LONG:
        pih.addLongField(fieldName, jp.getLongValue());
        break;
      default:
        throw new IllegalStateException("setNumberField:unknow number type " + nt);
    }
  }

  private void setNumberField(JsonParser jp, PdxListHelper pih) throws IOException {
    NumberType nt = jp.getNumberType();

    switch (nt) {
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
          pih.addShortField((short) val);
        } else {
          pih.addByteField((byte) val);
        }
      }
        break;
      case LONG:
        pih.addLongField(jp.getLongValue());
        break;
      default:
        throw new IllegalStateException("setNumberField:unknow number type " + nt);
    }
  }

  private PdxListHelper getList(JsonParser jp, states currentState, PdxListHelper currentPdxList)
      throws IOException {
    String currentFieldName = null;
    currentPdxList = new PdxListHelper(currentPdxList, null);
    while (true) {
      JsonToken nt = jp.nextToken();

      if (nt == null) {
        return currentPdxList;
      }
      switch (nt) {
        case START_OBJECT: {
          objectStarts(currentState);
          currentState = states.OBJECT_START;
          // need to create new PdxInstance
          // root object will not name, so create classname lazily from all members.
          // child object will have name; but create this as well lazily from all members
          JSONToPdxMapper tmp = getPdxInstance(jp, currentState, null);
          currentPdxList.addObjectField(currentFieldName, tmp);
          currentState = states.OBJECT_ENDS;
          break;
        }
        case END_OBJECT: {
          // pdxinstnce ends
          throw new IllegalStateException(
              "getList got token END_OBJECT while current state is " + currentState);
        }
        case FIELD_NAME: {
          throw new IllegalStateException(
              "getList got token FIELD_NAME while current state is " + currentState);
        }
        case NOT_AVAILABLE: {
          throw new IllegalStateException(
              "NOT_AVAILABLE token found in getList current state is " + currentState);
          // break;
        }
        case START_ARRAY: {
          // need to create array; fieldname may be there; will it case it not there
          arrayStarts(currentState);
          PdxListHelper tmp = currentPdxList.addListField();
          currentPdxList = tmp;
          currentState = states.LIST_FOUND;
          break;
        }
        case END_ARRAY: {
          // array is end
          arrayEnds(currentState);
          currentState = states.LIST_ENDS;
          if (currentPdxList.getParent() == null) {
            return currentPdxList;
          }
          currentPdxList = currentPdxList.getParent();
          break;
        }
        case VALUE_EMBEDDED_OBJECT: {
          throw new IllegalStateException("VALUE_EMBEDDED_OBJECT token found");
        }
        case VALUE_FALSE: {
          // write boolen
          boolFound(currentState);
          currentState = states.SCALAR_FOUND;
          currentPdxList.addBooleanField(jp.getBooleanValue());
          break;
        }
        case VALUE_NULL: {
          // write null
          nullFound(currentState);
          currentState = states.SCALAR_FOUND;
          currentPdxList.addNullField(null);
          break;
        }
        case VALUE_NUMBER_FLOAT: {
          // write double/float
          doubleFound(currentState);
          currentState = states.SCALAR_FOUND;
          // currentPdxList.addDoubleField(jp.getDoubleValue());
          setNumberField(jp, currentPdxList);
          break;
        }
        case VALUE_NUMBER_INT: {
          // write int
          intFound(currentState);
          currentState = states.SCALAR_FOUND;
          // currentPdxList.addIntField(jp.getIntValue());
          setNumberField(jp, currentPdxList);
          break;
        }
        case VALUE_STRING: {
          // write string
          stringFound(currentState);
          currentState = states.SCALAR_FOUND;
          currentPdxList.addStringField(jp.getText());
          currentFieldName = null;
          break;
        }
        case VALUE_TRUE: {
          // write bool
          boolFound(currentState);
          currentState = states.SCALAR_FOUND;
          currentPdxList.addBooleanField(jp.getBooleanValue());
          break;
        }
        default: {
          throw new IllegalStateException("Token not handled in getlist" + nt);
        }
      }
    }
  }

  private boolean objectStarts(states currentState) {
    switch (currentState) {
      case NONE:
      case FIELD_NAME:
      case OBJECT_ENDS:// in list
      case SCALAR_FOUND:// inlist
      case LIST_FOUND:
      case LIST_ENDS:
        return true;
      default:
        throw new IllegalStateException("Object start called when state is " + currentState);

    }
  }

  private boolean objectEnds(states currentState) {
    switch (currentState) {
      case OBJECT_START: // when empty object on field
      case SCALAR_FOUND:
      case LIST_ENDS:
      case OBJECT_ENDS:// inner object closes
        return true;
      default:
        throw new IllegalStateException("Object ends called when state is " + currentState);

    }
  }

  private boolean arrayStarts(states currentState) {
    switch (currentState) {
      case SCALAR_FOUND:
      case FIELD_NAME:
      case LIST_FOUND:
      case LIST_ENDS:
        return true;
      default:
        throw new IllegalStateException("Array start called when state is " + currentState);

    }
  }

  // enum states {NONE, OBJECT_START, FIELD_NAME, INNER_OBJECT_FOUND, SCALAR_FOUND, LIST_FOUND,
  // OBJECT_ENDS};
  private boolean arrayEnds(states currentState) {
    switch (currentState) {
      case FIELD_NAME:// when empty array
      case SCALAR_FOUND:
      case LIST_FOUND:
      case LIST_ENDS:
      case OBJECT_ENDS:
        return true;
      default:
        throw new IllegalStateException("Array ends called when state is " + currentState);

    }
  }

  private boolean stringFound(states currentState) {
    switch (currentState) {
      case FIELD_NAME:
      case SCALAR_FOUND:
      case LIST_FOUND:
      case LIST_ENDS:
      case OBJECT_ENDS:
        return true;
      default:
        throw new IllegalStateException("stringFound called when state is " + currentState);

    }
  }

  private boolean intFound(states currentState) {
    switch (currentState) {
      case FIELD_NAME:
      case SCALAR_FOUND:
      case LIST_FOUND:
      case LIST_ENDS:
      case OBJECT_ENDS:
        return true;
      default:
        throw new IllegalStateException("intFound called when state is " + currentState);

    }
  }

  private boolean boolFound(states currentState) {
    switch (currentState) {
      case FIELD_NAME:
      case SCALAR_FOUND:
      case LIST_FOUND:
      case LIST_ENDS:
      case OBJECT_ENDS:
        return true;
      default:
        throw new IllegalStateException("boolFound called when state is " + currentState);

    }
  }

  private boolean doubleFound(states currentState) {
    switch (currentState) {
      case FIELD_NAME:
      case SCALAR_FOUND:
      case LIST_FOUND:
      case LIST_ENDS:
      case OBJECT_ENDS:
        return true;
      default:
        throw new IllegalStateException("doubleFound called when state is " + currentState);

    }
  }

  private boolean fieldFound(states currentState) {
    switch (currentState) {
      case OBJECT_START:
      case SCALAR_FOUND:
      case LIST_FOUND:
      case LIST_ENDS:
      case OBJECT_ENDS:
        return true;
      default:
        throw new IllegalStateException("fieldFound called when state is " + currentState);

    }
  }

  private boolean nullFound(states currentState) {
    switch (currentState) {
      case FIELD_NAME:
      case SCALAR_FOUND:
      case LIST_FOUND:
      case LIST_ENDS:
      case OBJECT_ENDS:
        return true;
      default:
        throw new IllegalStateException("nullFound called when state is " + currentState);

    }
  }
}

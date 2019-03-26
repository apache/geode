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
package org.apache.geode.management.internal.cli.json;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A JSON serializer that has special handling for collections to limit the number of elements
 * written to the document. It also has special handling for PdxInstance and query Structs.
 */
public class QueryResultFormatter extends AbstractJSONFormatter {

  /**
   * map contains the named objects to be serialized
   */
  private final Map<String, List<Object>> map;

  /**
   * Create a formatter that will limit collection sizes to maxCollectionElements
   * and will limit object traversal to being the same but in depth.
   *
   * @param maxCollectionElements limit on collection elements and depth-first object traversal
   */
  public QueryResultFormatter(int maxCollectionElements) {
    this(maxCollectionElements, maxCollectionElements);
  }

  /**
   * Create a formatter that will limit collection sizes to maxCollectionElements
   *
   * @param maxCollectionElements limit on collection elements
   * @param serializationDepth when traversing objects, how deep should we go?
   */
  public QueryResultFormatter(int maxCollectionElements, int serializationDepth) {
    super(maxCollectionElements, serializationDepth, true);
    this.map = new LinkedHashMap<>();
  }

  /**
   * After instantiating a formatter add the objects you want to be formatted
   * using this method. Typically this will be add("result", queryResult)
   */
  public synchronized QueryResultFormatter add(String key, Object value) {
    List<Object> list = this.map.get(key);
    if (list != null) {
      list.add(value);
    } else {
      list = new ArrayList<>();
      if (value != null) {
        list.add(value);
      }
      this.map.put(key, list);
    }
    return this;
  }

  /* non-javadoc use Jackson to serialize added objects into JSON format */
  @Override
  public synchronized String toString() {
    Writer writer = new StringWriter();
    try {
      boolean addComma = false;
      writer.write('{');
      for (Map.Entry<String, List<Object>> entry : this.map.entrySet()) {
        if (addComma) {
          writer.write(',');
        }
        mapper.writerFor(entry.getKey().getClass()).writeValue(writer, entry.getKey());
        writer.write(':');
        writeList(writer, entry.getValue());
        addComma = true;
      }
      writer.write('}');

      return writer.toString();
    } catch (IOException exception) {
      new GfJsonException(exception).printStackTrace();
    } catch (GfJsonException e) {
      e.printStackTrace();
    }
    return null;
  }


  private Writer writeList(Writer writer, List<Object> values) throws GfJsonException {
    // for each object we clear out the serializedObjects recursion map so that
    // we don't immediately see "duplicate" entries
    serializedObjects.clear();
    try {
      boolean addComma = false;
      int length = values.size();
      writer.write('[');

      if (length == 0) {
        mapper.writeValue(writer, null);
      } else {
        for (int i = 0; i < length; i += 1) {
          if (addComma) {
            writer.write(',');
          }
          mapper.writerFor(values.get(i).getClass()).writeValue(writer, values.get(i));

          addComma = true;
        }
      }
      writer.write(']');
    } catch (IOException e) {
      throw new GfJsonException(e);
    }
    return writer;
  }


}

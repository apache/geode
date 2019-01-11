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
package org.apache.geode.experimental.driver;

import java.util.Objects;

import org.apache.geode.annotations.Experimental;

/**
 * JSONWrapper is a holder of a JSON document. Use it to wrap your JSON document to store in Geode
 * via the Region API. On the server the document will be stored in PDX serialized form, which is
 * query-capable.
 * <p>
 * For example,<br>
 * JSONWrapper wrapper = JSONWrapper.wrapJSON("{ \"name\", \"Xavier\", \"age\", \"35\"});<br>
 * region.put("key", wrapper);<br>
 * ...<br>
 * wrapper = region.get("key");<br>
 * String jsonDoc = wrapper.getJSON();<br>
 * </p>
 *
 */
@Experimental
public interface JSONWrapper extends Comparable<JSONWrapper> {

  static JSONWrapper wrapJSON(String jsonDocument) {
    if (jsonDocument == null) {
      throw new IllegalArgumentException("wrapped document may not be null");
    }
    return new JSONWrapperImpl(jsonDocument);
  }

  String getJSON();


  class JSONWrapperImpl implements JSONWrapper {


    protected final String jsonDocument;

    private JSONWrapperImpl(String jsonDocument) {
      this.jsonDocument = jsonDocument;
    }

    @Override
    public String getJSON() {
      return jsonDocument;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof JSONWrapper)) {
        return false;
      }
      JSONWrapper that = (JSONWrapper) o;
      return jsonDocument.equals(that.getJSON());
    }

    @Override
    public int hashCode() {
      return Objects.hash(jsonDocument);
    }

    @Override
    public int compareTo(JSONWrapper o) {
      return jsonDocument.compareTo(o.getJSON());
    }
  }
}

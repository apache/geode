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
package org.apache.geode.redis.internal.executor;

public enum ListQuery {

  LINDEX {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry WHERE key != 'head' AND key != 'tail' ORDER BY key asc LIMIT $1";
    }
  },
  LRANGE {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry WHERE key != 'head' AND key != 'tail' ORDER BY key asc LIMIT $1";
    }
  },
  LREMG {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry WHERE value = $1 AND key != 'head' AND key != 'tail' ORDER BY key asc LIMIT $2";
    }
  },
  LREML {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry WHERE value = $1 AND key != 'head' AND key != 'tail' ORDER BY key desc LIMIT $2";
    }
  },
  LREME {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry WHERE value = $1 ORDER BY key asc";
    }
  },
  LSET {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT * FROM " + fullpath
          + ".keySet key WHERE key != 'head' AND key != 'tail' ORDER BY key asc LIMIT $1";
    }
  },
  LTRIM {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT * FROM " + fullpath
          + ".keySet key WHERE key != 'head' AND key != 'tail' ORDER BY key asc LIMIT $1";
    }
  };

  public abstract String getQueryString(String fullpath);

}

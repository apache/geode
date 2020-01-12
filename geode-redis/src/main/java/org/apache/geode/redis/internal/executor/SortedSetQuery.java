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

public enum SortedSetQuery {

  ZCOUNTNINFI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".values value WHERE value.score <= $1";
    }
  },
  ZCOUNTNINF {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".values value WHERE value.score < $1";
    }
  },
  ZCOUNTPINFI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".values value WHERE value.score >= $1";
    }
  },
  ZCOUNTPINF {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".values value WHERE value.score > $1";
    }
  },
  ZCOUNTSTI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath
          + ".values value WHERE value.score >= $1 AND value.score < $2";
    }
  },
  ZCOUNTSTISI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath
          + ".values value WHERE value.score >= $1 AND value.score <= $2";
    }
  },
  ZCOUNTSI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath
          + ".values value WHERE value.score > $1 AND value.score <= $2";
    }
  },
  ZCOUNT {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath
          + ".values value WHERE value.score > $1 AND value.score < $2";
    }
  },
  ZLEXCOUNTNINFI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".keySet key WHERE key.compareTo($1) <= 0";
    }
  },
  ZLEXCOUNTNINF {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".keySet key WHERE key.compareTo($1) < 0";
    }
  },
  ZLEXCOUNTPINFI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".keySet key WHERE key.compareTo($1) >= 0";
    }
  },
  ZLEXCOUNTPINF {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".keySet key WHERE key.compareTo($1) > 0";
    }
  },
  ZLEXCOUNTSTI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath
          + ".keySet key WHERE key.compareTo($1) >= 0 AND key.compareTo($2) < 0";
    }
  },
  ZLEXCOUNTSTISI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath
          + ".keySet key WHERE key.compareTo($1) >= 0 AND key.compareTo($2) <= 0";
    }
  },
  ZLEXCOUNTSI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath
          + ".keySet key WHERE key.compareTo($1) > 0 AND key.compareTo($2) <= 0";
    }
  },
  ZLEXCOUNT {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath
          + ".keySet key WHERE key.compareTo($1) > 0 AND key.compareTo($2) < 0";
    }
  },
  ZRANGEBYLEXNINFI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT * FROM " + fullpath
          + ".keySet key WHERE key.compareTo($1) <= 0 ORDER BY key asc LIMIT $2";
    }
  },
  ZRANGEBYLEXNINF {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT * FROM " + fullpath
          + ".keySet key WHERE key.compareTo($1) < 0 ORDER BY key asc LIMIT $2";
    }
  },
  ZRANGEBYLEXPINFI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT * FROM " + fullpath
          + ".keySet key WHERE key.compareTo($1) >= 0 ORDER BY key asc LIMIT $2";
    }
  },
  ZRANGEBYLEXPINF {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT * FROM " + fullpath
          + ".keySet key WHERE key.compareTo($1) > 0 ORDER BY key asc LIMIT $2";
    }
  },
  ZRANGEBYLEXSTI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT * FROM " + fullpath
          + ".keySet key WHERE key.compareTo($1) >= 0 AND key.compareTo($2) < 0 ORDER BY key asc LIMIT $3";
    }
  },
  ZRANGEBYLEXSTISI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT * FROM " + fullpath
          + ".keySet key WHERE key.compareTo($1) >= 0 AND key.compareTo($2) <= 0 ORDER BY key asc LIMIT $3";
    }
  },
  ZRANGEBYLEXSI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT * FROM " + fullpath
          + ".keySet key WHERE key.compareTo($1) > 0 AND key.compareTo($2) <= 0 ORDER BY key asc LIMIT $3";
    }
  },
  ZRANGEBYLEX {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT * FROM " + fullpath
          + ".keySet key WHERE key.compareTo($1) > 0 AND key.compareTo($2) < 0 ORDER BY key asc LIMIT $3";
    }
  },
  ZREMRANGEBYRANK {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry ORDER BY entry.value asc LIMIT $1";
    }
  },
  ZRBSNINFI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry WHERE value.score <= $1 ORDER BY entry.value asc LIMIT $2";
    }
  },
  ZRBSNINF {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry WHERE entry.value.score < $1 ORDER BY entry.value asc LIMIT $2";
    }
  },
  ZRBSPINFI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry WHERE entry.value.score >= $1 ORDER BY entry.value asc LIMIT $2";
    }
  },
  ZRBSPINF {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry WHERE entry.value.score > $1 ORDER BY entry.value asc LIMIT $2";
    }
  },
  ZRBSSTISI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry WHERE entry.value.score >= $1 AND entry.value.score <= $2 ORDER BY entry.value asc LIMIT $3";
    }
  },
  ZRBSSTI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry WHERE entry.value.score >= $1 AND entry.value.score < $2 ORDER BY entry.value asc LIMIT $3";
    }
  },
  ZRBSSI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry WHERE entry.value.score > $1 AND entry.value.score <= $2 ORDER BY entry.value asc LIMIT $3";
    }
  },
  ZRBS {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry WHERE entry.value.score > $1 AND entry.value.score < $2 ORDER BY entry.value asc LIMIT $3";
    }
  },
  ZREVRBSNINFI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry WHERE value <= $1 ORDER BY entry.value desc, entry.key desc LIMIT $2";
    }
  },
  ZREVRBSNINF {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry WHERE entry.value.score < $1 ORDER BY entry.value desc, entry.key desc LIMIT $2";
    }
  },
  ZREVRBSPINFI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry WHERE entry.value.score >= $1 ORDER BY entry.value desc, entry.key desc LIMIT $2";
    }
  },
  ZREVRBSPINF {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry WHERE entry.value.score > $1 ORDER BY entry.value desc, entry.key desc LIMIT $2";
    }
  },
  ZREVRBSSTISI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry WHERE entry.value.score >= $1 AND entry.value.score <= $2 ORDER BY entry.value desc, entry.key desc LIMIT $3";
    }
  },
  ZREVRBSSTI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry WHERE entry.value.score >= $1 AND entry.value.score < $2 ORDER BY entry.value desc, entry.key desc LIMIT $3";
    }
  },
  ZREVRBSSI {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry WHERE entry.value.score > $1 AND entry.value.score <= $2 ORDER BY entry.value desc, entry.key desc LIMIT $3";
    }
  },
  ZREVRBS {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry WHERE entry.value.score > $1 AND entry.value.score < $2 ORDER BY entry.value desc, entry.key desc LIMIT $3";
    }
  },
  ZREVRANGE {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry ORDER BY entry.value asc, entry.key asc LIMIT $1";
    }
  },
  ZRANGE {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entrySet entry ORDER BY entry.value desc, entry.key desc LIMIT $1";
    }
  },
  GEORADIUS {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath
          + ".entries entry WHERE entry.value.toString LIKE $1 ORDER BY entry.value asc";
    }
  },
  ZRANK {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath
          + ".entrySet entry WHERE entry.value < $1 OR (entry.value = $2 AND entry.key.compareTo($3) < 0)";
    }
  },
  ZREVRANK {
    @Override
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath
          + ".entrySet entry WHERE entry.value > $1 OR (entry.value = $2 AND entry.key.compareTo($3) > 0)";
    }
  };

  public abstract String getQueryString(String fullpath);
}

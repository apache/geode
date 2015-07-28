package com.gemstone.gemfire.internal.redis.executor;

public enum SortedSetQuery {

  ZCOUNTNINFI {
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".values value WHERE value.score <= $1";
    }
  }, ZCOUNTNINF {
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".values value WHERE value.score < $1";
    }
  }, ZCOUNTPINFI {
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".values value WHERE value.score >= $1";
    }
  }, ZCOUNTPINF {
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".values value WHERE value.score > $1";
    }
  }, ZCOUNTSTI {
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".values value WHERE value.score >= $1 AND value.score < $2";
    }
  }, ZCOUNTSTISI {
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".values value WHERE value.score >= $1 AND value.score <= $2";
    }
  }, ZCOUNTSI {
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".values value WHERE value.score > $1 AND value.score <= $2";
    }
  }, ZCOUNT {
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".values value WHERE value.score > $1 AND value.score < $2";
    }
  }, ZLEXCOUNTNINFI {
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".keySet key WHERE key.compareTo($1) <= 0";
    }
  }, ZLEXCOUNTNINF {
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".keySet key WHERE key.compareTo($1) < 0";
    }
  }, ZLEXCOUNTPINFI {
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".keySet key WHERE key.compareTo($1) >= 0";
    }
  }, ZLEXCOUNTPINF {
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".keySet key WHERE key.compareTo($1) > 0";
    }
  }, ZLEXCOUNTSTI {
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".keySet key WHERE key.compareTo($1) >= 0 AND key.compareTo($2) < 0";
    }
  }, ZLEXCOUNTSTISI {
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".keySet key WHERE key.compareTo($1) >= 0 AND key.compareTo($2) <= 0";
    }
  }, ZLEXCOUNTSI {
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".keySet key WHERE key.compareTo($1) > 0 AND key.compareTo($2) <= 0";
    }
  }, ZLEXCOUNT {
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".keySet key WHERE key.compareTo($1) > 0 AND key.compareTo($2) < 0";
    }
  }, ZRANGEBYLEXNINFI {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT * FROM " + fullpath + ".keySet key WHERE key.compareTo($1) <= 0 ORDER BY key asc LIMIT $2";
    }
  }, ZRANGEBYLEXNINF {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT * FROM " + fullpath + ".keySet key WHERE key.compareTo($1) < 0 ORDER BY key asc LIMIT $2";
    }
  }, ZRANGEBYLEXPINFI {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT * FROM " + fullpath + ".keySet key WHERE key.compareTo($1) >= 0 ORDER BY key asc LIMIT $2";
    }
  }, ZRANGEBYLEXPINF {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT * FROM " + fullpath + ".keySet key WHERE key.compareTo($1) > 0 ORDER BY key asc LIMIT $2";
    }
  }, ZRANGEBYLEXSTI {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT * FROM " + fullpath + ".keySet key WHERE key.compareTo($1) >= 0 AND key.compareTo($2) < 0 ORDER BY key asc LIMIT $3";
    }
  }, ZRANGEBYLEXSTISI {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT * FROM " + fullpath + ".keySet key WHERE key.compareTo($1) >= 0 AND key.compareTo($2) <= 0 ORDER BY key asc LIMIT $3";
    }
  }, ZRANGEBYLEXSI {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT * FROM " + fullpath + ".keySet key WHERE key.compareTo($1) > 0 AND key.compareTo($2) <= 0 ORDER BY key asc LIMIT $3";
    }
  }, ZRANGEBYLEX {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT * FROM " + fullpath + ".keySet key WHERE key.compareTo($1) > 0 AND key.compareTo($2) < 0 ORDER BY key asc LIMIT $3";
    }
  }, ZREMRANGEBYRANK {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry ORDER BY entry.value asc LIMIT $1";
    }
  }, ZRBSNINFI {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry WHERE value.score <= $1 ORDER BY entry.value asc LIMIT $2";
    }
  }, ZRBSNINF {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry WHERE entry.value.score < $1 ORDER BY entry.value asc LIMIT $2";
    }
  }, ZRBSPINFI {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry WHERE entry.value.score >= $1 ORDER BY entry.value asc LIMIT $2";
    }
  }, ZRBSPINF {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry WHERE entry.value.score > $1 ORDER BY entry.value asc LIMIT $2";
    }
  }, ZRBSSTISI {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry WHERE entry.value.score >= $1 AND entry.value.score <= $2 ORDER BY entry.value asc LIMIT $3";
    }
  }, ZRBSSTI {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry WHERE entry.value.score >= $1 AND entry.value.score < $2 ORDER BY entry.value asc LIMIT $3";
    }
  }, ZRBSSI {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry WHERE entry.value.score > $1 AND entry.value.score <= $2 ORDER BY entry.value asc LIMIT $3";
    }
  }, ZRBS {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry WHERE entry.value.score > $1 AND entry.value.score < $2 ORDER BY entry.value asc LIMIT $3";
    }
  }, ZREVRBSNINFI {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry WHERE value <= $1 ORDER BY entry.value desc, entry.key desc LIMIT $2";
    }
  }, ZREVRBSNINF {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry WHERE entry.value.score < $1 ORDER BY entry.value desc, entry.key desc LIMIT $2";
    }
  }, ZREVRBSPINFI {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry WHERE entry.value.score >= $1 ORDER BY entry.value desc, entry.key desc LIMIT $2";
    }
  }, ZREVRBSPINF {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry WHERE entry.value.score > $1 ORDER BY entry.value desc, entry.key desc LIMIT $2";
    }
  }, ZREVRBSSTISI {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry WHERE entry.value.score >= $1 AND entry.value.score <= $2 ORDER BY entry.value desc, entry.key desc LIMIT $3";
    }
  }, ZREVRBSSTI {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry WHERE entry.value.score >= $1 AND entry.value.score < $2 ORDER BY entry.value desc, entry.key desc LIMIT $3";
    }
  }, ZREVRBSSI {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry WHERE entry.value.score > $1 AND entry.value.score <= $2 ORDER BY entry.value desc, entry.key desc LIMIT $3";
    }
  }, ZREVRBS {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry WHERE entry.value.score > $1 AND entry.value.score < $2 ORDER BY entry.value desc, entry.key desc LIMIT $3";
    }
  }, ZREVRANGE {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry ORDER BY entry.value asc, entry.key asc LIMIT $1";
    }
  }, ZRANGE {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry ORDER BY entry.value desc, entry.key desc LIMIT $1";
    }
  }, ZRANK {
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".entrySet entry WHERE entry.value < $1 OR (entry.value = $2 AND entry.key.compareTo($3) < 0)";
    }
  }, ZREVRANK {
    public String getQueryString(String fullpath) {
      return "SELECT COUNT(*) FROM " + fullpath + ".entrySet entry WHERE entry.value > $1 OR (entry.value = $2 AND entry.key.compareTo($3) > 0)";
    }
  };

  public abstract String getQueryString(String fullpath);
}

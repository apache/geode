package com.gemstone.gemfire.internal.redis.executor;

public enum ListQuery {

  LINDEX {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry WHERE key != 'head' AND key != 'tail' ORDER BY key asc LIMIT $1";
    }
  }, LRANGE {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry WHERE key != 'head' AND key != 'tail' ORDER BY key asc LIMIT $1";
    }
  }, LREMG {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry WHERE value = $1 AND key != 'head' AND key != 'tail' ORDER BY key asc LIMIT $2";
    }
  }, LREML {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry WHERE value = $1 AND key != 'head' AND key != 'tail' ORDER BY key desc LIMIT $2";
    }
  }, LREME {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT entry.key, entry.value FROM " + fullpath + ".entrySet entry WHERE value = $1 ORDER BY key asc";
    }
  }, LSET {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT * FROM " + fullpath + ".keySet key WHERE key != 'head' AND key != 'tail' ORDER BY key asc LIMIT $1";
    }
  }, LTRIM {
    public String getQueryString(String fullpath) {
      return "SELECT DISTINCT * FROM " + fullpath + ".keySet key WHERE key != 'head' AND key != 'tail' ORDER BY key asc LIMIT $1";
    }
  };

  public abstract String getQueryString(String fullpath);

}

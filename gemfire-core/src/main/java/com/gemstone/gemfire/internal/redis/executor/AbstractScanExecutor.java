package com.gemstone.gemfire.internal.redis.executor;

import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import com.gemstone.gemfire.internal.redis.org.apache.hadoop.fs.GlobPattern;


public abstract class AbstractScanExecutor extends AbstractExecutor {

  protected final String ERROR_CURSOR = "Invalid cursor";

  protected final String ERROR_COUNT = "Count must be numeric and positive";

  protected final String ERROR_INVALID_CURSOR = "Cursor is invalid, dataset may have been altered if this is cursor from a previous scan";

  protected final int DEFUALT_COUNT = 10;

  protected abstract List<?> getIteration(Collection<?> list, Pattern matchPatter, int count, int cursor);

  /**
   * @param pattern A glob pattern.
   * @return A regex pattern to recognize the given glob pattern.
   */
  protected final Pattern convertGlobToRegex(String pattern) {
    if (pattern == null)
      return null;
    return GlobPattern.compile(pattern);
  }
}

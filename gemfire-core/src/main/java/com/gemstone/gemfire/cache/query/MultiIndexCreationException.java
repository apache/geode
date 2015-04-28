package com.gemstone.gemfire.cache.query;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.query.internal.DefaultQueryService;

/**
 * Consists a map of index names and Exceptions thrown during index creation
 * using {@link QueryService#createDefinedIndexes()}.
 * An {@link Index} could throw one of the following exceptions:
 * <ul>
 *   <li>{@link IndexNameConflictException}</li>
 *   <li>{@link IndexExistsException}</li>
 *   <li>{@link IndexInvalidException}</li>
 *   <li>{@link UnsupportedOperationException}</li>
 * </ul>
 * @since 8.1
 * 
 */
public class MultiIndexCreationException extends Exception {
  private static final long serialVersionUID = 6312081720315894780L;
  /**
   * Map of indexName -> Exception
   */
  private final Map<String, Exception> exceptionsMap;

  /**
   * Creates an {@link MultiIndexCreationException}
   * 
   */
  public MultiIndexCreationException(HashMap<String, Exception> exceptionMap) {
    super();
    this.exceptionsMap = exceptionMap;
  }

  /**
   * Returns a map of index names and Exceptions
   * 
   * @return a map of index names and Exceptions
   */
  public Map<String, Exception> getExceptionsMap() {
    return exceptionsMap;
  }

  /**
   * Returns a set of names for the indexes that failed to create
   * 
   * @return set of failed index names
   */
  public Set<String> getFailedIndexNames() {
    return exceptionsMap.keySet();
  }
  
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    for(Map.Entry<String, Exception> entry : this.exceptionsMap.entrySet()) {
      sb.append("Creation of index: ").append(entry.getKey()).append(" failed due to: ").append(entry.getValue()).append(", ");
    }
    sb.delete(sb.length() - 2, sb.length());
    return sb.toString();
  }
  
}

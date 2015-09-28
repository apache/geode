package com.gemstone.gemfire.cache.lucene;


/**
 * <p>
 * Abstract data structure for one item in query result.
 * 
 * @author Xiaojian Zhou
 * @since 8.5
 */
public interface LuceneResultStruct<K, V> {
  /**
   * Return the value associated with the given field name
   *
   * @param fieldName the String name of the field
   * @return the value associated with the specified field
   * @throws IllegalArgumentException If this struct does not have a field named fieldName
   */
  public Object getProjectedField(String fieldName);
  
  /**
   * Return key of the entry
   *
   * @return key
   * @throws IllegalArgumentException If this struct does not contain key
   */
  public K getKey();
  
  /**
   * Return value of the entry
   *
   * @return value the whole domain object
   * @throws IllegalArgumentException If this struct does not contain value
   */
  public V getValue();
  
  /**
   * Return score of the query 
   *
   * @return score
   * @throws IllegalArgumentException If this struct does not contain score
   */
  public float getScore();
}


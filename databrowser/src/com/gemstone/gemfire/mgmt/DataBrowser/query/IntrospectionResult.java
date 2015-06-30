/*========================================================================= 
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query;

/**
 * This interface provides meta-data about the result of the query result by
 * performing runtime introspection on the same.
 * 
 * @author Hrishi
 **/
public interface IntrospectionResult {

  public static final int UNDEFINED = -1;
  public static final int UNKNOWN_TYPE_COLUMN = 0;
  public static final int PRIMITIVE_TYPE_COLUMN = 1;
  public static final int COMPOSITE_TYPE_COLUMN = 2;
  public static final int COLLECTION_TYPE_COLUMN = 4;
  public static final int MAP_TYPE_COLUMN = 8;
  public static final int PDX_TYPE_COLUMN = 16; //PDX Object
  public static final int PDX_OBJECT_TYPE_COLUMN = 18; //nested PDX Object
  
  public static final int PRIMITIVE_TYPE_RESULT = 10;
  public static final int COMPOSITE_TYPE_RESULT = 11;
  public static final int STRUCT_TYPE_RESULT = 12;
  public static final int COLLECTION_TYPE_RESULT = 13;
  public static final int MAP_TYPE_RESULT = 14;
  public static final int PDX_TYPE_RESULT = 20;
  
  
  public static final String CONST_COLUMN_NAME = "Result";
  
  public int getResultType();
  
  /**
   * This method returns the total number of fields (columns) in this result.
   **/
  public int getColumnCount();

  /**
   * This method returns the name of the field (column) at the given index.
   **/
  public String getColumnName(int index) throws ColumnNotFoundException;
  
  /**
   * This method returns the index of the column by specified name.
   **/
  public int getColumnIndex(String name) throws ColumnNotFoundException;
  
  /**
   * This method returns the class name of the field (column) at the given
   * index.
   **/
  public Class getColumnClass(int index) throws ColumnNotFoundException;
  
  public Class getColumnClass(Object tuple, int index) throws ColumnNotFoundException;

  /**
   * This method returns if the given field (column) type is composite.
   **/
  public int getColumnType(int index) throws ColumnNotFoundException;
  
  public int getColumnType(Object tuple, int index) throws ColumnNotFoundException;

  /**
   * This method returns the value of given field (column) for a given object
   * (tuple).
   **/
  public Object getColumnValue(Object tuple, int index)
      throws ColumnNotFoundException, ColumnValueNotAvailableException;

  /**
   * This method returns a Java type associated with this introspection result.
   * 
   * @return The Java type associated with this meta-data.
   */
  public Class getJavaType();

  /**
   * This method returns the name of a Java type associated with this
   * introspection result. This is in general same as calling
   * getJavaType().getName() except when the query contains PdxInstance objects.
   * For PdxInstance objects, this information is taken from PdxType. 
   * 
   * @return The name of a Java type associated with this meta-data.
   */
  public String getJavaTypeName();

  /**
   * Whether the given Object is compatible with this IntrospectionResult.
   * Currently, for all IntrospectionResult implementations except
   * PdxIntrospectionResult, this is same as checking whether
   * {@link IntrospectionResult#getJavaType()} is same as data.getClass()
   * 
   * @param data
   *          Object to check for compatibility
   * @return true if type of the given Object is same as the type for which this
   *         IntrospectionResult is created, false otherwise
   */
  public boolean isCompatible(Object data);

}

/*=========================================================================
 * (c) Copyright 2002-2007, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200,  Beaverton, OR 97006
 * All Rights Reserved.
 *========================================================================
 */

package com.gemstone.gemfire.tools.databrowser.data;

/**
 *  Interface for configurable objects that encode an <code>int</code>
 *  key ("index").  An object type can be configured using its
 *  corresponding parameter class, if one exists.
 */

public interface ConfigurableObject {

  /**
   *  Returns a new instance of the object encoded with the index.
   *
   *  @throws ObjectCreationException
   *          An error occured when creating the object.  See the error
   *          message for more details.
   */
  public void init( int index );

  /**
   *  Returns the index encoded in the object.
   *
   *  @throws ObjectAccessException
   *          An error occured when accessing the object.  See the error
   *          message for more details.
   */
  public int getIndex();

  /**
   *  Validates whether the index is encoded in the object, if this
   *  applies, and performs other validation checks as needed.
   *
   *  @throws ObjectAccessException
   *          An error occured when accessing the object.  See the error
   *          message for more details.
   *  @throws ObjectValidationException
   *          The object failed validation.  See the error message for more
   *          details.
   */
  public void validate( int index );
}

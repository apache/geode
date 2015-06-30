/*=========================================================================
 * (c) Copyright 2002-2007, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200,  Beaverton, OR 97006
 * All Rights Reserved.
 *========================================================================
 */

package com.gemstone.gemfire.tools.databrowser.data;

import hydra.*;

/**
 *
 *  Object creation helper used to create configurable objects encoded with
 *  an index.  Supported objects must either implement {@link ConfigurableObject}
 *  or be special-cased.  They can optionally rely on a parameter class for
 *  configuration.  See the various datatypes in this directory for examples.
 *  <p>
 *  Usage example:
 *  <blockquote>
 *  <pre>
 *     // In a hydra configuration file...
 *     objects.SizedStringPrms-size = 1024; // create strings of size 1K
 *
 *     // In one vm...
 *     Object name = ObjectHelper.createName( i );
 *     Object value = ObjectHelper.createObject( "objects.SizedString", i );
 *     cache.put( name, value );
 *
 *     // In a different vm...
 *     Object name = ObjectHelper.createName( i );
 *     Object value = cache.get( name );
 *     ObjectHelper.validate( i, value );
 *  </pre>
 *  </blockquote>
 */

public class ObjectHelper {
  
  //----------------------------------------------------------------------------
  // Helper methods
  //----------------------------------------------------------------------------

  /**
   * Generates an object name of type java.lang.String encoding the supplied
   * index.
   */
  public static Object createName( int index ) {
    return String.valueOf( index );
  }



  /**
   *  Generates an object of the specified type encoding the specified index,
   *  using the settings in the corresponding parameter class for the type, if
   *  any.  Invokes {@link ConfigurableObject#init} on the object, if it
   *  applies, otherwise handles specially supported types.
   *
   *  @throws HydraConfigException
   *          The class is not a supported type or could not be found.
   *  @throws ObjectCreationException
   *          An error occured when creating the object.  See the error
   *          message for more details.
   */
  public static Object createObject( String classname, int index ) {
      try {
	Class cls = Class.forName( classname );
	ConfigurableObject obj = (ConfigurableObject) cls.newInstance();
	obj.init( index );
	return obj;
      } catch( ClassCastException e ) {
	throw new HydraConfigException( classname + " is neither a specially supported type nor a ConfigurableObject", e );
      } catch( ClassNotFoundException e ) {
	throw new HydraConfigException( "Unable to find class for type " + classname, e );
      } catch( IllegalAccessException e ) {
	throw new ObjectCreationException( "Unable to instantiate object of type " + classname, e );
      } catch( InstantiationException e ) {
	throw new ObjectCreationException( "Unable to instantiate object of type " + classname, e );
      }
    }
}



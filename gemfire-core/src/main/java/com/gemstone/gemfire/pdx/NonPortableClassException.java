package com.gemstone.gemfire.pdx;

/**
 * Thrown if "check-portability" is enabled and an attempt is made to
 * pdx serialize a class that is not portable to non-java platforms.
 * 
 * @author darrel
 * @since 6.6.2
 */
public class NonPortableClassException extends PdxSerializationException {

  private static final long serialVersionUID = -743743189068362837L;

  public NonPortableClassException(String message) {
    super(message);
  }

}

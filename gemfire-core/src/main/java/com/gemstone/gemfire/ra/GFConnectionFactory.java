package com.gemstone.gemfire.ra;

import java.io.Serializable;

import javax.resource.Referenceable;
import javax.resource.ResourceException;



public interface GFConnectionFactory extends Referenceable, Serializable
{
  public GFConnection getConnection() throws ResourceException;
}

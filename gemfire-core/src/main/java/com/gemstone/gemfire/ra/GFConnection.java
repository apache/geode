package com.gemstone.gemfire.ra;

import java.io.Serializable;

import javax.resource.Referenceable;
import javax.resource.ResourceException;

public interface GFConnection extends Serializable, Referenceable
{
  public void close() throws ResourceException;
}

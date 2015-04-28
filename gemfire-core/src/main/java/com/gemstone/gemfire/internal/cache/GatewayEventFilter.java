package com.gemstone.gemfire.internal.cache;
/**
 * 
 * @author Asif
 *
 */
public interface GatewayEventFilter
{
  boolean enqueueEvent(EntryEventImpl event);

}

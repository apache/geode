package com.gemstone.gemfire.internal;

/**
 * A marker interface to identify internal objects that are stored in the Cache at the same place that user objects
 * would typically be stored. For example, registering functions for internal use. When determining what to do, or how
 * to display these objects, other classes may use this interface as a filter to eliminate internal objects.
 * 
 * @author David Hoots
 * @since 7.0
 * 
 */
public interface InternalEntity {

}

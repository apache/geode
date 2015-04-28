package com.gemstone.gemfire.management.internal.security;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface ResourceOperation {
  
  Resource resource();
  String label() default ResourceConstants.DEFAULT_LABEL;
  String operation() default ResourceConstants.LIST_DS;

}

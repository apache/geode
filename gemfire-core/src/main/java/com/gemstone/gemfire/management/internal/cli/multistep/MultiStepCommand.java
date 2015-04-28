package com.gemstone.gemfire.management.internal.cli.multistep;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/***
 * Just a marker interface to identify interactive command from other regular commands
 * @author tushark
 *
 */

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface MultiStepCommand {

}

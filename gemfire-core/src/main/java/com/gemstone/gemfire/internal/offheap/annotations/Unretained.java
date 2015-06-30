package com.gemstone.gemfire.internal.offheap.annotations;

import static com.gemstone.gemfire.internal.offheap.annotations.OffHeapIdentifier.*;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is used to mark off-heap values that have not have their reference counts altered (retained or released):
 * <ul>
 * <li>This annotation is used on a method to indicate the return value is an off-heap reference that has not had retain called on it.  The caller should call retain to ensure that the reference remains viable.</li>
 * <li>Use this annotation on a parameter to indicate that the parameter may be an off-heap value but it is neither retained nor released by the callee (or further sub-callees).</li>
 * <li>Use this annotation on a local variable to indicate that it references an off-heap value but it is neither retained nor released.</li>
 * <li>Use this annotation on a field member to indicate that the field is an off-heap reference that is neither retained or released by containing object.</li>
 * <li>Use this annotation on a constructor that does not retain off-heap field members.</li>
 * </ul>
 * 
 * One or more OffHeapIdentifiers may be supplied if the developer wishes to link this annotation with other
 * off-heap annotations.
 * 
 * @author rholmes
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.METHOD,
              ElementType.PARAMETER,
              ElementType.LOCAL_VARIABLE,
              ElementType.FIELD,
              ElementType.CONSTRUCTOR})
@Documented
public @interface Unretained {
  OffHeapIdentifier[] value() default DEFAULT;
}

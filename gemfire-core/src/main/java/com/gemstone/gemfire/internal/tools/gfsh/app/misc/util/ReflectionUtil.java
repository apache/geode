package com.gemstone.gemfire.internal.tools.gfsh.app.misc.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;



public class ReflectionUtil
{
	/**
     * Returns the values of all the public members in the specified object.
     * The returned value has the format: member1 = value1, member2 = value2, ...
     */
	public static String toStringPublicMembers(Object object)
    {
        if (object == null) {
            return null;
        }

        String retval = "";
        Class cls = object.getClass();
        Field fields[] = cls.getFields();
        String name;
        Object value;
        try {
            for (int i = 0; i < fields.length; i++) {
                name = fields[i].getName();
                value = fields[i].get(object);
                if (value instanceof byte[]) {
                    value = new String((byte[])value);
                } else if (value instanceof Byte) {
                    value = ((Byte)value).toString();
                }
                retval += name + " = " + value + ", ";
            }
        } catch (IllegalAccessException ex) {
            ex.printStackTrace();
        }

        // Remove the trailing ", ".
        if (retval.length() > 0) {
            retval = retval.substring(0, retval.length() - 2);
        }
        return retval;
    }

    public static String toStringGetters(Object object)
    {
        if (object == null) {
            return null;
        }

        String retval = "";
        Class cls = object.getClass();
        Method methods[] = cls.getMethods();
        Method method;
        Class retType;
        String name;
        Object value;
        try {
            for (int i = 0; i < methods.length; i++) {
                method = methods[i];
                name = method.getName();
                if (name.length() <= 3 || name.startsWith("get") == false) {
                    continue;
                }
                if ((method.getModifiers() & Modifier.STATIC) > 0) {
                	continue;
                }
                if (name.equals("getClass")) {
                	continue;
                }
                retType = method.getReturnType();
                if (retType == Void.TYPE) {
                    continue;
                } 
                try {
                    value = method.invoke(object, (Object[])null);
                    if (value instanceof byte[]) {
                        value = new String((byte[])value);
                    } else if (value instanceof Byte) {
                        value = ((Byte)value).toString();
                    }
                    retval += name.substring(3) + " = " + value + ", ";
                } catch (Exception ex) {
                }
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        // Remove the trailing ", ".
        if (retval.length() > 0) {
            retval = retval.substring(0, retval.length() - 2);
        }
        return retval;
    }
    
    public static String toStringGettersAnd(Object object)
    {
        if (object == null) {
            return null;
        }

        String retval = "";
        Class cls = object.getClass();
        Method methods[] = cls.getMethods();
        Method method;
        Class retType;
        String name;
        Object value;
        try {
            for (int i = 0; i < methods.length; i++) {
                method = methods[i];
                name = method.getName();
                if (name.length() <= 3 || name.startsWith("get") == false) {
                    continue;
                }
                if ((method.getModifiers() & Modifier.STATIC) > 0) {
                	continue;
                }
                if (name.equals("getClass")) {
                	continue;
                }
                retType = method.getReturnType();
                if (retType == Void.TYPE) {
                    continue;
                } 
                try {
                    value = method.invoke(object, (Object[])null);
                    if (value instanceof byte[]) {
                        value = new String((byte[])value);
                    } else if (value instanceof Byte) {
                        value = ((Byte)value).toString();
                    }
                    if (value instanceof String) {
                    	retval += name.substring(3) + "='" + value + "' and ";
                    } else {
                    	retval += name.substring(3) + "=" + value + " and ";
                    }
                } catch (Exception ex) {
                }
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        // Remove the trailing " and ".
        if (retval.length() > 0) {
            retval = retval.substring(0, retval.length() - 5);
        }
        return retval;
    }
    
    public static String toStringSetters(Object object)
    {
        if (object == null) {
            return null;
        }

        String retval = "";
        Class cls = object.getClass();
        Method methods[] = cls.getMethods();
        Method method;
        String name;
        try {
            for (int i = 0; i < methods.length; i++) {
                method = methods[i];
                name = method.getName();
                if (name.length() <= 3 || name.startsWith("set") == false) {
                    continue;
                }
                retval += name + ", ";
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        // Remove the trailing ", ".
        if (retval.length() > 0) {
            retval = retval.substring(0, retval.length() - 2);
        }
        return retval;
    }
    
    public static Method[] getAllSetters(Class cls)
    {
        Map map = getAllSettersMap(cls);
        if (map == null) {
        	return null;
        }
        Collection col = map.values();
        return (Method[])col.toArray(new Method[0]);
    }
    
    public static Map getAllSettersMap(Class cls)
    {
        if (cls == null) {
            return null;
        }
        Method methods[] = cls.getMethods();
        TreeMap map = new TreeMap();
        Method method;
        String name;
        try {
            for (int i = 0; i < methods.length; i++) {
                method = methods[i];
                name = method.getName();
                if (name.length() <= 3 || name.startsWith("set") == false) {
                    continue;
                }
               if (method.getParameterTypes().length == 1) {
            	   map.put(method.getName(), method);
               }
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return map;
    }
    
    
    public static Method[] getAllGetters(Class cls)
    {
		Map map = getAllGettersMap(cls);
		if (map == null) {
			return null;
		}
		Collection col = map.values();
        return (Method[])col.toArray(new Method[0]);
    }
    
    /**
     * Returns Map<String, Method>
     * @param cls
     */
    public static Map getAllGettersMap(Class cls)
    {
        if (cls == null) {
            return null;
        }
        Method methods[] = cls.getMethods();
        Method method;
        Class retType;
        String name;
        TreeMap map = new TreeMap();
        try {
            for (int i = 0; i < methods.length; i++) {
                method = methods[i];
                name = method.getName();
                if (name.length() <= 3 || name.startsWith("get") == false) {
                    continue;
                }
                if ((method.getModifiers() & Modifier.STATIC) > 0) {
                	continue;
                }
                if (name.equals("getClass")) {
                	continue;
                }
                retType = method.getReturnType();
                if (retType == Void.TYPE) {
                    continue;
                } 
               if (method.getParameterTypes().length == 0) {
            	   map.put(name, method);
               }
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return map;
    }
    
    /**
     * 
     * @param cls
     * @return setter map
     * @deprecated
     *///FIXME: java docs @return, check if this is used?
    public static Map getSetterMap(Class cls)
    {
    	TreeMap map = new TreeMap();
    	Method methods[] = getAllSetters(cls);
    	String memberName;
    	for (int i = 0; i < methods.length; i++) {
    		memberName = methods[i].getName();
    		memberName = memberName.substring(3);
    		map.put(memberName, methods[i]);
    	}
    	return map;
    }
}

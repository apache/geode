package com.gemstone.gemfire.internal.tools.gfsh.app.util;

import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.gemstone.gemfire.internal.tools.gfsh.app.misc.util.ReflectionUtil;

public class OutputUtil
{
	public static final int TYPE_KEYS = 0;
	public static final int TYPE_VALUES = 1;
	public static final int TYPE_KEYS_VALUES = 2;
	
	public static final String TAG_COLUMN_SEPARATOR = "#|";
	
	public static final String TAG_KEY = "#%key";
	public static final String TAG_DATE_FORMAT = "#%date_format";
	public static final String TAG_VALUE = "#%value";
	public static final String TAG_VALUE_KEY = "#%value_key";
	
	
	public static void printEntries(PrintWriter writer, Map map, 
			String fieldTerminator, String rowTerminator, 
			int firstRow, int lastRow,
			int printType, boolean printHeader, 
			SimpleDateFormat dateFormat, String valueKeyFieldName)
	{
		if (map == null) {
			System.out.println("Error: map is null");
			return;
		}
		
		// Get all getters
		Set<Map.Entry> entrySet = map.entrySet();
		Object key = null;
		Object value = null;
		Method keyGetters[] = null;
		Method valueGetters[] = null;
		for (Entry entry : entrySet) {
			key = entry.getKey();
			value = entry.getValue();
			keyGetters = ReflectionUtil.getAllGetters(key.getClass());
			if (value == null) {
				valueGetters = new Method[0];
			} else {
				valueGetters = ReflectionUtil.getAllGetters(value.getClass());
			}
			break;
		}
		
		if (value == null) {
			System.out.println("Error: value is null");
			return;
		}
		
		switch (printType) {
		case TYPE_KEYS:
			// Print keys
			if (printHeader) {
				writer.print(TAG_KEY + " " + key.getClass().getName());
				writer.print(rowTerminator);
				writer.print(TAG_DATE_FORMAT + " " + dateFormat.toPattern());
				writer.print(rowTerminator);
				printHeader(writer, key, keyGetters, fieldTerminator, rowTerminator);
			}
			for (Entry entry : entrySet) {
				key = entry.getKey();
				printObject(writer, keyGetters, key, fieldTerminator, rowTerminator, dateFormat);
			}
			break;
		case TYPE_VALUES:
			// Print values
			if (printHeader) {
				if (value != null) {
					writer.print(TAG_VALUE + " " + value.getClass().getName());
				}
				writer.print(rowTerminator);
				writer.print(TAG_VALUE_KEY + " " + valueKeyFieldName);
				writer.print(rowTerminator);
				writer.print(TAG_DATE_FORMAT + " " + dateFormat.toPattern());
				writer.print(rowTerminator);
				printHeader(writer, value, valueGetters, fieldTerminator, rowTerminator);
			}
			for (Entry entry : entrySet) {
				value = entry.getValue();
				printObject(writer, valueGetters, value, fieldTerminator, rowTerminator, dateFormat);
			}
			break;
		case TYPE_KEYS_VALUES:
		default:
			// Print keys and values
			if (printHeader) {
				writer.print(TAG_KEY + " " + key.getClass().getName());
				writer.print(rowTerminator);
				if (value != null) {
					writer.print(TAG_VALUE + " " + value.getClass().getName());
				}
				writer.print(rowTerminator);
				writer.print(TAG_DATE_FORMAT + " " + dateFormat.toPattern());
				writer.print(rowTerminator);
				printHeader(writer, key, keyGetters, fieldTerminator);
				writer.print(",");
				printHeader(writer, value, valueGetters, fieldTerminator, rowTerminator);
			}
			for (Entry entry : entrySet) {
				key = entry.getKey();
				value = entry.getValue();
				printObject(writer, keyGetters, key, fieldTerminator, dateFormat);
				writer.print(",");
				printObject(writer, valueGetters, value, fieldTerminator, dateFormat);
				writer.print(rowTerminator);
			}
			break;
		
		}
	}
	
	private static void printHeader(PrintWriter writer, Object object, Method methods[], 
			String fieldTerminator, String rowTerminator)
	{
		printHeader(writer, object, methods, fieldTerminator);
		writer.print(rowTerminator);
	}
	
	private static void printHeader(PrintWriter writer, Object object, Method methods[], 
			String fieldTerminator)
	{
		writer.print(TAG_COLUMN_SEPARATOR);

		if (object == null || object instanceof String || object.getClass().isPrimitive() || 
				object.getClass() == Boolean.class ||
				object.getClass() == Byte.class ||
				object.getClass() == Character.class ||
				object.getClass() == Short.class ||
				object.getClass() == Integer.class ||
				object.getClass() == Long.class ||
				object.getClass() == Float.class ||
				object.getClass() == Double.class ||
				object instanceof Date)  
		{
			writer.print("Value");
		} else {
			for (int i = 0; i < methods.length; i++) {
				String name = methods[i].getName().substring(3);
				writer.print(name);
				if (i < methods.length - 1) {
					writer.print(fieldTerminator);
				}
			}
		}
	}
	
	public static void printObject(PrintWriter writer, 
			Method methods[], 
			Object object, 
			String fieldTerminator, String rowTerminator, 
			SimpleDateFormat dateFormat)
	{
		printObject(writer, methods, object, fieldTerminator, dateFormat);
		writer.print(rowTerminator);
	}
	
	private static void printObject(PrintWriter writer, 
			Method methods[], 
			Object object, 
			String fieldTerminator, 
			SimpleDateFormat dateFormat)
	{
		if (object == null) {
			writer.print("null");
		} else if (object instanceof String ) { //FindBugs (Below) - Possible null pointer dereference of object
			String value = object.toString();
			
			// For each quote add matching quote
			value = value.replaceAll("\"", "\"\"");
			
			// If contains a quote then enclose it with quotes
			if (value.indexOf("\"") != -1) {
				value = "\"" + value;
				value = value + "\"";
			} else {
			
				// If begins with a " then prepend a ".
				if (value.startsWith("\"")) {
					value = "\"" + value;
				}
				
				// If ends with a " then end it with a ".
				if (value.endsWith("\"")) {
					value = value + "\"";
				}
			}
			writer.print(value);
			
		} else if(object.getClass().isPrimitive() || 
				object.getClass() == Boolean.class ||
				object.getClass() == Byte.class ||
				object.getClass() == Character.class ||
				object.getClass() == Short.class ||
				object.getClass() == Integer.class ||
				object.getClass() == Long.class ||
				object.getClass() == Float.class ||
				object.getClass() == Double.class)
		{
			writer.print(object.toString());
			
		} else if (object instanceof Date) {
			
			writer.print(dateFormat.format((Date)object));
			
		} else if (methods != null) {
			for (int i = 0; i < methods.length; i++) {
				Method method = methods[i];
				String name = method.getName();
				try {
					Object value = method.invoke(object, (Object[])null);
					value = getPrintableValue(value);
					printObject(writer, null, value, fieldTerminator, dateFormat);
					if (i < methods.length - 1) {
						writer.print(fieldTerminator);
					}
				} catch (Exception ex) {
				}
			}
		}
	}
	
	private static Object getPrintableValue(Object value)
	{
		if (value instanceof Byte) {
			value = ((Byte) value).toString();
		} else if (value instanceof byte[]) {
			value = "[B " + ((byte[])value).length;
		} else if (value instanceof boolean[]) {
			value = "[Z " + ((boolean[])value).length;
		} else if (value instanceof short[]) {
			value = "[S " + ((short[])value).length;
		} else if (value instanceof int[]) {
			value = "[I " + ((int[])value).length;
		} else if (value instanceof long[]) {
			value = "[J " + ((long[])value).length;
		} else if (value instanceof float[]) {
			value = "[F " + ((float[])value).length;
		} else if (value instanceof double[]) {
			value = "[D " + ((double[])value).length;
		}
		return value;
	}

}

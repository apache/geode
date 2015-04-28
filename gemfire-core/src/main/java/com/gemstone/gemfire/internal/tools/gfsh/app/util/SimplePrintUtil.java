package com.gemstone.gemfire.internal.tools.gfsh.app.util;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.types.CollectionType;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.cache.query.types.StructType;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.Mappable;
import com.gemstone.gemfire.internal.tools.gfsh.app.misc.util.StringUtil;

public class SimplePrintUtil
{
	private static boolean printType = false;
	private static int collectionEntryPrintCount = 5;

	public static boolean isPrintType()
	{
		return printType;
	}

	public static void setPrintType(boolean printType)
	{
		SimplePrintUtil.printType = printType;
	}

	public static int getCollectionEntryPrintCount()
	{
		return collectionEntryPrintCount;
	}

	public static void setCollectionEntryPrintCount(int collectionEntryPrintCount)
	{
		SimplePrintUtil.collectionEntryPrintCount = collectionEntryPrintCount;
	}

	/**
	 * Prints the region entries. It prints both keys and values formatted.
	 * @param region
	 * @param startIndex
	 * @param startRowNum
	 * @param rowCount
	 * @param keyList
	 * @return Returns the number of rows printed.
	 * @throws Exception
	 */
	public static int printEntries(Region region, Iterator regionIterator, int startIndex, int startRowNum, int rowCount, List keyList) throws Exception
	{
		
		if (region == null || regionIterator == null) {
			System.out.println("Error: Region is null");
			return 0;
		}
		
		int endIndex = startIndex + rowCount; // exclusive
		if (endIndex >= region.size()) {
			endIndex = region.size();
		}
		
		if (startIndex == endIndex) {
			return 0;
		}

		HashSet keyNameSet = new HashSet();
		HashSet valueNameSet = new HashSet();
		Object key = null;
		Object value = null;
		int index = startIndex;

		// Print keys and values
		int row = startRowNum;
		index = startIndex;
		for (Iterator itr = regionIterator; index < endIndex && itr.hasNext(); index++) {
			Region.Entry entry = (Region.Entry) itr.next();
			key = entry.getKey();
			value = entry.getValue();
			keyNameSet.add(key.getClass().getName());
			if (value != null) {
				valueNameSet.add(value.getClass().getName());
			}
			printObject(row, "Key", key, true);
			printObject(row, "Value", value, false);
			System.out.println();
			row++;
		}
		
		System.out.println();
		System.out.println(" Fetch size: " + rowCount);
		System.out.println("   Returned: " + (row-1) + "/" + region.size());
		for (Object keyName : keyNameSet) {
			System.out.println("  Key Class: " + keyName);
		}
		for (Object valueName : valueNameSet) {
			System.out.println("Value Class: " + valueName);

		}
		return endIndex - startIndex;
	}

	public static int printEntries(Region region, Map keyMap, List keyList) throws Exception
	{
		if (region == null) {
			System.out.println("Error: Region is null");
			return 0;
		}

		HashSet keyNameSet = new HashSet();
		HashSet valueNameSet = new HashSet();
		ArrayList indexList = new ArrayList(keyMap.keySet());
		Collections.sort(indexList);
		Object key = null;
		Object value = null;

		// Print keys and values
		int row = 1;
		int rowCount = keyMap.size();
		for (Iterator iterator = indexList.iterator(); iterator.hasNext();) {
			Object index = iterator.next();
			key = keyMap.get(index);
			value = region.get(key);
			keyNameSet.add(key.getClass().getName());
			if (value != null) {
				valueNameSet.add(value.getClass().getName());
			}
			printObject(row, "Key", key, true);
			printObject(row, "Value", value, false);
			System.out.println();
			row++;
		}

		System.out.println();
		for (Object keyName : keyNameSet) {
			System.out.println("  Key Class: " + keyName);
		}
		for (Object valueName : valueNameSet) {
			System.out.println("Value Class: " + valueName);
		}
		return rowCount;
	}

	public static int printEntries(Region region, Set keySet, List keyList) throws Exception
	{
		if (region == null) {
			System.out.println("Error: Region is null");
			return 0;
		}

		if (keySet.size() == 0) {
			return 0;
		}

		// Print keys and values
		HashSet keyNameSet = new HashSet();
		HashSet valueNameSet = new HashSet();
		int row = 1;
		Object key = null;
		Object value = null;
		for (Iterator iterator = keySet.iterator(); iterator.hasNext();) {
			key = iterator.next();
			value = region.get(key);
			if (keyList != null) {
				keyList.add(key);
			}
			keyNameSet.add(key.getClass().getName());
			if (value != null) {
				valueNameSet.add(value.getClass().getName());
			}
			printObject(row, "Key", key, true);
			printObject(row, "Value", value, false);
			row++;
		}
		System.out.println();
		for (Object keyName : keyNameSet) {
			System.out.println("  Key Class: " + keyName);
		}
		for (Object valueName : valueNameSet) {
			System.out.println("Value Class: " + valueName);
		}
		return row - 1;
	}
	
	public static int printEntries(Map map, int startIndex, int startRowNum, int rowCount, int actualSize, List keyList) throws Exception
	{
		if (map == null) {
			System.out.println("Error: map is null");
			return 0;
		}

		HashSet keyNameSet = new HashSet();
		HashSet valueNameSet = new HashSet();
		Object key = null;
		Object value = null;
		Set entrySet = map.entrySet();
		int count = 0;
		int row = startRowNum;
		int lastRow = startRowNum + rowCount - 1;
		for (Iterator itr = entrySet.iterator(); count < rowCount && itr.hasNext(); count++) {
			Map.Entry entry = (Map.Entry) itr.next();
			key = entry.getKey();
			value = entry.getValue();
			keyNameSet.add(key.getClass().getName());
			if (value != null) {
				valueNameSet.add(value.getClass().getName());
			}
			printObject(row, "Key", key, true, 2);
			printObject(row, "Value", value, false, 2);
			System.out.println();
			row++;
		}
		System.out.println();
		System.out.println(" Fetch size: " + rowCount);
		System.out.println("   Returned: " + (row-1) + "/" + actualSize);
		for (Object keyName : keyNameSet) {
			System.out.println("  Key Class: " + keyName);
		}
		for (Object valueName : valueNameSet) {
			System.out.println("Value Class: " + valueName);
		}
		return count;
	}
	
	public static int printEntries(Map map, int rowCount, List keyList, 
			boolean displaySummary, boolean showValues) throws Exception
	{
		return printEntries(map, rowCount, keyList, "Key", "Value", displaySummary, showValues);
	}
	
	public static int printEntries(Map map, int rowCount, List keyList, 
			String keyColumnName, String valueColumnName, 
			boolean displaySummary, boolean showValues) throws Exception
	{
		if (map == null) {
			System.out.println("Error: Region is null");
			return 0;
		}
		
		if (map.size() == 0) {
			return 0;
		}

		// Print keys and values
		int row = 1;
		Object key = null;
		Object value = null;
		int count = 0;
		HashSet keyNameSet = new HashSet();
		HashSet valueNameSet = new HashSet();
		Set nameSet = map.entrySet();
		for (Iterator itr = nameSet.iterator(); count < rowCount && itr.hasNext(); count++) {

			Map.Entry entry = (Map.Entry) itr.next();
			key = entry.getKey();
			value = entry.getValue();
			if (keyList != null) {
				keyList.add(key);
			}
			keyNameSet.add(key.getClass().getName());
			if (value != null) {
				valueNameSet.add(value.getClass().getName());
			}
			printObject(row, keyColumnName, key, true, 2);
			if (showValues) {
				printObject(row, valueColumnName, value, false, 2);
			}
			System.out.println();
			row++;
		}
		if (displaySummary) {
			System.out.println();
			System.out.println("Displayed (fetched): " + (row - 1));
			System.out.println("        Actual Size: " + map.size());
			for (Object keyName : keyNameSet) {
				System.out.println("          " + keyColumnName + " Class: " + keyName);
			}
			for (Object valueName : valueNameSet) {
				System.out.println("        " + valueColumnName + " Class: " + valueName);
	
			}
		}
		return row - 1;
	}
	
	public static int printSet(Set set, int rowCount, List keyList, 
			String keyColumnName, 
			boolean displaySummary) throws Exception
	{
		if (set == null) {
			return 0;
		}
		
		if (set.size() == 0) {
			return 0;
		}

		// Print keys and values
		int row = 1;
		Object key = null;
		int count = 0;
		HashSet keyNameSet = new HashSet();
//		HashSet valueNameSet = new HashSet(); //FindBugs - unused
		Set nameSet = set;
		for (Iterator itr = nameSet.iterator(); count < rowCount && itr.hasNext(); count++) {
			key = itr.next();
			if (keyList != null) {
				keyList.add(key);
			}
			keyNameSet.add(key.getClass().getName());
			printObject(row, keyColumnName, key, true, 2);
			System.out.println();
			row++;
		}
		if (displaySummary) {
			System.out.println();
			System.out.println("Displayed (fetched): " + (row - 1));
			System.out.println("        Actual Size: " + set.size());
			for (Object keyName : keyNameSet) {
				System.out.println("          " + keyColumnName + " Class: " + keyName);
			}
		}
		return row - 1;
	}

//FindBugs - private method never called
//	private static void computeMaxLengths(List keyList, List valueList, Object key, Object value)
//	{
//		computeMaxLengths(keyList, key, true);
//		computeMaxLengths(valueList, value, false);
//	}

	private static void printTopHeaders(List list, Object obj, 
			boolean printLastColumnSpaces, String primitiveHeader)
	{
		Object object = obj;
		if (object == null) {
			object = "null";
		}
		
		if (object instanceof String || object.getClass().isPrimitive() || 
				object.getClass() == Boolean.class ||
				object.getClass() == Byte.class ||
				object.getClass() == Character.class ||
				object.getClass() == Short.class ||
				object.getClass() == Integer.class ||
				object.getClass() == Long.class ||
				object.getClass() == Float.class ||
				object.getClass() == Double.class ||
				object.getClass().isArray() ||
				object instanceof Date) 
		{

			int maxLen = (Integer) list.get(0);
			if (maxLen < primitiveHeader.length()) {
				maxLen = primitiveHeader.length();
			}
			if (printLastColumnSpaces) {
				System.out.print(StringUtil.getRightPaddedString(primitiveHeader, maxLen, ' '));
			} else {
				System.out.print(primitiveHeader);
			}

		} else {

			Class cls = object.getClass();
			Method methods[] = cls.getMethods();
			Method method;
			Class retType;
			String name;
			Object value;
			int listIndex = 0;
			for (int i = 0; i < methods.length; i++) {
				method = methods[i];
				name = method.getName();
				if (name.length() <= 3 || name.startsWith("get") == false || name.equals("getClass")) {
					continue;
				}
				retType = method.getReturnType();
				if (retType == Void.TYPE) {
					continue;
				}
				try {
					value = method.invoke(object, (Object[])null);
					value = getPrintableValue(value);
					int maxLen = (Integer) list.get(listIndex);
					String header = name.substring(3);
					if (listIndex == list.size() - 1) {
						if (printLastColumnSpaces) {
							System.out.print(StringUtil.getRightPaddedString(header, maxLen, ' '));
						} else {
							System.out.print(header);
						}
					} else {
						System.out.print(StringUtil.getRightPaddedString(header, maxLen, ' '));
						System.out.print("  ");
					}

					listIndex++;
				} catch (Exception ex) {
				}
			}
		}
	}

	private static void printBottomHeaders(List list, Object obj, boolean printLastColumnSpaces,
			String primitiveHeader)
	{
		Object object = obj;
		if (object == null) {
			object = "null";
		}
		
		if (object instanceof String || object.getClass().isPrimitive() || 
				object.getClass() == Boolean.class ||
				object.getClass() == Byte.class ||
				object.getClass() == Character.class ||
				object.getClass() == Short.class ||
				object.getClass() == Integer.class ||
				object.getClass() == Long.class ||
				object.getClass() == Float.class ||
				object.getClass() == Double.class ||
				object.getClass().isArray() ||
				object instanceof Date) 
		{

			int maxLen = (Integer) list.get(0);
			if (maxLen < primitiveHeader.length()) {
				maxLen = primitiveHeader.length();
			}
			if (printLastColumnSpaces) {
				System.out.print(StringUtil.getRightPaddedString(StringUtil.getRightPaddedString("", primitiveHeader
						.length(), '-'), maxLen, ' '));
			} else {
				System.out.print(StringUtil.getRightPaddedString("", primitiveHeader.length(), '-'));
			}

		} else {

			Class cls = object.getClass();
			Method methods[] = cls.getMethods();
			Method method;
			Class retType;
			String name;
			Object value;
			int listIndex = 0;
			listIndex = 0;
			for (int i = 0; i < methods.length; i++) {
				method = methods[i];
				name = method.getName();
				if (name.length() <= 3 || name.startsWith("get") == false || name.equals("getClass")) {
					continue;
				}
				retType = method.getReturnType();
				if (retType == Void.TYPE) {
					continue;
				}
				try {
					value = method.invoke(object, (Object[])null);
					value = getPrintableValue(value);
					int maxLen = (Integer) list.get(listIndex);
					String header = name.substring(3);

					if (listIndex == list.size() - 1) {
						if (printLastColumnSpaces) {
							System.out.print(StringUtil.getRightPaddedString(StringUtil.getRightPaddedString("", header
									.length(), '-'), maxLen, ' '));
						} else {
							System.out.print(StringUtil.getRightPaddedString("", header.length(), '-'));
						}
					} else {
						System.out.print(StringUtil.getRightPaddedString(StringUtil.getRightPaddedString("", header
								.length(), '-'), maxLen, ' '));
						System.out.print("  ");
					}
					listIndex++;
				} catch (Exception ex) {
				}
			}
		}
	}

//	FindBugs - private method never called
//	private static void printHeaders(List keyList, List valueList, Object key, Object value, int rowMaxLen)
//			throws Exception
//	{
//		System.out.print(StringUtil.getRightPaddedString("Row", rowMaxLen, ' '));
//		System.out.print("  ");
//		printTopHeaders(keyList, key, true, "Key");
//		System.out.print(" | ");
//		printTopHeaders(valueList, value, false, "Value");
//		System.out.println();
//
//		if (rowMaxLen < 3) {
//			rowMaxLen = 3;
//		}
//		System.out.print(StringUtil.getRightPaddedString("", rowMaxLen, '-'));
//		System.out.print("  ");
//		printBottomHeaders(keyList, key, true, "Key");
//		System.out.print(" | ");
//		printBottomHeaders(valueList, value, false, "Value");
//		System.out.println();
//	}
	
	/**
	 * Prints the SelectResults contents up to the specified rowCount.
	 * @param sr
	 * @param startRowNum
	 * @param rowCount
	 * @return The number of rows printed
	 */
	public static int printSelectResults(SelectResults sr, int startIndex, int startRowNum, int rowCount)
	{
		if (sr == null) {
			System.out.println("Error: SelectResults is null");
			return 0;
		}

		int endIndex = startIndex + rowCount; // exclusive
		if (endIndex >= sr.size()) {
			endIndex = sr.size();
		}
		
		if (startIndex >= endIndex) {
			return 0;
		}
		
		CollectionType type = sr.getCollectionType();
		ObjectType elementType = type.getElementType();
		int row = 1;
		if (rowCount == -1) {
			rowCount = sr.size();
		}

		HashSet elementNameSet = new HashSet();
		Object element = null;
		boolean isStructType = false;
		StructType structType = null;
		Struct struct = null;
		List srList = sr.asList();

		row = startRowNum;
		for (int i = startIndex; i < endIndex; i++) {
			element = srList.get(i);

			if (elementType.isStructType()) {

				structType = (StructType) elementType;
				struct = (Struct) element;
				printStruct(row, structType, struct, 0);
				System.out.println();

			} else {
				System.out.println(row + ". " + getPrintableType(element));
				printObject(null, element, 1);
				System.out.println();
			}
			row++;
		}
		System.out.println();
		for (Object elementClassName : elementNameSet) {
			System.out.println("Class: " + elementClassName);
		}
		return endIndex - startIndex;
	}

	private static int printSelectResults_iterator(SelectResults sr, int startRowNum, int rowCount)
	{
		if (sr == null) {
			System.out.println("SelectResults is null");
			return 0;
		}

		CollectionType type = sr.getCollectionType();
		ObjectType elementType = type.getElementType();
		int row = 1;
		if (rowCount == -1) {
			rowCount = sr.size();
		}

		HashSet elementNameSet = new HashSet();
		ArrayList maxLenList = new ArrayList();
		Object element = null;
		boolean isStructType = false;
		StructType structType = null;
		Struct struct = null;
		for (Iterator iter = sr.iterator(); iter.hasNext() && row <= rowCount;) {
			element = iter.next();
			if (elementType.isStructType()) {
				structType = (StructType) elementType;
				struct = (Struct) element;
				computeMaxLengths(maxLenList, structType, struct);
				isStructType = true;
			} else {
				computeMaxLengths(maxLenList, element, false);
				elementNameSet.add(element.getClass().getName());
			}
			row++;
		}

		if (element == null && struct == null) {
			return 0;
		}

		int rowMax = String.valueOf(startRowNum + rowCount - 1).length();
		if (rowMax < 3) {
			rowMax = 3;
		}
		if (isStructType) {
			printHeaders(maxLenList, structType, struct, rowMax);
		} else {
			printHeaders(maxLenList, element, rowMax);
		}

		row = startRowNum;
		int lastRow = startRowNum + rowCount - 1;
		for (Iterator iter = sr.iterator(); iter.hasNext() && row <= lastRow;) {
			element = iter.next();

			if (elementType.isStructType()) {

				structType = (StructType) elementType;
				struct = (Struct) element;
				System.out.print(StringUtil.getRightPaddedString(row + "", rowMax, ' '));
				System.out.print("  ");
				System.out.println();

			} else {

				System.out.print(StringUtil.getRightPaddedString(row + "", rowMax, ' '));
				System.out.print("  ");
				printObject(maxLenList, element, false);
				System.out.println();
			}
			row++;
		}
		System.out.println();
		for (Object elementClassName : elementNameSet) {
			System.out.println("Class: " + elementClassName);
		}
		return row - 1;
	}

	private static void computeMaxLengths(List list, StructType structType, Struct struct)
	{
		ObjectType[] fieldTypes = structType.getFieldTypes();
		String[] fieldNames = structType.getFieldNames();
		Object[] fieldValues = struct.getFieldValues();

		int listIndex = 0;
		for (int i = 0; i < fieldTypes.length; i++) {
			ObjectType fieldType = fieldTypes[i];
			String fieldName = fieldNames[i];
			Object fieldValue = fieldValues[i];

			Integer len;
			if (listIndex >= list.size()) {
				len = fieldName.length();
				list.add(len);
			} else {
				len = (Integer) list.get(listIndex);
			}
			if (fieldValue == null) {
				if (len.intValue() < 4) {
					len = 4;
				}
			} else {
				int valueLen = fieldValue.toString().length();
				if (len.intValue() < valueLen) {
					len = valueLen;
				}
			}
			list.set(listIndex, len);
			listIndex++;
		}
	}

	private static void computeMaxLengths(List list, Object obj, boolean isKey)
	{
		Object object = obj;
		if (object == null) {
			object = "null";
		}
		
		if (object instanceof String || object.getClass().isPrimitive() || 
				object.getClass() == Boolean.class ||
				object.getClass() == Byte.class ||
				object.getClass() == Character.class ||
				object.getClass() == Short.class ||
				object.getClass() == Integer.class ||
				object.getClass() == Long.class ||
				object.getClass() == Float.class ||
				object.getClass() == Double.class ||
				object.getClass().isArray() ||
				object instanceof Date) 
		{
			if (list.size() > 0) {
				int len = (Integer) list.get(0);
				if (len < object.toString().length()) {
					list.set(0, object.toString().length());
				}
			} else {
				if (isKey) {
					if (object.toString().length() < 3) { // Key
						list.add(3);
					} else {
						list.add(object.toString().length());
					}
				} else {
					if (object.toString().length() < 5) { // Value
						list.add(5);
					} else {
						list.add(object.toString().length());
					}
				}
			}

		} else {

			Class cls = object.getClass();
			Method methods[] = cls.getMethods();
			Method method;
			Class retType;
			String name;
			Object value;
			int listIndex = 0;
			for (int i = 0; i < methods.length; i++) {
				method = methods[i];
				name = method.getName();
				if (name.length() <= 3 || name.startsWith("get") == false || name.equals("getClass")) {
					continue;
				}
				retType = method.getReturnType();
				if (retType == Void.TYPE) {
					continue;
				}
				try {
					value = method.invoke(object, (Object[])null);
					value = getPrintableValue(value);
					Integer len;
					if (listIndex >= list.size()) {
						len = name.length() - 3;
						list.add(len);
					} else {
						len = (Integer) list.get(listIndex);
					}
					if (value == null) {
						if (len.intValue() < 4) {
							len = 4;
						}
					} else {
						int valueLen = value.toString().length();
						if (len.intValue() < valueLen) {
							len = valueLen;
						}
					}
					list.set(listIndex, len);
					listIndex++;
				} catch (Exception ex) {
				}
			}
		}
	}

	private static void printHeaders(List list, StructType structType, Struct struct, int rowMaxLen)
	{
		System.out.print(StringUtil.getRightPaddedString("Row", rowMaxLen, ' '));
		System.out.print("  ");

		ObjectType[] fieldTypes = structType.getFieldTypes();
		String[] fieldNames = structType.getFieldNames();
		Object[] fieldValues = struct.getFieldValues();

		int listIndex = 0;
		for (int i = 0; i < fieldTypes.length; i++) {
			ObjectType fieldType = fieldTypes[i];
			String fieldName = fieldNames[i];
			Object fieldValue = fieldValues[i];
			fieldValue = getPrintableValue(fieldValue);
			int maxLen = (Integer) list.get(listIndex);
			String header = fieldName;
			System.out.print(StringUtil.getRightPaddedString(header, maxLen, ' '));
			System.out.print("  ");
			listIndex++;
		}
		System.out.println();
		System.out.print(StringUtil.getRightPaddedString("", rowMaxLen, '-'));
		System.out.print("  ");
		listIndex = 0;
		for (int i = 0; i < fieldTypes.length; i++) {
			ObjectType fieldType = fieldTypes[i];
			String fieldName = fieldNames[i];
			Object fieldValue = fieldValues[i];
			fieldValue = getPrintableValue(fieldValue);
			int maxLen = (Integer) list.get(listIndex);
			String header = fieldName;
			System.out.print(StringUtil.getRightPaddedString(StringUtil.getRightPaddedString("", header.length(), '-'),
					maxLen, ' '));
			System.out.print("  ");
			listIndex++;
		}
		System.out.println();
	}

	private static void printHeaders(List list, Object object, int rowMaxLen)
	{
		System.out.print(StringUtil.getRightPaddedString("Row", rowMaxLen, ' '));
		System.out.print("  ");
		printTopHeaders(list, object, false, "Value");
		System.out.println();
		
		System.out.print(StringUtil.getRightPaddedString("", rowMaxLen, '-'));
		System.out.print("  ");
		printBottomHeaders(list, object, false, "Value");
		System.out.println();
	}
	
	private static String getPrintableType(Object object)
	{
		if (isPrintType()) {
			if (object == null) {
				return " (N/A)";
			}
			return " (" + object.getClass().getSimpleName() + ")";
		} else {
			return "";
		}
	}

	private static void printStruct(int row, StructType structType, Struct struct, int level)
	{
		String spaces = getSpaces(level);
		String spaces2 = getSpaces(level+1);
		
		ObjectType[] fieldTypes = structType.getFieldTypes();
		String[] fieldNames = structType.getFieldNames();
		Object[] fieldValues = struct.getFieldValues();

		int listIndex = 0;
		System.out.println(spaces + row + ".");
		for (int i = 0; i < fieldTypes.length; i++) {
			ObjectType fieldType = fieldTypes[i];
			String fieldName = fieldNames[i];
			Object fieldValue = fieldValues[i];
			printObject(fieldName, fieldValue, level+1);
		}
	}
	
	private static void printObject(int row, String header, Object object, boolean printRow)
	{
		printObject(row, header, object, printRow, 1);
	}
	
	private static void printObject(int row, String header, Object object, boolean printRow, int level)
	{
		if (printRow) {
			System.out.print(row + ". ");
		} else {
			String rowStr = Integer.toString(row);
			String spaces = "";
			for (int i = 0; i < rowStr.length(); i++) {
				spaces += " ";
			}
			System.out.print(spaces + "  ");
		}
		if (header == null) {
			System.out.print(getPrintableType(object));
		} else {
			System.out.print(header + getPrintableType(object));
		}
		System.out.println();
		printObject(null, object, level);
	}
	
	private static void printObject(String name, Object obj, int level)
	{
		String spaces = getSpaces(level);
		
		Object object = obj;
		if (object == null) {
			object = "null";
		}
		
		if (object instanceof String || object.getClass().isPrimitive() || 
				object.getClass() == Boolean.class ||
				object.getClass() == Byte.class ||
				object.getClass() == Character.class ||
				object.getClass() == Short.class ||
				object.getClass() == Integer.class ||
				object.getClass() == Long.class ||
				object.getClass() == Float.class ||
				object.getClass() == Double.class ||
				object.getClass().isArray() ||
				object instanceof Date)  
		{
			printValue(name, object, level);
			
		} else if (object instanceof Map) {
			printMap(name, (Map)object, level);

		} else if (object instanceof Collection) {
			printCollection(name, (Collection)object, level);
			
		} else if (object instanceof Mappable) {
			printMappable(name, (Mappable)object, level);
			
//			} else if (object instanceof Struct) {
//				printStruct(name, (Struct)object, level);
			
		} else {
			Class cls = object.getClass();
			Method methods[] = cls.getMethods();
			Method method;
			Class retType;
			Object value;
			int listIndex = 0;
			ArrayList<String> methodList = new ArrayList();
			HashMap<String, Method> methodMap = new HashMap();
			for (int i = 0; i < methods.length; i++) {
				method = methods[i];
				name = method.getName();
				if (name.length() <= 3 || name.startsWith("get") == false || name.equals("getClass")) {
					continue;
				}
				retType = method.getReturnType();
				if (retType == Void.TYPE) {
					continue;
				}
				String propertyName = name.substring(3);
				methodMap.put(propertyName, method);
				methodList.add(propertyName);
			}
			Collections.sort(methodList);
			for (String propertyName : methodList) {
				try {
					method = methodMap.get(propertyName);
					value = method.invoke(object, (Object[])null);
					printObject(propertyName, value, level);
				} catch (Exception ex) {
				}
			}
		}
	}

	public static void printList(List resultList)
	{
		ArrayList maxLenList = new ArrayList();
		Object nonNullObject = null;
		for (int i = 0; i < resultList.size(); i++) {
			Object object = resultList.get(i);
			if (object != null) {
				nonNullObject = object;
			}
			computeMaxLengths(maxLenList, object, true); // TODO: true?
		}
		if (nonNullObject == null) {
			return;
		}

		int rowMax = String.valueOf(resultList.size()).length();
		if (rowMax < 3) {
			rowMax = 3;
		}
		printHeaders(maxLenList, nonNullObject, rowMax);
		for (int i = 0; i < resultList.size(); i++) {
			Object object = resultList.get(i);
			System.out.print(StringUtil.getRightPaddedString((i + 1) + "", rowMax, ' '));
			System.out.print("  ");
			printObject(maxLenList, object, false);
			System.out.println();
		}
	}
	
	public static int printList(List list, int startIndex, int startRowNum, int rowCount, int actualSize, List keyList) throws Exception
	{
		if (list == null) {
			System.out.println("Error: map is null");
			return 0;
		}

		HashSet objectNameSet = new HashSet();
//		HashSet valueNameSet = new HashSet(); //FindBugs - unused
		Object object = null;
		
		int count = 0;
		int row = startRowNum;
		int lastRow = startRowNum + rowCount - 1;
		for (Iterator itr = list.iterator(); count < rowCount && itr.hasNext(); count++) {
			object = itr.next();
			objectNameSet.add(object.getClass().getName());
			printObject(row, "Object", object, true, 2);
			System.out.println();
			row++;
		}
		System.out.println();
		System.out.println(" Fetch size: " + rowCount);
		System.out.println("   Returned: " + (row-1) + "/" + actualSize);
		for (Object keyName : objectNameSet) {
			System.out.println("      Class: " + keyName);
		}
		return count;
	}
	
	private static void computeMappableMaxLengths(List list, Mappable mappable)
	{
		String name;
		Object value;
		int listIndex = 0;
		ArrayList<String> keyList = new ArrayList(mappable.getKeys());
		Collections.sort(keyList);
		for (int i = 0; i < keyList.size(); i++) {
			name = keyList.get(i);
			value = mappable.getValue(name);
			value = getPrintableValue(value);
			Integer len;
			if (listIndex >= list.size()) {
				len = name.length();
				list.add(len);
			} else {
				len = (Integer) list.get(listIndex);
			}
			if (value == null) {
				if (len.intValue() < 4) {
					len = 4;
				}
			} else {
				int valueLen = value.toString().length();
				if (len.intValue() < valueLen) {
					len = valueLen;
				}
			}
			list.set(listIndex, len);
			listIndex++;
		}
	}
	
	private static void printMappableHeaders(List list, Mappable mappable, int rowMaxLen)
	{
		System.out.print(StringUtil.getRightPaddedString("Row", rowMaxLen, ' '));
		System.out.print("  ");
		printMappableTopHeaders(list, mappable, false);
		System.out.println();
		if (rowMaxLen < 3) {
			rowMaxLen = 3;
		}
		System.out.print(StringUtil.getRightPaddedString("", rowMaxLen, '-'));
		System.out.print("  ");
		printMappableBottomHeaders(list, mappable, false);
		System.out.println();
	}
	
	private static void printMappableTopHeaders(List list, Mappable mappable, 
			boolean printLastColumnSpaces)
	{
		int listIndex = 0;
		ArrayList<String> keyList = new ArrayList(mappable.getKeys());
		Collections.sort(keyList);
		for (int i = 0; i < keyList.size(); i++) {
			String header = keyList.get(i);
			int maxLen = (Integer) list.get(listIndex);
			if (listIndex == list.size() - 1) {
				if (printLastColumnSpaces) {
					System.out.print(StringUtil.getRightPaddedString(header, maxLen, ' '));
				} else {
					System.out.print(header);
				}
			} else {
				System.out.print(StringUtil.getRightPaddedString(header, maxLen, ' '));
				System.out.print("  ");
			}

			listIndex++;
		}
	}
	
	private static void printMappableBottomHeaders(List list, Mappable mappable, boolean printLastColumnSpaces)
	{
		int listIndex = 0;
		ArrayList<String> keyList = new ArrayList(mappable.getKeys());
		Collections.sort(keyList);
		for (int i = 0; i < keyList.size(); i++) {
			String header = keyList.get(i);
			int maxLen = (Integer) list.get(listIndex);
			if (listIndex == list.size() - 1) {
				if (printLastColumnSpaces) {
					System.out.print(StringUtil.getRightPaddedString(StringUtil.getRightPaddedString("", header
							.length(), '-'), maxLen, ' '));
				} else {
					System.out.print(StringUtil.getRightPaddedString("", header.length(), '-'));
				}
			} else {
				System.out.print(StringUtil.getRightPaddedString(StringUtil.getRightPaddedString("", header
						.length(), '-'), maxLen, ' '));
				System.out.print("  ");
			}
			listIndex++;
		}
	}
	
	private static void printMappable(String name, Mappable mappable, int level)
	{
		String spaces = getSpaces(level);
		int listIndex = 0;
		ArrayList<String> keyList = new ArrayList(mappable.getKeys());
		Collections.sort(keyList);
		for (int i = 0; i < keyList.size(); i++) {
			String n = keyList.get(i);
			Object value = mappable.getValue(n);
			printObject(n, value, level);
		}
	}
	
	public static void printMappableList(List<Mappable> resultList)
	{
		for (int i = 0; i < resultList.size(); i++) {
			Mappable mappable = resultList.get(i);
			printObject(i+1, null, mappable, true);
			System.out.println();
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
//		if (value instanceof Map) {
//			StringBuffer buffer = printMap(null, (Map)value, new StringBuffer("\n"), 2);
//			value = buffer.toString();
//		}
		return value;
	}
	
	private static void printValue(Object name, Object value, int level)
	{
		String spaces = getSpaces(level);
		Object printableValue = value;
		if (value instanceof Byte) {
			printableValue = ((Byte) value).toString();
		} else if (value instanceof byte[]) {
			printableValue = "[B " + ((byte[])value).length;
		} else if (value instanceof boolean[]) {
			printableValue = "[Z " + ((boolean[])value).length;
		} else if (value instanceof short[]) {
			printableValue = "[S " + ((short[])value).length;
		} else if (value instanceof int[]) {
			printableValue = "[I " + ((int[])value).length;
		} else if (value instanceof long[]) {
			printableValue = "[J " + ((long[])value).length;
		} else if (value instanceof float[]) {
			printableValue = "[F " + ((float[])value).length;
		} else if (value instanceof double[]) {
			printableValue = "[D " + ((double[])value).length;
		}
		if (value instanceof Map) {
			printMap(name, (Map)value, level);
		} else {
			if (name == null) {
				System.out.println(spaces + printableValue + getPrintableType(value));
			} else {
				if (name.toString().startsWith("[")) {
					System.out.println(spaces + name + " " + printableValue + getPrintableType(value));
				} else {
					System.out.println(spaces + name + " = " + printableValue + getPrintableType(value));
				}
			}
		}
	}
	
	private static void printMap(Object name, Map map, int level)
	{
		String spaces = getSpaces(level);
		String spaces2 = getSpaces(level+1);
				
		if (name == null) {
			System.out.println(spaces + "size: " + map.size() + getPrintableType(map));
		} else {
			System.out.println(spaces + name + " - size: " + map.size() + getPrintableType(map));
		}
		
		Set<Map.Entry> entrySet = map.entrySet();
		int count = 0;
		for (Map.Entry entry : entrySet) {
			Object key = entry.getKey();
			Object value = entry.getValue();
			if (key instanceof Map) {
				printMap(null, (Map)key, level+1);
			} else {
				if (isPrintType()) {
					if (value == null) {
						System.out.println(spaces2 + key + " (" + key.getClass().getSimpleName() + ", N/A");
					} else {
						System.out.println(spaces2 + key + " (" + key.getClass().getSimpleName() + ", " + value.getClass().getSimpleName() + ")");
					}
				} else {
					System.out.println(spaces2 + key);
				}
				printObject(key.toString(), value, level+2);
			}
			count++;
			if (count >= getCollectionEntryPrintCount()) {
				break;
			}
		}
		if (count < entrySet.size()) {
			System.out.println(spaces2 + "<" + (entrySet.size() - count) + " more ...>");
		}
	}
	
	private static void printCollection(Object name, Collection col, int level)
	{
		String spaces = getSpaces(level);
		String spaces2 = getSpaces(level+1);
		
//		if (name == null) {
//			if (isPrintType()) {
//				System.out.println(spaces + getPrintableType(map));
//			}
//		} else {
//			System.out.println(spaces + name + getPrintableType(map));
//		}
		
		if (name == null) {
			System.out.println(spaces + "size: " + col.size() + getPrintableType(col));
		} else {
			System.out.println(spaces + name + " - size: " + col.size() + getPrintableType(col));
		}
		

		int count = 0;
		for (Object value : col) {
			if (col instanceof Map) {
				printMap(null, (Map)value, level+1);
			} else if (value instanceof Collection) {
				printCollection(null, (Collection)value, level+1);
			} else {
				printObject("[" + count + "]", value, level+1);
			}
			count++;
			if (count >= getCollectionEntryPrintCount()) {
				break;
			}
		}
		if (count < col.size()) {
			System.out.println(spaces2 + "<" + (col.size() - count) + " more ...>");
		}
	}
	
	private static String getSpaces(int level)
	{
		String spaces = "";
		for (int i = 0; i < level; i++) {
			spaces += "   ";
		}
		return spaces;
	}
	
	private static void printObject(List list, Object obj, boolean printLastColumnSpaces)
	{
		Object object = obj;
		if (object == null) {
			object = "null";
		}
		
		if (object instanceof String || object.getClass().isPrimitive() || 
				object.getClass() == Boolean.class ||
				object.getClass() == Byte.class ||
				object.getClass() == Character.class ||
				object.getClass() == Short.class ||
				object.getClass() == Integer.class ||
				object.getClass() == Long.class ||
				object.getClass() == Float.class ||
				object.getClass() == Double.class ||
				object.getClass().isArray() ||
				object instanceof Date)  
		{
			object = getPrintableValue(object);
			if (list.size() > 0) {
				int maxLen = (Integer) list.get(0);
				if (printLastColumnSpaces) {
					System.out.print(StringUtil.getRightPaddedString(object.toString(), maxLen, ' '));
				} else {
					System.out.print(object.toString());
				}
			} else {
				System.out.print(object.toString());
			}

		} else {

			Class cls = object.getClass();
			Method methods[] = cls.getMethods();
			Method method;
			Class retType;
			String name;
			Object value;
			int listIndex = 0;
			for (int i = 0; i < methods.length; i++) {
				method = methods[i];
				name = method.getName();
				if (name.length() <= 3 || name.startsWith("get") == false || name.equals("getClass")) {
					continue;
				}
				retType = method.getReturnType();
				if (retType == Void.TYPE) {
					continue;
				}
				try {
					value = method.invoke(object, (Object[])null);
					value = getPrintableValue(value);

					int maxLen = (Integer) list.get(listIndex);
					if (listIndex == list.size() - 1) {
						if (value == null) {
							if (printLastColumnSpaces) {
								System.out.print(StringUtil.getRightPaddedString("null", maxLen, ' '));
							} else {
								System.out.print("null");
							}
						} else {
							if (printLastColumnSpaces) {
								System.out.print(StringUtil.getRightPaddedString(value.toString(), maxLen, ' '));
							} else {
								System.out.print(value.toString());
							}
						}

					} else {
						if (value == null) {
							System.out.print(StringUtil.getRightPaddedString("null", maxLen, ' '));
						} else {
							System.out.print(StringUtil.getRightPaddedString(value.toString(), maxLen, ' '));
						}
						System.out.print("  ");
					}

					listIndex++;
				} catch (Exception ex) {
				}
			}
		}
	}
}

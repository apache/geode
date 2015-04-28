package com.gemstone.gemfire.internal.tools.gfsh.app.util;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.types.CollectionType;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.cache.query.types.StructType;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.Mappable;
import com.gemstone.gemfire.internal.tools.gfsh.app.misc.util.StringUtil;

public class PrintUtil
{
	private static boolean tableFormat = false;
	
	public static boolean isTableFormat()
	{
		return tableFormat;
	}
	
	public static void setTableFormat(boolean tableFormat)
	{
		PrintUtil.tableFormat = tableFormat;
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
		if (isTableFormat() == false) {
			return SimplePrintUtil.printEntries(region, regionIterator, startIndex, startRowNum, rowCount, keyList);
		}
		
		if (region == null || regionIterator == null) {
			System.out.println("Error: Region is null");
			return 0;
		}
		
		int endIndex = startIndex + rowCount; // exclusive
		if (endIndex >= region.size()) {
			endIndex = region.size();
		}
		

		// Determine max column lengths
		HashSet keyNameSet = new HashSet();
		HashSet valueNameSet = new HashSet();
		ArrayList keyMaxLenList = new ArrayList();
		ArrayList valueMaxLenList = new ArrayList();
		Object key = null;
		Object value = null;
		Set nameSet = region.entrySet();
		int index = startIndex;

		ArrayList<Region.Entry> entryList = new ArrayList();
		for (Iterator itr = regionIterator; index < endIndex && itr.hasNext(); index++) {
			Region.Entry entry = (Region.Entry) itr.next();
			entryList.add(entry);
			key = entry.getKey();
			value = entry.getValue();
			computeMaxLengths(keyMaxLenList, valueMaxLenList, key, value);
			keyNameSet.add(key.getClass().getName());
			if (value != null) {
				valueNameSet.add(value.getClass().getName());
			}
		}

		if (key == null) {
			return 0;
		}

		// Print headers including row column
		int rowMax = String.valueOf(endIndex).length();
		if (rowMax < 3) {
			rowMax = 3;
		}
		printHeaders(keyMaxLenList, valueMaxLenList, key, value, rowMax);

		// Print keys and values
		int row = startRowNum;
		index = startIndex;
		for (Iterator itr = entryList.iterator(); index < endIndex && itr.hasNext(); index++) {

			System.out.print(StringUtil.getRightPaddedString(row + "", rowMax, ' '));
			System.out.print("  ");

			Region.Entry entry = (Region.Entry) itr.next();
			key = entry.getKey();
			value = entry.getValue();
			if (keyList != null) {
				keyList.add(key);
			}
			printObject(keyMaxLenList, key, true);
			System.out.print(" | ");
			printObject(valueMaxLenList, value, false);
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
		if (isTableFormat() == false) {
			return SimplePrintUtil.printEntries(region, keyMap, keyList);
		}
		
		if (region == null) {
			System.out.println("Error: Region is null");
			return 0;
		}

		// Determine max column lengths
		HashSet keyNameSet = new HashSet();
		HashSet valueNameSet = new HashSet();
		ArrayList keyMaxLenList = new ArrayList();
		ArrayList valueMaxLenList = new ArrayList();
		ArrayList indexList = new ArrayList(keyMap.keySet());
		Collections.sort(indexList);
		Object key = null;
		Object value = null;
		for (Iterator iterator = indexList.iterator(); iterator.hasNext();) {
			Object index = iterator.next();
			key = keyMap.get(index);
			value = region.get(key);
			computeMaxLengths(keyMaxLenList, valueMaxLenList, key, value);
			keyNameSet.add(key.getClass().getName());
			if (value != null) {
				valueNameSet.add(value.getClass().getName());
			}
		}
		if (key == null) {
			return 0;
		}

		// Print headers including row column
		int rowCount = keyMap.size();
		int rowMax = String.valueOf(rowCount).length();
		if (rowMax < 3) {
			rowMax = 3;
		}
		printHeaders(keyMaxLenList, valueMaxLenList, key, value, rowMax);

		// Print keys and values
		int row = 1;
		for (Iterator iterator = indexList.iterator(); iterator.hasNext();) {

			System.out.print(StringUtil.getRightPaddedString(row + "", rowMax, ' '));
			System.out.print("  ");

			Object index = iterator.next();
			key = keyMap.get(index);
			value = region.get(key);
			if (keyList != null) {
				keyList.add(key);
			}
			printObject(keyMaxLenList, key, true);
			System.out.print(" | ");
			printObject(valueMaxLenList, value, false);
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
		
		if (isTableFormat() == false) {
			return SimplePrintUtil.printEntries(region, keySet, keyList);
		}
		
		if (region == null) {
			System.out.println("Error: Region is null");
			return 0;
		}

		// Determine max column lengths
		HashSet keyNameSet = new HashSet();
		HashSet valueNameSet = new HashSet();
		ArrayList keyMaxLenList = new ArrayList();
		ArrayList valueMaxLenList = new ArrayList();
		Object key = null;
		Object value = null;
		for (Iterator iterator = keySet.iterator(); iterator.hasNext();) {
			key = iterator.next();
			value = region.get(key);
			computeMaxLengths(keyMaxLenList, valueMaxLenList, key, value);
			keyNameSet.add(key.getClass().getName());
			if (value != null) {
				valueNameSet.add(value.getClass().getName());
			}
		}
		if (key == null) {
			return 0;
		}

		// Print headers including row column
		int rowCount = keySet.size();
		int rowMax = String.valueOf(rowCount).length();
		if (rowMax < 3) {
			rowMax = 3;
		}
		printHeaders(keyMaxLenList, valueMaxLenList, key, value, rowMax);

		// Print keys and values
		int row = 1;
		for (Iterator iterator = keySet.iterator(); iterator.hasNext();) {

			System.out.print(StringUtil.getRightPaddedString(row + "", rowMax, ' '));
			System.out.print("  ");

			key = iterator.next();
			value = region.get(key);
			if (keyList != null) {
				keyList.add(key);
			}
			printObject(keyMaxLenList, key, true);
			System.out.print(" | ");
			printObject(valueMaxLenList, value, false);
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
	
	public static int printEntries(Map map, int startIndex, int startRowNum, int rowCount, int actualSize, List keyList) throws Exception
	{
		if (isTableFormat() == false) {
			return SimplePrintUtil.printEntries(map, startIndex, startRowNum, rowCount, actualSize, keyList);
		}
		
		if (map == null) {
			System.out.println("Error: map is null");
			return 0;
		}

		// Determine max column lengths
		HashSet keyNameSet = new HashSet();
		HashSet valueNameSet = new HashSet();
		ArrayList keyMaxLenList = new ArrayList();
		ArrayList valueMaxLenList = new ArrayList();
		Object key = null;
		Object value = null;
		Set entrySet = map.entrySet();
		int count = 0;
		for (Iterator itr = entrySet.iterator(); count < rowCount && itr.hasNext(); count++) {
			Map.Entry entry = (Map.Entry) itr.next();
			key = entry.getKey();
			value = entry.getValue();
			computeMaxLengths(keyMaxLenList, valueMaxLenList, key, value);
			keyNameSet.add(key.getClass().getName());
			if (value != null) {
				valueNameSet.add(value.getClass().getName());
			}
		}

		if (key == null) {
			return 0;
		}

		// Print headers including row column
		int rowMax = String.valueOf(startRowNum + rowCount - 1).length();
		if (rowMax < 3) {
			rowMax = 3;
		}
		printHeaders(keyMaxLenList, valueMaxLenList, key, value, rowMax);

		// Print keys and values
//		int row = 1;
		count = 0;
		int row = startRowNum;
		int lastRow = startRowNum + rowCount - 1;
		for (Iterator itr = entrySet.iterator(); count < rowCount && itr.hasNext(); count++) {

			System.out.print(StringUtil.getRightPaddedString(row + "", rowMax, ' '));
			System.out.print("  ");

			Map.Entry entry = (Map.Entry) itr.next();
			key = entry.getKey();
			value = entry.getValue();
			if (keyList != null) {
				keyList.add(key);
			}
			printObject(keyMaxLenList, key, true);
			System.out.print(" | ");
			printObject(valueMaxLenList, value, false);
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
	
	public static int printSet(Set set, int rowCount, List keyList) throws Exception
	{
		return printSet(set, rowCount, keyList, true);
	}
	
	public static int printSet(Set set, int rowCount, List keyList, boolean showSummary) throws Exception
	{
		return printSet(set, rowCount, keyList, "Key", showSummary);
	}
	
	public static int printSet(Set set, int rowCount, List keyList, 
			String keyColumnName,
			boolean showSummary) throws Exception
	{
		if (isTableFormat() == false) {
			return SimplePrintUtil.printSet(set, rowCount, keyList, 
					keyColumnName, showSummary);
		}
		
		if (set == null) {
			return 0;
		}

		// Determine max column lengths
		HashSet keyNameSet = new HashSet();
		ArrayList keyMaxLenList = new ArrayList();
		Object key = null;
		Set nameSet = set;
		int count = 0;
		int keyMin = keyColumnName.length();
		for (Iterator itr = nameSet.iterator(); count < rowCount && itr.hasNext(); count++) {
			key = itr.next();
			computeMaxLengths(keyMaxLenList, key, true, keyMin, 0);
			keyNameSet.add(key.getClass().getName());
		}

		if (key == null) {
			return 0;
		}

		// Print headers including row column
		int rowMax = String.valueOf(rowCount).length();
		if (rowMax < 3) {
			rowMax = 3;
		}
		printHeaders(keyMaxLenList, null, key, null, rowMax, keyColumnName, null, false);

		// Print keys and values
		int row = 1;
		count = 0;
		for (Iterator itr = nameSet.iterator(); count < rowCount && itr.hasNext(); count++) {

			System.out.print(StringUtil.getRightPaddedString(row + "", rowMax, ' '));
			System.out.print("  ");

			key = itr.next();
			if (keyList != null) {
				keyList.add(key);
			}
			printObject(keyMaxLenList, key, true);
			System.out.println();
			row++;
		}
		if (showSummary) {
			System.out.println();
			System.out.println("Displayed (fetched): " + (row - 1));
			System.out.println("        Actual Size: " + set.size());
			for (Object keyName : keyNameSet) {
				System.out.println("          " + keyColumnName + " Class: " + keyName);
			}
		}
		return row - 1;
	}
	
	public static int printEntries(Map map, int rowCount, List keyList) throws Exception
	{
		return printEntries(map, rowCount, keyList, true, true);
	}
	
	public static int printEntries(Map map, int rowCount, List keyList, boolean showSummary, boolean showValues) throws Exception
	{
		return printEntries(map, rowCount, keyList, "Key", "Value", showSummary, showValues);
	}
	
	public static int printEntries(Map map, int rowCount, List keyList, 
			String keyColumnName, String valueColumnName, 
			boolean showSummary, boolean showValues) throws Exception
	{
		if (isTableFormat() == false) {
			return SimplePrintUtil.printEntries(map, rowCount, keyList, 
					keyColumnName, valueColumnName, showSummary, showValues);
		}
		
		if (map == null) {
			System.out.println("Error: Region is null");
			return 0;
		}

		// Determine max column lengths
		HashSet keyNameSet = new HashSet();
		HashSet valueNameSet = new HashSet();
		ArrayList keyMaxLenList = new ArrayList();
		ArrayList valueMaxLenList = new ArrayList();
		Object key = null;
		Object value = null;
		Set nameSet = map.entrySet();
		int count = 0;
		int keyMin = keyColumnName.length();
		int valueMin = valueColumnName.length();
		for (Iterator itr = nameSet.iterator(); count < rowCount && itr.hasNext(); count++) {
			Map.Entry entry = (Map.Entry) itr.next();
			key = entry.getKey();
			value = entry.getValue();
			computeMaxLengths(keyMaxLenList, valueMaxLenList, key, value, keyMin, valueMin);
			keyNameSet.add(key.getClass().getName());
			if (value != null) {
				valueNameSet.add(value.getClass().getName());
			}
		}

		if (key == null) {
			return 0;
		}

		// Print headers including row column
		int rowMax = String.valueOf(rowCount).length();
		if (rowMax < 3) {
			rowMax = 3;
		}
		printHeaders(keyMaxLenList, valueMaxLenList, key, value, rowMax, keyColumnName, valueColumnName, showValues);

		// Print keys and values
		int row = 1;
		count = 0;
		for (Iterator itr = nameSet.iterator(); count < rowCount && itr.hasNext(); count++) {

			System.out.print(StringUtil.getRightPaddedString(row + "", rowMax, ' '));
			System.out.print("  ");

			Map.Entry entry = (Map.Entry) itr.next();
			key = entry.getKey();
			value = entry.getValue();
			if (keyList != null) {
				keyList.add(key);
			}
			printObject(keyMaxLenList, key, true);
			if (showValues) {
				System.out.print(" | ");
				printObject(valueMaxLenList, value, false);
			}
			System.out.println();
			row++;
		}
		if (showSummary) {
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
	
	private static void computeMaxLengths(List keyList, List valueList, Object key, Object value)
	{
		computeMaxLengths(keyList, valueList, key, value, 3, 5); // "Key", "Value"
	}

	private static void computeMaxLengths(List keyList, List valueList, Object key, Object value, int keyMin, int valueMin)
	{
		computeMaxLengths(keyList, key, true, keyMin, valueMin);
		computeMaxLengths(valueList, value, false, keyMin, valueMin);
	}

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
			
		} else if (object instanceof Map) {
			
			// Map
			Map map = (Map) object;
			Set set = map.keySet();
			if (set != null) {
			TreeSet keySet = new TreeSet(set);
			int listIndex = 0;
				for (Object header : keySet) {
					int maxLen = (Integer) list.get(listIndex);
					if (listIndex == list.size() - 1) {
						if (printLastColumnSpaces) {
							System.out.print(StringUtil.getRightPaddedString(header.toString(), maxLen, ' '));
						} else {
							System.out.print(header);
						}
					} else {
						System.out.print(StringUtil.getRightPaddedString(header.toString(), maxLen, ' '));
						System.out.print("  ");
					}
	
					listIndex++;
				}
			}
			
		} else {

			// print object
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

		} else if (object instanceof Map) {
			
			// Map
			Map map = (Map) object;
			Set<String> set = map.keySet();
			if (set != null) {
				TreeSet keySet = new TreeSet(set);
				int listIndex = 0;
				for (Object header : keySet) {
					Object value = map.get(header);
					value = getPrintableValue(value);
					int maxLen = (Integer) list.get(listIndex);

					if (listIndex == list.size() - 1) {
						if (printLastColumnSpaces) {
							System.out.print(StringUtil.getRightPaddedString(StringUtil.getRightPaddedString("", header.toString()
									.length(), '-'), maxLen, ' '));
						} else {
							System.out.print(StringUtil.getRightPaddedString("", header.toString().length(), '-'));
						}
					} else {
						System.out.print(StringUtil.getRightPaddedString(StringUtil.getRightPaddedString("", header.toString()
								.length(), '-'), maxLen, ' '));
						System.out.print("  ");
					}
					listIndex++;
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
	
	private static void printHeaders(List keyList, List valueList, 
			Object key, Object value, int rowMaxLen)
			throws Exception
	{
		printHeaders(keyList, valueList, key, value, rowMaxLen, "Key", "Value", true);
	}

	private static void printHeaders(List keyList, List valueList, 
			Object key, Object value, int rowMaxLen, 
			String keyColumnName, String valueColumnName, boolean showValues)
			throws Exception
	{
		System.out.print(StringUtil.getRightPaddedString("Row", rowMaxLen, ' '));
		System.out.print("  ");
		printTopHeaders(keyList, key, true, keyColumnName);
		if (showValues) {
			System.out.print(" | ");
			printTopHeaders(valueList, value, false, valueColumnName);
		}
		System.out.println();

		if (rowMaxLen < 3) {
			rowMaxLen = 3;
		}
		System.out.print(StringUtil.getRightPaddedString("", rowMaxLen, '-'));
		System.out.print("  ");
		printBottomHeaders(keyList, key, true, keyColumnName);
		if (showValues) {
			System.out.print(" | ");
			printBottomHeaders(valueList, value, false, valueColumnName);
		}
		System.out.println();
	}
	
	/**
	 * Prints the SelectResults contents up to the specified rowCount.
	 * @param sr
	 * @param startRowNum
	 * @param rowCount
	 * @return The number of rows printed
	 */
	public static int printSelectResults(SelectResults sr, int startIndex, int startRowNum, int rowCount)
	{
		if (isTableFormat() == false) {
			return SimplePrintUtil.printSelectResults(sr, startIndex, startRowNum, rowCount);
		}
		
		if (sr == null) {
			System.out.println("Error: SelectResults is null");
			return 0;
		}

		int endIndex = startIndex + rowCount; // exclusive
		if (endIndex >= sr.size()) {
			endIndex = sr.size();
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
		List srList = sr.asList();
		
		for (int i = startIndex; i < endIndex; i++) {
			element = srList.get(i);
			if (elementType.isStructType()) {
				structType = (StructType) elementType;
				struct = (Struct) element;
				computeMaxLengths(maxLenList, structType, struct);
				isStructType = true;
			} else {
				computeMaxLengths(maxLenList, element, false);
				if (element != null) {
					elementNameSet.add(element.getClass().getName());
				}
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
		for (int i = startIndex; i < endIndex; i++) {
			element = srList.get(i);

			if (elementType.isStructType()) {

				structType = (StructType) elementType;
				struct = (Struct) element;
				System.out.print(StringUtil.getRightPaddedString(row + "", rowMax, ' '));
				System.out.print("  ");
				printStruct(maxLenList, structType, struct);
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
				printStruct(maxLenList, structType, struct);
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
		computeMaxLengths(list, obj, isKey, 3, 5);
	}

	private static void computeMaxLengths(List list, Object obj, boolean isKey, int keyMin, int valueMin)
	{
		Object object = obj;
		if (obj == null) {
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
					if (object.toString().length() < keyMin) { // Key
						list.add(keyMin);
					} else {
						list.add(object.toString().length());
					}
				} else {
					if (object.toString().length() < valueMin) { // Value
						list.add(valueMin);
					} else {
						list.add(object.toString().length());
					}
				}
			}
			
		} else if (object instanceof Map) {
			
			// Map
			Map map = (Map) object;
			Set set = map.keySet();
			if (set != null) {
				TreeSet keySet = new TreeSet(set);
				int listIndex = 0;
				for (Object name : keySet) {
					Object value = map.get(name);
					value = getPrintableValue(value);
					Integer len;
					if (listIndex >= list.size()) {
						len = name.toString().length();
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

	private static void printStruct(List list, StructType structType, Struct struct)
	{
		ObjectType[] fieldTypes = structType.getFieldTypes();
		String[] fieldNames = structType.getFieldNames();
		Object[] fieldValues = struct.getFieldValues();

		int listIndex = 0;
		for (int i = 0; i < fieldTypes.length; i++) {
			ObjectType fieldType = fieldTypes[i];
			String fieldName = fieldNames[i];
			Object fieldValue = fieldValues[i];
			int maxLen = (Integer) list.get(listIndex);
			if (listIndex == list.size() - 1) {
				if (fieldValue == null) {
					System.out.println("null");
				} else {
					System.out.print(fieldValue.toString());
				}
			} else {
				if (fieldValue == null) {
					System.out.print(StringUtil.getRightPaddedString("null", maxLen, ' '));
				} else {
					System.out.print(StringUtil.getRightPaddedString(fieldValue.toString(), maxLen, ' '));
				}
				System.out.print("  ");
			}
			listIndex++;
		}
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
			
		} else if (object instanceof Map) {
			
			Map map = (Map) object;
			Set  set = map.keySet();
			if (set != null) {
				TreeSet keySet = new TreeSet(set);
				int listIndex = 0;
				for (Object key : keySet) {
					Object value = map.get(key);
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
		if (list == null || list.size() == 0) {
			return 0;
		}
		
		if (isTableFormat() == false) {
			return SimplePrintUtil.printList(list, startIndex, startRowNum, rowCount, actualSize, keyList);
		}

		// Determine max column lengths
		HashSet objectNameSet = new HashSet();
		ArrayList maxLenList = new ArrayList();
		int count = 0;
		Object object = null;
		for (Iterator itr = list.iterator(); count < rowCount && itr.hasNext(); count++) {
			object = itr.next();
			computeMaxLengths(maxLenList, object, true);
			objectNameSet.add(object.getClass().getName());
		}
		
		if (object == null) {
			return 0;
		}

		// Print headers including row column
		int rowMax = String.valueOf(startRowNum + rowCount - 1).length();
		if (rowMax < 3) {
			rowMax = 3;
		}
		printHeaders(maxLenList, object, rowMax);

		// Print keys and values
//		int row = 1;
		count = 0;
		int row = startRowNum;
		int lastRow = startRowNum + rowCount - 1;
		for (Iterator itr = list.iterator(); count < rowCount && itr.hasNext(); count++) {

			System.out.print(StringUtil.getRightPaddedString(row + "", rowMax, ' '));
			System.out.print("  ");

			object = itr.next();
			if (keyList != null) {
				keyList.add(object);
			}
			printObject(maxLenList, object, true);
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
		computeMappableMaxLengths(list, mappable, null);
	}
	
	private static void computeMappableMaxLengths(List list, Mappable mappable, List<String>keyList)
	{
		String name;
		Object value;
		int listIndex = 0;
		if (keyList == null) {
			keyList = new ArrayList(mappable.getKeys());
			Collections.sort(keyList);
		}
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
		printMappableHeaders(list, mappable, rowMaxLen, null);
	}
	
	private static void printMappableHeaders(List list, Mappable mappable, int rowMaxLen, List<String>keyList)
	{
		System.out.print(StringUtil.getRightPaddedString("Row", rowMaxLen, ' '));
		System.out.print("  ");
		printMappableTopHeaders(list, mappable, false, keyList);
		System.out.println();
		if (rowMaxLen < 3) {
			rowMaxLen = 3;
		}
		System.out.print(StringUtil.getRightPaddedString("", rowMaxLen, '-'));
		System.out.print("  ");
		printMappableBottomHeaders(list, mappable, false, keyList);
		System.out.println();
	}
	
//FindBugs - private method never called
//	private static void printMappableTopHeaders(List list, Mappable mappable, 
//			boolean printLastColumnSpaces)
//	{
//		printMappableTopHeaders(list, mappable, printLastColumnSpaces, null);
//	}
	
	private static void printMappableTopHeaders(List list, Mappable mappable, 
			boolean printLastColumnSpaces, List<String>keyList)
	{
		int listIndex = 0;
		if (keyList == null) {
			keyList = new ArrayList(mappable.getKeys());
			Collections.sort(keyList);
		}
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
	
//FindBugs - private method never called
//	private static void printMappableBottomHeaders(List list, Mappable mappable, boolean printLastColumnSpaces)
//	{
//		printMappableBottomHeaders(list, mappable, printLastColumnSpaces, null);
//	}
	
	private static void printMappableBottomHeaders(List list, Mappable mappable, boolean printLastColumnSpaces, List<String> keyList)
	{
		int listIndex = 0;
		if (keyList == null) {
			keyList = new ArrayList(mappable.getKeys());
			Collections.sort(keyList);
		}
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
	
	private static void printMappable(List list, Mappable mappable, boolean printLastColumnSpaces)
	{
		printMappable(list, mappable, printLastColumnSpaces, null);
	}
	
	private static void printMappable(List list, Mappable mappable, boolean printLastColumnSpaces, List<String>keyList)
	{
		int listIndex = 0;
		if (keyList == null) {
			keyList = new ArrayList(mappable.getKeys());
			Collections.sort(keyList);
		}
		for (int i = 0; i < keyList.size(); i++) {
			String name = keyList.get(i);
			Object value = mappable.getValue(name);
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
		}
	}
	
	public static void printMappableList(List<Mappable> resultList)
	{
		printMappableList(resultList, null);
	}
	
	public static void printMappableList(List<Mappable> resultList, String sortByKey)
	{
		List<Mappable> list;
		if (sortByKey != null) {
			TreeMap map = new TreeMap();
			for (int i = 0; i < resultList.size(); i++) {
				Mappable mappable = resultList.get(i);
				map.put(mappable.getValue(sortByKey), mappable);
			}
			list = new ArrayList(map.values());
		} else {
			list = resultList;
		}
		
		if (isTableFormat() == false) {
			SimplePrintUtil.printMappableList(list);
			return;
		}
		
		ArrayList maxLenList = new ArrayList();
		Mappable nonNullMappable = null;
		for (int i = 0; i < list.size(); i++) {
			Mappable mappable = list.get(i);
			if (mappable != null) {
				nonNullMappable = mappable;
			}
			computeMappableMaxLengths(maxLenList, mappable);
		}
		if (nonNullMappable == null) {
			return;
		}

		int rowMax = String.valueOf(list.size()).length();
		if (rowMax < 3) {
			rowMax = 3;
		}
		printMappableHeaders(maxLenList, nonNullMappable, rowMax);
		for (int i = 0; i < list.size(); i++) {
			Mappable mappable = list.get(i);
			System.out.print(StringUtil.getRightPaddedString((i + 1) + "", rowMax, ' '));
			System.out.print("  ");
			printMappable(maxLenList, mappable, false);
			System.out.println();
		}
	}
	
	public static void printMappableList(List<Mappable> resultList, String sortByKey, List<String>keyList)
	{
		List<Mappable> list;
		if (sortByKey != null) {
			TreeMap map = new TreeMap();
			for (int i = 0; i < resultList.size(); i++) {
				Mappable mappable = resultList.get(i);
				map.put(mappable.getValue(sortByKey), mappable);
			}
			list = new ArrayList(map.values());
		} else {
			list = resultList;
		}
		
		if (isTableFormat() == false) {
			SimplePrintUtil.printMappableList(list);
			return;
		}
		
		ArrayList maxLenList = new ArrayList();
		Mappable nonNullMappable = null;
		for (int i = 0; i < list.size(); i++) {
			Mappable mappable = list.get(i);
			if (mappable != null) {
				nonNullMappable = mappable;
			}
			computeMappableMaxLengths(maxLenList, mappable, keyList);
		}
		if (nonNullMappable == null) {
			return;
		}

		int rowMax = String.valueOf(list.size()).length();
		if (rowMax < 3) {
			rowMax = 3;
		}
		printMappableHeaders(maxLenList, nonNullMappable, rowMax, keyList);
		for (int i = 0; i < list.size(); i++) {
			Mappable mappable = list.get(i);
			System.out.print(StringUtil.getRightPaddedString((i + 1) + "", rowMax, ' '));
			System.out.print("  ");
			printMappable(maxLenList, mappable, false, keyList);
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
		return value;
	}
}

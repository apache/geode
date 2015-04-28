package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.QueryResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.QueryTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.misc.util.ReflectionUtil;
import com.gemstone.gemfire.internal.tools.gfsh.app.util.ObjectUtil;
import com.gemstone.gemfire.internal.tools.gfsh.app.util.OutputUtil;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;

/**
 * Bulk copy.
 * @author dpark
 *
 */
public class bcp implements CommandExecutable
{
	private Gfsh gfsh;
	
	public bcp(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("bcp <region path> {in | out} <data file>");
		gfsh.println("    [-k] [-v]");
		gfsh.println("    [-r <row terminator>]");
		gfsh.println("    [-t <field terminator>]");
		gfsh.println("    [-F <first row>]");
		gfsh.println("    [-L <last row>]");
		gfsh.println("    [-b <batch size>]");
		gfsh.println("    [-d <date format>]");
		gfsh.println("    [-f <value key field>]");
		gfsh.println("    Bulk-copy region entries to a file or bulk-copy file contents to region.");
		gfsh.println("    The region path can be absolute or relative. Note that if the region is");
		gfsh.println("    a partitioned region then the 'out' option retrieves data only from");
		gfsh.println("    the local dataset of the connected server due to the potentially large");
		gfsh.println("    size of the partitioend region.");
		gfsh.println("    -k Copy key fields. If both -k and -v are specified then the key field");
		gfsh.println("       takes precedence.");
		gfsh.println("    -v Copy value fields. If both -k and -v are specified then the key field");
		gfsh.println("       takes precedence.");
		gfsh.println("    -r Row terminator character. Default: platform specific.");
		gfsh.println("    -t Field terminator charater. Default: ,");
		gfsh.println("    -F The number of the first row in the file to load.");
		gfsh.println("    -L The number of the last row in the file to load.");
		gfsh.println("    -b The batch size. Default: 1000");
		gfsh.println("    -d The date format (conforms to SimpleDateFormat). Teh format must be");
		gfsh.println("       enclosed in double quotes. Default: \"EEE MMM dd HH:mm:ss zzz yyyy\"");
		gfsh.println("    -f The name of the key field to store or load.");	
		gfsh.println("    Default:");
		gfsh.println("        bcp <region path> out <file> -v -r \\n -t , -F 1 -b 1000 \\");
		gfsh.println("            -d \"EEE MMM dd HH:mm:ss zzz yyyy\"");
		gfsh.println("    Example:");
		gfsh.println("        bcp /prices out price_out -t ,");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("bcp -?")) {
			help();
		} else {
			bcp(command);
		}
	}
	
	private void bcp(String command) throws Exception
	{
		ArrayList<String> list = new ArrayList();
		gfsh.parseCommand(command, list);
		if (list.size() < 4) {
			gfsh.println("Error: incomplete bcp command. Run bcp -? for help.");
			return;
		} 
		
		// Parse command inputs
		String regionPath = list.get(1);
		String directionType = list.get(2);
		String filePath = list.get(3);
		
		// region 
		Cache cache = CacheFactory.getAnyInstance();
		String fullPath = gfsh.getFullPath(regionPath, gfsh.getCurrentPath());
		Region region = cache.getRegion(fullPath);
		if (region == null) {
			gfsh.println("Error: region undefined - " + fullPath);
			return;
		}
		
		// in | out
		boolean isIn = false;
		if (directionType.equalsIgnoreCase("in")) {
			isIn = true;
		} else if (directionType.equalsIgnoreCase("out")) {
			isIn = false;
		} else {
			gfsh.println("Error: invalid direction type - " + directionType);
			return;
		}
		
		// file
		File file = new File(filePath);
		if (isIn) {
			if (file.exists() == false) {
				gfsh.println("Error: input file does not exist - " + file.getAbsolutePath());
				return;
			}
		} else {
			if (file.exists()) {
				gfsh.println("Error: output file already exists - " + file.getAbsolutePath());
				return;
			} 
		}
		
		// options
		String fieldTerminator = ",";
		String rowTerminator = "\n";
		int firstRow = 1;
		int lastRow = -1;
		int batchSize = 1000;
		SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy"); 
		String valueKeyFieldName = null;
		boolean printKeys = false;
		boolean printValues = false;
		for (int i = 0; i < list.size(); i++) {
			String val = list.get(i);
			if (val.equals("-t")) {
				i++;
				if (list.size() > i) {
					fieldTerminator = list.get(i);
				}
			} else if (val.equals("-r")) {
				i++;
				if (list.size() > i) {
					rowTerminator = list.get(i);
					rowTerminator = "\r\n";
				}
			} else if (val.equals("-F")) {
				i++;
				if (list.size() > i) {
					val = list.get(i);
					firstRow = Integer.parseInt(val);
				}
			} else if (val.equals("-L")) {
				i++;
				if (list.size() > i) {
					val = list.get(i);
					lastRow = Integer.parseInt(val);
				}
			} else if (val.equals("-b")) {
				i++;
				if (list.size() > i) {
					val = list.get(i);
					batchSize = Integer.parseInt(val);
				}
			} else if (val.equals("-d")) {
				i++;
				if (list.size() > i) {
					String str = list.get(i);
					dateFormat = new SimpleDateFormat(str);
				}
			} else if (val.equals("-f")) {
				i++;
				if (list.size() > i) {
					valueKeyFieldName = list.get(i);
				}
			} else if (val.equals("-k")) {
				printKeys = true;
			} else if (val.equals("-v")) {
				printValues = true;
			}
		}
		
		if (isIn) {
			bcpIn(region, file, fieldTerminator, rowTerminator, firstRow, lastRow, batchSize, valueKeyFieldName);
		} else {
			if (printKeys == false && printValues == false) {
				printValues = true;
			}
			bcpOut(fullPath, file, fieldTerminator, rowTerminator, firstRow, lastRow, batchSize, printKeys, printValues, dateFormat, valueKeyFieldName);
		}
	}
	
	private void bcpIn(Region region, File file, 
			String fieldTerminator, String rowTerminator, 
			int firstRow, int lastRow, int batchSize, String valueKeyFieldName) throws Exception
	{
		BufferedReader reader = new BufferedReader(new FileReader(file));
		try {
			bcpIn(region, reader, file, fieldTerminator, rowTerminator, firstRow, lastRow, batchSize, valueKeyFieldName);
		} finally {
			reader.close();
		}
	}
	
	private void bcpIn(Region region, BufferedReader reader, File file,
			String fieldTerminator, String rowTerminator, 
			int firstRow, int lastRow, int batchSize, String valueKeyFieldName) throws Exception
	{
		if (lastRow < 0) {
			lastRow = Integer.MAX_VALUE;
		}
		
		Class keyClass = null;
		Class valueClass = null;
		String keyMethodNames[] = new String[0];
		String valueMethodNames[] = null;
		Map keySetterMap = null;
		Map valueSetterMap = null;
		Object key = null;
		Object value = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
		Method valueKeyGetterMethod = null;
		
		String line = reader.readLine();
		String split[];
		HashMap map = new HashMap();
		int count = 0;
		int rowCount = 0;
		
		long startTime = System.currentTimeMillis();
		while (line != null) {
			line = line.trim();
			if (line.length() == 0) {
				line = reader.readLine();
				continue;
			}
			
			// comment
			if (line.startsWith("#")) {
				if (line.startsWith(OutputUtil.TAG_KEY)) {
					int endIndex = line.indexOf(fieldTerminator);
					String keyClassName;
					if (endIndex == -1) {
						keyClassName = line.substring(5);
					} else {
						keyClassName = line.substring(5, endIndex);
					}
					keyClassName = keyClassName.trim();
					keyClass = Class.forName(keyClassName);
				} else if (line.startsWith(OutputUtil.TAG_VALUE_KEY)) {
					if (valueKeyFieldName == null) {
						int endIndex = line.indexOf(fieldTerminator);
						String dateFormatString;
						if (endIndex == -1) {
							valueKeyFieldName = line.substring(11);
						} else {
							valueKeyFieldName = line.substring(11, endIndex);
						}
					}
					valueKeyFieldName = valueKeyFieldName.trim();
					Map valueGetterMap = ReflectionUtil.getAllGettersMap(valueClass);
					valueKeyGetterMethod = (Method)valueGetterMap.get("get" + valueKeyFieldName);
				} else if (line.startsWith(OutputUtil.TAG_VALUE)) {
					int endIndex = line.indexOf(fieldTerminator);
					String valueClassName;
					if (endIndex == -1) {
						valueClassName = line.substring(7);
					} else {
						valueClassName = line.substring(7, endIndex);
					}
					valueClassName = valueClassName.trim();
					valueClass = Class.forName(valueClassName);
				} else if (line.startsWith(OutputUtil.TAG_DATE_FORMAT)) {
					int endIndex = line.indexOf(fieldTerminator);
					String dateFormatString;
					if (endIndex == -1) {
						dateFormatString = line.substring(13);
					} else {
						dateFormatString = line.substring(13, endIndex);
					}
					dateFormatString = dateFormatString.trim();
					dateFormat = new SimpleDateFormat(dateFormatString);
				} else if (line.startsWith(OutputUtil.TAG_COLUMN_SEPARATOR)) {
					// header
					String keyHeader = null;
					String valueHeader = null;
					String header = line.substring(2);
					int index = header.indexOf(OutputUtil.TAG_COLUMN_SEPARATOR);
					if (index != -1) {
						// key & value
						if (keyClass != null) {
							keyHeader = header.substring(0, index);
							keyMethodNames = keyHeader.split(",");
							for (int i = 0; i < keyMethodNames.length; i++) {
								keyMethodNames[i] = "set" + keyMethodNames[i];
							}
							keySetterMap = ReflectionUtil.getAllSettersMap(keyClass);
						}
						index = index + 2;
					} else {
						index = 0;
					}
					
					if (valueClass != null) {
						valueHeader = header.substring(index);
						valueMethodNames = valueHeader.split(",");
						for (int i = 0; i < valueMethodNames.length; i++) {
							valueMethodNames[i] = "set" + valueMethodNames[i];
						}
						valueSetterMap = ReflectionUtil.getAllSettersMap(valueClass);
					}
				}
			} else {

				count++;
				if (firstRow <= count && count <= lastRow) {
					// data
					String tokens[] = getTokens(line);
					int j = 0;
					if (keyClass != null) {
						key = updateObject(keyClass, keySetterMap, keyMethodNames, tokens, 0, dateFormat);
					}
					if (valueClass != null) {
						value = updateObject(valueClass, valueSetterMap, valueMethodNames, tokens, keyMethodNames.length, dateFormat);
					}
					if (keyClass == null && valueKeyGetterMethod != null) {
						key = valueKeyGetterMethod.invoke(value, (Object[])null);
					}
					
					if (key == null) {
						gfsh.println("Error: key is undefined. Use the option '-f' to specify the column (field) name.");
						return;
					}
					map.put(key, value);
					if (map.size() == batchSize) {
						region.putAll(map);
						rowCount+= map.size();
						map.clear();
					}

				} else if (count > lastRow) {
					break;
				}
			}
			
			line = reader.readLine();
		}
		
		if (map.size() > 0) {
			region.putAll(map);
			rowCount+= map.size();
		}
		long stopTime = System.currentTimeMillis();
		
		gfsh.println("bcp in complete");
		gfsh.println("       To (region): " + region.getFullPath());
		gfsh.println("       From (file): " + file.getAbsolutePath());
		gfsh.println("         Row count: " + rowCount);
		if (gfsh.isShowTime()) {
			gfsh.println("    elapsed (msec): " + (stopTime - startTime));
		}
	}
	
	private Object updateObject(Class clazz, 
			Map<String, Method> setterMap,
			String[] setterMethodNames, 
			String[] tokens, 
			int startTokenIndex, 
			SimpleDateFormat dateFormat) throws Exception
	{
		String value = tokens[startTokenIndex];
		if (clazz == byte.class || clazz == Byte.class) {
			return Byte.parseByte(value);
		} else if (clazz == char.class || clazz == Character.class) {
			return value.charAt(0);
		} else if (clazz == short.class || clazz == Short.class) {
			return Short.parseShort(value);
		} else if (clazz == int.class || clazz == Integer.class) {
			return Integer.parseInt(value);
		} else if (clazz == long.class || clazz == Long.class) {
			return Long.parseLong(value);
		} else if (clazz == float.class || clazz == Float.class) {
			return Float.parseFloat(value);
		} else if (clazz == double.class || clazz == Double.class) {
			return Double.parseDouble(value);
		} else if (clazz == Date.class) {
			return dateFormat.parse(value);
		} else if (clazz == String.class) {
			return value;
		}
		Object obj = clazz.newInstance();
		for (int i = 0; i < setterMethodNames.length; i++) {
			ObjectUtil.updateObject(gfsh, setterMap, obj, setterMethodNames[i], tokens[startTokenIndex + i], dateFormat, true);
		}
		return obj;
	}
	
	
	private static String[] getTokens(String line)
	{
		// HBAN,23.82,300,23.79,800,"Thu, ""test"", 'helo' Jun 08 09:41:19 EDT 2006",99895,1094931009,82,99895,8,HBAN
		ArrayList list = new ArrayList();
		boolean openQuote = false;
		String value = "";
		for (int i = 0; i < line.length(); i++) {
			char c = line.charAt(i);
			
			if (c == ',') {
				if (openQuote == false) {
					value = value.trim();
					if (value.startsWith("\"") && value.indexOf(" ") != -1) {
						value = value.substring(1);
						if (value.endsWith("\"")) {
							value = value.substring(0, value.length() - 1);
						}
					}
					
					list.add(value);
					value = "";
					continue;
				}
			} else if (c == '"') {
				openQuote = !openQuote;
			} 
			value += c;
		}
		list.add(value);
		return (String[])list.toArray(new String[0]);
	}
	
	private void bcpOut(String regionPath, File file, 
			String fieldTerminator, String rowTerminator, 
			int firstRow, int lastRow, int batchSize,
			boolean printKeys, boolean printValues,
			SimpleDateFormat dateFormat,
			String valueKeyFieldName) throws IOException
	{
		PrintWriter writer = new PrintWriter(file);
		try {
			bcpOut(regionPath, writer, file, fieldTerminator, rowTerminator, firstRow, lastRow, batchSize, printKeys, printValues, dateFormat, valueKeyFieldName);
		} finally {
			writer.close();
		}
	}
	
	private void bcpOut(String regionPath, PrintWriter writer, File file, 
			String fieldTerminator, String rowTerminator, 
			int firstRow, int lastRow, int batchSize,
			boolean printKeys, boolean printValues,
			SimpleDateFormat dateFormat,
			String valueKeyFieldName) throws IOException
	{
		int totalPrinted = 0;
		int actualSize = 0;
		boolean printHeader = true;
		int printType = OutputUtil.TYPE_VALUES;
		if (printKeys && printValues) {
			printType = OutputUtil.TYPE_KEYS_VALUES;
		} else if (printKeys) {
			printType = OutputUtil.TYPE_KEYS;
		}
		String taskRegionPath = regionPath;
		
		long startTime = System.currentTimeMillis();
		do {
			CommandResults cr = gfsh.getCommandClient().execute(new QueryTask(taskRegionPath, batchSize, true));
			if (cr.getCode() == QueryTask.ERROR_QUERY) {
				gfsh.println(cr.getCodeMessage());
				return;
			}
			QueryResults results = (QueryResults) cr.getDataObject();
			if (results == null || results.getResults() == null) {
				gfsh.println("No results");
				return;
			}
			
			Map map = (Map)results.getResults();
			if (map.size() == 0) {
				gfsh.println("Region empty. File not created.");
				writer.close();
				file.delete();
				return;
			}
			OutputUtil.printEntries(writer, map, fieldTerminator, rowTerminator, firstRow, lastRow, printType, printHeader, dateFormat, valueKeyFieldName);
			totalPrinted += map.size();
			actualSize = results.getActualSize();
			taskRegionPath = null;
			printHeader = false;
		} while (totalPrinted < actualSize);
		
		long stopTime = System.currentTimeMillis();
		writer.close();
		
		gfsh.println("bcp out complete");
		gfsh.println("   From (region): " + regionPath);
		gfsh.println("       To (file): " + file.getAbsolutePath());
		gfsh.println("       Row count: " + totalPrinted);
		if (gfsh.isShowTime()) {
			gfsh.println("  elapsed (msec): " + (stopTime - startTime));
		}
		
	}
	
	public static void main(String args[]) {
		
		String line = "H\"B\"AN,23.82,300,23.79,800,\"Thu, \"test\", 'helo' Jun 08 09:41:19 EDT 2006\",99895,1094931009,82,99895,8,HBAN";
		String tokens[] = getTokens(line);
		System.out.println(line);
		for (int i = 0; i < tokens.length; i++) {
			System.out.print(tokens[i] + ",");
		}
		System.out.println();
		for (int i = 0; i < tokens.length; i++) {
			System.out.println(tokens[i]);
		}
	}
}

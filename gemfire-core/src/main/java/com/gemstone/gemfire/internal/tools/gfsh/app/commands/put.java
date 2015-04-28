package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.lang.reflect.Method;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.misc.util.ReflectionUtil;
import com.gemstone.gemfire.internal.tools.gfsh.app.util.ObjectUtil;
import com.gemstone.gemfire.internal.tools.gfsh.app.util.PrintUtil;

public class put implements CommandExecutable
{
	private Gfsh gfsh;
	
	public put(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("put [-k] [-v] | [-?] (<key1>,<value1>)(<key2>,<value2>)...");
		gfsh.println("put (<key1>,<value1>)(<key2>,<value2>)...");
		gfsh.println("put -k (<key num1>,<value1>)(<key num2>,<value2>)...");
		gfsh.println("put -v (<key1>,<value num1>)(<key2>,<value num2>)...");
		gfsh.println("put -k -v (<key num1>,<value num1>)(<key num2>,<value num2>)...");
		gfsh.println("     Put entries in both local and remote regions.");
		gfsh.println("     All changes will be reflected in the server(s) also.");
		gfsh.println("     Keys are enumerated when one of the following commands");
		gfsh.println("     is executed: " + gfsh.getEnumCommands());
		gfsh.println();
		gfsh.println("     <key> and <value> support primitive, String, and java.util.Date");
		gfsh.println("     types. These types must be specifed with special tags as follows:");
		gfsh.println("         <decimal>b|B - Byte      (e.g., 1b)");
		gfsh.println("         <decimal>c|C - Character (e.g., 1c)");
		gfsh.println("         <decimal>s|S - Short     (e.g., 12s)");
		gfsh.println("         <decimal>i|I - Integer   (e.g., 15 or 15i)");
		gfsh.println("         <decimal>l|L - Long      (e.g., 20l)");
		gfsh.println("         <decimal>f|F - Float     (e.g., 15.5 or 15.5f)");
		gfsh.println("         <decimal>d|D - Double    (e.g., 20.0d)");
		gfsh.println("         '<string with \\ delimiter>' (e.g., '\\'Wow!\\'!' Hello, world')");
		gfsh.println("         to_date('<date string>', '<simple date format>')");
		gfsh.println("                       (e.g., to_date('04/10/2009', 'MM/dd/yyyy')");
		gfsh.println();
		gfsh.println("     If a suffix letter is not specifed then it is read as Integer");
		gfsh.println("     unless the decimal point or the letter 'e' or 'E' is specified,");
		gfsh.println("     in which case, it is read as Double. Note that if the <key> or");
		gfsh.println("     <value> class is used then a suffix letter is *not* required.");
		gfsh.println();
		gfsh.println("     <key> The key class defined by the 'key' command is used");
		gfsh.println("           to construct the key object.");
		gfsh.println("     <value> The value class defined by the 'value' command is used");
		gfsh.println("           to construct the value object.");
		gfsh.println("     The <key> and <value> objects are created using the following");
		gfsh.println("     format:");
		gfsh.println("         <property name1>=<property value1> and ");
		gfsh.println("         <property name2>=<property value2> and ...");
		gfsh.println();
		gfsh.println("     -k Put enumerated keys. If this option is not specified, then");
		gfsh.println("        <key> is expected.");
		gfsh.println("     -v Put enumerated values. If this option is not specified, then");
		gfsh.println("        <value> is expected.");
		gfsh.println();
		gfsh.println("     Examples:");
		gfsh.println("        put (15L, to_date('04/10/2009', 'MM/dd/yyyy')");
		gfsh.println("        put ('GEMSTONE', Price=125.50 and Date=to_date('04/09/2009',\\");
		gfsh.println("             'MM/dd/yyyy')");
		gfsh.println("        put -k -v (1, 5)  - puts the enum key 1 with the enum 5 value");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("put -?")) {
			help();
		} else {
			put(command);
		}
	}
	
	private void put(String command) throws Exception
	{
		LinkedList<String> list = new LinkedList();
		gfsh.parseCommand(command, list);
		
		boolean keyEnumerated = false;
		boolean valueEnumerated = false;
		
		String val;
		int keyIndex = 0;
		
		for (int i = 1; i < list.size(); i++) {
			val = list.get(i);
			if (val.equals("-k")) {
				keyEnumerated = true;
			} else if (val.equals("-v")) {
				valueEnumerated = true;
			} else {
				keyIndex = i;
				break;
			}
		}
		
		Region region = gfsh.getCurrentRegion();
		String numbers;
		Object key;

		if (region != null) {
      Map map = getEntryMap(list, keyEnumerated, valueEnumerated,
          keyIndex);
      region.putAll(map);
      PrintUtil.printEntries(region, map.keySet(), null);
    } else {
      gfsh.println("Error: Please 'cd' to the required region to perform put.");
    }
	}
	
	/**
	 * Returns the index of the enclosed parenthesis, i.e., ')'.
	 * @param buffer
	 * @param startIndex
	 * @return the index of the enclosed parenthesis
	 */
	private int getEnclosingParenthesis(StringBuffer buffer, int startIndex)
	{
		int enclosedIndex = -1;
		int parenCount = 0;
		boolean inQuote = false;
		// to_date('04/09/2009', 'MM/dd/yyyy')
		for (int i = startIndex; i < buffer.length(); i++) {
			char c = buffer.charAt(i);
			if (c == '(') {
				if (inQuote == false) {
					parenCount++;
				}
			} else if (c == ')') {
				if (inQuote == false) {
					parenCount--;
				}
				if (parenCount == 0) {
					enclosedIndex = i;
					break;
				}
			} else if (c == '\'') {
				inQuote = !inQuote;
			} 
		}
		return enclosedIndex;
	}
	
	private Map getEntryMap(List<String> list, boolean keyEnumerated, boolean valueEnumerated, int startIndex) throws Exception
	{
		String pairs = "";
		for (int i = startIndex; i < list.size(); i++) {
			pairs += list.get(i) + " ";
		}
		
		Map<String, Method> keySetterMap = ReflectionUtil.getAllSettersMap(gfsh.getQueryKeyClass());
		Map<String, Method> valueSetterMap = ReflectionUtil.getAllSettersMap(gfsh.getValueClass());
		Region region = gfsh.getCurrentRegion();
		
		// (x='1,2,3' and y='2',a='hello, world' and b='test')
		HashMap map = new HashMap();
		StringBuffer buffer = new StringBuffer(pairs);
		boolean keySearch = false;
		boolean fieldSearch = false;
		boolean openQuote = false;
		boolean delimiter = false;
		boolean quoted = false;
		boolean openParen = false;
		String fieldString = "";
		String valueString = "";
		String and = "";
		
		Object key = null;
		Object value = null;
		for (int i = 0; i < buffer.length(); i++) {
			char c = buffer.charAt(i);
			if (c == '(') {
				if (openQuote == false) {
				
					String function = null;
					String functionCall = null;
					String functionString = null;
					if (valueString.length() > 0) {
						functionString = valueString;
					} else if (fieldString.length() > 0) {
						functionString = fieldString;
					}
					if (functionString != null) {
	
						// it's a function
	
						// get enclosed parenthesis
						int enclosedIndex = getEnclosingParenthesis(buffer, i);
						
						function = functionString.toLowerCase();
						if (enclosedIndex == -1) {
							throw new ParseException("Malformed function call: " + function, i);
						} 
						
						functionCall = function + buffer.substring(i, enclosedIndex+1);
						if (gfsh.isDebug()) {
							gfsh.println("functionCall = |" + functionCall + "|");
						}
						i = enclosedIndex;
					}
					if (functionCall != null) {
						if (valueString.length() > 0) {
							valueString = functionCall;
						} else if (fieldString.length() > 0) {
							fieldString = functionCall;
						}
									
					} else {
						key = null;
						value = null;
						keySearch = true;
						fieldSearch = true;
						fieldString = "";
						valueString = "";
					}
					
					quoted = false;
	
					continue;
				}
				
			} else if (c == '=') {
				if (keySearch && key == null && keyEnumerated == false) {
				  if (gfsh.getQueryKeyClass() == null) {
            throw new ClassNotFoundException("Undefined key class. Use the 'key' command to set the class name");
          }
					key = gfsh.getQueryKeyClass().newInstance();
				}
				if (keySearch == false && value == null && valueEnumerated == false) {
					if (gfsh.getValueClass() == null) {
						throw new ClassNotFoundException("Undefined value class. Use the 'value' command to set the class name");
					}
					value = gfsh.getValueClass().newInstance();
				}
				fieldSearch = false;
				continue;
			} else if (c == ')') {
				if (openQuote == false) {
					if (gfsh.isDebug()) {
						gfsh.println("v: field = " + fieldString);
						gfsh.println("v: value = " + valueString);
						gfsh.println();
					}
					if (valueEnumerated) {
						Object k = gfsh.getKeyFromKeyList(Integer.parseInt(fieldString));
						if (k == null) {
							gfsh.println("Error: value not found in the cache for the key number " + fieldString);
							gfsh.println("       run 'key -l' to view the enumerated keys.");
							map.clear();
							break;
						}
						value = region.get(k);
						if (key == null) {
							gfsh.println("Error: value not in the cache - " + fieldString);
							map.clear();
							break;
						} 
						if (gfsh.isDebug()) {
							gfsh.println("k = " + k);
							gfsh.println("key = " + key);
							gfsh.println("value = " + value);
						}
					} else {
						if (valueString.length() == 0) {
							// primitive
							value = ObjectUtil.getPrimitive(gfsh, fieldString, quoted);
						} else {
							updateObject(valueSetterMap, value, fieldString, valueString);
						}
					}
					
					map.put(key, value);
					
					fieldSearch = true;
					quoted = false;
					fieldString = "";
					valueString = "";
					key = null;
					and = "";
					continue;
				}
			} else if (c == '\\') {
				// ignore and treat the next character as a character
				delimiter = true;
				continue;
			} else if (c == '\'') {
				if (delimiter) {
					delimiter = false;
				} else {
					if (openQuote) {
						quoted = true;
					}
					openQuote = !openQuote;
					continue;
				}
			} else if (c == ' ') {
				if (openQuote == false) {
					boolean andExpected = false;
					if (keySearch) {
						if (gfsh.isDebug()) {
							gfsh.println("k: field = " + fieldString);
							gfsh.println("k: value = " + valueString);
							gfsh.println();
						}
						if (fieldString.length() > 0) {
							updateObject(keySetterMap, key, fieldString, valueString);
							andExpected = true;
						}
					} else {
						if (gfsh.isDebug()) {
							gfsh.println("v: field = " + fieldString);
							gfsh.println("v: value = " + valueString);
							gfsh.println();
						}
						if (fieldString.length() > 0) {
							updateObject(valueSetterMap, value, fieldString, valueString);
							andExpected = true;
						}
					}
					
					if (andExpected) {
						and = "";
						int index = -1;
						for (int j = i; j < buffer.length(); j++) {
							and += buffer.charAt(j);
							and = and.trim().toLowerCase();
							if (and.equals("and")) {
								index = j;
								break;
							} else if (and.length() > 3) {
								break;
							}
						}
						if (index != -1) {
							i = index;
						}
					}
					
					fieldSearch = true;
					fieldString = "";
					valueString = "";
					and = "";
					quoted = false;
					continue;
				}
			}
			
			if (c == ',') {
				
				// if ',' is not enclosed in quotes...
				if (openQuote == false) {
					
					fieldString = fieldString.trim();
					valueString = valueString.trim();
					
					// end of key
					if (gfsh.isDebug()) {
						gfsh.println("k: field = " + fieldString);
						gfsh.println("k: value = " + valueString);
						gfsh.println();
					}
					
					if (keySearch) {
						if (keyEnumerated) {
							key = gfsh.getKeyFromKeyList(Integer.parseInt(fieldString));
							if (key == null) {
								gfsh.println("Error: value not found in the cache for the key number " + fieldString);
								gfsh.println("       run 'key -l' to view the enumerated keys.");
								map.clear();
								break;
							}
						} else {
							if (valueString.length() == 0) {
								key = ObjectUtil.getPrimitive(gfsh, fieldString, quoted);
							} else {
								updateObject(keySetterMap, key, fieldString, valueString);
							}
						}
					} else {
						
						if (valueEnumerated) {
							Object k = gfsh.getKeyFromKeyList(Integer.parseInt(fieldString));
							value = region.get(k);
							if (value == null) {
								gfsh.println("Error: undefined value num " + fieldString);
								map.clear();
								break;
							}
						} else {
							if (valueString.length() == 0) {
								value = ObjectUtil.getPrimitive(gfsh, fieldString, quoted);
							} else {
								
								updateObject(valueSetterMap, value, fieldString, valueString);
							}
						}
						
					}
					
					fieldSearch = true;
					keySearch = false;
					quoted = false;
					fieldString = "";
					valueString = "";
					and = "";
					continue;
				}	
			} 
			
			if (fieldSearch) {
				fieldString += c;
			} else if (quoted == false) {
				valueString += c;
			}
		}
		
		return map;
	}
	
	private Object getFunctionValue(String functionCall) throws ParseException
	{
		if (functionCall.startsWith("to_date")) {
			return gfsh.getDate(functionCall);
		}
		return null;
	}
	
	private void updateObject(Map<String, Method> setterMap, Object obj, String field, String value) throws Exception
	{
		String setterMethodName = "set" + field.trim();
		Method setterMethod = setterMap.get(setterMethodName);
		if (setterMethod == null) {
			return;
		}
		
		Class types[] = setterMethod.getParameterTypes();
		Class arg = types[0];
		if (arg == byte.class || arg == Byte.class) {
			setterMethod.invoke(obj, Byte.parseByte(value));
		} else if (arg == char.class || arg == Character.class) {
			setterMethod.invoke(obj, value.charAt(0));
		} else if (arg == short.class || arg == Short.class) {
			setterMethod.invoke(obj, Short.parseShort(value));
		} else if (arg == int.class || arg == Integer.class) {
			setterMethod.invoke(obj, Integer.parseInt(value));
		} else if (arg == long.class || arg == Long.class) {
			setterMethod.invoke(obj, Long.parseLong(value));
		} else if (arg == float.class || arg == Float.class) {
			setterMethod.invoke(obj, Float.parseFloat(value));
		} else if (arg == double.class || arg == Double.class) {
			setterMethod.invoke(obj, Double.parseDouble(value));
		} else if (arg == Date.class) {
			Date date = gfsh.getDate(value);
			if (date == null) {
				gfsh.println("   Unable to parse date.");
			} else {
				setterMethod.invoke(obj, date);
			}
		} else if (arg == String.class) {
			setterMethod.invoke(obj, value);
		} else {
			gfsh.println("   Unsupported type: " + setterMethod.getName() + "(" + arg.getName() + ")");
			return;
		}
	}

	public static void main(String[] args) throws Exception
	{
		String command = "put (x=123 and y='2' and z=123, a='hello, world' and b=12)(x='abc' and y='de' and z=456, a='test1' and b=99')";
		ArrayList list = new ArrayList();
		Gfsh gfsh = new Gfsh(new String[0]);
		put p = new put(gfsh);
		p.getEntryMap(list, false, false, 1);	
	}
}

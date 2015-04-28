package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;

import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.misc.util.ReflectionUtil;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class show implements CommandExecutable
{
	private Gfsh gfsh;
	
	public show(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("show [-p [true|false]]");
		gfsh.println("     [-t [true|false]]");
		gfsh.println("     [-c|-k|-v]");
		gfsh.println("     [-table [true|false]");
		gfsh.println("     [-type [true|false]");
		gfsh.println("     [-col <collection entry print count>]");
		gfsh.println();
		gfsh.println("     [-?]");
		gfsh.println("     Show or toggle settings.");
		gfsh.println("     <no option> Show all current settings.");
		
		gfsh.println("     -p Toggle print. If enabled, results are printed to stdout.");
		gfsh.println("     -t Toggle the time taken to execute each command.");
		gfsh.println("     -c Show configuration");
		gfsh.println("     -k Show key class fields. Use the 'key' command to set key class.");
		gfsh.println("     -v Show value class fields. Use the 'value' command to set value class.");
		gfsh.println("     -table Set the print format to the tabular or catalog form. The");
		gfsh.println("         tabular form prints in a table with a column header. The catalog");
		gfsh.println("         form prints in each row in a data structure form.");
		gfsh.println("     -type Enable or disable printing the data type. This option is");
		gfsh.println("         valid only for the '-table false' option.");
		gfsh.println("     -col <collection entry print count> In the catalog mode, gfsh");
		gfsh.println("         prints the contents of Map and Collection objects. By default, it");
		gfsh.println("         prints 5 entries per object. Use this option to change the count.");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("show -?")) {
			help();
		} else if (command.startsWith("show -table")) {
			show_table(command);
		} else if (command.startsWith("show -type")) {
			show_type(command);
		} else if (command.startsWith("show -col")) {
			show_count(command);
		} else if (command.startsWith("show -p")) {
			show_p(command);
		} else if (command.startsWith("show -t")) {
			show_t(command);
		} else if (command.startsWith("show -c")) {
			show_c();
		} else if (command.startsWith("show -k")) {
			show_k();
		} else if (command.startsWith("show -v")) {
			show_v();
		} else {
			show();
		}
	}
	
	private void show_table(String command) throws Exception
	{
		ArrayList<String> list = new ArrayList();
		gfsh.parseCommand(command, list);
		if (list.size() >= 3) {
			boolean enable = list.get(2).equalsIgnoreCase("true");
			gfsh.setTableFormat(enable);
		} else {
			gfsh.setTableFormat(!gfsh.isTableFormat());
		}
		gfsh.println("show -table is " + (gfsh.isTableFormat() ? "true" : "false"));
		
	}
	
	private void show_type(String command) throws Exception
	{
		ArrayList<String> list = new ArrayList();
		gfsh.parseCommand(command, list);
		if (list.size() >= 3) {
			boolean enable = list.get(2).equalsIgnoreCase("true");
			gfsh.setPrintType(enable);
		} else {
			gfsh.setPrintType(!gfsh.isPrintType());
		}
		gfsh.println("show -type is " + (gfsh.isPrintType() ? "true" : "false"));
		
	}

	private void show_count(String command) throws Exception
	{
		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		if (list.size() < 3) {
			gfsh.println("Error: must specify <collection entry print count>. Current count is " + gfsh.getCollectionEntryPrintCount());
			return;
		}
		try {
			int count = Integer.parseInt((String)list.get(2));
			gfsh.setCollectionEntryPrintCount(count);
		} catch (Exception ex) {
			gfsh.println("Error: " + ex.getClass().getSimpleName() + " - " + ex.getMessage());
		}
	}
	
	public void show_p(String command)
	{
		ArrayList<String> list = new ArrayList();
		gfsh.parseCommand(command, list);
		if (list.size() >= 3) {
			boolean enable = list.get(2).equalsIgnoreCase("true");
			gfsh.setShowResults(enable);
		} else {
			gfsh.setShowResults(!gfsh.isShowResults());
		}
		gfsh.println("show -p is " + (gfsh.isShowResults() ? "true" : "false"));
	}

	private void show_t(String command) throws Exception
	{
		ArrayList<String> list = new ArrayList();
		gfsh.parseCommand(command, list);
		if (list.size() >= 3) {
			boolean enable = list.get(2).equalsIgnoreCase("true");
			gfsh.setShowResults(enable);
		} else {
			gfsh.setShowTime(!gfsh.isShowTime());
		}
		gfsh.println("show -t is " + (gfsh.isShowTime() ? "true" : "false"));
	}
	
	@SuppressFBWarnings(value="NM_METHOD_CONSTRUCTOR_CONFUSION",justification="This is method and not constructor")
	public void show()
	{
		show_c();
		gfsh.println();
		show_k();
		gfsh.println();
		show_v();
	}
	
	public void show_c()
	{
		db dbCommand = (db)gfsh.getCommand("db");
//		String dbInit = dbCommand.getDbInitCommand();
		
		gfsh.println("     connected = " + gfsh.isConnected());
		if (dbCommand/*dbInit*/ != null) {
			gfsh.println("            db = " + /*dbInit*/dbCommand.getDbInitCommand());	
		}
		gfsh.println("          echo = " + gfsh.isEcho());
		if (gfsh.getEndpoints() == null && gfsh.getEndpoints() == null) {
			gfsh.println("      locators = null");
			gfsh.println("       servers = null");
		} else {
			if (gfsh.isLocator()) {
				gfsh.println("      locators = " + gfsh.getEndpoints());
				if (gfsh.getServerGroup() == null) {
					gfsh.println("  server group = <undefined>");
				} else {
					gfsh.println("  server group = " + gfsh.getServerGroup());
				}
			} else {
				gfsh.println("       servers = " + gfsh.getEndpoints());
			}
			gfsh.println("  read timeout = " + gfsh.getReadTimeout());
		}
		
		gfsh.println("  select limit = " + gfsh.getSelectLimit());
		gfsh.println("         fetch = " + gfsh.getFetchSize());
		gfsh.println("           key = " + gfsh.getQueryKeyClassName());
		gfsh.println("         value = " + gfsh.getValueClassName());
		gfsh.println("       show -p = " + gfsh.isShowResults());
		gfsh.println("       show -t = " + gfsh.isShowTime());
		gfsh.println("   show -table = " + gfsh.isTableFormat());
		gfsh.println("    show -type = " + gfsh.isPrintType());
		gfsh.println("     show -col = " + gfsh.getCollectionEntryPrintCount());
		gfsh.println("  zone (hours) = " + gfsh.getZoneDifference() / (60 * 60 * 1000));
//		gfsh.println("command region = " + gfsh.getCommandRegionPath());
	}
	
	public void show_k()
	{
		printClassSetters(gfsh.getQueryKeyClass(), "key");
	}
	
	public void show_v()
	{
		printClassSetters(gfsh.getValueClass(), "value");
	}
	
	private void printClassSetters(Class cls, String header)
	{
		if (cls == null) {
			gfsh.println(header + " class: undefined");
		} else {
			gfsh.println(header + " class " + cls.getName());
			gfsh.println("{");
			try {
				Map<String, Method> setterMap = ReflectionUtil.getAllSettersMap(cls);
				ArrayList list = new ArrayList(setterMap.keySet());
				Collections.sort(list);
				for (Object object : list) {
					Method method = setterMap.get(object);
					if (isSupportedMethod(method)) {
						gfsh.println("    " + method.getName().substring(3) + "::"
								+ method.getParameterTypes()[0].getCanonicalName());
//						gfsh.println("    " + method.getParameterTypes()[0].getCanonicalName() + " " + method.getName().substring(3));
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			gfsh.println("}");
		}
	}
	
	
	private boolean isSupportedMethod(Method method)
	{
		return true;
	}

}

package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.util.LinkedList;
import java.util.List;

import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.Nextable;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.QueryResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.QueryTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.util.PrintUtil;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;

public class select implements CommandExecutable, Nextable
{
	private final static int DEFAULT_LIMIT = 1000;
	private final static int MAX_LIMIT = 5000;
	
	private Gfsh gfsh;
	
	private int limit = DEFAULT_LIMIT;

	public select(Gfsh gfsh)
	{
		this.gfsh = gfsh;
		gfsh.setSelectLimit(limit);
	}
	
	public void help()
	{
		gfsh.println("select [-l <limit>|-show] | -?");
		gfsh.println("select <tuples where ...> | -?");
		gfsh.println("     Execute the specified query in the remote cache.");
		gfsh.println("     -l set the result set size limit, i.e.,");
		gfsh.println("        'select ... from ... limit <limit>'. Note that gfsh automatically");
		gfsh.println("        appends 'limit' to your select statement. Do not add your own limit.");
		gfsh.println("        The default limit is 1000. The allowed max limit is 5000.");
		gfsh.println("     -show Displays the select limit value.");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{	
		LinkedList<String> list = new LinkedList();
		gfsh.parseCommand(command, list);
		
		String queryString = command;
		
		if (list.contains("-?")) {
			help();
		} else {
			for (int i = 1; i < list.size(); i++) {
				String token = list.get(i);
				if (token.equals("-l")) {
					if (i + 1 >= list.size()) {
						gfsh.println("Error: '-l' requires limit value");
						return;
					}
					int val = Integer.parseInt((String) list.get(++i));
					if (val > MAX_LIMIT) {
						limit = MAX_LIMIT;
					} else if (val < 0) {
						limit = 0;
					} else {
						limit = val;
					}
					gfsh.setSelectLimit(limit);
					return;
				} else if (token.equals("-show")) {
					select_show();
					return;
				}
			}
			
			queryString += " limit " + limit;
			select(queryString, true);
		}
	}
	
	public List getRemoteKeys(String regionPath) throws Exception
	{
		List list = select("select * from " + regionPath + ".keySet limit " + limit, true);	
		return list;
	}
	
	public void select_show()
	{
		gfsh.println("select limit = " + limit);
	}
	
	public List select(String queryString, boolean nextEnabled) throws Exception
	{
		long startTime = System.currentTimeMillis();
		CommandResults cr = gfsh.getCommandClient().execute(new QueryTask(queryString, gfsh.getFetchSize(), nextEnabled));
		long stopTime = System.currentTimeMillis();
		if (cr.getCode() == QueryTask.ERROR_QUERY) {
			gfsh.println(cr.getCodeMessage());
			return null;
		}
		QueryResults results = (QueryResults) cr.getDataObject();
		if (results == null) {
			gfsh.println("No results");
			return null;
		}

		List list = null;
		Object obj = results.getResults();
		if (obj instanceof SelectResults) {
			SelectResults sr = (SelectResults) results.getResults();
			list = sr.asList();
			int startRowNum = results.getReturnedSize() - sr.size() + 1;
			if (gfsh.isShowResults()) {
				int rowsPrinted = PrintUtil.printSelectResults(sr, 0, startRowNum, sr.size());
				gfsh.println("Fetch size: " + gfsh.getFetchSize() + ", Limit: " + limit);
				gfsh.println("   Results: " + sr.size()
						+ ", Returned: " + results.getReturnedSize() + "/" + results.getActualSize());
				next n = (next)gfsh.getCommand("next");
				n.setCommand(getClass().getSimpleName());
			} else {
				gfsh.println("Fetch size: " + gfsh.getFetchSize() + ", Limit: " + limit);
				gfsh.println("   Results: " + sr.size() + 
						", Returned: " + results.getReturnedSize() + "/" + results.getActualSize());
			}
		} else {
			gfsh.println("Results: " + obj);
		}
		if (gfsh.isShowTime()) {
			gfsh.println("elapsed (msec): " + (stopTime - startTime));
		}
		return list;
	}
	
	public List next(Object userData) throws Exception
	{
		select(null, true);
		return null;
	}
}

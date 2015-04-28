package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.util.ArrayList;

import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class connect implements CommandExecutable
{
	private Gfsh gfsh;
	
	public connect(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
//		gfsh.println("connect [-s <host:port> | -l <host:port> [<server group>]] [-r <region path>] | [-?]");
		gfsh.println("connect [-s <host:port> | -l <host:port> [<server group>]] | [-?]");
		gfsh.println("     -s <host:port>  Connect to the specified cache servers.");
		gfsh.println("     -l <host:port> [<server group>] Connect to the specified locator");
		gfsh.println("           and the server group if specified.");
		//TODO: with integrated gfsh, we should not allow command regions other than __command
//		gfsh.println("     -r <region path>  Use the specified region in making connection.");
//		gfsh.println("            The default region path is '/__command'.");
		gfsh.println("     -t <readTimeout>  readTimeout in msec.");
		gfsh.println("            The default value is 300000 ms (5 min).");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("connect -?")) {
			help();
		} else {
			connect(command);
      if (gfsh.isConnected()) {
        gfsh.execute("refresh");
      }
		}
	}
	
	private void connect(String command)
	{
		ArrayList<String> list = new ArrayList();
		gfsh.parseCommand(command, list);
		if (list.size() < 3) {
			gfsh.println("Error: incomplete command.");
			return;
		} 
		String endpoints = null;
		String locators = null;
//		String regionPath = null;
		String serverGroup = null;
		int readTimeout = 300000;
		try {
			for (int i = 1; i < list.size(); i++) {
				String token = list.get(i);
				if (token.equals("-s")) {
					i++;
					endpoints = list.get(i);
				} else if (token.equals("-l")) {
					i++;
					locators = list.get(i);
					if (i < list.size() - 1) {
						if (list.get(i+1).startsWith("-") == false) {
							serverGroup = list.get(++i);
						}
					}
				} /*else if (token.equals("-r")) {
					i++;
					regionPath = list.get(i);
				}*/ else if (token.equals("-t")) {
					i++;
					readTimeout = Integer.parseInt(list.get(i));
				}
			}
		} catch (Exception ex) {
			gfsh.println("Error: invalid command - " + command);
			return;
		}
		
		if (endpoints != null && locators != null) {
			gfsh.println("Error: invalid command. -s and -l are not allowed together.");
		}
		
//		if (regionPath != null) {
//			gfsh.setCommandRegionPath(regionPath);
//		}
		if (endpoints != null) {
			gfsh.setEndpoints(endpoints, false, null, readTimeout);
			connect();
		}
		if (locators != null) {
			gfsh.setEndpoints(locators, true, serverGroup, readTimeout);
			connect();
		}
	}
	
	@SuppressFBWarnings(value="NM_METHOD_CONSTRUCTOR_CONFUSION",justification="This is method and not constructor")
	private void connect()
	{
		try {
			gfsh.reconnect();
			gfsh.setCurrentPath("/");
			gfsh.setCurrentRegion(null);
			if (gfsh.isConnected()) {
				gfsh.println("connected: " + gfsh.getEndpoints());
			} else {
				gfsh.println("Error: Endpoints set but unable to connect: " + gfsh.getEndpoints());
			}
			
		} catch (Exception ex) {
			gfsh.println(gfsh.getCauseMessage(ex));
      if (gfsh.isDebug()) {
        ex.printStackTrace();
      }
		}

	}
}

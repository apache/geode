/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pivotal.jvsd;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.pivotal.jvsd.stats.StatFileManager;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 *
 * @author Vince Ford
 */
public class VisualDisplayApp {

	VSDMainWindow mainWindow = null;
	StatFileManager statfiles = null;
	static private Logger logger;

	/**
	 *
	 */
	public VisualDisplayApp() {
		logger = Logger.getLogger("com.gemstone.gemfire.support");
		ConsoleHandler ch = new ConsoleHandler();
		ch.setLevel(Level.CONFIG);
		logger.addHandler(ch);
		logger.setLevel(Level.CONFIG);

		System.out.println("JVSD - Version 0.01");
		System.out.
						println("This tool is provided as-is with no support or warranty. Use at your own risk.");
		System.out.
						println("If you encounter issues or want to request features - please send email to:");
		System.out.println("vincef@vmware.com");
	}

	/**
	 *
	 * @param argv
	 */
	public static void main(String argv[]) {
		VisualDisplayApp app = new VisualDisplayApp();
		logger.config("Log level:" + logger.getLevel().toString());
		app.init(argv);
		app.createWindows();
	}

	private void init(String[] argv) {
		statfiles = new StatFileManager();
		try {
			Option opt = (OptionBuilder.hasArgs()).create('f');
			Options cliOptions = new Options();
			cliOptions.addOption(opt);
			CommandLineParser cliParser = new PosixParser();
			CommandLine lin = cliParser.parse(cliOptions, argv);
			if (lin.hasOption('f')) {

				String[] opts = lin.getOptionValues('f');
				System.out.println(opts.length);
				for (String filename : opts) {
					System.out.println("opening file:" + filename);
					statfiles.addFile(filename);
				}
			}

		} catch (ParseException ex) {
			Logger.getLogger(VisualDisplayApp.class.getName()).
							log(Level.SEVERE, null, ex);
		}
	}

	private void createWindows() {
		mainWindow = new VSDMainWindow(statfiles);
		mainWindow.setVisible(true);
	}

}

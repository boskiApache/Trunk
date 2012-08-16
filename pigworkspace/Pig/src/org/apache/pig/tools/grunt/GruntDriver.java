/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.tools.grunt;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import jline.ConsoleReader;
import jline.ConsoleReaderInputStream;

import org.antlr.runtime.ANTLRReaderStream;
import org.antlr.runtime.BaseRecognizer;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.Tree;
import org.apache.pig.impl.util.StringUtils;
import org.apache.pig.impl.util.TupleFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigServer;
import org.apache.pig.backend.datastorage.ContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.DataStorageException;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileLocalizer.FetchFileRet;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.parser.ParserException;
import org.apache.pig.parser.QueryLexer;
import org.apache.pig.parser.QueryParser;
import org.apache.pig.parser.QueryParserStreamUtil;
import org.apache.pig.parser.QueryParserUtils;
import org.apache.pig.tools.grunt.GruntParser.StreamPrinter;
import org.apache.pig.tools.parameters.ParameterSubstitutionPreprocessor;
import org.apache.pig.tools.pigscript.parser.ParseException;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.apache.pig.tools.pigstats.ScriptState;

public class GruntDriver {

	private static final Log log = LogFactory.getLog(GruntDriver.class);

	public GruntDriver(PigServer pigServer) {
		this(pigServer, false);
	}
	
//	public GruntDriver(Reader stream) {
//        init();
//    }

	public GruntDriver(PigServer pigServer, boolean isInteractive) {
		mPigServer = pigServer;
		mDfs = mPigServer.getPigContext().getDfs();
		mLfs = mPigServer.getPigContext().getLfs();
		mConf = mPigServer.getPigContext().getProperties();
		shell = new FsShell(ConfigurationUtil.toConfiguration(mConf));

		// TODO: this violates the abstraction layer decoupling between
		// front end and back end and needs to be changed.
		// Right now I am not clear on how the Job Id comes from to tell
		// the back end to kill a given job (mJobClient is used only in
		// processKill)
		//
		HExecutionEngine execEngine = mPigServer.getPigContext()
				.getExecutionEngine();
		mJobConf = execEngine.getJobConf();

		this.mInteractive = isInteractive;
		if (isInteractive) {
			setValidateEachStatement(true);
		}
	}

//	private void init() {
//        mDone = false;
//        mLoadOnly = false;
////        mExplain = null;
//        mScriptIllustrate = false;
//    }

	public void setValidateEachStatement(boolean b) {
		mPigServer.setValidateEachStatement(b);
	}

	public int[] process(Reader input, boolean sameBatch) throws IOException,
			ParseException {
		ScriptState ss = ScriptState.get();
		CommonTokenStream tokenStream = tokenize(input, ss.getFileName());

		Tree ast = parse(tokenStream);

		walk(ast);

		if (!sameBatch) {
			try {
				executeBatch();
			} finally {
				discardBatch();
			}
		}
		int[] res = { mNumSucceededJobs, mNumFailedJobs };
		return res;

	}

	public void walk(Tree ast) throws IOException, ParseException {
		for (int i = 0; i < ast.getChildCount(); i++) {
			Tree child = ast.getChild(i);
			switch (child.getType()) {
			case QueryLexer.HELP:
				printHelp();
				break;
			case QueryLexer.CAT:
				processCat(child.getChild(0).getText());
				break;
			case QueryLexer.CD:
				processCD(child.getChild(0).getText());
				break;
			case QueryLexer.PWD:
				processPWD();
				break;
			case QueryLexer.LS:
				if (child.getChildCount() != 0) {
					processLS(child.getChild(0).getText());
				} else {
					processLS(null);
				}
				break;
			case QueryLexer.MOVE:
				processMove(child.getChild(0).getText(), child.getChild(1)
						.getText());
				break;
			case QueryLexer.MKDIR:
				processMkdir(child.getChild(0).getText());
				break;
			case QueryLexer.REMOVE:
				processRemove(child.getChild(0).getText(), null);
				break;
			case QueryLexer.REMOVEFORCE:
				processRemove(child.getChild(0).getText(), "force");
				break;
			case QueryLexer.SET:
				processSet(child.getChild(0).getText(), child.getChild(1)
						.getText());
				break;
			case QueryLexer.FS:
				int length= child.getChildCount();
				String [] cmdTokens=new String[length];
				for(int param=0; param< length;param++){
					cmdTokens[param]=child.getChild(param).getText();
				}
				processFsCommand(cmdTokens);
				break;
			case QueryLexer.SH:
				int lengthSH= child.getChildCount();
				String [] cmdTokensSH=new String[lengthSH];
				for(int param=0; param< lengthSH;param++){
					cmdTokensSH[param]=StringUtils.unescapeInputString(unquote(child.getChild(param).getText()));
				}
				processShCommand(cmdTokensSH);
				break;
			case QueryLexer.COPYFROMLOCAL:
				processCopyFromLocal(child.getChild(0).getText(), child.getChild(1).getText());
				break;
			case QueryLexer.COPYTOLOCAL:
				processCopyToLocal(child.getChild(0).getText(), child.getChild(1).getText());
				break;
			case QueryLexer.DUMP:
				processDump(child.getChild(0).getText());
				break;
			case QueryLexer.DESCRIBE:
				if(child.getChildCount() != 0){
					processDescribe(child.getChild(0).getText());
				}
				else{
					processDescribe(null);
				}
				break;
			case QueryLexer.ALIASES:
				printAliases();
				break;
			case QueryLexer.HISTORY:
				boolean withNumbers = true;
				if(child.getChildCount() != 0)
					withNumbers=false;
				processHistory(withNumbers);
				break;
			case QueryLexer.KILL:
				processKill(child.getChild(0).getText());
				break;
			case QueryLexer.REGISTER:
				processRegister(unquote(child.getChild(0).getText()),child.getChild(1).getText(),child.getChild(2).getText());
				break;
			case QueryLexer.QUIT:
				quit();
				break;
			case QueryParser.STATEMENT:
				// get the original string of the query
				int size = child.getChildCount();
				Tree query = child.getChild(size - 1);
				processPig(query.getText());
				break;
			}
		}
	}

	static Tree parse(CommonTokenStream tokens) throws ParserException {
		QueryParser parser = QueryParserUtils.createParser(tokens);

		QueryParser.query_return result = null;
		try {
			result = parser.query();
		} catch (RecognitionException e) {
			String msg = parser.getErrorHeader(e) + " "
					+ parser.getErrorMessage(e, parser.getTokenNames());
			throw new ParserException(msg);
		} catch (RuntimeException ex) {
			throw new ParserException(ex.getMessage());
		}

		Tree ast = (Tree) result.getTree();
		checkError(parser);

		return ast;
	}

	private static void checkError(BaseRecognizer recognizer)
			throws ParserException {
		int errorCount = recognizer.getNumberOfSyntaxErrors();
		if (0 < errorCount)
			throw new ParserException("Encountered " + errorCount
					+ " parsing errors in the query");
	}

	static CommonTokenStream tokenize(Reader stream, String source)
			throws ParserException {
		CharStream input;
		try {
			input = new GruntParserReader(stream, source);
		} catch (IOException ex) {
			throw new ParserException("Unexpected IOException: "
					+ ex.getMessage());
		}
		QueryLexer lexer = new QueryLexer(input);
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		checkError(lexer);
		return tokens;
	}

	static class GruntParserReader extends ANTLRReaderStream {
		public GruntParserReader(Reader input, String source)
				throws IOException {
			super(input);
			this.name = source;
		}

		@Override
		public int LA(int i) {
			return QueryParserStreamUtil.LA(i, n, p, data);
		}
	}

	private void processPig(String cmd) throws IOException {
		String query = cmd.endsWith(";") ? cmd : cmd + ";";
		mPigServer.registerQuery(query);
	}

	protected void printHelp() {
		System.out.println("Commands:");
		System.out.println("<pig latin statement>; - See the PigLatin manual for details: http://hadoop.apache.org/pig");
		System.out.println("File system commands:");
		System.out.println("    fs <fs arguments> - Equivalent to Hadoop dfs command: http://hadoop.apache.org/common/docs/current/hdfs_shell.html");
		System.out.println("Diagnostic commands:");
		System.out.println("    describe <alias>[::<alias] - Show the schema for the alias. Inner aliases can be described as A::B.");
		System.out.println("    explain [-script <pigscript>] [-out <path>] [-brief] [-dot] [-param <param_name>=<param_value>]");
		System.out.println("        [-param_file <file_name>] [<alias>] - Show the execution plan to compute the alias or for entire script.");
		System.out.println("        -script - Explain the entire script.");
		System.out.println("        -out - Store the output into directory rather than print to stdout.");
		System.out.println("        -brief - Don't expand nested plans (presenting a smaller graph for overview).");
		System.out.println("        -dot - Generate the output in .dot format. Default is text format.");
		System.out.println("        -param <param_name - See parameter substitution for details.");
		System.out.println("        -param_file <file_name> - See parameter substitution for details.");
		System.out.println("        alias - Alias to explain.");
		System.out.println("    dump <alias> - Compute the alias and writes the results to stdout.");
		System.out.println("Utility Commands:");
		System.out.println("    exec [-param <param_name>=param_value] [-param_file <file_name>] <script> - ");
		System.out.println("        Execute the script with access to grunt environment including aliases.");
		System.out.println("        -param <param_name - See parameter substitution for details.");
		System.out.println("        -param_file <file_name> - See parameter substitution for details.");
		System.out.println("        script - Script to be executed.");
		System.out.println("    run [-param <param_name>=param_value] [-param_file <file_name>] <script> - ");
		System.out.println("        Execute the script with access to grunt environment. ");
		System.out.println("        -param <param_name - See parameter substitution for details.");
		System.out.println("        -param_file <file_name> - See parameter substitution for details.");
		System.out.println("        script - Script to be executed.");
		System.out.println("    sh  <shell command> - Invoke a shell command.");
		System.out.println("    kill <job_id> - Kill the hadoop job specified by the hadoop job id.");
		System.out.println("    set <key> <value> - Provide execution parameters to Pig. Keys and values are case sensitive.");
		System.out.println("        The following keys are supported: ");
		System.out.println("        default_parallel - Script-level reduce parallelism. Basic input size heuristics used by default.");
		System.out.println("        debug - Set debug on or off. Default is off.");
		System.out.println("        job.name - Single-quoted name for jobs. Default is PigLatin:<script name>");
		System.out.println("        job.priority - Priority for jobs. Values: very_low, low, normal, high, very_high. Default is normal");
		System.out.println("        stream.skippath - String that contains the path. This is used by streaming.");
		System.out.println("        any hadoop property.");
		System.out.println("    help - Display this message.");
		System.out.println("    history [-n] - Display the list statements in cache.");
		System.out.println("        -n Hide line numbers. ");
		System.out.println("    quit - Quit the grunt shell.");
	}

	protected void processCat(String path) throws IOException {
		executeBatch();

		try {
			byte buffer[] = new byte[65536];
			ElementDescriptor dfsPath = mDfs.asElement(path);
			int rc;

			if (!dfsPath.exists())
				throw new IOException("Directory " + path + " does not exist.");

			if (mDfs.isContainer(path)) {
				ContainerDescriptor dfsDir = (ContainerDescriptor) dfsPath;
				Iterator<ElementDescriptor> paths = dfsDir.iterator();

				while (paths.hasNext()) {
					ElementDescriptor curElem = paths.next();

					if (mDfs.isContainer(curElem.toString())) {
						continue;
					}

					InputStream is = curElem.open();
					while ((rc = is.read(buffer)) > 0) {
						System.out.write(buffer, 0, rc);
					}
					is.close();
				}
			} else {
				InputStream is = dfsPath.open();
				while ((rc = is.read(buffer)) > 0) {
					System.out.write(buffer, 0, rc);
				}
				is.close();
			}
		} catch (DataStorageException e) {
			throw new IOException("Failed to Cat: " + path, e);
		}
	}

	protected void processCD(String path) throws IOException {
		ContainerDescriptor container;
		try {
			if (path == null) {
				container = mDfs.asContainer(((HDataStorage) mDfs).getHFS()
						.getHomeDirectory().toString());
				mDfs.setActiveContainer(container);
			} else {
				container = mDfs.asContainer(path);

				if (!container.exists()) {
					throw new IOException("Directory " + path
							+ " does not exist.");
				}

				if (!mDfs.isContainer(path)) {
					throw new IOException(path + " is not a directory.");
				}

				mDfs.setActiveContainer(container);
			}
		} catch (DataStorageException e) {
			throw new IOException("Failed to change working directory to "
					+ ((path == null) ? (((HDataStorage) mDfs).getHFS()
							.getHomeDirectory().toString()) : (path)), e);
		}
	}

	protected void processPWD() throws IOException {
		System.out.println(mDfs.getActiveContainer().toString());
	}

	protected void processExplain(String alias, String script,
			boolean isVerbose, String format, String target,
			List<String> params, List<String> files) throws IOException,
			ParseException {
		processExplain(alias, script, isVerbose, format, target, params, files,
				false);
	}

	protected void processExplain(String alias, String script,
			boolean isVerbose, String format, String target,
			List<String> params, List<String> files, boolean dontPrintOutput)
			throws IOException, ParseException {
		// TODO
	}

	protected void processLS(String path) throws IOException {
		try {
			ElementDescriptor pathDescriptor;

			if (path == null) {
				pathDescriptor = mDfs.getActiveContainer();
			} else {
				pathDescriptor = mDfs.asElement(path);
			}

			if (!pathDescriptor.exists()) {
				throw new IOException("File or directory " + path
						+ " does not exist.");
			}

			if (mDfs.isContainer(pathDescriptor.toString())) {
				ContainerDescriptor container = (ContainerDescriptor) pathDescriptor;
				Iterator<ElementDescriptor> elems = container.iterator();

				while (elems.hasNext()) {
					ElementDescriptor curElem = elems.next();

					if (mDfs.isContainer(curElem.toString())) {
						System.out.println(curElem.toString() + "\t<dir>");
					} else {
						printLengthAndReplication(curElem);
					}
				}
			} else {
				printLengthAndReplication(pathDescriptor);
			}
		} catch (DataStorageException e) {
			throw new IOException("Failed to LS on " + path, e);
		}
	}

	protected void processMove(String src, String dst) throws IOException {
		executeBatch();
		try {
			ElementDescriptor srcPath = mDfs.asElement(src);
			ElementDescriptor dstPath = mDfs.asElement(dst);

			if (!srcPath.exists()) {
				throw new IOException("File or directory " + src
						+ " does not exist.");
			}

			srcPath.rename(dstPath);
		} catch (DataStorageException e) {
			throw new IOException("Failed to move " + src + " to " + dst, e);
		}
	}

	protected void processMkdir(String dir) throws IOException {
		ContainerDescriptor dirDescriptor = mDfs.asContainer(dir);
		dirDescriptor.create();
	}

	protected void processRemove(String path, String options)
			throws IOException {
		ElementDescriptor dfsPath = mDfs.asElement(path);
		executeBatch();

		if (!dfsPath.exists()) {
			if (options == null || !options.equalsIgnoreCase("force")) {
				throw new IOException("File or directory " + path
						+ " does not exist.");
			}
		} else {

			dfsPath.delete();
		}
	}
	
	protected void processFsCommand(String[] cmdTokens) throws IOException{
        executeBatch();
        int retCode = -1;
        try {
            retCode = shell.run(cmdTokens);
        } catch (Exception e) {
            throw new IOException(e);
        }
        
        if (retCode != 0 && !mInteractive) {
            String s = LoadFunc.join(
                    (AbstractList<String>) Arrays.asList(cmdTokens), " ");
            throw new IOException("fs command '" + s
                    + "' failed. Please check output logs for details");
        }
    }
    
	protected void processShCommand(String[] cmdTokens) throws IOException{
        try {
            executeBatch();
            
            Process executor = Runtime.getRuntime().exec(cmdTokens);
            StreamPrinter outPrinter = new StreamPrinter(executor.getInputStream(), null, System.out);
            StreamPrinter errPrinter = new StreamPrinter(executor.getErrorStream(), null, System.err);

            outPrinter.start();
            errPrinter.start();

            int ret = executor.waitFor();
            outPrinter.join();
            errPrinter.join();
            if (ret != 0 && !mInteractive) {
                String s = LoadFunc.join(
                        (AbstractList<String>) Arrays.asList(cmdTokens), " ");
                throw new IOException("sh command '" + s
                        + "' failed. Please check output logs for details");
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

	 protected void processCopyToLocal(String src, String dst) throws IOException
	    {
	            executeBatch();
	        
	            try {
	                ElementDescriptor srcPath = mDfs.asElement(src);
	                ElementDescriptor dstPath = mLfs.asElement(dst);
	                
	                srcPath.copy(dstPath, false);
	            }
	            catch (DataStorageException e) {
	                throw new IOException("Failed to copy " + src + "to (locally) " + dst, e);
	            }
	    }

	    protected void processCopyFromLocal(String src, String dst) throws IOException
	    {
	            executeBatch();
	        
	            try {
	                ElementDescriptor srcPath = mLfs.asElement(src);
	                ElementDescriptor dstPath = mDfs.asElement(dst);
	                
	                srcPath.copy(dstPath, false);
	            }
	            catch (DataStorageException e) {
	                throw new IOException("Failed to copy (loally) " + src + "to " + dst, e);
	            }
	    }
	    
	    protected void processDump(String alias) throws IOException
	    {
        	executeBatch();
            Iterator<Tuple> result = mPigServer.openIterator(alias);
            while (result.hasNext())
            {
                Tuple t = result.next();
                System.out.println(TupleFormat.format(t));
            }
	    }
	    
	    protected void processDescribe(String alias) throws IOException {
	        String nestedAlias = null;
            if(alias==null) {
                alias = mPigServer.getPigContext().getLastAlias();
            }
            if(alias.contains("::")) {
                nestedAlias = alias.substring(alias.indexOf("::") + 2);
                alias = alias.substring(0, alias.indexOf("::"));
                mPigServer.dumpSchemaNested(alias, nestedAlias);
            }
            else {
                mPigServer.dumpSchema(alias);
            }
	    }
	 
	    protected void processRegister(String path, String scriptingLang, String namespace) throws IOException, ParseException {
	        if(path.endsWith(".jar")) {
	            if(scriptingLang != null || namespace != null) {
	                throw new ParseException("Cannot register a jar with a scripting language or namespace");
	            }
	            mPigServer.registerJar(path);
	        }
	        else {
	            mPigServer.registerCode(path, scriptingLang, namespace);
	        }
	    }    

	    
	    protected void printAliases() throws IOException {
	    	mPigServer.printAliases();
	    }
	    
	    protected void processHistory(boolean withNumbers) {
	    	mPigServer.printHistory(withNumbers);
	    }
	    
	    protected void processKill(String jobid) throws IOException
	    {
	        if (mJobConf != null) {
	            JobClient jc = new JobClient(mJobConf);
	            JobID id = JobID.forName(jobid);
	            RunningJob job = jc.getJob(id);
	            if (job == null)
	                System.out.println("Job with id " + jobid + " is not active");
	            else
	            {    
	                job.killJob();
	                log.info("Kill " + id + " submitted.");
	            }
	        }
	    }
	        
    private String runPreprocessor(String script, List<String> params, 
                                   List<String> files) 
        throws IOException, ParseException {

        ParameterSubstitutionPreprocessor psp = new ParameterSubstitutionPreprocessor(50);
        StringWriter writer = new StringWriter();

        try{
            psp.genSubstitutedFile(new BufferedReader(new FileReader(script)), 
                                   writer,  
                                   params.size() > 0 ? params.toArray(new String[0]) : null, 
                                   files.size() > 0 ? files.toArray(new String[0]) : null);
        } catch (org.apache.pig.tools.parameters.ParseException pex) {
            throw new ParseException(pex.getMessage());
        }

        return writer.toString();
    }
	  
//    protected void processScript(String script, boolean batch, List<String> params, List<String> files) throws IOException, ParseException {
//        
//        if (script == null) {
//            executeBatch();
//            return;
//        }
//        
//        if (batch) {
//            setBatchOn();
//            mPigServer.setJobName(script);
//            try {
//                loadScript(script, true, false, mLoadOnly, params, files);
//                executeBatch();
//            } finally {
//                discardBatch();
//            }
//        } else {
//            loadScript(script, false, false, mLoadOnly, params, files);
//        }
//    }

//    private void loadScript(String script, boolean batch, boolean loadOnly, boolean illustrate,
//                            List<String> params, List<String> files) 
//        throws IOException, ParseException {
//        
//        Reader inputReader;
//        ConsoleReader reader;
//        boolean interactive;
//         
//        try {
//            FetchFileRet fetchFile = FileLocalizer.fetchFile(mConf, script);
//            String cmds = runPreprocessor(fetchFile.file.getAbsolutePath(), params, files);
//
//            if (mInteractive && !batch) { // Write prompt and echo commands
//                // Console reader treats tabs in a special way
//                cmds = cmds.replaceAll("\t","    ");
//
//                reader = new ConsoleReader(new ByteArrayInputStream(cmds.getBytes()),
//                                           new OutputStreamWriter(System.out));
//                reader.setHistory(mConsoleReader.getHistory());
//                InputStream in = new ConsoleReaderInputStream(reader);
//                inputReader = new BufferedReader(new InputStreamReader(in));
//                interactive = true;
//            } else { // Quietly parse the statements
//                inputReader = new StringReader(cmds);
//                reader = null;
//                interactive = false;
//            }
//        } catch (FileNotFoundException fnfe) {
//            throw new ParseException("File not found: " + script);
//        } catch (SecurityException se) {
//            throw new ParseException("Cannot access file: " + script);
//        }
//
//        GruntDriver deriver = new GruntDriver(inputReader);
//        deriver.setParams(mPigServer);
//        deriver.setConsoleReader(reader);
//        deriver.setInteractive(interactive);
//        deriver.setLoadOnly(loadOnly);
//        if (illustrate)
//            deriver.setScriptIllustrate();
//        deriver.mExplain = mExplain;
//        
//        deriver.prompt();
//        while(!deriver.isDone()) {
//            deriver.parse();
//        }
//
//        if (interactive) {
//            System.out.println("");
//        }
//    }

	protected void processSet(String key, String value) throws IOException,
			ParseException {
		if (key.equals("debug")) {
			if (value.equals("on"))
				mPigServer.debugOn();
			else if (value.equals("off"))
				mPigServer.debugOff();
			else
				throw new ParseException("Invalid value " + value
						+ " provided for " + key);
		} else if (key.equals("job.name")) {
			mPigServer.setJobName(value);
		} else if (key.equals("job.priority")) {
			mPigServer.setJobPriority(value);
		} else if (key.equals("stream.skippath")) {
			// Validate
			File file = new File(value);
			if (!file.exists() || file.isDirectory()) {
				throw new IOException("Invalid value for stream.skippath:"
						+ value);
			}
			mPigServer.addPathToSkip(value);
		} else if (key.equals("default_parallel")) {
			// Validate
			try {
				mPigServer.setDefaultParallel(Integer.parseInt(value));
			} catch (NumberFormatException e) {
				throw new ParseException("Invalid value for default_parallel");
			}
		} else {
			// mPigServer.getPigContext().getProperties().setProperty(key,
			// value);
			// PIG-2508 properties need to be managed through JobConf
			// since all other code depends on access to properties,
			// we need to re-populate from updated JobConf
			// java.util.HashSet<?> keysBefore = new
			// java.util.HashSet<Object>(mPigServer.getPigContext().getProperties().keySet());
			// set current properties on jobConf
			Properties properties = mPigServer.getPigContext().getProperties();
			Configuration jobConf = mPigServer.getPigContext()
					.getExecutionEngine().getJobConf();
			Enumeration<Object> propertiesIter = properties.keys();
			while (propertiesIter.hasMoreElements()) {
				String pkey = (String) propertiesIter.nextElement();
				String val = properties.getProperty(pkey);
				// We do not put user.name, See PIG-1419
				if (!pkey.equals("user.name"))
					jobConf.set(pkey, val);
			}
			// set new value, JobConf will handle deprecation etc.
			jobConf.set(key, value);
			// re-initialize to reflect updated JobConf
			properties.clear();
			Iterator<Map.Entry<String, String>> iter = jobConf.iterator();
			while (iter.hasNext()) {
				Map.Entry<String, String> entry = iter.next();
				properties.put(entry.getKey(), entry.getValue());
			}
			// keysBefore.removeAll(mPigServer.getPigContext().getProperties().keySet());
			// log.info("PIG-2508: keys dropped from properties: " +
			// keysBefore);
		}
	}

	private void printLengthAndReplication(ElementDescriptor elem)
			throws IOException {
		Map<String, Object> stats = elem.getStatistics();

		long replication = (Short) stats
				.get(ElementDescriptor.BLOCK_REPLICATION_KEY);
		long len = (Long) stats.get(ElementDescriptor.LENGTH_KEY);

		System.out.println(elem.toString() + "<r " + replication + ">\t" + len);
	}

	private void setBatchOn() {
        mPigServer.setBatchOn();
    }


	private void executeBatch() throws IOException {
		if (mPigServer.isBatchOn()) {
			if (!mLoadOnly) {
				mPigServer.executeBatch();
				PigStats stats = PigStats.get();
				JobGraph jg = stats.getJobGraph();
				Iterator<JobStats> iter = jg.iterator();
				while (iter.hasNext()) {
					JobStats js = iter.next();
					if (!js.isSuccessful()) {
						mNumFailedJobs++;
						Exception exp = (js.getException() != null) ? js
								.getException()
								: new ExecException(
										"Job failed, hadoop does not return any error message",
										2244);
						LogUtils.writeLog(
								exp,
								mPigServer.getPigContext().getProperties()
										.getProperty("pig.logfile"),
								log,
								"true".equalsIgnoreCase(mPigServer
										.getPigContext().getProperties()
										.getProperty("verbose")),
								"Pig Stack Trace");
					} else {
						mNumSucceededJobs++;
					}
				}
			}
		}
	}

	private void discardBatch() throws IOException {
		if (mPigServer.isBatchOn()) {
			mPigServer.discardBatch();
		}
	}
	
	static String unquote(String s)
	{
		if (s.charAt(0) == '\'' && s.charAt(s.length()-1) == '\'')
			return s.substring(1, s.length()-1);
		else
			return s;
	}
	
	public void setParams(PigServer pigServer)
    {
        mPigServer = pigServer;
        
        mDfs = mPigServer.getPigContext().getDfs();
        mLfs = mPigServer.getPigContext().getLfs();
        mConf = mPigServer.getPigContext().getProperties();
        shell = new FsShell(ConfigurationUtil.toConfiguration(mConf));
        
        // TODO: this violates the abstraction layer decoupling between
        // front end and back end and needs to be changed.
        // Right now I am not clear on how the Job Id comes from to tell
        // the back end to kill a given job (mJobClient is used only in 
        // processKill)
        //
        HExecutionEngine execEngine = mPigServer.getPigContext().getExecutionEngine();
        mJobConf = execEngine.getJobConf();
    }

//	public void setConsoleReader(ConsoleReader c)
//    {
//        mConsoleReader = c;
//        token_source.consoleReader = c;
//    }
	
	protected void quit() {
		mDone = true;
	}

	public boolean isDone() {
		return mDone;
	}

	private boolean mInteractive = false;
	private ConsoleReader mConsoleReader;
	private PigServer mPigServer;
	private DataStorage mDfs;
	private DataStorage mLfs;
	private Properties mConf;
	private JobConf mJobConf;
	private boolean mDone;
	private boolean mLoadOnly;
	//private ExplainState mExplain;
    private int mNumFailedJobs;
	private int mNumSucceededJobs;
	private FsShell shell;
	private boolean mScriptIllustrate;
}
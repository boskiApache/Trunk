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
import java.io.InputStreamReader;
import java.util.ArrayList;

import jline.ConsoleReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigServer;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.grunt.GruntParser;
import org.apache.pig.tools.grunt.PigCompletor;
import org.apache.pig.tools.grunt.PigCompletorAliases;
import org.apache.pig.tools.pigstats.PigStatsUtil;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.util.LogUtils;

public class Grunt 
{
    private final Log log = LogFactory.getLog(getClass());
    
    BufferedReader in;
    PigServer pig;
    ConsoleReader consoleReader;

    public Grunt(BufferedReader in, PigContext pigContext) throws ExecException
    {
        this.in = in;
        this.pig = new PigServer(pigContext);        
    }

    public void setConsoleReader(ConsoleReader c)
    {
    	this.consoleReader = c;
        c.addCompletor(new PigCompletorAliases(pig));
        c.addCompletor(new PigCompletor());
    }

    public void run() {        
        boolean verbose = "true".equalsIgnoreCase(pig.getPigContext().getProperties().getProperty("verbose"));
        PigStatsUtil.getEmptyPigStats();
        
        GruntDriver driver = new GruntDriver(pig, true);

        while(!driver.isDone()) {
            try {
            	String line = consoleReader.readLine();
                driver.process(new BufferedReader(new InputStreamReader(new ByteArrayInputStream(line.getBytes()))), true);
            } catch(Throwable t) {
                LogUtils.writeLog(t, pig.getPigContext().getProperties().getProperty("pig.logfile"), 
                        log, verbose, "Pig Stack Trace");
            }
        }
    }

    public int[] exec() throws Throwable {
        boolean verbose = "true".equalsIgnoreCase(pig.getPigContext().getProperties().getProperty("verbose"));
        try {
            PigStatsUtil.getEmptyPigStats();
            GruntDriver driver = new GruntDriver(pig, false);
            return driver.process(in, false);
        } catch (Throwable t) {
            LogUtils.writeLog(t, pig.getPigContext().getProperties().getProperty("pig.logfile"), 
                    log, verbose, "Pig Stack Trace");
            throw (t);
        }
    }
    
    public void checkScript(String scriptFile) throws Throwable {
        boolean verbose = "true".equalsIgnoreCase(pig.getPigContext().getProperties().getProperty("verbose"));
        GruntDriver driver = new GruntDriver(pig, false);
        driver.setValidateEachStatement(true);

        try {
            boolean dontPrintOutput = true;
            driver.processExplain(null, scriptFile, false, "text", null, 
                    new ArrayList<String>(), new ArrayList<String>(),
                    dontPrintOutput);
        } catch (Throwable t) {
            LogUtils.writeLog(t, pig.getPigContext().getProperties().getProperty("pig.logfile"), 
                    log, verbose, "Pig Stack Trace");
            throw (t);
        }
    }

}

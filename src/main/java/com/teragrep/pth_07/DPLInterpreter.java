/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.teragrep.pth_07;

import com.teragrep.pth_07.stream.BatchHandler;
import com.teragrep.pth_07.ui.UserInterfaceManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.apache.zeppelin.spark.SparkInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DPL-Spark SQL interpreter for Zeppelin.
 */
public class DPLInterpreter extends AbstractInterpreter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DPLInterpreter.class);

    private static final AtomicInteger SESSION_NUM = new AtomicInteger(0);
    private SparkInterpreter sparkInterpreter;
    private SparkContext sparkContext;

    private final DPLExecutor dplExecutor;

    // Parameter handling
    private final Config config;

    private final DPLKryo dplKryo;

    private final HashMap<String, HashMap<String, UserInterfaceManager>> notebookParagraphUserInterfaceManager;


    public DPLInterpreter(Properties properties) {
        super(properties);
        config = ConfigFactory.parseProperties(properties);
        dplExecutor = new DPLExecutor(config);
        dplKryo = new DPLKryo();
        LOGGER.info("DPL-interpreter initialize properties:" + properties);
        notebookParagraphUserInterfaceManager = new HashMap<>();
    }

    @Override
    public void open() throws InterpreterException {
        LOGGER.info("DPL-interpreter Open():" + properties);

        sparkInterpreter = getInterpreterInTheSameSessionByClassName(SparkInterpreter.class, true);
        sparkContext = sparkInterpreter.getSparkContext();

        // increase open counter
        SESSION_NUM.incrementAndGet();


    }

    @Override
    public void close() throws InterpreterException {
        LOGGER.info("Close DPLInterpreter");
        SESSION_NUM.decrementAndGet();

        if (sparkInterpreter != null) {
            sparkInterpreter.close();
        }
    }

    @Override
    public ZeppelinContext getZeppelinContext() {
        return sparkInterpreter.getZeppelinContext();
    }

    @Override
    public InterpreterResult internalInterpret(String lines, InterpreterContext interpreterContext)
            throws InterpreterException {

        // clear old output
        interpreterContext.out.clear();
        // FIXME clear fron NgDPLRenderer too

        // setup UserInterfaceManager
        UserInterfaceManager userInterfaceManager = new UserInterfaceManager(interpreterContext);

        // store UserInterfaceManager
        if (!notebookParagraphUserInterfaceManager.containsKey(interpreterContext.getNoteId())) {
            // notebookId does not exist
            HashMap<String, UserInterfaceManager> paragraphUserInterfaceManager = new HashMap<>();
            notebookParagraphUserInterfaceManager.put(interpreterContext.getNoteId(), paragraphUserInterfaceManager);
        }

        // update the userInterfaceManager
        notebookParagraphUserInterfaceManager.get(interpreterContext.getNoteId()).put(interpreterContext.getParagraphId(), userInterfaceManager);

        // setup batchHandler
        BatchHandler batchHandler = new BatchHandler(
                userInterfaceManager,
                getZeppelinContext()
        );

        Utils.printDeprecateMessage(sparkInterpreter.getSparkVersion(),
                interpreterContext, properties);
        final String jobGroup = Utils.buildJobGroupId(interpreterContext);
        final String jobDesc = Utils.buildJobDesc(interpreterContext);
        sparkContext.setJobGroup(jobGroup, jobDesc, false);

        LOGGER.info("DPL-interpreter jobGroup=" + jobGroup + "-" + jobDesc);
        sparkContext.setLocalProperty("spark.scheduler.pool", interpreterContext.getLocalProperties().get("pool"));

        if (sparkInterpreter.isUnsupportedSparkVersion()) {
            return new InterpreterResult(Code.ERROR,
                    "Spark " + sparkInterpreter.getSparkVersion().toString() + " is not supported");
        }

        LOGGER.info("DPL-interpreter interpret incoming string:" + lines);

        if(lines == null || lines.isEmpty() || lines.trim().isEmpty() ){
            return new InterpreterResult(Code.SUCCESS);
        }

        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();

        if (!sparkInterpreter.isScala212()) {
            // TODO(zjffdu) scala 2.12 still doesn't work for codegen (ZEPPELIN-4627)
            Thread.currentThread().setContextClassLoader(sparkInterpreter.getScalaShellClassLoader());
        }

        // execute query
        final InterpreterResult output = dplExecutor.interpret(
                userInterfaceManager,
                (SparkSession) sparkInterpreter.getSparkSession(),
                batchHandler,
                interpreterContext.getNoteId(),
                interpreterContext.getParagraphId(),
                lines
        );

        if (!sparkInterpreter.isScala212()) {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }

        return output;
    }

    @Override
    protected boolean isInterpolate() {
        return true;
    }

    @Override
    public void cancel(InterpreterContext context) throws InterpreterException {
        LOGGER.info("CANCEL job id:" + Utils.buildJobGroupId(context));
        // Stop streaming after current batch
        if (dplExecutor != null) {
            dplExecutor.stop();
        }
        sparkContext.cancelJobGroup(Utils.buildJobGroupId(context));
        sparkInterpreter.cancel(context);
    }


    @Override
    public FormType getFormType() {
        return FormType.NATIVE;
    }


    @Override
    public int getProgress(InterpreterContext context) throws InterpreterException {
        if (sparkInterpreter != null) {
            return sparkInterpreter.getProgress(context);
        } else {
            return 0;
        }
    }

    @Override
    public Scheduler getScheduler() {
        return SchedulerFactory.singleton().createOrGetFIFOScheduler(
                DPLInterpreter.class.getName() + this.hashCode());
    }

    @Override
    public List<InterpreterCompletion> completion(String buf, int cursor, InterpreterContext interpreterContext) {
        return null;
    }
}

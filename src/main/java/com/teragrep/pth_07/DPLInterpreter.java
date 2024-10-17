/*
 * Teragrep DPL Spark Integration PTH-07
 * Copyright (C) 2022  Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
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
import java.util.concurrent.TimeoutException;
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
        LOGGER.info("DPL-interpreter initialize properties: {}", properties);
        notebookParagraphUserInterfaceManager = new HashMap<>();
    }

    @Override
    public void open() throws InterpreterException {
        LOGGER.info("DPL-interpreter Open(): {}", properties);

        sparkInterpreter = getInterpreterInTheSameSessionByClassName(SparkInterpreter.class, true);
        sparkContext = sparkInterpreter.getSparkContext();

        // increase open counter
        SESSION_NUM.incrementAndGet();
        LOGGER.debug("Current session count after opening: {}", SESSION_NUM);

    }

    @Override
    public void close() throws InterpreterException {
        LOGGER.info("Close DPLInterpreter called");
        LOGGER.debug("Current session count before closing: {}", SESSION_NUM);
        SESSION_NUM.decrementAndGet();
        if (dplExecutor != null) {
            LOGGER.info("Closing dplExecutor");
            try {
                dplExecutor.stop();
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
        if (sparkInterpreter != null) {
            LOGGER.info("Closing sparkInterpreter");
            sparkInterpreter.close();
        }
        LOGGER.debug("Current session count after closing: {}", SESSION_NUM);
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

        final String jobGroup = Utils.buildJobGroupId(interpreterContext);
        final String jobDesc = Utils.buildJobDesc(interpreterContext);
        sparkContext.setJobGroup(jobGroup, jobDesc, false);

        LOGGER.info("DPL-interpreter jobGroup={}-{}", jobGroup, jobDesc);
        sparkContext.setLocalProperty("spark.scheduler.pool", interpreterContext.getLocalProperties().get("pool"));

        if (sparkInterpreter.isUnsupportedSparkVersion()) {
            return new InterpreterResult(Code.ERROR,
                    "Spark " + sparkInterpreter.getSparkVersion().toString() + " is not supported");
        }

        LOGGER.info("DPL-interpreter interpret incoming string: {}", lines);

        if(lines == null || lines.isEmpty() || lines.trim().isEmpty() ){
            return new InterpreterResult(Code.SUCCESS);
        }

        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();

        if (!sparkInterpreter.isScala212()) {
            // TODO(zjffdu) scala 2.12 still doesn't work for codegen (ZEPPELIN-4627)
            Thread.currentThread().setContextClassLoader(sparkInterpreter.getScalaShellClassLoader());
        }

        // execute query
        final InterpreterResult output;
        try {
            output = dplExecutor.interpret(
                    userInterfaceManager,
                    sparkInterpreter.getSparkSession(),
                    batchHandler,
                    interpreterContext.getNoteId(),
                    interpreterContext.getParagraphId(),
                    lines
            );
            LOGGER.info("Query done, return code: {}", output.code());
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }

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
        LOGGER.info("CANCEL job id: {}", Utils.buildJobGroupId(context));
        LOGGER.debug("Current session count before canceling: {}", SESSION_NUM);
        // Stop streaming after current batch
        if (dplExecutor != null) {
            try {
                dplExecutor.stop();
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
        sparkContext.cancelJobGroup(Utils.buildJobGroupId(context));
        sparkInterpreter.cancel(context);
        LOGGER.debug("Current session count after canceling: {}", SESSION_NUM);
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

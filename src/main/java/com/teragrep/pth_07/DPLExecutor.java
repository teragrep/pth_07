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

import com.teragrep.pth10.ast.DPLAuditInformation;
import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.DPLParserCatalystVisitor;
import com.teragrep.pth10.ast.bo.TranslationResultNode;
import com.teragrep.pth_07.stream.DPLStreamingQueryListener;
import com.teragrep.pth_07.stream.BatchHandler;
import com.typesafe.config.Config;
import com.teragrep.pth_07.ui.UserInterfaceManager;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.*;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.*;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;

import com.teragrep.functions.dpf_02.BatchCollect;

import java.util.concurrent.TimeoutException;

// FIXME remove dpl.Streaming.mode config, it is no longer used

public class DPLExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(DPLExecutor.class);

    private final Config config; // TODO create DPLQueryConfig which has defined getters, so no new properties are introduced outside

    private String queryId;

    private static long runIncrement = 0;

    private final BatchCollect batchCollect;

    // Active query
    StreamingQuery streamingQuery = null;

    public DPLExecutor(Config config) {
        LOGGER.info("DPLExecutor() was initialized");
        this.config = config;
        this.batchCollect = new BatchCollect("_time", this.config.getInt("dpl.recall-size"));
    }

    private DPLAuditInformation setupAuditInformation(String query) {
        LOGGER.debug("Setting audit information");
        DPLAuditInformation auditInformation = new DPLAuditInformation();
        auditInformation.setQuery(query);
        auditInformation.setReason(""); // TODO new UI element for this
        auditInformation.setUser(System.getProperty("user.name"));
        auditInformation.setTeragrepAuditPluginClassName("com.teragrep.rad_01.DefaultAuditPlugin");
        return auditInformation;
    }

    public InterpreterResult interpret(UserInterfaceManager userInterfaceManager,
                                       SparkSession sparkSession,
                                       BatchHandler batchHandler,
                                       String noteId,
                                       String paragraphId,
                                       String lines) throws TimeoutException {
        LOGGER.debug("Running in interpret()");
        batchCollect.clear(); // do not store old values // TODO remove from NotebookDatasetStore too

        queryId = "`" + sparkSession.sparkContext().applicationId() + "-" + runIncrement + "`";

        // TODO use QueryIdentifier instead of MessageLog
        String resultOutput = "Application ID: " + sparkSession.sparkContext().applicationId()
                +  " , Query ID: " + queryId;
        LOGGER.info("Result output is: {}", resultOutput);

        LOGGER.debug("Adding message");
        userInterfaceManager.getMessageLog().addMessage(resultOutput);


        LOGGER.info("DPL-interpreter initialized sparkInterpreter incoming query:<{}>", lines);
        DPLParserCatalystContext catalystContext = new DPLParserCatalystContext(sparkSession,config);

        LOGGER.debug("Adding audit information");
        catalystContext.setAuditInformation(setupAuditInformation(lines));

        LOGGER.debug("Setting baseurl");
        catalystContext.setBaseUrl(config.getString("dpl.web.url"));
        LOGGER.debug("Setting notebook url");
        catalystContext.setNotebookUrl(noteId);
        LOGGER.debug("Setting paragraph url");
        catalystContext.setParagraphUrl(paragraphId);

        LOGGER.debug("Creating lexer");
        DPLLexer lexer = new DPLLexer(CharStreams.fromString(lines));
        // Catch also lexer-errors i.e. missing '"'-chars and so on. 
		lexer.addErrorListener(new DPLErrorListener("Lexer"));

        LOGGER.debug("Creating parser");
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        LOGGER.debug("Setting earliest");
        catalystContext.setEarliest("-1Y"); // TODO take from TimeSet
        LOGGER.debug("Creating visitor");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor( catalystContext);

        // Get syntax errors and throw then to zeppelin before executing stream handling
        LOGGER.debug("Added error listener");
        parser.addErrorListener(new DPLErrorListener("Parser"));

        ParseTree tree;
        try {
            LOGGER.debug("Running parser tree root");
            tree = parser.root();
        }
        catch (IllegalStateException e) {
            return new InterpreterResult(InterpreterResult.Code.ERROR, e.toString());
        }
        catch (StringIndexOutOfBoundsException e) {
            final String msg = "Parsing error: String index out of bounds. Check for unbalanced quotes - " +
                    "make sure each quote (\") has a pair!";
            return new InterpreterResult(InterpreterResult.Code.ERROR, msg);
        }

        // set output consumer
        LOGGER.debug("Creating consumer");
        visitor.setConsumer(batchHandler);

        // set BatchCollect size
        LOGGER.debug("Setting recall size");
        visitor.setDPLRecallSize(config.getInt("dpl.recall-size"));

        LOGGER.debug("Creating translationResultNode");
        TranslationResultNode n = (TranslationResultNode) visitor.visit(tree);
        DataStreamWriter<Row> dsw;
        if (n == null) {
            return new InterpreterResult(InterpreterResult.Code.ERROR, "parser can't  construct processing pipeline");
        }
        // execute steplist
        try {
             dsw = n.stepList.execute();
        } catch (Exception e) {
            // This will also catch AnalysisExceptions, however Spark does not differentiate between
            // different types, they're all Exceptions.
            // log initial exception
            LOGGER.error("Got exception: <{}>:", e.getMessage(), e);

            // get root cause of the exception
            Throwable exception = e;
            while (exception.getCause() != null) {
                exception = exception.getCause();
            }
            return new InterpreterResult(InterpreterResult.Code.ERROR, exception.getMessage());
        }


        LOGGER.debug("Checking if aggregates are used");
        boolean aggregatesUsed = visitor.getAggregatesUsed();
        LOGGER.info("-------DPLExecutor aggregatesUsed: {} visitor: {}", aggregatesUsed, visitor.getClass().getName());

        LOGGER.debug("Running startQuery");
        streamingQuery = startQuery(dsw, visitor, batchHandler);
        LOGGER.debug("Query started");

        //outQ.explain(); // debug output

        // attach listener for query termination
        LOGGER.debug("Creating new DPLStreamingQueryListener");
        DPLStreamingQueryListener dplStreamingQueryListener = new DPLStreamingQueryListener(streamingQuery, config, userInterfaceManager, catalystContext);
        LOGGER.debug("Adding the listener");
        sparkSession.streams().addListener(dplStreamingQueryListener);

        try {
            if(LOGGER.isDebugEnabled()) {
                LOGGER.debug("Streaming query is active: {}", streamingQuery.isActive());
                LOGGER.debug("Streaming query status: {}", streamingQuery.status().toString());
                LOGGER.debug("Awaiting streamingQuery termination for <[{}]>", getQueryTimeout() );
            }
            streamingQuery.awaitTermination(getQueryTimeout());

            if (streamingQuery.isActive()) {
                LOGGER.debug("Forcing streamingQuery termination");
                streamingQuery.stop();
            }
            LOGGER.debug("Streaming query terminated");
        } catch (StreamingQueryException e) {
            // log initial exception
            LOGGER.error("Got exception: <{}>:", e.getMessage(), e);

            // get root cause of the exception
            Throwable exception = e;
            while (exception.getCause() != null) {
                exception = exception.getCause();
            }
            return new InterpreterResult(InterpreterResult.Code.ERROR, exception.getMessage());
        }

        LOGGER.debug("Returning from interpret()");
        return new InterpreterResult(InterpreterResult.Code.SUCCESS, "");
    }

    // Set up target stream
    private StreamingQuery startQuery(DataStreamWriter<Row> rowDataset,
                                      DPLParserCatalystVisitor visitor,
                                      BatchHandler batchHandler
    ) throws TimeoutException {
        LOGGER.debug("Running startQuery");
        StreamingQuery outQ;

        LOGGER.debug("Setting catalystVisitor");
        batchHandler.setCatalystVisitor(visitor);

        long processingTimeMillis = 0;
        if (config.hasPath("dpl.pth_07.trigger.processingTime")) {
            LOGGER.debug("Got processingTime trigger");
            processingTimeMillis = config.getLong("dpl.pth_07.trigger.processingTime");
        }

        LOGGER.debug("Running rowDataset trigger");
        outQ = rowDataset
                .trigger(Trigger.ProcessingTime(processingTimeMillis))
                .queryName(queryId)
                .start();
        LOGGER.debug("Trigger done");

        LOGGER.debug("Incrementing runs, before: {}", runIncrement);
        runIncrement++;
        LOGGER.debug("Incrementing runs, after: {}", runIncrement);
        return outQ;
    }

    private long getQueryTimeout() {
        final long rv;
        if (config.hasPath("dpl.pth_07.query.timeout")) {
            long configuredValue = config.getLong("dpl.pth_07.query.timeout");
            if (configuredValue < 0) {
                rv = Long.MAX_VALUE;
            }
            else {
                rv = configuredValue;
            }
        }
        else {
            rv = Long.MAX_VALUE;
        }
        return rv;
    }

    public void stop() throws TimeoutException {
        LOGGER.debug("Request to stop streaming query");
        if (streamingQuery != null && !streamingQuery.sparkSession().sparkContext().isStopped() && streamingQuery.isActive()) {
            LOGGER.info("Stopping streaming query");
            streamingQuery.stop();
        }
    }
}

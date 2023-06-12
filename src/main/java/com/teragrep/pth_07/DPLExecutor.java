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
import com.teragrep.pth10.ast.bo.CatalystNode;
import com.teragrep.pth_07.stream.DPLStreamingQueryListener;
import com.teragrep.pth_07.stream.BatchHandler;
import com.typesafe.config.Config;
import com.teragrep.pth_07.ui.UserInterfaceManager;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.*;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;

import com.teragrep.functions.dpf_02.BatchCollect;

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
        this.config = config;
        this.batchCollect = new BatchCollect("_time", this.config.getInt("dpl.recall-size"));
    }

    private DPLAuditInformation setupAuditInformation(String query) {
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
                                       String lines) {
        batchCollect.clear(); // do not store old values // TODO remove from NotebookDatasetStore too

        queryId = "`" + sparkSession.sparkContext().applicationId() + "-" + runIncrement + "`";

        // TODO use QueryIdentifier instead of MessageLog
        String resultOutput = "Application ID: " + sparkSession.sparkContext().applicationId()
                +  " , Query ID: " + queryId;

        userInterfaceManager.getMessageLog().addMessage(resultOutput);


        LOGGER.info("DPL-interpreter initialized sparkInterpreter incoming query:<"+lines+">");
        DPLParserCatalystContext catalystContext = new DPLParserCatalystContext(sparkSession,config);

        catalystContext.setAuditInformation(setupAuditInformation(lines));

        catalystContext.setBaseUrl(config.getString("dpl.web.url"));
        catalystContext.setNotebookUrl(noteId);
        catalystContext.setParagraphUrl(paragraphId);

        DPLLexer lexer = new DPLLexer(CharStreams.fromString(lines));
        // Catch also lexer-errors i.e. missing '"'-chars and so on. 
		lexer.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
                throw new IllegalStateException("failed to parse at line " + line +":"+charPositionInLine+ " due to " + msg, e);
            }
        }
        );

        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        catalystContext.setEarliest("-1Y"); // TODO take from TimeSet
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor( catalystContext);

        // Get syntax errors and throw then to zeppelin before executing stream handling
        parser.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
                throw new IllegalStateException("failed to parse at line " + line + ":" + charPositionInLine + " due to " + msg, e);
            }
        });

        ParseTree tree;
        try {
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
        visitor.setConsumer(batchHandler);

        // set BatchCollect size
        visitor.setDPLRecallSize(config.getInt("dpl.recall-size"));

        CatalystNode n = (CatalystNode) visitor.visit(tree);

        if (n == null) {
            return new InterpreterResult(InterpreterResult.Code.ERROR, "parser can't  construct processing pipeline");
        }
        boolean aggregatesUsed = visitor.getAggregatesUsed();
        LOGGER.info("-------DPLExecutor aggregatesUsed:"+aggregatesUsed+ " visitor:"+visitor.getClass().getName()+" field:"+visitor.getAggregateField());

        streamingQuery = startQuery(n.getDataStreamWriter(), visitor, batchHandler);

        //outQ.explain(); // debug output

        // attach listener for query termination
        DPLStreamingQueryListener dplStreamingQueryListener = new DPLStreamingQueryListener(streamingQuery, config, userInterfaceManager, catalystContext);
        sparkSession.streams().addListener(dplStreamingQueryListener);

        try {
            streamingQuery.awaitTermination();
        } catch (StreamingQueryException e) {
            LOGGER.error(e.toString());
            return new InterpreterResult(InterpreterResult.Code.ERROR, e.toString());
        }

        return new InterpreterResult(InterpreterResult.Code.SUCCESS, "");
    }

    // Set up target stream
    private StreamingQuery startQuery(DataStreamWriter<Row> rowDataset,
                                      DPLParserCatalystVisitor visitor,
                                      BatchHandler batchHandler
    ) {
        StreamingQuery outQ;

        batchHandler.setAggregatesUsed(visitor.getAggregatesUsed());

        outQ = rowDataset
                .trigger(Trigger.ProcessingTime(0))
                .queryName(queryId)
                .start();

        runIncrement++;
        return outQ;
    }

    public void stop() {
        if (streamingQuery != null)
            streamingQuery.stop();
    }
}

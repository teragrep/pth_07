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
package com.teragrep.pth_07.ui.elements;

import org.apache.zeppelin.interpreter.InterpreterContext;

public class QueryIdentifier extends AbstractUserInterfaceElement {

    // TODO this should be in it's own place on the UI not in the message log

    protected QueryIdentifier(InterpreterContext interpreterContext) {
        super(interpreterContext);
    }

    @Override
    protected void draw() {

    }

    @Override
    public void emit() {

    }
/*
    private void drawQueryIdentifier() {

        boolean timesets = config.getBoolean("dpl.ui.timesets");
        try{
            //the UI operation chunk

            //resetting the messenger variable
            htmlHandler.messagerReset();
            htmlHandler.messager(message);
            //Collecting environment data
            String noteId = InterpreterContext.get().getNoteId();
            String thisParId = InterpreterContext.get().getParagraphId();
            String appID = sparkInterpreter.getSparkContext().applicationId();
            String resultOutput = "Collected data: App ID: " + appID +  ", note ID: " + noteId + ", Paragraph ID: " + thisParId +  " , Query ID: " + queryId;
            htmlHandler.messager(resultOutput);

        }catch(Exception e)   // URISyntaxException|IOException|java.lang.InterruptedException
        {
            System.out.println(e);
        }
    }


 */
}

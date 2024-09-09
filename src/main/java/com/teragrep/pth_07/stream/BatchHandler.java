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
package com.teragrep.pth_07.stream;

import com.teragrep.pth10.ast.DPLParserCatalystVisitor;
import com.teragrep.pth_07.ui.UserInterfaceManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.zeppelin.interpreter.ZeppelinContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class BatchHandler implements Consumer<Dataset<Row>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchHandler.class);

    private final UserInterfaceManager userInterfaceManager;
    private final ZeppelinContext zeppelinContext;
    private DPLParserCatalystVisitor catalystVisitor = null;

    public BatchHandler(UserInterfaceManager userInterfaceManager, ZeppelinContext zeppelinContext) {
        this.userInterfaceManager = userInterfaceManager;
        this.zeppelinContext = zeppelinContext;
    }

    @Override
    public void accept(Dataset<Row> rowDataset) {
        LOGGER.error("BatchHandler accept called LOGGER");
        if (catalystVisitor != null && catalystVisitor.getAggregatesUsed()) {
            // need to check aggregatesUsed from visitor at this point, since it can be updated in sequential mode
            // after the parallel operations are performed
            
            // use legacy table
            userInterfaceManager.getOutputContent().setOutputContent(zeppelinContext.showData(rowDataset));
        }
        else {
            // use DTTableNg
            userInterfaceManager.getDtTableDatasetNg().setParagraphDataset(rowDataset);
        }
    }
    
    public void setCatalystVisitor(DPLParserCatalystVisitor catVisitor) {
        this.catalystVisitor = catVisitor;
    }
}

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

import com.typesafe.config.Config;
import org.apache.spark.sql.streaming.StreamingQuery;

public final class SourceStatus {

    public static boolean isArchiveDone(Config config, StreamingQuery outQ) {
        if (config.getBoolean("dpl.archive.enabled")) {
            boolean archiveDone = true;
            for (int i = 0; i < outQ.lastProgress().sources().length; i++) {
                String startOffset = outQ.lastProgress().sources()[i].startOffset();
                String endOffset = outQ.lastProgress().sources()[i].endOffset();
                String description = outQ.lastProgress().sources()[i].description();

                if (description != null && !description.startsWith("com.teragrep.pth06.ArchiveMicroBatchReader@")) {
                    // ignore others than archive
                    continue;
                }

                if (startOffset != null) {
                    if (!startOffset.equalsIgnoreCase(endOffset)) {
                        archiveDone = false;
                    }
                } else {
                    archiveDone = false;
                }
            }
            return archiveDone;
        }
        else {
            return true;
        }
    }

    public static boolean isKafkaDone(Config config, StreamingQuery outQ) {
        if (config.getBoolean("dpl.kafka.enabled")) {
            boolean kafkaDone = true;

            for (int i = 0; i < outQ.lastProgress().sources().length; i++) {
                Long numInputRows = outQ.lastProgress().sources()[i].numInputRows();
                String description = outQ.lastProgress().sources()[i].description();

                if (description != null && !description.startsWith("KafkaV2")) {
                    // ignore others than KafkaV2
                    continue;
                }

                if (true) { // TODO implement proper stopping
                    kafkaDone = false;
                }
            }
            return kafkaDone;
        }
        else {
            return true;
        }
    }
}

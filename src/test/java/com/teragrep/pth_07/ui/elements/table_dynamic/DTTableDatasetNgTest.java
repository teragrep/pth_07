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
package com.teragrep.pth_07.ui.elements.table_dynamic;

import com.google.gson.Gson;
import com.teragrep.pth_07.ui.elements.table_dynamic.pojo.AJAXRequest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DTTableDatasetNgTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(DTTableDatasetNgTest.class);

    @Test
    public void parseAsPojo() {
        Gson gson = new Gson();

        String ajaxRequestString = "{\"draw\":1,\"columns\":[{\"data\":0,\"name\":\"\",\"searchable\":true,\"orderable\":true,\"search\":{\"value\":\"\",\"regex\":false}},{\"data\":1,\"name\":\"\",\"searchable\":true,\"orderable\":true,\"search\":{\"value\":\"\",\"regex\":false}},{\"data\":2,\"name\":\"\",\"searchable\":true,\"orderable\":true,\"search\":{\"value\":\"\",\"regex\":false}},{\"data\":3,\"name\":\"\",\"searchable\":true,\"orderable\":true,\"search\":{\"value\":\"\",\"regex\":false}},{\"data\":4,\"name\":\"\",\"searchable\":true,\"orderable\":true,\"search\":{\"value\":\"\",\"regex\":false}},{\"data\":5,\"name\":\"\",\"searchable\":true,\"orderable\":true,\"search\":{\"value\":\"\",\"regex\":false}},{\"data\":6,\"name\":\"\",\"searchable\":true,\"orderable\":true,\"search\":{\"value\":\"\",\"regex\":false}},{\"data\":7,\"name\":\"\",\"searchable\":true,\"orderable\":true,\"search\":{\"value\":\"\",\"regex\":false}}],\"order\":[{\"column\":0,\"dir\":\"desc\"},{\"column\":0,\"dir\":\"asc\"}],\"start\":0,\"length\":5,\"search\":{\"value\":\"\",\"regex\":false}}";

        AJAXRequest ajaxRequest = gson.fromJson(ajaxRequestString, AJAXRequest.class);

        // main
        assertEquals(1, ajaxRequest.getDraw());
        assertEquals(0, ajaxRequest.getStart());
        assertEquals(5, ajaxRequest.getLength());

        // search
        assertEquals(false, ajaxRequest.getSearch().getRegex());
        assertEquals("", ajaxRequest.getSearch().getValue());
        assertEquals(1, ajaxRequest.getDraw());

        // order
        assertEquals(0, ajaxRequest.getOrder().get(0).getColumn());
        assertEquals("desc", ajaxRequest.getOrder().get(0).getDir());

        assertEquals(0, ajaxRequest.getOrder().get(1).getColumn());
        assertEquals("asc", ajaxRequest.getOrder().get(1).getDir());

        // columns
        // column 0
        assertEquals(0, ajaxRequest.getColumns().get(0).getData());
        assertEquals("", ajaxRequest.getColumns().get(0).getName());
        assertEquals(true, ajaxRequest.getColumns().get(0).getSearchable());
        assertEquals(true, ajaxRequest.getColumns().get(0).getOrderable());
        assertEquals("", ajaxRequest.getColumns().get(0).getSearch().getValue());
        assertEquals(false, ajaxRequest.getColumns().get(0).getSearch().getRegex());
        // column 1
        assertEquals(1, ajaxRequest.getColumns().get(1).getData());
        assertEquals("", ajaxRequest.getColumns().get(1).getName());
        assertEquals(true, ajaxRequest.getColumns().get(1).getSearchable());
        assertEquals(true, ajaxRequest.getColumns().get(1).getOrderable());
        assertEquals("", ajaxRequest.getColumns().get(1).getSearch().getValue());
        assertEquals(false, ajaxRequest.getColumns().get(1).getSearch().getRegex());
        // column 2
        assertEquals(2, ajaxRequest.getColumns().get(2).getData());
        assertEquals("", ajaxRequest.getColumns().get(2).getName());
        assertEquals(true, ajaxRequest.getColumns().get(2).getSearchable());
        assertEquals(true, ajaxRequest.getColumns().get(2).getOrderable());
        assertEquals("", ajaxRequest.getColumns().get(2).getSearch().getValue());
        assertEquals(false, ajaxRequest.getColumns().get(2).getSearch().getRegex());
        // column 3
        assertEquals(3, ajaxRequest.getColumns().get(3).getData());
        assertEquals("", ajaxRequest.getColumns().get(3).getName());
        assertEquals(true, ajaxRequest.getColumns().get(3).getSearchable());
        assertEquals(true, ajaxRequest.getColumns().get(3).getOrderable());
        assertEquals("", ajaxRequest.getColumns().get(3).getSearch().getValue());
        assertEquals(false, ajaxRequest.getColumns().get(3).getSearch().getRegex());
        // column 4
        assertEquals(4, ajaxRequest.getColumns().get(4).getData());
        assertEquals("", ajaxRequest.getColumns().get(4).getName());
        assertEquals(true, ajaxRequest.getColumns().get(4).getSearchable());
        assertEquals(true, ajaxRequest.getColumns().get(4).getOrderable());
        assertEquals("", ajaxRequest.getColumns().get(4).getSearch().getValue());
        assertEquals(false, ajaxRequest.getColumns().get(4).getSearch().getRegex());
        // column 5
        assertEquals(5, ajaxRequest.getColumns().get(5).getData());
        assertEquals("", ajaxRequest.getColumns().get(5).getName());
        assertEquals(true, ajaxRequest.getColumns().get(5).getSearchable());
        assertEquals(true, ajaxRequest.getColumns().get(5).getOrderable());
        assertEquals("", ajaxRequest.getColumns().get(5).getSearch().getValue());
        assertEquals(false, ajaxRequest.getColumns().get(5).getSearch().getRegex());
        // column 6
        assertEquals(6, ajaxRequest.getColumns().get(6).getData());
        assertEquals("", ajaxRequest.getColumns().get(6).getName());
        assertEquals(true, ajaxRequest.getColumns().get(6).getSearchable());
        assertEquals(true, ajaxRequest.getColumns().get(6).getOrderable());
        assertEquals("", ajaxRequest.getColumns().get(6).getSearch().getValue());
        assertEquals(false, ajaxRequest.getColumns().get(6).getSearch().getRegex());
        // column 7
        assertEquals(7, ajaxRequest.getColumns().get(7).getData());
        assertEquals("", ajaxRequest.getColumns().get(7).getName());
        assertEquals(true, ajaxRequest.getColumns().get(7).getSearchable());
        assertEquals(true, ajaxRequest.getColumns().get(7).getOrderable());
        assertEquals("", ajaxRequest.getColumns().get(7).getSearch().getValue());
        assertEquals(false, ajaxRequest.getColumns().get(7).getSearch().getRegex());
        
    }

    @Test
    public void testAJAXResponse() {

       StructType testSchema = new StructType(
                new StructField[] {
                        new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                        new StructField("id", DataTypes.LongType, false, new MetadataBuilder().build()),
                        new StructField("_raw", DataTypes.StringType, false, new MetadataBuilder().build()),
                        new StructField("index", DataTypes.StringType, false, new MetadataBuilder().build()),
                        new StructField("sourcetype", DataTypes.StringType, false, new MetadataBuilder().build()),
                        new StructField("host", DataTypes.StringType, false, new MetadataBuilder().build()),
                        new StructField("source", DataTypes.StringType, false, new MetadataBuilder().build()),
                        new StructField("partition", DataTypes.StringType, false, new MetadataBuilder().build()),
                        new StructField("offset", DataTypes.LongType, false, new MetadataBuilder().build()),
                        new StructField("origin", DataTypes.StringType, false, new MetadataBuilder().build())
                }
        );

        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
                .config("checkpointLocation","/tmp/pth_10/test/StackTest/checkpoints/" + UUID.randomUUID() + "/")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        List<Row> rows = makeRowsList(
                0L, 				// _time
                0L, 					// id
                "data data", 			// _raw
                "index_A", 				// index
                "stream", 				// sourcetype
                "host", 				// host
                "input", 				// source
                String.valueOf(0), 	    // partition
                0L, 				    // offset
                "test data",            // origin
                49                     // make n amount of rows
        );

        Dataset<Row> testDs = sparkSession.createDataFrame(rows, testSchema);
        List<String> datasetAsJSON = testDs.toJSON().collectAsList();

        List<String> subList = datasetAsJSON.subList(0, 5);

        JsonArray formated = DTTableDatasetNg.dataStreamParser(subList);
        JsonObject response = DTTableDatasetNg.DTNetResponse(formated, 0, datasetAsJSON.size());

        assertEquals("" +
                        "{\"data\":" +
                        "[" +
                        "{\"_time\":\"1970-01-01T00:00:49.000Z\",\"id\":0,\"_raw\":\"data data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test data\"}," +
                        "{\"_time\":\"1970-01-01T00:00:48.000Z\",\"id\":0,\"_raw\":\"data data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test data\"}," +
                        "{\"_time\":\"1970-01-01T00:00:47.000Z\",\"id\":0,\"_raw\":\"data data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test data\"}," +
                        "{\"_time\":\"1970-01-01T00:00:46.000Z\",\"id\":0,\"_raw\":\"data data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test data\"}," +
                        "{\"_time\":\"1970-01-01T00:00:45.000Z\",\"id\":0,\"_raw\":\"data data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test data\"}" +
                        "]," +
                        "\"ID\":0," +
                        "\"datalength\":49" +
                        "}"
                , response.toString()
        );
    }

    private List<Row> makeRowsList(long _time, Long id, String _raw, String index, String sourcetype, String host, String source, String partition, Long offset, String origin, long amount) {
        ArrayList<Row> rowArrayList = new ArrayList<>();

        while (amount > 0) {
            // creates rows in inverse order
            Timestamp timestamp = Timestamp.from(Instant.ofEpochSecond(_time+amount));
            Row row = RowFactory.create(timestamp, id, _raw, index, sourcetype, host, source, partition, offset, origin);
            rowArrayList.add(row);
            amount--;
        }


        return rowArrayList;
    }


}

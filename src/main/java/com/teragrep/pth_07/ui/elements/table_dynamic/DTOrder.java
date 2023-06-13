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

import com.teragrep.pth_07.ui.elements.table_dynamic.pojo.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.*;
import java.io.StringReader;
import java.util.*;

/*
{"draw":3,
"columns":[{"data":0,"name":"",
"searchable":true,
"orderable":true,
"search":{"value":"","regex":false}},
{"data":1,"name":"","searchable":true,"orderable":true,"search":{"value":"","regex":false}},
{"data":2,"name":"","searchable":true,"orderable":true,"search":{"value":"","regex":false}},
{"data":3,"name":"","searchable":true,"orderable":true,"search":{"value":"","regex":false}},
{"data":4,"name":"","searchable":true,"orderable":true,"search":{"value":"","regex":false}},
{"data":5,"name":"","searchable":true,"orderable":true,"search":{"value":"","regex":false}},
{"data":6,"name":"","searchable":true,"orderable":true,"search":{"value":"","regex":false}},
{"data":7,"name":"","searchable":true,"orderable":true,"search":{"value":"","regex":false}},
{"data":8,"name":"","searchable":true,"orderable":true,"search":{"value":"","regex":false}}
],
"order":[{"column":0,"dir":"desc"},{"column":1,"dir":"asc"}],
"start":0,"length":25,
"search":{"value":"","regex":false}}
 */

public class DTOrder {
    protected static final Logger LOGGER = LoggerFactory.getLogger(DTOrder.class);

    public static List<String> order(List<String> rowList, List<Order> currentOrder) {

        /*
        TODO currently only auto orders by _time@desc
        // input validation
        if (currentOrder.size() < 1) {
            return rowList;
        }

        Order order = currentOrder.get(0);

        if (order == null) {
            return rowList;
        }

        String orderDirection = order.getDir();

        if (orderDirection == null) {
            return rowList;
        }

        Integer orderBy = order.getColumn();

        if (orderBy == null) {
            return rowList;
        }
        */

        // asc sorting treeMap

        TreeMap<String, List<String>> timestampStringListMap = new TreeMap<>(
                Comparator.reverseOrder()
        );
        /*
        // desc sorting treeMap
        TreeMap<String, List<String>> timestampStringListMap = new TreeMap<>();
        */
        try {
            for (String row : rowList) {
                JsonReader reader = Json.createReader(new StringReader(row));
                JsonObject line = reader.readObject();

                // NOTE hard coded to _time column
                JsonString _time = line.getJsonString("_time");

                if (_time != null) {
                    String _timeString = _time.getString();
                    if (_timeString != null) {
                        if (!timestampStringListMap.containsKey(_timeString)) {
                            timestampStringListMap.put(_timeString, new ArrayList<>());
                        }
                        List<String> arrayList = timestampStringListMap.get(_timeString);
                        arrayList.add(row);
                    }
                    else {
                        // no _time in a row, then no sorting either
                        reader.close();
                        return rowList;
                    }
                }
                else {
                    // no _time in a row, then no sorting either
                    reader.close();
                    return rowList;
                }
                reader.close();
            }

            List<String> orderedList = new ArrayList<>();
            for (List<String> rows : timestampStringListMap.values()) {
                orderedList.addAll(rows);
            }

            return orderedList;

        } catch (JsonException | IllegalStateException e) {
            LOGGER.error(e.toString());
            return rowList;
        }
    }
}

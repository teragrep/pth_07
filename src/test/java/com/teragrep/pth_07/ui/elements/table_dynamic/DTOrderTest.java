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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;

public class DTOrderTest {

    @Test
    public void timeOrderAll() {
        List<String> listToOrder = new LinkedList<>();

        String zero1 = "{\"_time\":\"1970-01-01T00:00:01.000Z\",\"id\":0,\"_raw\":\"test abc data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"}";
        listToOrder.add(zero1);
        String zero2 = "{\"_time\":\"1970-01-01T00:00:02.000Z\",\"id\":0,\"_raw\":\"test def data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"}";
        listToOrder.add(zero2);
        String zero3 = "{\"_time\":\"1970-01-01T00:00:03.000Z\",\"id\":0,\"_raw\":\"test ghi data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"}";
        listToOrder.add(zero3);
        String zero4 = "{\"_time\":\"1970-01-01T00:00:04.000Z\",\"id\":0,\"_raw\":\"test jkl data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"}";
        listToOrder.add(zero4);
        String zero5 = "{\"_time\":\"1970-01-01T00:00:05.000Z\",\"id\":0,\"_raw\":\"test mno data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"}";
        listToOrder.add(zero5);

        List<String> resultList = DTOrder.order(listToOrder, null);
        Assertions.assertEquals(zero5, resultList.get(0));
        Assertions.assertEquals(zero4, resultList.get(1));
        Assertions.assertEquals(zero3, resultList.get(2));
        Assertions.assertEquals(zero2, resultList.get(3));
        Assertions.assertEquals(zero1, resultList.get(4));

    }

    @Test
    public void orderAllNoTime() {
        List<String> listToOrder = new LinkedList<>();

        listToOrder.add("{\"id\":0,\"_raw\":\"test abc data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"}");
        listToOrder.add("{\"id\":0,\"_raw\":\"test def data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"}");
        listToOrder.add("{\"id\":0,\"_raw\":\"test ghi data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"}");
        listToOrder.add("{\"id\":0,\"_raw\":\"test jkl data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"}");
        listToOrder.add("{\"id\":0,\"_raw\":\"test mno data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"}");

        List<String> resultList = DTOrder.order(listToOrder, null);
        Assertions.assertEquals(5, resultList.size());
    }

    @Test
    public void orderAllTimeSome() {
        List<String> listToOrder = new LinkedList<>();

        listToOrder.add("{\"id\":0,\"_raw\":\"test abc data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"}");
        listToOrder.add("{\"id\":0,\"_raw\":\"test def data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"}");
        listToOrder.add("{\"_time\":\"1970-01-01T00:00:05.000Z\",\"id\":0,\"_raw\":\"test ghi data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"}");
        listToOrder.add("{\"id\":0,\"_raw\":\"test jkl data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"}");
        listToOrder.add("{\"id\":0,\"_raw\":\"test mno data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"}");

        List<String> resultList = DTOrder.order(listToOrder, null);
        Assertions.assertEquals(5, resultList.size());
    }
}

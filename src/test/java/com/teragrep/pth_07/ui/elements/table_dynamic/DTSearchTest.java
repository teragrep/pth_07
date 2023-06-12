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

public class DTSearchTest {

    @Test
    public void searchMatchAll() {
        List<String> listToSearch = new LinkedList<>();

        listToSearch.add("{\"_time\":\"1970-01-01T00:00:00.000Z\",\"id\":0,\"_raw\":\"test abc data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"},");
        listToSearch.add("{\"_time\":\"1970-01-01T00:00:00.000Z\",\"id\":0,\"_raw\":\"test def data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"},");
        listToSearch.add("{\"_time\":\"1970-01-01T00:00:00.000Z\",\"id\":0,\"_raw\":\"test ghi data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"},");
        listToSearch.add("{\"_time\":\"1970-01-01T00:00:00.000Z\",\"id\":0,\"_raw\":\"test jkl data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"},");
        listToSearch.add("{\"_time\":\"1970-01-01T00:00:00.000Z\",\"id\":0,\"_raw\":\"test mno data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"}");

        List<String> resultList = DTSearch.search(listToSearch, "test");
        Assertions.assertEquals(5, resultList.size());
    }

    @Test
    public void searchMatchFirst() {
        List<String> listToSearch = new LinkedList<>();

        listToSearch.add("{\"_time\":\"1970-01-01T00:00:00.000Z\",\"id\":0,\"_raw\":\"test abc data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"},");
        listToSearch.add("{\"_time\":\"1970-01-01T00:00:00.000Z\",\"id\":0,\"_raw\":\"test def data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"},");
        listToSearch.add("{\"_time\":\"1970-01-01T00:00:00.000Z\",\"id\":0,\"_raw\":\"test ghi data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"},");
        listToSearch.add("{\"_time\":\"1970-01-01T00:00:00.000Z\",\"id\":0,\"_raw\":\"test jkl data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"},");
        listToSearch.add("{\"_time\":\"1970-01-01T00:00:00.000Z\",\"id\":0,\"_raw\":\"test mno data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"}");

        List<String> resultList = DTSearch.search(listToSearch, "test abc data");
        System.out.println(resultList);
        Assertions.assertEquals(1, resultList.size());
    }

    @Test
    public void searchWithout_raw() {
        List<String> listToSearch = new LinkedList<>();

        listToSearch.add("{\"_time\":\"1970-01-01T00:00:00.000Z\",\"id\":0,\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"},");
        listToSearch.add("{\"_time\":\"1970-01-01T00:00:00.000Z\",\"id\":0,\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"},");
        listToSearch.add("{\"_time\":\"1970-01-01T00:00:00.000Z\",\"id\":0,\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"},");
        listToSearch.add("{\"_time\":\"1970-01-01T00:00:00.000Z\",\"id\":0,\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"},");
        listToSearch.add("{\"_time\":\"1970-01-01T00:00:00.000Z\",\"id\":0,\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"}");

        List<String> resultList = DTSearch.search(listToSearch, "test");
        Assertions.assertTrue(resultList.isEmpty());
    }

    @Test
    public void searchNoMatch() {
        List<String> listToSearch = new LinkedList<>();

        listToSearch.add("{\"_time\":\"1970-01-01T00:00:00.000Z\",\"id\":0,\"_raw\":\"test abc data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"},");
        listToSearch.add("{\"_time\":\"1970-01-01T00:00:00.000Z\",\"id\":0,\"_raw\":\"test def data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"},");
        listToSearch.add("{\"_time\":\"1970-01-01T00:00:00.000Z\",\"id\":0,\"_raw\":\"test ghi data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"},");
        listToSearch.add("{\"_time\":\"1970-01-01T00:00:00.000Z\",\"id\":0,\"_raw\":\"test jkl data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"},");
        listToSearch.add("{\"_time\":\"1970-01-01T00:00:00.000Z\",\"id\":0,\"_raw\":\"test mno data\",\"index\":\"index_A\",\"sourcetype\":\"stream\",\"host\":\"host\",\"source\":\"input\",\"partition\":\"0\",\"offset\":0,\"origin\":\"test test\"}");

        List<String> resultList = DTSearch.search(listToSearch, "68b329da9893e34099c7d8ad5cb9c940");
        System.out.println(resultList);
        Assertions.assertEquals(0, resultList.size());
    }
}

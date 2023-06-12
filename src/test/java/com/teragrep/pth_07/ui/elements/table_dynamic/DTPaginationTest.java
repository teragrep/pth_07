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

public class DTPaginationTest {
    @Test
    public void testSecondPage() {
        List<String> rowList = new LinkedList<>();
        for (int a = 0 ; a < 100 ; a++) {
            rowList.add(String.valueOf(a));
        }
        int pageSize = 5;
        int pageStart = 5;

        List<String> secondPage = DTPagination.paginate(rowList, pageSize, pageStart);

        List<String> expectedList = new LinkedList<>();
        expectedList.add("5");
        expectedList.add("6");
        expectedList.add("7");
        expectedList.add("8");
        expectedList.add("9");
        Assertions.assertEquals(expectedList, secondPage);
    }

    @Test
    public void testNegativePageSize() {
        List<String> rowList = new LinkedList<>();
        for (int a = 0 ; a < 100 ; a++) {
            rowList.add(String.valueOf(a));
        }
        int pageSize = -1;
        int pageStart = 5;

        List<String> page = DTPagination.paginate(rowList, pageSize, pageStart);
        Assertions.assertTrue(page.isEmpty());
    }

    @Test
    public void testNegativePageStart() {
        List<String> rowList = new LinkedList<>();
        for (int a = 0 ; a < 100 ; a++) {
            rowList.add(String.valueOf(a));
        }
        int pageSize = 5;
        int pageStart = -1;

        List<String> firstPage = DTPagination.paginate(rowList, pageSize, pageStart);
        List<String> expectedList = new LinkedList<>();
        expectedList.add("0");
        expectedList.add("1");
        expectedList.add("2");
        expectedList.add("3");
        expectedList.add("4");
        Assertions.assertEquals(expectedList, firstPage);
    }

    @Test
    public void testNegativePageStartAndPageSize() {
        List<String> rowList = new LinkedList<>();
        for (int a = 0 ; a < 100 ; a++) {
            rowList.add(String.valueOf(a));
        }
        int pageSize = -70;
        int pageStart = -123;

        List<String> page = DTPagination.paginate(rowList, pageSize, pageStart);
        Assertions.assertTrue(page.isEmpty());
    }

    @Test
    public void testPageStartAtEndOfList() {
        List<String> rowList = new LinkedList<>();
        for (int a = 0 ; a < 100 ; a++) {
            rowList.add(String.valueOf(a));
        }
        int pageSize = 1;
        int pageStart = 100;

        List<String> page = DTPagination.paginate(rowList, pageSize, pageStart);
        Assertions.assertTrue(page.isEmpty());
    }

    @Test
    public void testPageStartAtLastElementOfList() {
        List<String> rowList = new LinkedList<>();
        for (int a = 0 ; a < 100 ; a++) {
            rowList.add(String.valueOf(a));
        }
        int pageSize = 1;
        int pageStart = 99;

        List<String> lastPage = DTPagination.paginate(rowList, pageSize, pageStart);
        List<String> expectedList = new LinkedList<>();
        expectedList.add("99");
        Assertions.assertEquals(expectedList, lastPage);
    }

    @Test
    public void testPageStartAtLastAndPageSizeBeyondListSizeElementOfList() {
        List<String> rowList = new LinkedList<>();
        for (int a = 0 ; a < 100 ; a++) {
            rowList.add(String.valueOf(a));
        }
        int pageSize = 1000;
        int pageStart = 99;

        List<String> lastPage = DTPagination.paginate(rowList, pageSize, pageStart);
        List<String> expectedList = new LinkedList<>();
        expectedList.add("99");
        Assertions.assertEquals(expectedList, lastPage);
    }
}

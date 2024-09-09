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

import org.apache.zeppelin.display.AngularObject;
import org.apache.zeppelin.display.AngularObjectWatcher;
import org.apache.zeppelin.interpreter.InterpreterContext;

import java.util.ArrayList;

public class MessageLog extends AbstractUserInterfaceElement {

    private final AngularObject<String> message;
    private final ArrayList<String> content;

    public MessageLog(InterpreterContext interpreterContext) {
        super(interpreterContext);

        content = new ArrayList<>();

        AngularObjectWatcher angularObjectWatcher = new AngularObjectWatcher(getInterpreterContext()) {
            @Override
            public void watch(Object o, Object o1, InterpreterContext interpreterContext) {
                if(LOGGER.isTraceEnabled()) {
                    LOGGER.trace("message <- {}", o.toString());
                    LOGGER.trace("message -> {}", o1.toString());
                }
            }
        };

        message = getInterpreterContext().getAngularObjectRegistry().add(
                "message",
                "",
                getInterpreterContext().getNoteId(),
                getInterpreterContext().getParagraphId(),
                true
        );
        message.addWatcher(angularObjectWatcher);
    }

    @Override
    protected void draw() {
        StringBuilder rendered = new StringBuilder();
        for (String msg : content) {
            rendered.append(msg);
            rendered.append('\n');
        }
        message.set(rendered.toString());
    }

    @Override
    public void emit() {
        message.emit();
    }

    public void addMessage(String message) {
        content.add(message);
        draw();
    }

    public void clear() {
        // TODO test me
        content.clear();
        draw();
    }

}

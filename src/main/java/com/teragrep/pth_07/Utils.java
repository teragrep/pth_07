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
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.teragrep.pth_07;

import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.apache.zeppelin.spark.SparkVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

/**
 * Utility and helper functions for the Spark Interpreter
 */
class Utils {
  public static Logger logger = LoggerFactory.getLogger(Utils.class);
  public static String DEPRRECATED_MESSAGE =
          "%html <font color=\"red\">Spark lower than 2.2 is deprecated, " +
          "if you don't want to see this message, please set " +
          "zeppelin.spark.deprecateMsg.show to false.</font>";

  static Object invokeMethod(Object o, String name) {
    return invokeMethod(o, name, new Class[]{}, new Object[]{});
  }

  static Object invokeMethod(Object o, String name, Class<?>[] argTypes, Object[] params) {
    try {
      return o.getClass().getMethod(name, argTypes).invoke(o, params);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      logger.error(e.getMessage(), e);
    }
    return null;
  }

  static Object invokeStaticMethod(Class<?> c, String name, Class<?>[] argTypes, Object[] params) {
    try {
      return c.getMethod(name, argTypes).invoke(null, params);
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
      logger.error(e.getMessage(), e);
    }
    return null;
  }

  static Object invokeStaticMethod(Class<?> c, String name) {
    return invokeStaticMethod(c, name, new Class[]{}, new Object[]{});
  }

  static Class<?> findClass(String name) {
    return findClass(name, false);
  }

  static Class<?> findClass(String name, boolean silence) {
    try {
      return Class.forName(name);
    } catch (ClassNotFoundException e) {
      if (!silence) {
        logger.error(e.getMessage(), e);
      }
      return null;
    }
  }

  static Object instantiateClass(String name, Class<?>[] argTypes, Object[] params) {
    try {
      Constructor<?> constructor = Utils.class.getClassLoader()
              .loadClass(name).getConstructor(argTypes);
      return constructor.newInstance(params);
    } catch (NoSuchMethodException | ClassNotFoundException | IllegalAccessException |
      InstantiationException | InvocationTargetException e) {
      logger.error(e.getMessage(), e);
    }
    return null;
  }

  // function works after intp is initialized
  static boolean isScala2_10() {
    try {
      Class.forName("org.apache.spark.repl.SparkIMain");
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    } catch (IncompatibleClassChangeError e) {
      return false;
    }
  }

  public static String buildJobGroupId(InterpreterContext context) {
    String uName = "anonymous";
    if (context.getAuthenticationInfo() != null) {
      uName = getUserName(context.getAuthenticationInfo());
    }
    return "zeppelin|" + uName + "|" + context.getNoteId() + "|" + context.getParagraphId();
  }

  public static String buildJobDesc(InterpreterContext context) {
    return "Started by: " + getUserName(context.getAuthenticationInfo());
  }

  public static String getUserName(AuthenticationInfo info) {
    String uName = "";
    if (info != null) {
      uName = info.getUser();
    }
    if (uName == null || uName.isEmpty()) {
      uName = "anonymous";
    }
    return uName;
  }

  public static void printDeprecateMessage(SparkVersion sparkVersion,
                                            InterpreterContext context,
                                            Properties properties) throws InterpreterException {
    context.out.clear();
    if (sparkVersion.olderThan(SparkVersion.SPARK_2_2_0)
            && Boolean.parseBoolean(
                    properties.getProperty("zeppelin.spark.deprecatedMsg.show", "true"))) {
      try {
        context.out.write(DEPRRECATED_MESSAGE);
        context.out.write("%text ");
      } catch (IOException e) {
        throw new InterpreterException(e);
      }
    }
  }
}

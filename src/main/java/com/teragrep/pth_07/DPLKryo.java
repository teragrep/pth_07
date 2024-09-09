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
package com.teragrep.pth_07;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DPLKryo {
    private static final Logger LOGGER = LoggerFactory.getLogger(DPLKryo.class);

    /*
    spark.serializer=org.apache.spark.serializer.KryoSerializer
    spark.kryo.setWarnUnregisteredClasses=true
    spark.kryo.registrationRequired=true
    spark.kryo.registrator=com.teragrep.pth_07.DPLKryo$DPLKryoRegistrator
     */

    public DPLKryo() {
        LOGGER.info("Use 'spark.kryo.registrator', '{}'", DPLKryoRegistrator.class.getName());
    }

    public static class DPLKryoRegistrator implements KryoRegistrator {
        @Override
        public void registerClasses(Kryo kryo) {
            kryo.setRegistrationRequired(true);
            kryo.setWarnUnregisteredClasses(true);

            try {
                Class[] classes = new Class[]{
                        Class.forName("org.apache.spark.sql.types.NullType$"),
                        Class.forName("org.apache.spark.sql.types.IntegerType$"),
                        Class.forName("org.apache.spark.sql.types.TimestampType$"),
                        Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
                        Class.forName("scala.collection.immutable.Set$EmptySet$"),
                        Class.forName("scala.reflect.ClassTag$$anon$1"),
                        Class.forName("java.lang.Class"),
                        org.apache.spark.sql.types.DataType[].class,
                        org.apache.spark.sql.types.DataType.class,
                        org.apache.spark.util.collection.BitSet.class,
                        Class.forName("org.apache.spark.sql.execution.joins.LongHashedRelation"),
                        Class.forName("org.apache.spark.sql.execution.joins.LongToUnsafeRowMap"),
                        org.apache.spark.sql.catalyst.expressions.UnsafeRow.class,
                        org.apache.spark.sql.catalyst.InternalRow[].class,
                        org.apache.spark.sql.catalyst.InternalRow.class,
                        Class.forName("org.apache.spark.sql.execution.joins.UnsafeHashedRelation"),
                        org.apache.spark.sql.types.ArrayType.class,
                        org.apache.spark.sql.types.Metadata.class,
                        Class.forName("[[B"),
                        Class.forName("org.apache.spark.sql.types.StringType$"),
                        Class.forName("org.apache.spark.sql.types.LongType$"),
                        Class.forName("org.apache.spark.sql.types.BooleanType$"),
                        Class.forName("org.apache.spark.sql.types.DoubleType$"),
                        org.apache.spark.sql.types.StructField[].class,
                        org.apache.spark.sql.types.StructField.class,
                        org.apache.spark.sql.types.StructType[].class,
                        org.apache.spark.sql.types.StructType.class,
                        scala.collection.mutable.WrappedArray.ofRef.class,
                        org.apache.spark.sql.Row[].class,
                        org.apache.spark.sql.Row.class,
                        org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema.class,
                        scala.math.Ordering.class
                };

                for (Class clazz : classes) {
                    kryo.register(clazz);
                }
            } catch (ClassNotFoundException e) {
                LOGGER.error(e.toString());
            }
        }
    }


}

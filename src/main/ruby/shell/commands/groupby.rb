#
# Copyright 2010 The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

module Shell
  module Commands
    class Groupby < Command
      def help
        return <<-EOF
Execute a Coprocessor groupby function; pass tablename, groupby key expression, select expression and optionally a dictionary of groupby specifications. Groupby specifications may include STARTROW, STOPROW and FILTER. Available expressions are add, subtract, multiply, divide, remainder, columnValue, eq, neq, lt, le, gt, ge, constant, sum, avg, count, min, max, stdDev, groupByKey, and, or, not, row, stringConcat, StringPart, subSequence, subString, ternary, toBigDecimal, toBoolean, toByte, toBytes, toDouble, toFloat, toInteger, toLong, toShort and toString. Be careful that the evalation result of the key expression must be byte array.

Some examples:

  hbase> groupby 't1',{KEY => Exp.constant(0)},{SELECT => Exp.count(Exp.columnValue('f','c2'))}
  hbase> groupby 't1',{KEY => Exp.columnValue('f','c1')},{SELECT => Exp.groupByKey(Exp.columnValue('f','c1'))},{SELECT => Exp.sum(Exp.toLong(Exp.columnValue('f','c2')))}
  hbase> groupby 't1',{KEY => Exp.constant(0)},{SELECT => Exp.count(Exp.columnValue('f','c2'))},{STARTROW => 'abc', STOPROW => 'def'}
EOF
      end

      def command(table_name, *args)
	now = Time.now
        list = hbase_coprocessor.groupby(table_name, *args)
        list.each do |results|
          row = []
          results.each do |result|
             row.push(result.toString)
          end
          formatter.row(row)
        end
        formatter.footer(now, list.size)
      end
    end
  end
end


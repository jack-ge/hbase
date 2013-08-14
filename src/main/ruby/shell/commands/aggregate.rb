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
    class Aggregate < Command
      def help
        return <<-EOF
Execute a Coprocessor aggregation function; pass aggregation function name, table name, column name, column interpreter and optionally a dictionary of aggregation specifications. Aggregation specifications may include STARTROW, STOPROW or FILTER. For a cross-site big table, if no clusters are specified, all clusters will be counted for aggregation.
Usage: aggregate 'subcommand','table','column',[{COLUMN_INTERPRETER => org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter.new, STARTROW => 'abc', STOPROW => 'def', FILTER => org.apache.hadoop.hbase.filter.ColumnPaginationFilter.new(1, 0)}]
Available subcommands:
  rowcount
  min
  max
  sum
  std
  avg
  median
Available COLUMN_INTERPRETER:
  org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter.new
  org.apache.hadoop.hbase.client.coprocessor.LongStrColumnInterpreter.new
  org.apache.hadoop.hbase.client.coprocessor.CompositeLongStrColumnInterpreter.new(",", 0)
The default COLUMN_INTERPRETER is org.apache.hadoop.hbase.client.coprocessor.LongStrColumnInterpreter.new.

Some examples:

  hbase>  aggregate 'min','t1','f1:c1'
  hbase>  aggregate 'sum','t1','f1:c1','f1:c2'
  hbase>  aggregate 'rowcount','t1','f1:c1' ,{COLUMN_INTERPRETER => org.apache.hadoop.hbase.client.coprocessor.CompositeLongStrColumnInterpreter.new(",", 0)}
  hbase>  aggregate 'min','t1','f1:c1',{STARTROW => 'abc', STOPROW => 'def'}
EOF
      end

      def command(command, table_name, *args)
        raise(ArgumentError, "Command name must be of type String") unless command.kind_of?(String)
        format_simple_command do
          case command.downcase
          when "rowcount"
            result = hbase_coprocessor.rowcount(table_name, *args)
          when "min"
            result = hbase_coprocessor.min(table_name, *args)
          when "max"
            result = hbase_coprocessor.max(table_name, *args)
          when "sum"
            result = hbase_coprocessor.sum(table_name, *args)
          when "std"
            result = hbase_coprocessor.std(table_name, *args)
          when "median"
            result = hbase_coprocessor.median(table_name, *args)
          when "avg"
            result = hbase_coprocessor.avg(table_name, *args)
          else
            puts "Subcommand must be rowcount, min, max, sum, std, avg or median."
            return
          end   
          puts "The result of " + command.downcase + " for table " + table_name + " is " +  result.to_s
        end
      end
    end
  end
end

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

include Java

java_import java.util.ArrayList
java_import java.util.List
java_import java.util.Map

module Hbase
  class Coprocessor
    include HBaseConstants

    def initialize(configuration, formatter)
      @aClient = org.apache.hadoop.hbase.client.coprocessor.AggregationClient.new(configuration)
      @groupByClient = org.apache.hadoop.hbase.client.coprocessor.GroupByClient.new(configuration)
      @formatter = formatter
    end

    def rowcount(table, *args)
      ci, scan = parse_args(table, *args)
      @aClient.rowCount(table.to_s.to_java_bytes, ci, scan)
    end
    
    def min(table, *args)
      ci, scan = parse_args(table, *args)
      @aClient.min(table.to_s.to_java_bytes, ci, scan)
    end

    def max(table, *args)
      ci, scan = parse_args(table, *args)
      @aClient.max(table.to_s.to_java_bytes, ci, scan)
    end

    def sum(table, *args)
      ci, scan = parse_args(table, *args)
      @aClient.sum(table.to_s.to_java_bytes, ci, scan)
    end

    def std(table, *args)
      ci, scan = parse_args(table, *args)
      @aClient.std(table.to_s.to_java_bytes, ci, scan)
    end

    def median(table, *args)
      ci, scan = parse_args(table, *args)
      @aClient.median(table.to_s.to_java_bytes, ci, scan)
    end

    def avg(table, *args)
      ci, scan = parse_args(table, *args)
      @aClient.avg(table.to_s.to_java_bytes, ci, scan)
    end

    def groupby(table, *args)
      scan, groupby_keys, groupby_selects = parse_groupby_args(args)
      scans = org.apache.hadoop.hbase.client.Scan[1].new
      scans[0] = scan
      return @groupByClient.groupBy(table.to_s.to_java_bytes, scans, groupby_keys, groupby_selects, nil)
    end

    def parse_args(table, *args)
       # Fail if table name is not a string
      raise(ArgumentError, "Table name must be of type String") unless table.kind_of?(String)
      args = args.flatten.compact
      raise(ArgumentError, "Table must have at least one column or column family") if args.empty?
      
      scan = org.apache.hadoop.hbase.client.Scan.new()
      ci = nil
      args.each do |arg|
        unless arg.kind_of?(String) || arg.kind_of?(Hash)
          raise(ArgumentError, "#{arg.class} of #{arg.inspect} is not of Hash or String type")
        end
        if arg.kind_of?(Hash) 
          if arg.has_key?(COLUMN_INTERPRETER)
            ci = arg[COLUMN_INTERPRETER]
            raise(ArgumentError, "COLUMN_INTERPRETER must be a subclass of org.apache.hadoop.hbase.coprocessor.ColumnInterpreter") unless ci.kind_of?(org.apache.hadoop.hbase.coprocessor.ColumnInterpreter)
          end
          if arg.has_key?(STARTROW)
            scan.setStartRow(arg[STARTROW].to_java_bytes)
          end
          if arg.has_key?(STOPROW)
            scan.setStopRow(arg[STOPROW].to_java_bytes)
          end
          if arg.has_key?(FILTER)
            scan.setFilter(arg[FILTER])
          end
        else
          parse_column(scan, arg)
        end
      end
      (ci == nil)? ci = org.apache.hadoop.hbase.client.coprocessor.LongStrColumnInterpreter.new(): ci
      return ci,scan
    end


    def parse_column(scan, column)
      split = org.apache.hadoop.hbase.KeyValue.parseColumn(column.to_java_bytes)
      if split.length > 1
        scan.addColumn(split[0],split[1])
      else
        scan.addFamily(split[0])
      end
    end

    def parse_groupby_args(args)
      groupby_keys = ArrayList.new()
      groupby_selects = ArrayList.new()
      args = args.flatten.compact
      raise(ArgumentError, "Table must have at least one KEY field and one SELECT field") if args.empty?
      scan = org.apache.hadoop.hbase.client.Scan.new()
      args.each do |arg|
        unless arg.kind_of?(Hash)
          raise(ArgumentError, "#{arg.class} of #{arg.inspect} is not of Hash or String type")
        end
        if arg.has_key?(KEY)
          groupby_keys.add(arg[KEY])
          next
        end
        if arg.has_key?(SELECT)
          groupby_selects.add(arg[SELECT])
          next
        end

        if arg.has_key?(STARTROW)
          scan.setStartRow(arg[STARTROW].to_java_bytes)
        end
        if arg.has_key?(STOPROW)
          scan.setStopRow(arg[STOPROW].to_java_bytes)
        end
        if arg.has_key?(FILTER)
          scan.setFilter(arg[FILTER])
        end
      end
      return scan, groupby_keys, groupby_selects
    end
  end
end


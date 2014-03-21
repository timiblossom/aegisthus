/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.aegisthus.mapred.reduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.aegisthus.tools.AegisthusSerializer;

public class CassReducer extends Reducer<Text, Text, Text, Text> {
	public static final AegisthusSerializer as = new AegisthusSerializer();
	Set<Text> valuesSet = new HashSet<Text>();

	@SuppressWarnings("unchecked")
	protected Long getTimestamp(Map<String, Object> map, String columnName) {
		return (Long) ((List<Object>) map.get(columnName)).get(2);
	}
	
	public void reduce(Text key, Iterable<Text> values, Context ctx)
			throws IOException, InterruptedException {
		Map<String, Map<String, Object>> mergedData = Maps.newHashMap();
		Set<String> masterSetColumnNames = Sets.newHashSet();
		String referenceToken = null;
		
		Long deletedAt = Long.MIN_VALUE;
		// If we only have one distinct value we don't need to process
		// differences, which is slow and should be avoided.
		valuesSet.clear();
		for (Text value :values) {
			valuesSet.add(new Text(value));
		}
		if (valuesSet.size() == 1) {
			ctx.write(key, new Text("0"));
			return;
		}
		
		for (Text val : valuesSet) {
			String[] rowVals = val.toString().split(AegisthusSerializer.VAL_DELIMITER);
			String token = rowVals[1];
			System.out.println("Token : " + token);

			if (referenceToken == null) {
				referenceToken = token;
			}

			Map<String, Object> map = as.deserialize(rowVals[0]);
			// The json has one key value pair, the data is always under the
			// first key so do it once to save lookup
			map.remove(AegisthusSerializer.KEY);
			Long curDeletedAt = (Long) map.remove(AegisthusSerializer.DELETEDAT);
			
			
			Map<String, Object> columns = mergedData.get(token);
			if (columns == null) {
				columns = Maps.newTreeMap();
				mergedData.put(token, columns);
				columns.put(AegisthusSerializer.DELETEDAT, Long.MIN_VALUE);
				columns.putAll(map);
			} 
			
			deletedAt = (Long) columns.get(AegisthusSerializer.DELETEDAT);
			
			if (curDeletedAt > deletedAt) {
				deletedAt = curDeletedAt;
				columns.put(AegisthusSerializer.DELETEDAT, deletedAt);
			}

			Set<String> columnNames = Sets.newHashSet();
			columnNames.addAll(map.keySet());
			columnNames.addAll(columns.keySet());
			masterSetColumnNames.addAll(map.keySet());
			
			for (String columnName : columnNames) {
				boolean oldKey = columns.containsKey(columnName);
				boolean newKey = map.containsKey(columnName);
				if (oldKey && newKey) {
					if (getTimestamp(map, columnName) > getTimestamp(columns, columnName)) {
						columns.put(columnName, map.get(columnName));
					}
				} else if (newKey) {
					columns.put(columnName, map.get(columnName));
				}
			}
			
			// When cassandra compacts it removes columns that are in deleted rows
			// that are older than the deleted timestamp.
			// we will duplicate this behavior. If the etl needs this data at some
			// point we can change, but it is only available assuming
			// cassandra hasn't discarded it.
			List<String> delete = Lists.newArrayList();
			for (Map.Entry<String, Object> e : columns.entrySet()) {
				if (e.getValue() instanceof Long) {
				    continue;	
				}
				
				@SuppressWarnings("unchecked")
				Long ts = (Long) ((List<Object>) e.getValue()).get(2);
				if (ts < deletedAt) {
					delete.add(e.getKey());
				}
			}

			for (String k : delete) {
				columns.remove(k);
			}
			
		}
		
		//checking on data on all nodes
		Iterator<String> columnNameIter = masterSetColumnNames.iterator();
		Map<String, Object> referenceRow = mergedData.get(referenceToken);
		while (columnNameIter.hasNext()) {
		    String colName = columnNameIter.next();
		    
		    //reference row does not have such column name
		    if (!referenceRow.containsKey(colName)) {
		    	ctx.write(key, new Text("1"));
				return;
		    }
		    
		    Object referenceCol = referenceRow.get(colName);

		    //for each node or token
			for (Map.Entry<String, Map<String, Object>> e : mergedData.entrySet()) {
				Map<String, Object> targetRow = e.getValue();
				
				//target row does not have such column name
			   	if (!targetRow.containsKey(colName)) {
			   		ctx.write(key, new Text("1"));
					return;
			   	}
			   	
			   	//reference col value is not equal to target col val
			   	if (!referenceCol.equals(targetRow.get(colName))) {
			   		ctx.write(key, new Text("1"));
					return;
			   	}
			}
		}

		ctx.write(key, new Text("0"));
	
	}


}

package com.netflix.aegisthus.mapred.map;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.collect.Maps;
import com.netflix.aegisthus.input.AegSplit;
import com.netflix.aegisthus.tools.AegisthusSerializer;

public class CassMapper extends Mapper<Text, Text, Text, Text> {
	public static final AegisthusSerializer as = new AegisthusSerializer();
	Set<Text> valuesSet = new HashSet<Text>();

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		valuesSet.clear();
		Map<String, Object> columns = Maps.newTreeMap();
		Map<String, Object> map = as.deserialize(value.toString());
		
		AegSplit split = (AegSplit) context.getInputSplit();
		Path filePath = split.getPath();
		String fileName = filePath.getName();
		Path tokenLevelPath = filePath.getParent().getParent().getParent().getParent().getParent();
		String token = tokenLevelPath.getName();
		
        //add my signature
		//map.put("minh-token", fileName);	
		context.write(key, new Text(value.toString() + AegisthusSerializer.VAL_DELIMITER + token));
		
	}
	
}
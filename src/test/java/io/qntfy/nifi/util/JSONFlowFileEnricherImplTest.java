package io.qntfy.nifi.util;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;

public class JSONFlowFileEnricherImplTest {
    @Test
    public void makeJsonFromMapOfJsonsTest() {
        Map<String, String> jsonMap = new TreeMap<>();
        jsonMap.put("jsonKey1", "{\"text\": \"json blob 1\"}");
        jsonMap.put("jsonKey2", "{\"text\": \"json blob 2\"}");
        String combinedJson = JSONFlowFileEnricherImpl.makeJsonFromMapOfJsons(jsonMap);
        String expectedJson = "{\"jsonKey1\": {\"text\": \"json blob 1\"}, \"jsonKey2\": {\"text\": \"json blob 2\"}}";
        assertEquals("Combined json is malformed.", expectedJson, combinedJson);
    }
    
    @Test
    public void makeJsonFromEmptyMapOfJsonsTest() {
        Map<String, String> jsonMap = new TreeMap<>();
        String combinedJson = JSONFlowFileEnricherImpl.makeJsonFromMapOfJsons(jsonMap);
        String expectedJson = "{}";
        assertEquals("Combined json is malformed.", expectedJson, combinedJson);
    }
}

package io.qntfy.nifi.util;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

import org.junit.Test;

public class StreamingJSONAppenderTest {
    
    @Test
    public void testAppendToExistingEnrichmentField() throws IOException {
        String data = "{\"test\": \"value\"}";
        StreamingJSONAppender sja = new StreamingJSONAppender("enrichment", data);
        
        String input = "{\"test\": {\"test2\": \"ABC\", \"test1\": 123}, \"enrichment\": {\"existing\": \"value3\"}}";
        String output = "{\"test\": {\"test2\": \"ABC\", \"test1\": 123}, \"enrichment\": {\"existing\": \"value3\", \"test\": \"value\"}}";
        
        InputStream is = new ByteArrayInputStream(input.getBytes(Charset.defaultCharset()));
        OutputStream os = new ByteArrayOutputStream();
        sja.process(is, os);
        
        assertEquals("JSON string is malformed.", output, os.toString());
    }

    @Test
    public void testAppendToExistingEmptyEnrichmentField() throws IOException {
        String data = "{\"test\": \"value\"}";
        StreamingJSONAppender sja = new StreamingJSONAppender("enrichment", data);
        
        String input = "{\"test\": {\"test2\": \"ABC\", \"test1\": 123}, \"enrichment\": {}}";
        String output = "{\"test\": {\"test2\": \"ABC\", \"test1\": 123}, \"enrichment\": {\"test\": \"value\"}}";
        
        InputStream is = new ByteArrayInputStream(input.getBytes(Charset.defaultCharset()));
        OutputStream os = new ByteArrayOutputStream();
        sja.process(is, os);
        
        assertEquals("JSON string is malformed.", output, os.toString());
    }
    
    @Test
    public void testAddNewEnrichmentField() throws IOException {
        String data = "{\"test\": \"value\"}";
        StreamingJSONAppender sja = new StreamingJSONAppender("enrichment", data);
        
        String input = "{\"test\": {\"test2\": \"ABC\", \"test1\": 123}}";
        String output = "{\"test\": {\"test2\": \"ABC\", \"test1\": 123}, \"enrichment\": {\"test\": \"value\"}}";
        
        InputStream is = new ByteArrayInputStream(input.getBytes(Charset.defaultCharset()));
        OutputStream os = new ByteArrayOutputStream();
        sja.process(is, os);
        
        assertEquals("JSON string is malformed.", output, os.toString());
    }
    
    
    @Test
    public void testEscapedJson() throws IOException {
        String data = "{\"test\": \"value\"}";
        StreamingJSONAppender sja = new StreamingJSONAppender("enrichment", data);
        
        String input =  "{\"test\": {\"test2\": \"ABC\", \"test1\": \"<a href=\\\"http:\\/\\/test-url.com\\\"\\/>\"}}";
        String output = "{\"test\": {\"test2\": \"ABC\", \"test1\": \"<a href=\\\"http:\\/\\/test-url.com\\\"\\/>\"}, \"enrichment\": {\"test\": \"value\"}}";
        
        InputStream is = new ByteArrayInputStream(input.getBytes(Charset.defaultCharset()));
        OutputStream os = new ByteArrayOutputStream();
        sja.process(is, os);
        
        assertEquals("JSON string is malformed.", output, os.toString());
    }
    
    @Test
    public void testEscapedJsonWithBoolean() throws IOException {
        String data = "{\"test\": \"value\"}";
        StreamingJSONAppender sja = new StreamingJSONAppender("enrichment", data);
        
        String input =  "{\"test\": {\"test2\": true, \"test1\": \"<a href=\\\"http:\\/\\/test-url.com\\\"\\/>\"}}";
        String output = "{\"test\": {\"test2\": true, \"test1\": \"<a href=\\\"http:\\/\\/test-url.com\\\"\\/>\"}, \"enrichment\": {\"test\": \"value\"}}";
        
        InputStream is = new ByteArrayInputStream(input.getBytes(Charset.defaultCharset()));
        OutputStream os = new ByteArrayOutputStream();
        sja.process(is, os);
        
        assertEquals("JSON string is malformed.", output, os.toString());
    }
    
    @Test
    public void testArraysOfJsons() throws IOException {
        String data = "{\"test\": \"value\"}";
        StreamingJSONAppender sja = new StreamingJSONAppender("enrichment", data);
        
        String input =  "{\"test\": {\"test2\": [{\"a\": \"b\"}, {\"c\": \"d\"}], \"test1\": \"<a href=\\\"http:\\/\\/test-url.com\\\"\\/>\"}}";
        String output = "{\"test\": {\"test2\": [{\"a\": \"b\"}, {\"c\": \"d\"}], \"test1\": \"<a href=\\\"http:\\/\\/test-url.com\\\"\\/>\"}, \"enrichment\": {\"test\": \"value\"}}";
        
        InputStream is = new ByteArrayInputStream(input.getBytes(Charset.defaultCharset()));
        OutputStream os = new ByteArrayOutputStream();
        sja.process(is, os);
        
        assertEquals("JSON string is malformed.", output, os.toString());
    }
    
    @Test
    public void testNestedArrays() throws IOException {
        String data = "{\"test\": \"value\"}";
        StreamingJSONAppender sja = new StreamingJSONAppender("enrichment", data);
        
        String input =  "{\"test\": {\"coordinates\": [[[-12, 23], [-72.18, 14.23]]], \"type\": \"Polygon\"}}";
        String output = "{\"test\": {\"coordinates\": [[[-12, 23], [-72.18, 14.23]]], \"type\": \"Polygon\"}, \"enrichment\": {\"test\": \"value\"}}";
        
        InputStream is = new ByteArrayInputStream(input.getBytes(Charset.defaultCharset()));
        OutputStream os = new ByteArrayOutputStream();
        sja.process(is, os);
        
        assertEquals("JSON string is malformed.", output, os.toString());
    }
    
    
}

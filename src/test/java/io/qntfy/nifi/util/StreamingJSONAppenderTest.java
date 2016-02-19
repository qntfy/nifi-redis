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
    public void testEmptyAppendToExistingEnrichmentField() throws IOException {
        String data = "{}";
        StreamingJSONAppender sja = new StreamingJSONAppender("enrichment", data);
        
        String input = "{\"test\": {\"test2\": \"ABC\", \"test1\": 123}, \"enrichment\": {\"existing\": \"value3\"}}";
        String output = "{\"test\": {\"test2\": \"ABC\", \"test1\": 123}, \"enrichment\": {\"existing\": \"value3\"}}";
        
        InputStream is = new ByteArrayInputStream(input.getBytes(Charset.defaultCharset()));
        OutputStream os = new ByteArrayOutputStream();
        sja.process(is, os);
        
        assertEquals("JSON string is malformed.", output, os.toString());
    }
    
    @Test
    public void testMultipleEnrichments() throws IOException {
        String data = "{\"spinseeker\": {\"text\": \"spinseeker value\"}}";
        StreamingJSONAppender sja = new StreamingJSONAppender("metrics", data);
        
        String input = "{\"uuid\": \"abcd1234\", \"original\": {\"text\": \"original tweet\"}}";
        String output = "{\"uuid\": \"abcd1234\", \"original\": {\"text\": \"original tweet\"}, \"metrics\": {\"spinseeker\": {\"text\": \"spinseeker value\"}}}";
        
        InputStream is = new ByteArrayInputStream(input.getBytes(Charset.defaultCharset()));
        OutputStream os = new ByteArrayOutputStream();
        sja.process(is, os);
        
        assertEquals("JSON string is malformed after first enrichment.", output, os.toString());

        data = "{\"english_text\": {\"text\": \"translated value\"}}";
        sja = new StreamingJSONAppender("metrics", data);
        
        is = new ByteArrayInputStream(os.toString().getBytes(Charset.defaultCharset()));
        os = new ByteArrayOutputStream();
        sja.process(is, os);
        
        output = "{\"uuid\": \"abcd1234\", \"original\": {\"text\": \"original tweet\"}, \"metrics\": {\"spinseeker\": {\"text\": \"spinseeker value\"}, \"english_text\": {\"text\": \"translated value\"}}}";
        assertEquals("JSON string is malformed after second enrichment.", output, os.toString());
    }

    @Test
    public void testMultipleEnrichmentsWithAdvancedObjects() throws IOException {
        String data = "{\"spinseeker\": {\"text\": \"spinseeker value\", \"data\": {\"score\": 12, \"ver\": \"beta\"}}}";
        StreamingJSONAppender sja = new StreamingJSONAppender("metrics", data);
        
        String input = "{\"uuid\": \"abcd1234\", \"original\": {\"text\": \"original tweet\", \"user\": {\"name\": \"ryan\", \"id_str\": \"12345\"}}}";
        String output = "{\"uuid\": \"abcd1234\", \"original\": {\"text\": \"original tweet\", \"user\": {\"name\": \"ryan\", \"id_str\": \"12345\"}}, \"metrics\": {\"spinseeker\": {\"text\": \"spinseeker value\", \"data\": {\"score\": 12, \"ver\": \"beta\"}}}}";
                
        InputStream is = new ByteArrayInputStream(input.getBytes(Charset.defaultCharset()));
        OutputStream os = new ByteArrayOutputStream();
        sja.process(is, os);
                
        assertEquals("JSON string is malformed after first enrichment.", output, os.toString());

        data = "{\"english_text\": {\"text\": \"translated value\", \"abc\": {\"key\": \"value\"}}}";
        sja = new StreamingJSONAppender("metrics", data);
        
        is = new ByteArrayInputStream(os.toString().getBytes(Charset.defaultCharset()));
        os = new ByteArrayOutputStream();
        sja.process(is, os);
        
        output = "{\"uuid\": \"abcd1234\", \"original\": {\"text\": \"original tweet\", \"user\": {\"name\": \"ryan\", \"id_str\": \"12345\"}}, \"metrics\": {\"spinseeker\": {\"text\": \"spinseeker value\", \"data\": {\"score\": 12, \"ver\": \"beta\"}}, \"english_text\": {\"text\": \"translated value\", \"abc\": {\"key\": \"value\"}}}}";
        assertEquals("JSON string is malformed after second enrichment.", output, os.toString());
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
    public void testAppendToExistingEmptyEnrichmentFieldEmpty() throws IOException {
        String data = "{}";
        StreamingJSONAppender sja = new StreamingJSONAppender("enrichment", data);
        
        String input = "{\"test\": {\"test2\": \"ABC\", \"test1\": 123}, \"enrichment\": {}}";
        String output = "{\"test\": {\"test2\": \"ABC\", \"test1\": 123}, \"enrichment\": {}}";
        
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
    public void testAddNewEnrichmentFieldEmpty() throws IOException {
        String data = "{}";
        StreamingJSONAppender sja = new StreamingJSONAppender("enrichment", data);
        
        String input = "{\"test\": {\"test2\": \"ABC\", \"test1\": 123}}";
        String output = "{\"test\": {\"test2\": \"ABC\", \"test1\": 123}, \"enrichment\": {}}";
        
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

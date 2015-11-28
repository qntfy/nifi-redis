package io.qntfy.nifi.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Stack;

import javax.json.Json;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParser.Event;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.nifi.processor.io.StreamCallback;

public class StreamingJSONAppender implements StreamCallback {
    private final String field;
    private final String content;
    private final Stack<String> depth = new Stack<>();
    private static final String A = "A";
    private static final String O = "O";
    
    public StreamingJSONAppender(String field, String content) {
        this.field = field;
        int start = content.indexOf('{');
        int end = content.lastIndexOf('}');
        if (start == -1 || end == -1) {
            throw new IllegalArgumentException("content must be valid JSON");
        }
        this.content = content.substring(start + 1, end);
    }
    
    private static void writeComma(Event prev, OutputStream out) throws IOException {
        if (prev != Event.START_ARRAY && prev != Event.START_OBJECT && prev != Event.KEY_NAME) {
            out.write(", ".getBytes());
        }
    }
    
    @Override
    public void process(InputStream in, OutputStream out)
            throws IOException {
        JsonParser jp = Json.createParser(in);
        Event prev = null;
        int depthPastTarget = 0;
        boolean insideTarget = false;
        boolean seenTarget = false;
        while (jp.hasNext()) {
            Event e = jp.next();
            String str = null;
            
            switch (e) {
            case KEY_NAME:
                writeComma(prev, out);
                str = "\"" + StringEscapeUtils.escapeJson(jp.getString()) + "\": ";
                out.write(str.getBytes());
                if (depth.size() == 1) {
                    // this could be the top-level target
                    if (jp.getString().equals(this.field)) {
                        // this is a match
                        insideTarget = true;
                        seenTarget = true;
                    }
                }
                
                break;
            case START_ARRAY:
                if (prev == Event.END_ARRAY) {
                    out.write(", ".getBytes());
                }
                out.write("[".getBytes());
                depth.push(A);
                break;
            case START_OBJECT:
                if (insideTarget) {
                    depthPastTarget += 1;
                }
                if (prev == Event.END_OBJECT) {
                    out.write(", ".getBytes());
                }
                out.write("{".getBytes());
                depth.push(O);
                break;
            case END_ARRAY:
                out.write("]".getBytes());
                depth.pop();
                break;
            case END_OBJECT:
                if (depth.size() == 1 && !seenTarget) {
                    writeComma(prev, out);
                    str = "\"" + StringEscapeUtils.escapeJson(this.field) + "\": {" + this.content + "}";
                    out.write(str.getBytes());
                }
                if (insideTarget && depthPastTarget == 1) {
                    writeComma(prev, out);
                    out.write(this.content.getBytes());
                    insideTarget = false;
                } else if (insideTarget) {
                    depthPastTarget -= 1;
                }
                out.write("}".getBytes());
                depth.pop();
                break;
            case VALUE_STRING:
                writeComma(prev, out);
                str = "\"" + StringEscapeUtils.escapeJson(jp.getString()) + "\"";
                out.write(str.getBytes());
                break;
            case VALUE_NUMBER:
                writeComma(prev, out);
                out.write(jp.getString().getBytes());
                break;
            case VALUE_TRUE:
                writeComma(prev, out);
                out.write("true".getBytes());
                break;
            case VALUE_FALSE:
                writeComma(prev, out);
                out.write("false".getBytes());
                break;
            case VALUE_NULL:
                writeComma(prev, out);
                out.write("null".getBytes());
                break;
                
            default:
                break;
            }
            prev = e;
        } 
    }
}
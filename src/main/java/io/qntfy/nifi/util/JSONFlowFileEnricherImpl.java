package io.qntfy.nifi.util;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.StopWatch;

public class JSONFlowFileEnricherImpl implements FlowFileEnricher {
    private final String field;
    
    public JSONFlowFileEnricherImpl(String field) {
        this.field = field;
    }

    @Override
    public FlowFile enrich(ProcessSession session, FlowFile ff, Map<String, String> content) {
        StopWatch stopWatch = new StopWatch(true);
        ff = session.write(ff, new StreamingJSONAppender(this.field, makeJsonFromMapOfJsons(content)));
        session.getProvenanceReporter().modifyContent(ff, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
        return ff;
    }
    
    private static String makeJsonFromMapOfJsons(Map<String, String> content) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        int i = 0;
        for (Entry<String, String> e : content.entrySet()) {
            if (i != 0) {
                sb.append(", ");
            }
            i += 1;
            sb.append(e.getKey() + ": " + e.getValue());
        }
        sb.append("}");
        return sb.toString();
    }
}

package io.qntfy.nifi.util;

import java.util.Map;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

public interface FlowFileEnricher {
    public FlowFile enrich(ProcessSession session, FlowFile ff, Map<String, String> content);
}

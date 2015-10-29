package io.qntfy.nifi.util;

import java.util.Map;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

public class AttributeFlowFileEnricherImpl implements FlowFileEnricher {

    @Override
    public FlowFile enrich(ProcessSession session, FlowFile ff, Map<String, String> content) {
        ff = session.putAllAttributes(ff, content);
        session.getProvenanceReporter().modifyAttributes(ff, "FlowFile enriched with desired records");
        return ff;
    }
}

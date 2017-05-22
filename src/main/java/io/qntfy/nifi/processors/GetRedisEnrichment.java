package io.qntfy.nifi.processors;

import io.qntfy.nifi.util.AttributeFlowFileEnricherImpl;
import io.qntfy.nifi.util.FlowFileEnricher;
import io.qntfy.nifi.util.JSONFlowFileEnricherImpl;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.FlowFileFilters;
import org.apache.nifi.processor.util.StandardValidators;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

@SupportsBatching
@Tags({ "Redis", "Get", "Consume", "Message", "PubSub" })
@CapabilityDescription("Poll a Kafka set for values reaching a given threshold. Create a flowfile for the content and attributes stored.")
public class GetRedisEnrichment extends AbstractProcessor {
	private volatile JedisPool jedisPool;
	
	public static final PropertyDescriptor REDIS_CONNECTION_STRING = new PropertyDescriptor.Builder()
            .name("Redis Connection String")
            .description("The Connection String to use in order to connect to Redis.")
            .required(true)
            .addValidator(StandardValidators.URI_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();
	public static final PropertyDescriptor CLIENT_NAME = new PropertyDescriptor.Builder()
            .name("Client Name")
            .description("Client Name to use when communicating with Redis")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();
	public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
            .name("Source Topic")
            .description("Enrichments should be gathered only if they were generated from this source")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();
	public static final PropertyDescriptor ENRICHMENTS = new PropertyDescriptor.Builder()
            .name("Number of Required Enrichments")
            .description("Number of enrichment elements required to continue processing")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();
	public static final AllowableValue VALUE_TRUE = new AllowableValue("True");
    public static final AllowableValue VALUE_FALSE = new AllowableValue("False");
	public static final PropertyDescriptor JSON_CONTENT_MODE = new PropertyDescriptor.Builder()
	        .name("JSON Content Mode")
	        .description("If original message and enrichment data are JSON, this can be enabled to merge based on content")
	        .allowableValues(VALUE_TRUE, VALUE_FALSE)
	        .required(true)
	        .expressionLanguageSupported(false)
	        .build();
	public static final PropertyDescriptor JSON_ENRICHMENT_FIELD = new PropertyDescriptor.Builder()
	        .name("JSON Enrichment Field")
	        .description("If JSON Content Mode is enabled, add enrichment data to this JSON map")
	        .required(true)
	        .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are successfully enriched are routed to this relationship")
            .build();
    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("Any FlowFiles that cannot be enriched, but may be able to in the future, are routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFiles that cannot be enriched are routed to this relationship")
            .build();
	
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final PropertyDescriptor clientNameWithDefault = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(CLIENT_NAME)
            .defaultValue("NiFi-" + getIdentifier())
            .build();

        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(REDIS_CONNECTION_STRING);
        props.add(ENRICHMENTS);
        props.add(TOPIC);
        props.add(JSON_CONTENT_MODE);
        props.add(JSON_ENRICHMENT_FIELD);
        props.add(clientNameWithDefault);
        return props;
    }
    
    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>(2);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_RETRY);
        return relationships;
    }
	
	@OnScheduled
	public void createRedisPool(final ProcessContext context) {
		try {
            jedisPool = new JedisPool(new JedisPoolConfig(), new URI(context.getProperty(REDIS_CONNECTION_STRING).getValue()));
        } catch (URISyntaxException e) {
            getLogger().error("Unable to establish Redis connection pool.");
            // this should be previously caught by the input validation
        }
	}
	
	@OnStopped
	public void closeRedisPool(final ProcessContext context) {
		jedisPool.destroy();
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        String attemptsAttribute = context.getProperty(CLIENT_NAME) + ".attempts";
        String source = context.getProperty(TOPIC).getValue();
        Long requiredEnrichments = context.getProperty(ENRICHMENTS).asLong();
        
        
        boolean jsonMode = context.getProperty(JSON_CONTENT_MODE).asBoolean();
        String jsonField = context.getProperty(JSON_ENRICHMENT_FIELD).getValue();
        
        FlowFileEnricher ffe = null;
        if (jsonMode) {
            ffe = new JSONFlowFileEnricherImpl(jsonField);
        } else {
            ffe = new AttributeFlowFileEnricherImpl();
        }

        final List<FlowFile> flowFiles = session.get(FlowFileFilters.newSizeBasedFilter(1, DataUnit.MB, 100));
        if (flowFiles.isEmpty()) {
            return;
        }

        final ComponentLog logger = getLogger();
        try (Jedis jedis = jedisPool.getResource()) {
            for (FlowFile flowFile : flowFiles) {
                // try to hit the needed values in redis
                String uuid = flowFile.getAttribute("uuid");
                String flowFileKey = source + ":" + uuid;
                
                if (requiredEnrichments == 0) {
                    logger.info("Transferred {} to 'success'", new Object[] {flowFile});
                    session.transfer(flowFile, REL_SUCCESS);
                } else if (jedis.hlen(flowFileKey) != requiredEnrichments) {
                    String attemptsStr = flowFile.getAttribute(attemptsAttribute);
                    Integer attempts = (attemptsStr == null) ? 0 : Integer.valueOf(attemptsStr);
                    attempts += 1;
                    
                    flowFile = session.putAttribute(flowFile, attemptsAttribute, String.valueOf(attempts));                 
                    flowFile = session.penalize(flowFile);
                    logger.info("Transferred {} to 'retry', attempt {}", new Object[] {uuid, attempts});
                    session.getProvenanceReporter().modifyAttributes(flowFile, "FlowFile modified with attempt attribute");
                    session.transfer(flowFile, REL_RETRY);
                } else {
                    Map<String, String> enrichments = jedis.hgetAll(flowFileKey);
                    flowFile = ffe.enrich(session, flowFile, enrichments);
                    logger.info("Transferred {} to 'success'", new Object[] {flowFile});
                    session.transfer(flowFile, REL_SUCCESS);
                    jedis.del(flowFileKey);
                }
            }
        } catch (JedisConnectionException e) { 
            logger.error("Failed to connect to Redis due to {}; routing flowfiles to failure", new Object[] { e.getMessage() });
            session.transfer(flowFiles, REL_FAILURE);
        }
	}

}

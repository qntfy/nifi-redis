package io.qntfy.nifi.processors;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@SupportsBatching
@Tags({ "Redis", "Get", "Consume", "Message", "PubSub" })
@CapabilityDescription("Poll a Kafka set for values reaching a given threshold. Create a flowfile for the content and attributes stored.")
public class GetRedisProcessor extends AbstractProcessor {
	private volatile JedisPool jedisPool;
	
	public static final PropertyDescriptor REDIS_CONNECTION_STRING = new PropertyDescriptor.Builder()
            .name("Redis Connection String")
            .description("The Connection String to use in order to connect to Redis. This is often a comma-separated list of <host>:<port>"
                    + " combinations. For example, host1:2181,host2:2181,host3:2188")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();
	public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
            .name("Topic Name")
            .description("The Redis key to watch for work")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();
	public static final PropertyDescriptor CLIENT_NAME = new PropertyDescriptor.Builder()
            .name("Client Name")
            .description("Client Name to use when communicating with Kafka")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();
	public static final PropertyDescriptor ENRICHMENTS = new PropertyDescriptor.Builder()
            .name("Required Enrichments")
            .description("Number of enrichment elements required to process")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();
	public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are created are routed to this relationship")
            .build();
	
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final PropertyDescriptor clientNameWithDefault = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(CLIENT_NAME)
            .defaultValue("NiFi-" + getIdentifier())
            .build();

        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(REDIS_CONNECTION_STRING);
        props.add(TOPIC);
        props.add(ENRICHMENTS);
        props.add(clientNameWithDefault);
        return props;
    }
    
    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>(1);
        relationships.add(REL_SUCCESS);
        return relationships;
    }
	
	@OnScheduled
	public void createRedisPool(final ProcessContext context) {
		jedisPool = new JedisPool(new JedisPoolConfig(), context.getProperty(REDIS_CONNECTION_STRING).getValue());
	}
	
	@OnStopped
	public void closeRedisPool(final ProcessContext context) {
		jedisPool.destroy();
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final long start = System.nanoTime();
        
		try (Jedis jedis = jedisPool.getResource()) {
			String desiredKey = context.getProperty(TOPIC).getValue();
			int requiredEnrichments = context.getProperty(ENRICHMENTS).asInteger();
			int numMessages = 0;
			Set<String> possibleResults = jedis.zrangeByScore(desiredKey, requiredEnrichments, requiredEnrichments);
			for (String flowKey : possibleResults) {
				String contentKey = "content:" + flowKey;
				String attributesKey = "attrib:" + flowKey;
				String enrichmentKey = "enrich:" + flowKey;
				final byte[] content = jedis.get(contentKey.getBytes());
				Map<String, String> attributes = jedis.hgetAll(attributesKey);
				Map<String, String> enrichment = jedis.hgetAll(enrichmentKey);
				
				if (requiredEnrichments != enrichment.size()) {
					continue;
				}
				
				numMessages += 1;
				FlowFile ff = session.create();
				ff = session.write(ff, new OutputStreamCallback() {
					@Override
					public void process(final OutputStream out)  throws IOException {
						out.write(content);
					}
				});
				ff = session.putAllAttributes(ff, attributes);
				ff = session.putAllAttributes(ff, enrichment);
				
				jedis.zrem(desiredKey, flowKey);
				jedis.del(contentKey, attributesKey, enrichmentKey);
				

                final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                session.getProvenanceReporter().receive(ff, "redis://" + desiredKey, "Received " + numMessages + " Kafka messages", millis);
                getLogger().info("Successfully received {} from Redis with {} messages in {} millis", new Object[]{ff, numMessages, millis});
                session.transfer(ff, REL_SUCCESS);
			}
			context.yield();
		
		}
	}

}

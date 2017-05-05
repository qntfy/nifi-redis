package io.qntfy.nifi.processors;

import io.qntfy.nifi.examplebean.RedisPair;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Neil on 2017/5/2.
 * Put data into redis
 */
@SupportsBatching
@Tags({"Redis", "Put", "PubSub"})
@CapabilityDescription("Poll flowfile with content reaching a given threshold. Put key and value (decoded from the content in a predefined way) into Redis.")
public class PutRedis extends AbstractProcessor {
    private volatile JedisPool jedisPool;

    static final PropertyDescriptor REDIS_CONNECTION_STRING = new PropertyDescriptor.Builder()
            .name("Redis Connection String")
            .description("The Connection String to use in order to connect to Redis. " +
                    "For example, host1, host2, 192.168.203.15")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    static final PropertyDescriptor REDIS_MAX_TOTAL = new PropertyDescriptor.Builder()
            .name("Max total of Redis pool")
            .description("The configuration of Redis connection pool")
            .required(false)
            .defaultValue("20")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    static final PropertyDescriptor REDIS_MAX_IDEL = new PropertyDescriptor.Builder()
            .name("Max IDEL of Redis pool")
            .description("The configuration of Redis connection pool")
            .required(false)
            .defaultValue("10")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    static final PropertyDescriptor REDIS_MIN_IDEL = new PropertyDescriptor.Builder()
            .name("Min IDEL of Redis pool")
            .description("The configuration of Redis connection pool")
            .required(false)
            .defaultValue("0")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    static final PropertyDescriptor REDIS_MAX_WAIT_MILLIES = new PropertyDescriptor.Builder()
            .name("Max wait millies in Redis pool")
            .description("The configuration of Redis connection pool")
            .required(false)
            .defaultValue("-1")
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The Character Set in which the data is encoded")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .build();

    static final PropertyDescriptor CLIENT_NAME = new PropertyDescriptor.Builder()
            .name("Client Name")
            .description("Client Name to use when communicating with Redis (no use)")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are written to Redis are routed to this relationship")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot be written to Redis are routed to this relationship")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final PropertyDescriptor clientNameWithDefault = new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(CLIENT_NAME)
                .defaultValue("NiFi-" + getIdentifier())
                .build();

        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(REDIS_CONNECTION_STRING);
        props.add(CHARACTER_SET);
        props.add(REDIS_MAX_TOTAL);
        props.add(REDIS_MAX_IDEL);
        props.add(REDIS_MIN_IDEL);
        props.add(REDIS_MAX_WAIT_MILLIES);
        props.add(clientNameWithDefault);
        return props;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>(1);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @OnScheduled
    public void createRedisPool(final ProcessContext context) {
        String host = context.getProperty(REDIS_CONNECTION_STRING).getValue();
        int maxTotal = context.getProperty(REDIS_MAX_TOTAL).asInteger();
        int minIDEL = context.getProperty(REDIS_MIN_IDEL).asInteger();
        int maxIDEL = context.getProperty(REDIS_MAX_IDEL).asInteger();
        long maxWaitMillies = context.getProperty(REDIS_MAX_WAIT_MILLIES).asLong();

        // config redis pool,
        // connection num ranges in [10,20],
        // if above this num, just wait.
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(maxTotal);
        poolConfig.setMaxIdle(maxIDEL);
        poolConfig.setMinIdle(minIDEL);
        poolConfig.setMaxWaitMillis(maxWaitMillies);

        jedisPool = new JedisPool(poolConfig, host);
    }

    @OnStopped
    public void closeRedisPool(final ProcessContext context) {
        jedisPool.destroy();
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        final long start = System.nanoTime();

        final ComponentLog logger = getLogger();
        final Charset charset = Charset.forName(processContext.getProperty(CHARACTER_SET).getValue());

        final FlowFile flowFile = processSession.get();
        if (flowFile == null) return;

        Jedis jedis = jedisPool.getResource();
        Pipeline pipeline = jedis.pipelined();

        try {
            final byte[] content = new byte[(int) flowFile.getSize()];
            processSession.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, content, true);
                }
            });

            // mock data
            List<RedisPair> redisPairs = new ArrayList<>();
            for (int i = 0; i < 30; i++) {
                UUID uuid = UUID.randomUUID();
                String rkey = "key" + uuid.toString();
                String rvalue = "value" + new String(content, charset);
                redisPairs.add(new RedisPair(rkey, rvalue));
            }
            // formatted redis data into pipeline
            for (RedisPair pair : redisPairs) {
                pipeline.lpush(pair.getKey(), pair.getValue());
                pipeline.ltrim(pair.getKey(), 0, 59);
            }
            pipeline.sync();

            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            logger.info("Successfully inserted {} into Redis with {} messages in {} millis",
                    new Object[]{flowFile, redisPairs.size(), millis});
            processSession.transfer(flowFile, REL_SUCCESS);
        } catch (final RuntimeException e) {
            logger.error("Failed to execute insert {} due to {}", new Object[]{flowFile, e}, e);
            processSession.transfer(flowFile, REL_FAILURE);
            processContext.yield();
            processSession.rollback();
        } finally {
            jedisPool.returnResourceObject(jedis);
        }
    }
}

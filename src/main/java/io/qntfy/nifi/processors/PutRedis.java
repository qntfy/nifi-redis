package io.qntfy.nifi.processors;

import io.qntfy.nifi.examplebean.RedisPair;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import redis.clients.jedis.*;

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
public class PutRedis extends BasePutRedis {
    protected volatile JedisPool jedisPool;

    protected static final PropertyDescriptor REDIS_IP = new PropertyDescriptor.Builder()
            .name("redis-ip")
            .displayName("Redis IP")
            .description("Specifies the IP of Redis server to put data. " +
                    "For example, 127.0.0.1 . " +
                    "IP works with following property port as <IP>:<PORT>.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    protected static final PropertyDescriptor REDIS_PORT = new PropertyDescriptor.Builder()
            .name("redis-port")
            .displayName("Redis PORT")
            .description("Specifies the PORT of Redis server to put data. " +
                    "For example, 2183. If not set, DEFAULT port 6379 will be used. " +
                    "PORT works together with above property IP as <IP>:<PORT>.")
            .required(true)
            .defaultValue(Protocol.DEFAULT_PORT + "")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    @Override
    protected void addSupportedPropertyDescriptors(List<PropertyDescriptor> props) {
        props.add(REDIS_PORT);
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>(1);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    public void createRedisContext(ProcessContext context) {
        String host = context.getProperty(REDIS_IP).getValue();
        int port = context.getProperty(REDIS_PORT).asInteger();
        String password = context.getProperty(REDIS_PASSWORD).isSet()
                ? context.getProperty(REDIS_PASSWORD).getValue() : null;
        int database = context.getProperty(REDIS_DATABASE).isSet()
                ? context.getProperty(REDIS_DATABASE).asInteger() : DEFAULT_DATABASE;
        int maxTotal = context.getProperty(REDIS_MAX_TOTAL).isSet() ?
                context.getProperty(REDIS_MAX_TOTAL).asInteger() : DEFAULT_MAX_TOTAL;
        int minIDEL = context.getProperty(REDIS_MIN_IDEL).isSet() ?
                context.getProperty(REDIS_MIN_IDEL).asInteger() : DEFAULT_MIN_IDEL;
        int maxIDEL = context.getProperty(REDIS_MAX_IDEL).isSet() ?
                context.getProperty(REDIS_MAX_IDEL).asInteger() : DEFAULT_MAX_IDEL;
        long maxWaitMillies = context.getProperty(REDIS_MAX_WAIT_MILLIES).isSet() ?
                context.getProperty(REDIS_MAX_WAIT_MILLIES).asLong() : DEFAULT_MAX_WAIT_MILLIES;

        getLogger().info("Config args for JedisPool:\n" +
                        "\t\tRedis Server Address = {}:{}\n" +
                        "\t\tPassword = {}\n" +
                        "\t\tDatabase = {}\n" +
                        "\t\tMax Total = {}\n" +
                        "\t\tMin IDEL = {}\n" +
                        "\t\tMax IDEL = {}\n" +
                        "\t\tMax Wait Millies= {}"
                , new Object[]{host, port, password == null ? "null" : password,
                        database, maxTotal, minIDEL, maxIDEL, maxWaitMillies});

        // config redis pool,
        // connection num ranges in [10,20],
        // if above this num, just wait.
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(maxTotal);
        poolConfig.setMaxIdle(maxIDEL);
        poolConfig.setMinIdle(minIDEL);
        poolConfig.setMaxWaitMillis(maxWaitMillies);

        jedisPool = password == null
                ? new JedisPool(poolConfig, host, port, Protocol.DEFAULT_TIMEOUT)
                : new JedisPool(poolConfig, host, port, Protocol.DEFAULT_TIMEOUT, password, database);
    }

    @Override
    public void destroyRedisContext(ProcessContext context) {
        if (jedisPool != null) jedisPool.destroy();
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

            //TODO --- do content decoding and put redis following!!!
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
            //TODO --- do content decoding and put redis above!!!

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

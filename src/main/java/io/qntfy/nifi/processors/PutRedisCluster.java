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
 * Created by Neil on 2017/5/22.
 * PutRedisCluster
 */
@SupportsBatching
@Tags({"Redis", "Cluster", "Put", "PubSub"})
@CapabilityDescription("Poll flowfile with content reaching a given threshold. Put key and value (decoded from the content in a predefined way) into Redis Cluster.")
public class PutRedisCluster extends BasePutRedis {

    private JedisCluster jedisCluster;

    private static final int DEFAULT_TIMEOUT = 2000;
    private static final int DEFAULT_MAX_REDIRECTIONS = 5;

    protected static final PropertyDescriptor REDIS_NODES = new PropertyDescriptor.Builder()
            .name("redis-nodes")
            .displayName("Redis Nodes")
            .description("A comma-separated list of known Redis nodes in the format <IP>:<PORT>")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    protected static final PropertyDescriptor REDIS_CLUSTER_TIMEOUT = new PropertyDescriptor.Builder()
            .name("timeout-of-redis-cluster")
            .displayName("Timeout of Redis Cluster")
            .description("A configuration of JedisCluster defined timeout property.")
            .required(false)
            .defaultValue(DEFAULT_TIMEOUT + "")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    protected static final PropertyDescriptor REDIS_CLUSTER_MAX_REDIRECTIONS = new PropertyDescriptor.Builder()
            .name("max-redirections-of-redis-cluster")
            .displayName("MAX REDIRECTIONS of Redis Cluster")
            .description("A configuration of JedisCluster defined max redirections of the cluster.")
            .required(false)
            .defaultValue(DEFAULT_MAX_REDIRECTIONS + "")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    @Override
    protected void addSupportedPropertyDescriptors(List<PropertyDescriptor> props) {
        props.add(REDIS_NODES);
        props.add(REDIS_CLUSTER_TIMEOUT);
        props.add(REDIS_CLUSTER_MAX_REDIRECTIONS);
        props.remove(REDIS_IP);
        props.remove(REDIS_DATABASE);
    }

    @Override
    public void createRedisContext(ProcessContext context) {
        String host = context.getProperty(REDIS_NODES).getValue();
        String password = context.getProperty(REDIS_PASSWORD).isSet()
                ? context.getProperty(REDIS_PASSWORD).getValue() : null;
        int timeout = context.getProperty(REDIS_CLUSTER_TIMEOUT).isSet() ?
                context.getProperty(REDIS_MAX_TOTAL).asInteger() : DEFAULT_TIMEOUT;
        int maxRedirections = context.getProperty(REDIS_CLUSTER_MAX_REDIRECTIONS).isSet() ?
                context.getProperty(REDIS_MAX_TOTAL).asInteger() : DEFAULT_MAX_REDIRECTIONS;
        int maxTotal = context.getProperty(REDIS_MAX_TOTAL).isSet() ?
                context.getProperty(REDIS_MAX_TOTAL).asInteger() : DEFAULT_MAX_TOTAL;
        int minIDEL = context.getProperty(REDIS_MIN_IDEL).isSet() ?
                context.getProperty(REDIS_MIN_IDEL).asInteger() : DEFAULT_MIN_IDEL;
        int maxIDEL = context.getProperty(REDIS_MAX_IDEL).isSet() ?
                context.getProperty(REDIS_MAX_IDEL).asInteger() : DEFAULT_MAX_IDEL;
        long maxWaitMillies = context.getProperty(REDIS_MAX_WAIT_MILLIES).isSet() ?
                context.getProperty(REDIS_MAX_WAIT_MILLIES).asLong() : DEFAULT_MAX_WAIT_MILLIES;

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(maxTotal);
        config.setMaxIdle(maxIDEL);
        config.setMinIdle(minIDEL);
        config.setMaxWaitMillis(maxWaitMillies);

        Set<HostAndPort> set = decodeHosts(host, context);
        jedisCluster = new JedisCluster(set, timeout, maxRedirections, config);
        if (password != null) jedisCluster.auth(password);
    }

    @Override
    public void destroyRedisContext(ProcessContext context) {
        if (jedisCluster != null) jedisCluster.close();
    }

    private Set<HostAndPort> decodeHosts(String hostStr, ProcessContext context) {
        Set<HostAndPort> set = new HashSet<>();
        try {
            String[] ips = hostStr.split(",");
            for (String ip : ips) {
                String[] hostAndPort = ip.split(":");
                if (hostAndPort.length == 2) {
                    String host = hostAndPort[0];
                    int port = Integer.valueOf(hostAndPort[1]);
                    HostAndPort ipPort = new HostAndPort(host, port);
                    set.add(ipPort);
                } else {
                    getLogger().error("Failed to decode REDIS_NODES: {}",
                            new String[]{hostStr});
                    context.yield();
                }
            }
        } catch (Exception e) {
            getLogger().error("Failed to decode REDIS_NODES {}, Exception {}",
                    new String[]{hostStr, e.toString()});
            context.yield();
        }
        return set;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final long start = System.nanoTime();

        final ComponentLog logger = getLogger();
        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());

        final FlowFile flowFile = session.get();
        if (flowFile == null) return;

        try {
            final byte[] content = new byte[(int) flowFile.getSize()];
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, content, true);
                }
            });

            // TODO --- do content decoding and put redis following!!!
            // mock data
            List<RedisPair> redisPairs = new ArrayList<>();
            for (int i = 0; i < 30; i++) {
                UUID uuid = UUID.randomUUID();
                String rkey = String.format("key%s", uuid.toString());
                String rvalue = String.format("value%s", new String(content, charset));
                redisPairs.add(new RedisPair(rkey, rvalue));
            }
            // formatted redis data into pipeline
            for (RedisPair pair : redisPairs) {
                jedisCluster.lpush(pair.getKey(), pair.getValue());
                jedisCluster.ltrim(pair.getKey(), 0, 59);
            }
            // TODO --- do content decoding and put redis above!!!

            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            logger.info("Successfully inserted {} into Redis with {} messages in {} millis",
                    new Object[]{flowFile, redisPairs.size(), millis});
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final RuntimeException e) {
            logger.error("Failed to execute insert {} due to {}", new Object[]{flowFile, e}, e);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
            session.rollback();
        }
    }
}

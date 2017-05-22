package io.qntfy.nifi.examplebean;

/**
 * Created by Neil on 2017/5/4.
 * Object inserted into redis.
 */
public class RedisPair {
    private String key;
    private String value;

    public RedisPair(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}

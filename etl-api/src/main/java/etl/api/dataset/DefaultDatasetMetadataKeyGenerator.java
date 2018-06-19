package etl.api.dataset;

import etl.api.parameter.Parameter;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import java.util.TreeSet;

public class DefaultDatasetMetadataKeyGenerator implements DatasetMetadataKeyGenerator {

    /**
     * Generates the data set meta data key to be used based on the {@link Parameter} Parameter set
     * provided.
     */
    @Override
    public String generateKey(Set<Parameter> source) {

        StringBuilder stringBuffer = new StringBuilder();
        if (source == null || source.isEmpty()) {
            stringBuffer.append((String) null);
        } else {
            new TreeSet<>(source).forEach(stringBuffer::append);
        }

        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(
                    "MD5 algorithm not available.  Fatal (should be in the JDK).", e);
        }

        try {
            byte[] bytes = digest.digest(stringBuffer.toString().getBytes(
                    "UTF-8"));
            return String.format("%032x", new BigInteger(1, bytes));
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(
                    "UTF-8 encoding not available.  Fatal (should be in the JDK).", e);
        }
    }

}

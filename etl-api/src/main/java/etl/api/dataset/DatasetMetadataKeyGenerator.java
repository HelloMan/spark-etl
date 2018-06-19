package etl.api.dataset;

import etl.api.parameter.Parameter;

import java.util.Set;
@FunctionalInterface
public interface DatasetMetadataKeyGenerator {

    String generateKey(Set<Parameter> source);
}

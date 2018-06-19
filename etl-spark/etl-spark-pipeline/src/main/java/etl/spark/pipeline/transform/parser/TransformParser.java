package etl.spark.pipeline.transform.parser;

import etl.api.pipeline.Transform;

/**
 * Parse Transform metadata into a spark Dataset by using the spark DataSet transformation api
 */
public interface TransformParser<T extends Transform>   {

    void parse(T transform);

}
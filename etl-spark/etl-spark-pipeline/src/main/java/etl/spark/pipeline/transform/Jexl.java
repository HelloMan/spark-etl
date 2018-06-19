package etl.spark.pipeline.transform;

import lombok.Builder;
import org.apache.commons.jexl3.JexlEngine;

import java.util.Map;

@Builder
public class Jexl {

    private static final int CACHE_SIZE = 512;

    public final static JexlEngine JEXL_ENGINE = Jexl.builder().cache(CACHE_SIZE).strict(true).silent(false).build().create();

    private int cache;

    private boolean strict;

    private boolean silent;

    private boolean debug;

    private Map<String,Object> functions ;


    public JexlEngine create(){
		org.apache.commons.jexl3.JexlBuilder builder = new org.apache.commons.jexl3.JexlBuilder();
		return builder.cache(cache)
				.silent(silent)
				.strict(strict)
				.namespaces(functions)
				.debug(debug)
				.create();
    }


}

package etl.common.lang;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import javaslang.Tuple;
import javaslang.Tuple2;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Getter
public final class CachedPattern {
	private static final Logger LOGGER = LoggerFactory.getLogger(CachedPattern.class);
	
	private static final LoadingCache<Tuple2<String, Integer>, Pattern> PATTERN_CACHE = CacheBuilder.newBuilder()
			.softValues()
			.build(new CacheLoader<Tuple2<String, Integer>, Pattern>() {
					   @Override
					   public Pattern load(Tuple2<String, Integer> regex) throws Exception {
						   return Pattern.compile(regex._1, regex._2);
					   }
				   }
			);
	
	private String regex;
	private int flag;

	public CachedPattern(String regex, int flag) {
		this.regex = regex;
		this.flag = flag;
	}
	
	public static CachedPattern compile(String regex) {
		return new CachedPattern(regex, 0);
	}
	
	public static CachedPattern compile(String regex, int flag) {
		return new CachedPattern(regex, flag);
	}


	private Pattern pattern() {
		try {
			return PATTERN_CACHE.get(Tuple.of(regex, flag));
		} catch (ExecutionException e) {
			LOGGER.error("Failed to get regex Pattern in cache: " + e.getMessage(), e);
			return Pattern.compile(regex, flag);
		}
	}

	public boolean find(String text) {
		return pattern().matcher(text).find();
	}

	public boolean matches(String text) {
		return pattern().matcher(text).matches();
	}

	public Stream<String[]> toStream(String text) {
		return StreamSupport.stream(new MatcherSpliterator(pattern().matcher(text)), false);
	}

	public class MatcherSpliterator extends Spliterators.AbstractSpliterator<String[]> {
		private final Matcher m;

		public MatcherSpliterator(Matcher m) {
			super(Long.MAX_VALUE, ORDERED | NONNULL | IMMUTABLE);
			this.m = m;
		}

		@Override
		public boolean tryAdvance(Consumer<? super String[]> action) {
			if (!m.find()) {
				return false;
			}
			final String[] groups = new String[m.groupCount() + 1];
			for (int i = 0; i <= m.groupCount(); i++) {
				groups[i] = m.group(i);
			}
			action.accept(groups);
			return true;
		}
	}


}
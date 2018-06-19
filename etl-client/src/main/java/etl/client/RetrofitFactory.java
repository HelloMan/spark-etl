package etl.client;

import etl.common.annotation.ToQuery;
import etl.common.json.MapperWrapper;
import lombok.experimental.UtilityClass;
import retrofit2.Converter;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Arrays;

@UtilityClass
public final class RetrofitFactory {



    public static Retrofit getInstance(String baseUrl) {
        return new Retrofit.Builder()
                .addConverterFactory(new Converter.Factory() {
                    @Override
                    public Converter<?, String> stringConverter(Type type, Annotation[] annotations, Retrofit retrofit) {
                        boolean hasToQuery = Arrays.stream(annotations).anyMatch(a -> ToQuery.class.equals(a.getClass()));
                        if (hasToQuery) {
                            return MapperWrapper.MAPPER::writeValueAsString;
                        }
                        return super.stringConverter(type, annotations, retrofit);
                    }
                })
                .addConverterFactory(JacksonConverterFactory.create())
                .baseUrl(baseUrl)
                .build();
    }


}


package etl.common.json;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public final class JsonCodec {

    private final Charset charset;

    public JsonCodec() {
        this(StandardCharsets.UTF_8);
    }

    public JsonCodec( Charset charset) {
        this.charset = charset;
    }

    public String encode(Object object) throws IOException{
        return encode(MapperWrapper.MAPPER.writeValueAsString(object));
    }

    public <T> T decode(String json,Class<T> tClass) throws IOException {
        return MapperWrapper.MAPPER.readValue(decode(json), tClass);
    }

    public String encode(String json){
        try {
            return URLEncoder.encode(json, charset.name());
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException(e);
        }
    }
    public String decode(String json){
        try {
            return URLDecoder.decode(json, charset.name());
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

}

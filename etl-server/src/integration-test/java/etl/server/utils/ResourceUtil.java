package etl.server.utils;

import lombok.experimental.UtilityClass;

import java.io.File;
import java.io.InputStream;
import java.net.URISyntaxException;

@UtilityClass
public class ResourceUtil {

    public static InputStream getResourceAsStream(String resource) {

        return ResourceUtil.class.getClassLoader().getResourceAsStream(resource);
    }

	public static File getResourceAsFile(String resource) throws URISyntaxException {
		return new File(ResourceUtil.class.getClassLoader().getResource(resource).toURI());
	}

    public static String getResourceRootPath() {
        File file = new File(ResourceUtil.class.getClassLoader().getResource(".").getPath());
        return file.getAbsolutePath();
    }
}

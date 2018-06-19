package etl.server.util;

import lombok.experimental.UtilityClass;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.servlet.HandlerMapping;

import javax.servlet.http.HttpServletRequest;

@UtilityClass
public class HttpUrlPathUtils {

    /**
     * Extract path from a controller mapping. /controllerUrl/** => return matched **
     * @param request incoming request.
     * @return extracted path
     */
    public static String extractPathFromPattern(final HttpServletRequest request){


        String path = (String) request.getAttribute(
                HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE);
        String bestMatchPattern = (String ) request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);

        AntPathMatcher apm = new AntPathMatcher();

        return apm.extractPathWithinPattern(bestMatchPattern, path);


    }
}

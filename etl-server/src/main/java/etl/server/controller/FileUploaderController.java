package etl.server.controller;

import etl.common.hdfs.FileSystemTemplate;
import etl.server.util.HttpUrlPathUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.annotation.MultipartConfig;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.InputStream;

/**
 * Upload schema files to HDFS
 * You can open web page http://etl-server:port/hdfs/{path/to/file} and drag-drop files to upload
 * Or you can use curl to upload file/delete file/download file:
 * curl -i -X POST -H "Content-Type: multipart/form-data" -F "file=@path/to/required_schema.xml" http://etl-server:port/hdfs/{path/to/file}
 * curl -X DELETE  http://etl-server:port/hdfs/{path/to/file}
 * curl -O http://etl-server:port/hdfs/file/{path/to/file}
 */
@Controller
@MultipartConfig(fileSizeThreshold = 20971520)
@RequestMapping(value = "/hdfs")
public class FileUploaderController {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileUploaderController.class);
    public static final String SUCCESS = "{\"success\":true}";
    public static final String FAIL = "{\"success\":false}";
    public static final String PATH_SEPARATE = "/";

    @Autowired
    private FileSystemTemplate fileSystemTemplate;

    @RequestMapping(value = "/**", headers = ("content-type=multipart/*"), method = RequestMethod.POST)
    public
    @ResponseBody
    String uploadFile(
            @RequestParam("file") MultipartFile uploadedFileRef,
            HttpServletRequest request) throws IOException {
        // Get name of uploaded file.
        String fileName = uploadedFileRef.getOriginalFilename();
        String targetPath = HttpUrlPathUtils.extractPathFromPattern(request);

        String targetFile = PATH_SEPARATE + StringUtils.appendIfMissing(targetPath, PATH_SEPARATE) + fileName;
        LOGGER.info("uploading file {} to HDFS {}", fileName, targetFile);

        fileSystemTemplate.copy(uploadedFileRef.getInputStream(), targetFile);

        return SUCCESS;
    }

    @RequestMapping(value = "/**", method = RequestMethod.GET)
    public String dropzone(HttpServletRequest request) {
        String targetPath = HttpUrlPathUtils.extractPathFromPattern(request);
        LOGGER.info("requesting uploading path {} ", targetPath);
        return "Dropzone";
    }

    @RequestMapping(value = "/**", method = RequestMethod.DELETE)
    public
    @ResponseBody
    String deleteFile(HttpServletRequest request)
            throws IOException {
        String targetFile = PATH_SEPARATE + HttpUrlPathUtils.extractPathFromPattern(request);

        LOGGER.info("requesting deleting path {} ", targetFile);
        if (!targetFile.equals("/")) {

            fileSystemTemplate.delete(targetFile);
            return SUCCESS;
        }

        return FAIL;
    }


    @RequestMapping(value = "/file/**",method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<Resource> serveFile(HttpServletRequest request) throws IOException {

        String targetFile = PATH_SEPARATE + HttpUrlPathUtils.extractPathFromPattern(request);

        String filename = org.springframework.util.StringUtils.getFilename(targetFile);
        LOGGER.info("downloading file {} from {}", filename, targetFile);

        InputStream inputStream = fileSystemTemplate.open(new org.apache.hadoop.fs.Path(targetFile));
        ByteArrayResource resource = new ByteArrayResource(StreamUtils.copyToByteArray(inputStream));

        return ResponseEntity
                .ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
                .body(resource);
    }


}

package etl.server.controller;

import com.google.common.io.ByteStreams;
import com.google.common.net.HttpHeaders;
import etl.common.hdfs.FileSystemTemplate;
import etl.server.IgnisApplication;
import etl.server.utils.ResourceUtil;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.io.File;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = IgnisApplication.class,webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@AutoConfigureMockMvc
public class FileUploaderControllerIT {
    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private FileSystemTemplate fileSystemTemplate;

    @LocalServerPort
    private int port;

    @Test
    public void shouldUploadFile() throws Exception {
//        ClassPathResource resource = new ClassPathResource("schema/employee.xml", getClass());
        ByteArrayResource resource = new ByteArrayResource(ByteStreams.toByteArray(ResourceUtil.getResourceAsStream("schema/employee.json")));
        File file = new File(ResourceUtil.class.getClassLoader().getResource("schema/employee.json").getPath());
        String path = file.getAbsolutePath();
        FileSystemResource fileSystemResource = new FileSystemResource(path);
        MultiValueMap<String, Object> map = new LinkedMultiValueMap<String, Object>();
        map.add("file", fileSystemResource);
        ResponseEntity<String> response = this.restTemplate.postForEntity("/hdfs/datasets/schema", map, String.class);

        assertThat(response.getStatusCode()).isEqualByComparingTo(HttpStatus.OK);

        assertThat(response.getBody()).isEqualTo("{\"success\":true}");

        byte[] encoded = ByteStreams.toByteArray(fileSystemTemplate.open(new Path("/datasets/schema/employee.json")));
        String fileContent = new String(encoded, StandardCharsets.UTF_8);
        assertThat(new String(resource.getByteArray())).isEqualTo(fileContent);
    }

    @Test
    public void shouldDownloadFile() throws Exception {
//        ClassPathResource resource = new ClassPathResource("schema/employee.xml", getClass());
//        given(this.fileSystemTemplate.open(new Path("/datasets/schema/employee.xml"))).willReturn(ResourceUtil.getResourceAsStream("schema/employee.xml"));
        fileSystemTemplate.copy(ResourceUtil.getResourceAsStream("schema/employee.json"), new Path("/datasets/schema/employee.json"));
//        fileSystemTemplate.copy(resource.getInputStream(), new Path("/datasets/schema/employee.xml"));
        ResponseEntity<String> response = this.restTemplate
                .getForEntity("/hdfs/file/datasets/schema/employee.json", String.class);

        assertThat(response.getStatusCodeValue()).isEqualTo(200);
        assertThat(response.getHeaders().getFirst(HttpHeaders.CONTENT_DISPOSITION))
                .isEqualTo("attachment; filename=\"employee.json\"");
        byte[] encoded = ByteStreams.toByteArray(ResourceUtil.getResourceAsStream("schema/employee.json"));
        String fileContent = new String(encoded, StandardCharsets.UTF_8);

        assertThat(response.getBody()).isEqualTo(fileContent);
    }
}

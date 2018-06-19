package etl.server.controller;

import etl.common.hdfs.FileSystemTemplate;
import etl.server.TestIgnisApplication;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.BDDMockito.then;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.refEq;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.fileUpload;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.forwardedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.view;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest(classes = TestIgnisApplication.class,webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class FileUploaderControllerTest {
    @Autowired
    private MockMvc mvc;

    @MockBean
    private FileSystemTemplate fileSystemTemplate;

    @Test
    public void shouldCopyUploadFile() throws Exception {
        MockMultipartFile multipartFile =
                new MockMultipartFile("file", "test.txt", "text/xml", "schema content".getBytes());

        this.mvc.perform(fileUpload("/hdfs/datasets/schema").file(multipartFile))
                .andExpect(status().isOk());


        then(this.fileSystemTemplate).should().copy(refEq(multipartFile.getInputStream()),
                eq("/datasets/schema/" + multipartFile.getOriginalFilename()));
    }

    @Test
    public void shouldDeleteFile() throws Exception {
        this.mvc.perform(delete("/hdfs/datasets/schema"))
                .andExpect(status().isOk());


        then(this.fileSystemTemplate).should().delete(anyString());
        verifyNoMoreInteractions(fileSystemTemplate);
    }

    @Test
    public void shouldReturnUpoladFilePage() throws Exception {
        this.mvc.perform(get("/hdfs/datasets/schema"))
                .andExpect(view().name("Dropzone"))
                .andExpect(forwardedUrl(null));
    }

    @Test
    public void should404WhenWrongPath() throws Exception {

        this.mvc.perform(get("/not/exist/path"))
                .andExpect(status().isNotFound());
    }
}

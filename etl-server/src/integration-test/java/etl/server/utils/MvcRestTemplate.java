package etl.server.utils;

import etl.common.json.MapperWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static etl.common.json.MapperWrapper.MAPPER;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Component
public class MvcRestTemplate {
    @Autowired
    private MockMvc mvc;

    public  <T> T post(String url,Object body,Class<T> responseBody, Object... parameters) throws Exception {
        MvcResult mvcResult =mvc.perform(MockMvcRequestBuilders
                .post(String.format(url, parameters))
                .contentType(MediaType.APPLICATION_JSON)
                .content(MAPPER.writeValueAsString(body)))
                .andExpect(status().isOk())
                .andReturn();

        return readValue(responseBody, mvcResult);
    }

    public  void put(String url,Object body,Object... parameters) throws Exception {
        mvc.perform(MockMvcRequestBuilders
                .put(String.format(url, parameters))
                .contentType(MediaType.APPLICATION_JSON)
                        .content(MAPPER.writeValueAsString(body)))
                .andExpect(status().isOk())
                .andReturn();
    }

    public void delete(String url, Object... urlVariables) throws Exception {
        mvc.perform(MockMvcRequestBuilders
                .delete(String.format(url, urlVariables))
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn();
    }

    public <T> T get(String url,Class<T> clazz,Object... parameters) throws Exception {
        MvcResult mvcResult =  this.mvc.perform(MockMvcRequestBuilders
                .get(String.format(url, parameters))
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn();

        return readValue(clazz, mvcResult);
    }

    private <T> T readValue(Class<T> clazz, MvcResult mvcResult) throws Exception {
        String jsonResult = mvcResult.getResponse().getContentAsString();

        if (String.class == clazz) {
            return (T)jsonResult;
        }
        return MapperWrapper.MAPPER.readValue(jsonResult, clazz);
    }



}

package ru.nsu.raft.api;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@DirtiesContext
class OpenApiTest {
    @Autowired
    private MockMvc mvc;

    @Test
    void documentsAllEndpointsAndUnwrapsAsyncResponseSchemas() throws Exception {
        String entryResponse = "$.paths['/entries'].post.responses['200'].content['application/json'].schema['$ref']";
        String errorResponse = "$.paths['/entries'].post.responses['409'].content['application/json'].schema['$ref']";
        String voteResponse = "$.paths['/raft/request-vote'].post.responses['200'].content['application/json'].schema['$ref']";
        String appendResponse = "$.paths['/raft/append-entries'].post.responses['200']"
            + ".content['application/json'].schema['$ref']";

        mvc.perform(get("/v3/api-docs"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.info.title").value("Raft API"))
            .andExpect(jsonPath("$.paths['/status'].get").exists())
            .andExpect(jsonPath("$.paths['/log'].get").exists())
            .andExpect(jsonPath("$.paths['/entries'].post").exists())
            .andExpect(jsonPath("$.paths['/raft/request-vote'].post").exists())
            .andExpect(jsonPath("$.paths['/raft/append-entries'].post").exists())
            .andExpect(jsonPath(entryResponse).value("#/components/schemas/EntryResponse"))
            .andExpect(jsonPath(errorResponse).value("#/components/schemas/ApiError"))
            .andExpect(jsonPath(voteResponse).value("#/components/schemas/RequestVoteResponse"))
            .andExpect(jsonPath(appendResponse).value("#/components/schemas/AppendEntriesResponse"))
            .andExpect(jsonPath("$.paths['/entries'].post.responses['400']").exists())
            .andExpect(jsonPath("$.paths['/entries'].post.responses['503']").exists())
            .andExpect(jsonPath("$.paths['/status'].get.responses['409']").doesNotExist());
    }

    @Test
    void servesSwaggerUiAndPointsItToTheApplicationSpec() throws Exception {
        mvc.perform(get("/swagger-ui.html"))
            .andExpect(status().is3xxRedirection())
            .andExpect(redirectedUrl("/swagger-ui/index.html"));
        mvc.perform(get("/swagger-ui/index.html"))
            .andExpect(status().isOk())
            .andExpect(content().contentTypeCompatibleWith("text/html"))
            .andExpect(content().string(containsString("Swagger UI")));
        mvc.perform(get("/v3/api-docs/swagger-config"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.url").value("/v3/api-docs"));
    }

    @Test
    void exportsOpenApiAsYaml() throws Exception {
        mvc.perform(get("/v3/api-docs.yaml"))
            .andExpect(status().isOk())
            .andExpect(content().string(containsString("openapi:")));
    }
}

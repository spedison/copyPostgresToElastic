package br.com.spedison.elastic;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.Getter;
import lombok.Setter;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;


public class ConexaoElastic {
    private static Logger log = Logger.getLogger(ConexaoElastic.class.getName());
    private ElasticsearchClient esClient;
    private RestClient restClient;
    @Setter
    private String user = "123456";
    @Setter
    private String pass = "elastic";
    @Setter
    private String host = "localhost";
    @Setter
    private String port = "9200";

    @Setter
    private boolean removeIndexAntesIniciar = false;

    @Setter
    @Getter
    private String indexName = "index_name";

    public boolean isRemoveIndexAntesIniciar() {
        return removeIndexAntesIniciar;
    }

    public boolean carregaDadosConexaoDoArquivo(String nomeArquivo) {

        try (FileInputStream is = new FileInputStream(new File(nomeArquivo))) {
            Properties prop = new Properties();
            prop.load(is);
            host = prop.getProperty("host", host);
            port = prop.getProperty("port", port);
            user = prop.getProperty("user", user);
            pass = prop.getProperty("pass", pass);
            indexName = prop.getProperty("index", indexName);
            removeIndexAntesIniciar = Boolean.parseBoolean(prop.getProperty("removeIndexAntesIniciar", "false"));
            return true;
        } catch (IOException ioe) {
            log.severe("Arquivo de configuração %s não conseguimos ler.".formatted(nomeArquivo));
            return false;
        }
    }

    public RestClient getRestClient() {
        return restClient;
    }

    private RestClient createRestClient() {
// Create the low-level client

        final BasicCredentialsProvider credentialsProvider =
                new BasicCredentialsProvider();

        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(user, pass));

        restClient = RestClient.builder(
                        new HttpHost(host, Integer.parseInt(port)))
                .setHttpClientConfigCallback(
                        (HttpAsyncClientBuilder httpClientBuilder) -> {
                            return httpClientBuilder
                                    .setDefaultCredentialsProvider(credentialsProvider);
                        }
                ).build();

        return restClient;
    }

    public ElasticsearchClient createElasticClient(RestClient restClient) {

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

// And create the API client
        esClient = new ElasticsearchClient(transport);

        return esClient;
    }

    public ElasticsearchClient getElasticClient() {
        return esClient;
    }

    public void abreConexao() {
        restClient = createRestClient();
        esClient = createElasticClient(restClient);
    }

    public void fechaConexao() {
        try {
            restClient.close();
        } catch (IOException ioe) {
            log.severe("Problemas ao fechar thread restClient.");
        }
    }

    public boolean criaIndice(String nome) {
        try {
            esClient
                    .indices()
                    .create(c -> c.index(nome));
            return true;
        } catch (IOException ioe) {
            return false;
        } catch (ElasticsearchException ese) {
            return false;
        }
    }

    public boolean removeIndice(String nome) {
        try {
            esClient
                    .indices()
                    .delete(c -> c.index(nome));
            return true;
        } catch (IOException | ElasticsearchException ioe) {
            log.severe("Erro ao remover ínidice " + ioe.getMessage());
            return false;
        }
    }

}

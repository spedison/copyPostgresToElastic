package br.com.spedison.elastic;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.util.BinaryData;
import co.elastic.clients.util.ContentType;
import org.elasticsearch.client.Request;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

public class GravaDados {

    private static Logger log = Logger.getLogger(GravaDados.class.getName());
    private ConexaoElastic conexaoElastic;

    private BulkRequest.Builder br;

    public boolean initBulkInsert() {
        br = new BulkRequest.Builder();
        return true;
    }

    public boolean addNewItemBulkInsert(String index, String id, String jsonString) {

        log.finest("Adicionando o JSon no BulkInsert -> " + jsonString);
        BinaryData data = BinaryData.of(jsonString.getBytes(StandardCharsets.UTF_8),
                ContentType.APPLICATION_JSON);

        br.operations(op -> op
                .index(idx -> idx
                        .index(index)
                        .id(id)
                        .document(data)));

        log.finest("Fim ao adicionar no BulkInsert");
        return true;
    }

    public boolean fechaBulkInsert() {
        try {
            BulkResponse result = conexaoElastic.getElasticClient().bulk(br.build());
            if (result.errors()) {
                for (BulkResponseItem item : result.items()) {
                    if (item.error() != null) {
                        log.severe(item.error().reason());
                    }
                }
                return false;
            }
            return true;
        } catch (IOException e) {
            System.err.println(e.getMessage());
            return false;
        }
    }

    public boolean criaIndice() {
        return conexaoElastic.criaIndice(getConexaoElastic().getIndexName());
    }

    public boolean criaIndice(String nome) {
        return conexaoElastic.criaIndice(nome);
    }

    public boolean removeIndice() {
        if (conexaoElastic.isRemoveIndexAntesIniciar())
            return removeIndice(getIndex());
        else
            return false;
    }

    public boolean removeIndice(String nome) {
        return conexaoElastic.removeIndice(nome);
    }

    public boolean adicionaJson(String index, String id, String json) {
        try {
            Request r = new Request(
                    "PUT",
                    "/" + index + "/_doc/" + id
            );
            r.setJsonEntity(json);
            conexaoElastic.getRestClient().performRequest(r);
            return true;
        } catch (IOException ioe) {
            System.err.println("Problemas ao adicionar o Json :: " + ioe.getMessage());
            return false;
        }
    }

    public boolean adicionaObjeto(String index, String id, Object objectToAdd) {
        try {
            IndexResponse response = conexaoElastic.getElasticClient().index(i -> i
                    .index(index)
                    .id(id)
                    .document(objectToAdd)
            );
            return response.id().equals(id);
        } catch (IOException ioe) {
            return false;
        } catch (ElasticsearchException ese) {
            return false;
        }
    }

    public boolean removeObjeto(String index, String id) {
        try {
            DeleteResponse dr = conexaoElastic.getElasticClient().delete(d -> d.index(index).id(id));
            return dr.id().equals(id);
        } catch (IOException ioe) {
            return false;
        } catch (ElasticsearchException ese) {
            return false;
        }
    }

    public void abreConexao(String nomeArquivoConexao) {
        conexaoElastic = new ConexaoElastic();
        if (!conexaoElastic.carregaDadosConexaoDoArquivo(nomeArquivoConexao)) {
            log.severe("Não conseguimos carregar dados da conexao do elastic " + nomeArquivoConexao);
        }
        conexaoElastic.abreConexao();
    }

    public void fechaConexao() {
        conexaoElastic.fechaConexao();
    }

    public ConexaoElastic getConexaoElastic() {
        return conexaoElastic;
    }

    public String getIndex() {
        return conexaoElastic.getIndexName();
    }
}


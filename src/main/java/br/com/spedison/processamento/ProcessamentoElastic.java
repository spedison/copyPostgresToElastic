package br.com.spedison.processamento;

import br.com.spedison.elastic.GravaDados;
import org.apache.commons.io.FileUtils;

import java.util.List;
import java.util.logging.Logger;

public class ProcessamentoElastic {

    private static Logger log = Logger.getLogger(ProcessamentoElastic.class.getName());

    private GravaDados gravaDados = new GravaDados();
    int tamanhoPacote;

    String nomeArquivoConexao;
    Long inicioProcessamento;

    public ProcessamentoElastic() {
    }

    public ProcessamentoElastic(String nomeArquivoConexao) {
        inicioProcessamento = System.currentTimeMillis();
        this.nomeArquivoConexao = nomeArquivoConexao;
        this.gravaDados.abreConexao(nomeArquivoConexao);
    }

    private String getIndex() {
        return gravaDados.getIndex();
    }

    public void gravaBulk(List<DadosJson> dados) {
        dados
                .stream()
                .forEach(p -> {
                            gravaDados
                                    .addNewItemBulkInsert(
                                            getIndex(),
                                            p.getId(),
                                            p.getJson());
                            tamanhoPacote += (p.getJson().length() + 1);
                        }
                );

        if (tamanhoPacote > (5 * 1024 * 1024))
            log.warning("Pacote com tamanho maior que 5 MB.");
    }

    public void inicializaGravacao(String nomeArquivoConexao) {
        inicioProcessamento = System.currentTimeMillis();
        this.nomeArquivoConexao = nomeArquivoConexao;
        tamanhoPacote = 0;
        gravaDados.abreConexao(nomeArquivoConexao);
        gravaDados.initBulkInsert();
    }

    public void terminaGravacao() {
        gravaDados.fechaBulkInsert();
        fechaConexao();
        log.info(
                "Enviando pacote para elastic de %s em %f segundos"
                        .formatted(
                                FileUtils.byteCountToDisplaySize(tamanhoPacote),
                                (System.currentTimeMillis() - inicioProcessamento) / 1000.0)
        );
    }

    public void reiniciarIndice() {
        gravaDados.removeIndice();
        gravaDados.criaIndice();
    }

    public void fechaConexao() {
        gravaDados.fechaConexao();
    }
}
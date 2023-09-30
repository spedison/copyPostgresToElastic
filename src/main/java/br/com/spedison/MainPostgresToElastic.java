package br.com.spedison;

import br.com.spedison.log.LogSetup;
import br.com.spedison.processamento.DadosJson;
import br.com.spedison.processamento.ProcessamentoBanco;
import br.com.spedison.processamento.ProcessamentoElastic;

import java.nio.file.Paths;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class MainPostgresToElastic {


    private record Px(String id, String json) {
    }

    ;

    public static void main(String[] args) {

        String dirBase = args.length == 1 ? args[1] : "/mnt/dados/git/acessando-elastic/conf/";

        // Configura o arquivo de Logs.
        long inicioPrograma = System.currentTimeMillis();
        LogSetup.loadConfig();
        Logger log = Logger.getLogger(MainPostgresToElastic.class.getName());

        log.info("Iniciando o processamento.");

        ProcessamentoBanco processamentoBanco = new ProcessamentoBanco(Paths.get(dirBase, "postgres-connection.properties").toString());
        //pb.setNomeArquivoConexao(Paths.get(dirBase, "postgres-connection.properties").toString());
        processamentoBanco.setNomeArquivoQueryIDs(Paths.get(dirBase, "define_id.sql").toString());
        processamentoBanco.setNomeArquivoQueryJson(Paths.get(dirBase, "monta_json.sql").toString());
        String nomeArquivoConfiguracaoElastic = Paths.get(dirBase, "elastic-connection.properties").toString();

        // Levantamentos de todos os IDs para processamento
        long inicio = System.currentTimeMillis();
        processamentoBanco.carregaDadosIdsParaProcessar();
        long tempoGastoParaLerIDs = System.currentTimeMillis() - inicio;

        // Reiniciar (se necessário) o índice do elasticsearch.
        ProcessamentoElastic iniciaIndice = new ProcessamentoElastic(nomeArquivoConfiguracaoElastic);
        iniciaIndice.reiniciarIndice();
        iniciaIndice.fechaConexao();

        // Agrupa os processos para processar.
        inicio = System.currentTimeMillis();
        List<String> registrosParaProcessar = processamentoBanco.montaPacotesProcessamentos();
        long tempoGastoParaMontarPacotesDeIDs = System.currentTimeMillis() - inicio;
        // Usa cada um dos lotes para inserir no ElasticSearch. (Esse é a função que será usada)
        Consumer<String> processaRegistrosPostgresToElastic = (strRegistrosParaProcessar) -> {
            // Lê dados do Postgres
            List<DadosJson> dadosJson = processamentoBanco.carregaDadosJson(strRegistrosParaProcessar);
            // Se tiver dados ele tentará fazer um BulkInsert no ElasticSearch
            if (dadosJson.size() > 0) {
                // Com os dados em memória, é possível copiá-los no elasticsearch na forma de bulkinsert.
                ProcessamentoElastic processamentoElastic = new ProcessamentoElastic();
                processamentoElastic.inicializaGravacao(nomeArquivoConfiguracaoElastic);
                processamentoElastic.gravaBulk(dadosJson);
                processamentoElastic.terminaGravacao();
            }
        };

        inicio = System.currentTimeMillis();
        // Stream baseado nas Strings usadas nas consultas para realizar o processamento
        registrosParaProcessar
                .stream()
                .parallel()
                .filter(s -> !s.isEmpty())
                .forEach(processaRegistrosPostgresToElastic);
        long tempoGastoParaLerPostgresEGravadoNoPostgres = System.currentTimeMillis() - inicio;

        // Registros dos tempos.
        log.info("tempoGastoParaLerIDs =  " + tempoGastoParaLerIDs);
        log.info("tempoGastoParaMontarPacotesDeIDs =  " + tempoGastoParaMontarPacotesDeIDs);
        log.info("tempoGastoParaLerPostgresEGravadoNoPostgres =  " + tempoGastoParaLerPostgresEGravadoNoPostgres);
        log.info("tempoTotal =  " + (System.currentTimeMillis() - inicioPrograma));
        log.info("Processamento terminado.");
    }
}

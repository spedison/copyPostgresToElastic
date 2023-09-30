package br.com.spedison;

import br.com.spedison.elastic.GravaDados;
import br.com.spedison.test.convertjson.ConvertJson;
import br.com.spedison.test.convertjson.DadosLocalizacao;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationConfig;
import jakarta.json.JsonObjectBuilder;

import java.io.File;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

public class MainJson {
    public static void main(String[] args) {

        final String strDirBase = args.length == 0 ?
                "/media/spedison/994a778f-10d5-422f-a2d9-acdd538edb7e/home/spedison/processaJson/entrada/" : args[0];

        ConvertJson convertJson = new ConvertJson();
        List<DadosLocalizacao> listaDadosLocalizacao = Collections.synchronizedList(new LinkedList<DadosLocalizacao>());
        File dirBase = new File(strDirBase);

        // Processa todos os arquivos e os coloca em objetos.
        Arrays
                .stream(dirBase.list((File dir, String name) -> name.toLowerCase().endsWith(".json")))
                .parallel()
                .forEach(nomeArquivoProcessado -> {
                    try {
                        DadosLocalizacao dl = convertJson.processaUmArquivo(Paths.get(strDirBase, nomeArquivoProcessado).toString());
                        if (Objects.nonNull(dl)) {
                            listaDadosLocalizacao.add(dl);
                            System.out.println("Processado arquivo " + nomeArquivoProcessado);
                        } else {
                            System.out.println("Arquivo " + nomeArquivoProcessado + " Não Processado");
                        }
                    } catch (NullPointerException npe) {
                        System.err.println("Problema ao processar aquivo " + nomeArquivoProcessado);
                    }
                });


//        listaDadosLocalizacao
//                .stream()
//                .map(DadosLocalizacao::toString)
//                .map(s -> {
//                    return s.replaceAll(", ", ",\n") + "\n---------------\n\n";
//                })
//                .forEach(System.out::println);

        System.out.println("-----------------------------------------");

        String nomeIndice = "localizacao";
        final GravaDados gd = new GravaDados();
        // Função para converter o objeto em string json.
        final ObjectMapper mapper = new ObjectMapper();
        Consumer<DadosLocalizacao> pk = (dlp) -> {
            try {
                String ret = mapper.writeValueAsString(dlp);
                gd.addNewItemBulkInsert(nomeIndice, dlp.getNomeArquivo(), ret);
            } catch (JsonProcessingException jpe) {
            }
        };

//        listaDadosLocalizacao
//                .stream()
//                .map(pk)
//                .forEach(System.out::println);

        gd.abreConexao("/mnt/dados/git/acessando-elastic/conf/elastic-connection.properties");

        //gd.criaIndice(nomeIndice);

        gd.initBulkInsert();

        listaDadosLocalizacao
                .stream()
                .forEach(pk);
        gd.fechaBulkInsert();
        gd.fechaConexao();
    }
}

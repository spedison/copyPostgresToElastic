package br.com.spedison;

import br.com.spedison.elastic.GravaDados;
import br.com.spedison.elastic.Pessoa;
import br.com.spedison.log.LogSetup;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;

public class MainElastic {


    private record Px(String id, String json){};

    public static void main(String[] args) {

        LogSetup.loadConfig();
        Logger log = Logger.getLogger(MainElastic.class.getName());

        log.info("Meu teste.");

        GravaDados gravaDados = new GravaDados();
        gravaDados.abreConexao("/mnt/dados/git/acessando-elastic/conf/elastic-connection.properties");
        String indice = "pessoa";
        boolean retCriaIndice = true; // gravaDados.criaIndice(indice);
        String resultado = retCriaIndice ? "Indice Criado" : "Índice não Criado";
        System.out.println(resultado);
        if (!retCriaIndice)
            return;

        Pessoa p = new Pessoa(1L, "Edison", "Ribeiro Araújo", true, "spedison@gmail.com");
        boolean retAdicionaObjeto = gravaDados.adicionaObjeto(indice, "%d".formatted(p.id()), p);
        resultado = retAdicionaObjeto ? "Objeto Adicionado" : "Objeto não Adicionado";
        System.out.println(resultado);

        p = new Pessoa(2L, "Neci", "Lima Guimarães Araújo", true, "necilg@gmail.com");
        retAdicionaObjeto = gravaDados.adicionaObjeto(indice, "%d".formatted(p.id()), p);
        resultado = retAdicionaObjeto ? "Objeto Adicionado" : "Objeto não Adicionado";
        System.out.println(resultado);

        try {
            p = new Pessoa(3L, "Emerson", "Ribeiro Araújo", false, "emerson@gmail.com");
            ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
            String json = ow.writeValueAsString(p);
            log.info("Json gerado : " + json);
            gravaDados.adicionaJson(indice, "%d".formatted(p.id()), json);
        } catch (JsonProcessingException jpe) {
            System.out.println("Problemas ao converter o objeto em json.");
        }


        List<Pessoa> lista = List.of(
                new Pessoa(4L, "Emerson 1", "Ribeiro Araújo", false, "emerson@gmail.com"),
                new Pessoa(5L, "Emerson 2", "Ribeiro Araújo", true, "emerson@gmail.com"),
                new Pessoa(6L, "Emerson 3", "Ribeiro Araújo", false, "emerson@gmail.com"),
                new Pessoa(7L, "Emerson 4", "Ribeiro Araújo", true, "emerson@gmail.com"),
                new Pessoa(8L, "Emerson 5", "Ribeiro Araújo", false, "emerson@gmail.com"),
                new Pessoa(9L, "Emerson 6", "Ribeiro Araújo", true, "emerson@gmail.com"),
                new Pessoa(10L, "Emerson 7", "Ribeiro Araújo", false, "emerson@gmail.com")
        );


        gravaDados.initBulkInsert();
        ObjectWriter ow = new ObjectMapper().writer();
        lista
                .stream()
                .map(px -> {
                    try {
                        return new Px("%d".formatted(px.id()), ow.writeValueAsString(px));
                    } catch (JsonProcessingException e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .forEach(s -> gravaDados.addNewItemBulkInsert(indice, s.id(), s.json()));

        gravaDados.fechaBulkInsert();


        gravaDados.fechaConexao();
    }
}

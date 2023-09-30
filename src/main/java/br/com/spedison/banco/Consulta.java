package br.com.spedison.banco;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Logger;

public class Consulta {
    private static Logger log = Logger.getLogger(Consulta.class.getName());

    private Consulta() {
    }

    public static String carregaConsulta(String arquivo) {
        try {
            StringBuilder ret = new StringBuilder();
            Files
                    .readAllLines(Path.of(arquivo))
                    .stream()
                    .map(String::trim)
                    .filter(s -> !s.startsWith("--"))
                    .map(s -> s + " \n ")
                    .forEach(ret::append);
            log.config("Carregada consulta : " + arquivo);
            return ret.toString();
        } catch (IOException e) {
            log.severe("Problema ao carregar o arquivo " + arquivo);
            throw new RuntimeException(e);
        }
    }
    //TODO: Fazer uma carga com troca de variáveis. Apresento um map ele ele carrega o arquivo e executa o replace.

    //TODO: Carregar um carquivo dentro do Resources
}

package br.com.spedison.log;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class LogSetup {

    static public void loadConfig(){
        InputStream is = LogSetup.class.getClassLoader().getResourceAsStream("log.properties");

        // Para ler de um arquivo externo.
        //LogManager.getLogManager().readConfiguration(new FileInputStream("mylogging.properties"));
        Logger log = null;
        try {
            LogManager.getLogManager().readConfiguration(is);
            log = Logger.getLogger(LogSetup.class.getName());
            log.info("Configuração do Log Carregada do resource.");
        } catch (IOException e) {
            System.err.println("Problemas ao ler arquivos de configuração de log. Vou parar.");
            throw new RuntimeException(e);
        }
    }
}

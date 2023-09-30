package br.com.spedison.banco;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

public class ConexaoBanco {
    private Properties prop = new Properties();
    private static Logger log = Logger.getLogger(ConexaoBanco.class.getName());

    public boolean load(String fileConetion) {

        log.fine("Carregado o arquivo :: " + fileConetion);

        File file = new File(fileConetion);

        if (!file.exists()) {
            log.severe("Arquivo %s não existe.".formatted(fileConetion));
            return false;
        }

        try (FileInputStream fis = new FileInputStream(file)) {
            prop.load(fis);
            log.fine("Arquivo %s foi carregado.".formatted(fileConetion));
        } catch (IOException e) {
            log.severe("Problemas ao carregar arquivo " + fileConetion);
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public Connection getConection() {
        // Somente para carregar o Driver pedido.
        String driver = prop.getProperty("driver").trim();
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            log.severe("Drive não foi carregado. Verifique. " + e.getMessage());
            e.printStackTrace();
            return null;
        }

        log.info("A classe %s foi carregada.".formatted(driver));

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(
                    prop.getProperty("url"),
                    prop.getProperty("user"),
                    prop.getProperty("pass"));
        } catch (SQLException e) {
            log.severe(
                    "Problemas ao conectar com o Banco URL :: " + prop.getProperty("url") +
                            " Usuário :: " + prop.getProperty("user") + "  -- " + e.getMessage());
            e.printStackTrace();
            return null;
        }

        log.info("A conexão foi criada.");

        return connection;
    }

    public int getLoteInsercao() {
        return Integer.parseInt(prop.getProperty("loteInsercao", "10"));
    }

}
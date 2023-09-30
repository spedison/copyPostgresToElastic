package br.com.spedison.banco;

import java.io.BufferedWriter;
import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.StringJoiner;
import java.util.logging.Logger;

public class ConsultasParaStr {
    static final String DELIMITER = "|";
    static final String ENCODING = "UTF-8";
    private static Logger log = Logger.getLogger(ConsultasParaStr.class.getName());

    int[] camposTipos;
    String[] camposNomes;
    String[] camposCampo;

    boolean jaCarregouMetaDados = false;

    String sep = ";";

    public String[] getCamposNomes() {
        return camposNomes;
    }

    public void setSep(String sep) {
        this.sep = sep;
    }

    void carregaTipos(ResultSet resultSet) throws SQLException {

        if (jaCarregouMetaDados) {
            return;
        }

        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

        camposTipos = new int[resultSetMetaData.getColumnCount()];
        camposNomes = new String[resultSetMetaData.getColumnCount()];
        camposCampo = new String[resultSetMetaData.getColumnCount()];

        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            camposCampo[i - 1] = resultSetMetaData.getColumnName(i);
            camposNomes[i - 1] = resultSetMetaData.getColumnLabel(i);
            camposTipos[i - 1] = resultSetMetaData.getColumnType(i);
        }

        jaCarregouMetaDados = true;
    }

    static Object getValue(ResultSet rs, int field, int tipo) throws SQLException {
        Object ret = null;
        try {
            switch (tipo) {
                case Types.BIT:
                    ret = Boolean.valueOf(rs.getBoolean(field));
                    break;
                case Types.TINYINT:
                    ret = Integer.valueOf(rs.getInt(field));
                    break;
                case Types.SMALLINT:
                    ret = Integer.valueOf(rs.getInt(field));
                    break;
                case Types.INTEGER:
                    ret = Integer.valueOf(rs.getInt(field));
                    break;
                case Types.BIGINT:
                    ret = Long.valueOf(rs.getLong(field));
                    break;
                case Types.FLOAT:
                    ret = Double.valueOf(rs.getDouble(field));
                    break;
                case Types.REAL:
                    ret = Double.valueOf(rs.getDouble(field));
                    break;
                case Types.DOUBLE:
                    ret = Double.valueOf(rs.getDouble(field));
                    break;
                case Types.NUMERIC:
                    ret = Double.valueOf(rs.getDouble(field));
                    break;
                case Types.DECIMAL:
                    ret = Double.valueOf(rs.getDouble(field));
                    break;
                case Types.CHAR:
                    ret = rs.getString(field);
                    break;
                case Types.VARCHAR:
                    ret = rs.getString(field);
                    break;
                case Types.LONGVARCHAR:
                    ret = rs.getString(field);
                    break;
                case Types.DATE:
                    ret = rs.getDate(field);
                    if (!rs.wasNull()) {
                        ret = ((Date) ret).toString();
                    }
                    break;
                case Types.TIME:
                    if (!rs.wasNull()) {
                        ret = rs.getTime(field).toString();
                    } else
                        ret = "null";
                    break;
                case Types.TIMESTAMP:
                    if (!rs.wasNull()) {
                        ret = rs.getTimestamp(field).toString();
                    } else
                        ret = "null";
                    break;
                case Types.BINARY:
                    ret = rs.getBinaryStream(field);
                    break;
                case Types.VARBINARY:
                    ret = rs.getBinaryStream(field);
                    break;
                case Types.LONGVARBINARY:
                    ret = rs.getBinaryStream(field);
                    break;
                case Types.NULL:
                    ret = "";
                    break;
                case Types.OTHER:
                    ret = "";
                    break;
                case Types.BLOB:
                    ret = rs.getBinaryStream(field);
                    break;
                case Types.CLOB:
                    ret = rs.getAsciiStream(field);
                    break;
                default:
                    log.warning("Campo [%d] sem definição de tipo".formatted(field));
                    ret = "";
            }
            if (rs.wasNull() || ret == null) {
                return "";
            }
            return ret;
        } catch (RuntimeException rte) {
            log.severe("Problemas na conversão dos campos : " + rte.getMessage());
            return "";
        }
    }

    public List<List<String>> carregaRegistrosLinhaStr(String arquivoConexao, String sql) {
        sql = sql.trim();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }

        ConexaoBanco db = new ConexaoBanco();
        if (!db.load(arquivoConexao)) {
            log.severe("Problemas com a carga do aquivo de conexao : " + arquivoConexao);
            return null;
        }
        Connection con = db.getConection();
        if (con == null)
            return null;
        List<List<String>> ret = new LinkedList<List<String>>();


        // Create statement so that we can execute
        // all of our queries
        // 3. Create a statement object
        try (Statement statement = con.createStatement()) {

            // 4. Executing the query
            ResultSet resultSet
                    = statement.executeQuery(sql);

            carregaTipos(resultSet);

            while (resultSet.next()) {
                List<String> line = new LinkedList<>();
                for (int i = 0; i < camposTipos.length; i++) {
                    String valorCampo = getValue(resultSet, i + 1, camposTipos[i]).toString();
                    line.add(valorCampo);
                }
                ret.add(line);
            }
            con.close();
        }
        // in case of any SQL exceptions
        catch (SQLException s) {
            log.severe("SQL Não executado :: " + s.getMessage());
            try {
                if (con != null && !con.isClosed())
                    con.close();
            } catch (SQLException sqlcc) {
            }
        } catch (Exception e) {
            log.severe("SQL Não executado [Outros problemas] :: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // After completing the operations, we
            // need to null resultSet and connection
            //resultSet = null;
            try {
                if (con != null && !con.isClosed())
                    con.close();
            } catch (SQLException sqlcc) {
            }
            con = null;
        }
        return ret;
    }

    public List<List<String>> carregaRegistrosLinhaStr(String arquivoConexao, String sql, boolean comHeader) {

        sql = sql.trim();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }

        ConexaoBanco db = new ConexaoBanco();
        if (db.load(arquivoConexao) == false) {
            log.severe("problemas com a carga do aquivo de conexao : " + arquivoConexao);
            return null;
        }

        List<List<String>> ret = new LinkedList<List<String>>();
        try (Connection con = db.getConection()) {
            // Create statement so that we can execute
            // all of our queries
            // 3. Create a statement object
            try (Statement statement = con.createStatement()) {
                // 4. Executing the query
                try (ResultSet resultSet
                             = statement.executeQuery(sql)) {

                    carregaTipos(resultSet);

                    if (comHeader)
                        imprimeCabecalhoLinhaStr(ret);

                    while (resultSet.next()) {
                        List<String> line = new LinkedList<>();
                        for (int i = 0; i < camposTipos.length; i++) {
                            String valorCampo = getValue(resultSet, i + 1, camposTipos[i]).toString();
                            line.add(valorCampo);
                        }
                        ret.add(line);
                    }
                }
            }
        }
        // in case of any SQL exceptions
        catch (SQLException s) {
            s.printStackTrace();
            log.severe("SQL statement is not executed! " + s.getMessage());
        }
        // in case of general exceptions
        // other than SQLException
        catch (Exception e) {
            e.printStackTrace();
            log.severe("Outra excption! " + e.getMessage());
        }
        return ret;
    }

    private void imprimeCabecalho(BufferedWriter ret) throws IOException {
        StringJoiner sRet = new StringJoiner(DELIMITER);
        Arrays.stream(camposNomes).forEach(sRet::add);
        ret.write(sRet.toString());
        ret.newLine();
    }

    private void imprimeCabecalhoLinhaStr(List<List<String>> ret) {
        List<String> line = new LinkedList<>();
        Arrays.stream(camposNomes).forEach(line::add);
        ret.add(line);
    }
}

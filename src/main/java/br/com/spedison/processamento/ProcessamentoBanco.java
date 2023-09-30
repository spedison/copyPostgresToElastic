package br.com.spedison.processamento;

import br.com.spedison.banco.ConexaoBanco;
import br.com.spedison.banco.Consulta;
import br.com.spedison.banco.ConsultasParaStr;
import lombok.Data;

import java.util.LinkedList;
import java.util.List;
import java.util.StringJoiner;

@Data
public class ProcessamentoBanco {

    private List<String> idsParaProcessar;
    private String nomeArquivoConexao;
    private String nomeArquivoQueryIDs;
    private String nomeArquivoQueryJson;

    private static int TAMANHO_PACOTE = 5_000;

    private ConexaoBanco conexaoBanco = new ConexaoBanco();

    public ProcessamentoBanco(String nomeArquivoConexao) {
        this.nomeArquivoConexao = nomeArquivoConexao;
        conexaoBanco.load(nomeArquivoConexao);
    }

    public void carregaDadosIdsParaProcessar() {
        String consultaIDs = Consulta.carregaConsulta(nomeArquivoQueryIDs);
        ConsultasParaStr consultas = new ConsultasParaStr();
        List<List<String>> registros = consultas.carregaRegistrosLinhaStr(nomeArquivoConexao, consultaIDs, false);
        idsParaProcessar = new LinkedList<>();
        registros.stream().map(r -> r.get(0)).forEach(idsParaProcessar::add);
    }

    public int getQuantidadeRegistrosParaProcessar() {
        return idsParaProcessar.size();
    }

    public List<DadosJson> carregaDadosJson(String ids) {
        List<DadosJson> ret = new LinkedList<>();

        if (ids.length() == 0)
            return ret;

        String consultaJSON =
                Consulta
                        .carregaConsulta(nomeArquivoQueryJson)
                        .replace("?", ids.toString());

        ConsultasParaStr consulta = new ConsultasParaStr();
        consulta
                .carregaRegistrosLinhaStr(nomeArquivoConexao, consultaJSON, false)
                .stream()
                .map(s -> new DadosJson(s.get(0), s.get(1)))
                .forEach(ret::add);

        return ret;
    }

    public List<String> montaPacotesProcessamentos() {
        List<String> ret = new LinkedList<>();
        int pos = 0;
        for (int i = 0; i < ((idsParaProcessar.size() / getLoteInsercao()) + 1); i++) {
            StringJoiner itemToAdd = new StringJoiner(",");
            for (int pacote = 0; pacote < getLoteInsercao() && pos < idsParaProcessar.size(); pacote++) {
                itemToAdd.add("'" + idsParaProcessar.get(pos++) + "'");
            }
            ret.add(itemToAdd.toString());
        }
        return ret;
    }

    public List<DadosJson> carregaDadosJson(int inicio, int fim) {
        List<DadosJson> ret = new LinkedList<>();
        StringJoiner ids = new StringJoiner(",");

        idsParaProcessar
                .stream()
                .skip(inicio)
                .limit(fim - inicio)
                .map("'%s'"::formatted)
                .forEach(ids::add);

        return carregaDadosJson(ids.toString());
    }

    public int getLoteInsercao() {
        return conexaoBanco.getLoteInsercao();
    }
}
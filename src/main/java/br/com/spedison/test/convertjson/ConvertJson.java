package br.com.spedison.test.convertjson;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.util.Objects;
import java.util.stream.IntStream;

public class ConvertJson {
    public DadosLocalizacao processaUmArquivo(String nomeArquivo) {

        String arquivo = nomeArquivo;
        if (nomeArquivo == null) {
            arquivo = "/media/spedison/994a778f-10d5-422f-a2d9-acdd538edb7e/home/spedison/processaJson/entrada/010159894.json";
        }
        JSONParser jsonParser = new JSONParser();

        final DadosLocalizacao dadosLocalizacao = new DadosLocalizacao();
        try (FileReader reader = new FileReader(arquivo)) {

            dadosLocalizacao.setNomeArquivo(arquivo);

            //Read JSON file
            Object obj = jsonParser.parse(reader);

            JSONObject results = (JSONObject) obj;
            JSONArray arrResult = (JSONArray) results.get("results");
            JSONObject result = (JSONObject) arrResult.get(0);

            dadosLocalizacao.setLocationType(((JSONObject) result.get("geometry")).get("location_type").toString());

            dadosLocalizacao.setLatViewPortSouthwest(Double.parseDouble(
                    ((JSONObject)
                            ((JSONObject)
                                    ((JSONObject) result
                                            .get("geometry"))
                                            .get("viewport"))
                                    .get("southwest"))
                            .get("lat").toString()
            ));

            dadosLocalizacao.setLngViewPortSouthwest(Double.parseDouble(
                    ((JSONObject)
                            ((JSONObject)
                                    ((JSONObject) result
                                            .get("geometry"))
                                            .get("viewport"))
                                    .get("southwest"))
                            .get("lng").toString()
            ));


            dadosLocalizacao.setLatViewPortNortheast(Double.parseDouble(
                    ((JSONObject)
                            ((JSONObject)
                                    ((JSONObject) result
                                            .get("geometry"))
                                            .get("viewport"))
                                    .get("northeast"))
                            .get("lat").toString()
            ));

            dadosLocalizacao.setLngViewPortNortheast(Double.parseDouble(
                    ((JSONObject)
                            ((JSONObject)
                                    ((JSONObject) result
                                            .get("geometry"))
                                            .get("viewport"))
                                    .get("northeast"))
                            .get("lng").toString()
            ));

            Object pm = result.get("partial_match");
            if (Objects.isNull(pm))
                dadosLocalizacao.setPartialMath(false);
            else
                dadosLocalizacao.setPartialMath(Boolean.parseBoolean(
                        pm.toString()
                ));

            dadosLocalizacao.setPlaceID(
                    result.get("place_id").toString()
            );

            dadosLocalizacao.setStatus(
                    results.get("status").toString()
            );

            dadosLocalizacao.setNomeFormatado(
                    result.get("formatted_address").toString().replace(",", "-")
            );

            JSONArray arrComponent = (JSONArray) result.get("address_components");

            IntStream.range(0, arrComponent.size()).forEach(i -> {
                JSONObject obj1 = (JSONObject) arrComponent.get(i);
                if (
                        Objects.isNull(dadosLocalizacao.getNomeCurtoLocalidade()) &&
                                (temStringNoArray(obj1, "locality") ||
                                temStringNoArray(obj1, "route"))) {
                    dadosLocalizacao.setNomeCurtoLocalidade(obj1.get("short_name").toString());
                    dadosLocalizacao.setNomeLongLocalidade(obj1.get("long_name").toString());
                } else if (Objects.isNull(dadosLocalizacao.getCep()) && temStringNoArray(obj1, "postal_code")) {
                    dadosLocalizacao.setCep(obj1.get("short_name").toString());
                }
            });
            return dadosLocalizacao;
        } catch (ParseException ex) {
            System.out.println(ex.toString());
            return null;
        } catch (IOException ioe) {
            System.out.println(ioe.toString());
            return null;
        }
    }

    private boolean temStringNoArray(JSONObject objJson, String type) {
        JSONArray itens = (JSONArray) objJson.get("types");
        for (int i = 0; i < itens.size(); i++) {
            if (itens.get(i).toString().equalsIgnoreCase(type))
                return true;
        }
        return false;
    }
}

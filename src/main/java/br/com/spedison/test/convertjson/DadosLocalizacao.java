package br.com.spedison.test.convertjson;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.file.Paths;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class DadosLocalizacao {
    // Nome do arquivo processado. (Completo)
    String nomeArquivo;

    // results [0] -> address_components [types in 'locality'] -> short_name
    String nomeCurtoLocalidade;
    // results [0] -> address_components [types in 'locality'] -> long_name
    String nomeLongLocalidade;

    // results [0] -> formatted_address
    String nomeFormatado;

    // results [0] -> address_components [types in 'postal_code'] -> short_name
    String cep;

    // results [0] -> geometry -> location_type
    String locationType;

    // results [0] -> geometry -> viewport -> northeast -> lat
    Double latViewPortNortheast;
    // Results [0] -> geometry -> viewport -> northeast -> lng
    Double lngViewPortNortheast;
    // results [0] -> geometry -> viewport -> southwest -> lat
    Double latViewPortSouthwest;
    // results [0] -> geometry -> viewport -> southwest -> lng
    Double lngViewPortSouthwest;

    // results [0] -> partial_match
    Boolean partialMath;
    // results [0] -> place_id
    String placeID;
    // status
    String status;

    public String getNomeArquivo(){
        String fileName = Paths.get(nomeArquivo).getFileName().toString();
        var pos = fileName.lastIndexOf(".");
        return fileName.substring(0,pos);
    }

}
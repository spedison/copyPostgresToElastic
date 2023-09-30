package br.com.spedison.elastic;

public record Pessoa(Long id, String nome, String sobreNome, boolean validado, String email) {
}

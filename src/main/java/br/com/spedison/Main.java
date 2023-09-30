package br.com.spedison;

import java.util.stream.IntStream;

public class Main {
    public static void main(String[] args) {
        long inicio = System.currentTimeMillis();
        for (int l = 0; l < 30; l++) {
            MainPostgresToElastic.main(args);
        }
        System.out.println("Tempo gasto Total = " + (System.currentTimeMillis() - inicio));
    }
}

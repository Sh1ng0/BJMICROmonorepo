package com.bjpractice.bets;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BetsApplication {

	public static void main(String[] args) {
		SpringApplication.run(BetsApplication.class, args);
	}


	// AHORA NO ARRANCA POR QUE FALTA EL PRODUCTOR DE KAFKA PARA ENVIAR A USER EL RESULTADO DE LA PARTIDA
}

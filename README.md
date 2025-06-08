# 🃏 Blackjack Microservices

Proyecto modular de Blackjack basado en microservicios, diseñado para escalabilidad, trazabilidad y control de flujos mediante Apache Kafka (modo KRaft) y APISIX como API Gateway. Integración completa con Docker.

## 🧱 Tecnologías principales

- **Apache Kafka (KRaft mode)** — Mensajería entre microservicios.
- **APISIX** — API Gateway para control de tráfico, seguridad (JWT), y routing.
- **Spring Boot** — Framework principal de backend.
- **JUnit 5** — Testing unitario y de integración.
- **SQL (H2 / MySQL)** — Persistencia de datos.
- **Docker / Docker Compose** — Contenerización y orquestación.

## 🧩 Estructura de microservicios

- `player-service` — Gestión de jugadores y su estado.
- `game-service` — Lógica central del juego de Blackjack.
- `bet-service` — Gestión de apuestas y validación de fondos.
- `event-bus` — Comunicación basada en Kafka.
- `gateway` — Punto de entrada unificado vía APISIX.

## 🚀 Arranque rápido

```bash
docker-compose up --build

🔧 JSON Mapping con Hypersistence Utils
Este proyecto utiliza la librería hypersistence-utils para mapear objetos Java directamente como JSON en columnas SQL (json / jsonb), simplificando el trabajo con estructuras anidadas.

✅ Ventajas
Serializa y deserializa objetos automáticamente (usando Jackson).

Sin necesidad de @Converter manuales ni configuraciones adicionales.

Compatible con PostgreSQL (jsonb) y MySQL 5.7+ (json).

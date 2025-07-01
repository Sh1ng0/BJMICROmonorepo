


PLAN DE ATAQUE ACTUAL (MINIMO SERVICIO POSIBLE)


Recordando Nuestro Plan: El "Corte Vertical" slice 🔪
El plan que acordamos no era terminar game-core al 100%, sino construir un "esqueleto funcional" de extremo a extremo. El flujo era:

bets-service: Un usuario crea una apuesta (placeBet).

game-core: El usuario juega la partida usando el betId.

Kafka: game-core notifica el resultado de la partida.

bets-service: Recibe la notificación y actualiza el estado de la apuesta.

Para lograr esto, dijimos que haríamos lo siguiente:

Paso 1 (Casi Completo ✅): Asegurarnos de que game-core puede jugar una partida de principio a fin y, lo más importante, enviar la notificación GameFinishedEvent. El test de playerStand que acabamos de hacer nos da esa garantía.

Paso 2 (Lo que toca ahora 🚀): Enseñar a bets-service a recibir esa notificación.

Paso 3: Probar que la comunicación entre ambos funciona.



🎯 Foco Principal: Requisitos y Estabilidad
(Estas son las tareas bloqueantes. Sin esto, el resto del desarrollo se complica).

Entender el Dominio USER

[ ] Leer el documento de requisitos: KafkaBlackJack.odt.
Objetivo: Tener claras las reglas de negocio antes de escribir más código para el microservicio user.
Estabilizar el Entorno Local

[ ] Levantar cada microservicio (game-core, bets, user, auth) de forma individual.
[ ] Validar que cada uno arranca sin errores y responde en su endpoint de salud (/actuator/health).
Objetivo: Asegurar que la base del proyecto es funcional y podemos desarrollar sobre seguro.


✅ Siguiente Paso: Confianza y Calidad del Código
(Una vez que todo arranca, aseguramos una calidad mínima para no arrastrar problemas).

Testing para KAFKA
Preparar testcontainers

[ ] user: Crear un test básico que verifique que el contexto de Spring carga (@SpringBootTest).
[ ] auth: Crear un test básico que verifique que el contexto de Spring carga.
[ ] bets: (Si no lo tiene ya) test básico de carga de contexto.
Objetivo: Ganar confianza. Si este test pasa, sabemos que la configuración, inyección de dependencias y componentes principales del micro están bien.
Mejorar Cobertura de Tests (Enfoque 80/20)

[ ] Identificar las 1-2 clases más críticas de cada microservicio (probablemente las clases de Service o de lógica de dominio).
[ ] Añadir los tests unitarios que falten para cubrir la lógica de negocio principal de esas clases.
Objetivo: No buscamos 100% de cobertura, sino asegurar que el corazón de nuestra aplicación está bien probado.
Revisión con SonarQube

[ ] Lanzar un análisis de SonarQube.
[ ] Corregir únicamente los errores Blocker y Critical que encuentre. Ignorar el resto por ahora.
Objetivo: Eliminar los problemas más graves de forma rápida y continuar.


🚀 A Futuro / Opcional
(Tareas para cuando haya tiempo o se conviertan en una necesidad).

Preparar Dockerización

[ ] Crear un Dockerfile base para el microservicio más estable.
[ ] Empezar a documentar los puertos y variables de entorno necesarias para cada servicio en un README.md.
Documentación Mínima Vital

[ ] En el README.md principal, añadir una sección "Cómo arrancar el proyecto" que liste los microservicios y sus puertos.
Objetivo: Que tu "yo" del futuro no te odie. No es documentar la API, es dejar una nota de cómo funciona el puzzle.

----------------------------------------------



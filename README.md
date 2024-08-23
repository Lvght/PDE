# Trabalho de Processamento de Dados em Escala

Este repositório contém o código e a documentação acerca do trabalho realizado para a disciplina **Processamento de Dados em Escala** (1001515) no primeiro semestre de 2024 pelo curso de Ciência da Computação pela Universidade Federal de São Carlos.

# Sobre a aplicação

A aplicação aqui proposta realiza a filtragem de dados ruidosos recebidos a partir de uma *stream*, propagando dados limpos em tempo real para serem consumidos por outro módulo que efetue a lógica de negócios.

## Sobre os dados utilizados

> ⚠️ Os dados aqui utilizados foram cedidos pela empresa **Centro de Pesquisas Avançadas Wernher von Braun**, para os fins desta disciplina, com devida materialização destas permissões por registro eletrônico.

Os dados utilizados representam leituras captadas por leitores de RFID no contexto de uma aplicação de controle de estoque de ármazem. Há muitos registros, superando 8 milhões.

A estrutura de cada registro é explicada na tabela que segue.

| Campo          | Descrição                                                                         |
| -------------- | --------------------------------------------------------------------------------- |
| forkliftid     | Identificador do veículo                                                          |
| tabletid       | Identificador do tablet                                                           |
| readerid       | Identificador do leitor                                                           |
| record_start   | Início do *uptime* do dispositivo de leitura quando a leitura foi efetuada        |
| record_end     | Término do *uptime* do dispositivo de leitura quando a leitura foi efetuada       |
| read_timestamp | Timestamp de quando a leitura foi efetuada                                        |
| rssi_mean      | *Received Signal Strenght Indicator* (Indicador de Força do Sinal Recebido) médio |



# Infraestrutura utilizada

![[docs/c4.png]]

A fim de simular o envio de dados observado em aplicações do mundo real, onde dispositivos IoT enviam informações para algum serviço receptor, que por sua vez disponibiliza esta stream para processamento, optamos por:

- Simular o dispotivo IoT através do arquivo de dados e de um container que lê as informações e as disponibiliza em um tópico de nosso container Kafka.
- Utilizar o Kafka para disponibilizar a stream de dados para o processamento.
- Utilizar o Spark para processar os dados em tempo real.

---

Dada a quantidade massiva de dados disponíveis, com milhões de registros, fazemos uso de tecnologias de processamento em tempo real para remover os dados ruidosos.

A estratégia consiste em executar os seguites passos:

- Um módulo fornecedor lê o arquivo e emite-os em tempo real utilizando para isso um canal WebSocket.
- Um módulo *worker*, utilizando Spark, se conecta a este websocket, avalia os dados recebidos sob uma determinada lógica, e então emite um resultado (como o recebeu ou filtrado).
  - Tal lógica, por ora, consiste em remover dados cujo RSSI se encontre fora de determinado limiar, vez que isto indica uma leitura por reflexão de sinal, o que é indesejado.
- Os resultados são disponibilizados em um arquivo ou stream.
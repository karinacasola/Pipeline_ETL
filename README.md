# Pipeline de ETL com PostgreSQL e Python

[Minhas redes sociais](https://linktr.ee/karinacasola)

## ÍNDICE

* [Pré-requisitos](#Pré-requisitos)
* [Biblioteca Python ](#Bibliotecas-Python)
* [Visão dos dados e do código](#Visão-dos-dados-e-do-código)
* [Modelo de dados](#Modelo-de-dados)
* [Como executar](#Instruções-sobre-como-executar)

## Pré-requisitos:

*  [PostgreSQL já instalado e configurado.]( https://www.postgresql.org/download/)
*  [Instalação do Jupyter Notebook com Python 3 - Podendo ser dentro da distribuição ANACONDA](https://www.anaconda.com/products/individual-d)


## Bibliotecas Python:

* ipython-sql
* psycopg2
* Pandas
* glob
* os


## Visão dos dados e do código:

* ** data/song_data/- contém nossos dados de música no formato json.
* ** data/log_data/- contém nossos registros de atividades do usuário em formato json. 
* ** [`test.ipynb`](test.ipynb) exibe as primeiras linhas de cada tabela para verificar o banco de dados.
* ** [`create_tables.py`](create_tables.py) descarta e cria suas tabelas. Você executa esse arquivo para redefinir suas 
tabelas antes de cada vez que executa seus scripts ETL.
* ** [`etl.ipynb`](etl.ipynb) lê e processa um único arquivo de song_data e log_data e carrega os dados em suas tabelas. 
Este bloco de notas contém instruções detalhadas sobre o processo ETL para cada uma das tabelas.
* ** [`etl.py`](etl.py) nosso pipeline de ETL. Faz a iteração por meio de nossos arquivos de música e log 
e canaliza os dados para nossas tabelas de banco de dados.
* ** [`sql_queries.py`](sql_queries.py) contém todas as consultas SQL e é importado para os três últimos arquivos acima.


## Modelo de dados:

* Esquema em estrela

* Benefícios:

1. Consultas simples e agregações mais rápidas em dados desnormalizados (como é caso dos dados utilizados).


* Desvantagens:

1. Desnormalização pode significar na diminuição da integridade dos dados e flexibilidade na consulta.


## Instruções sobre como executar:

Certifique-se de fechar todas as conexões de banco de dados e executar create_tables.py antes de executar etl.py ou etl.ipynb. 
Depois de executar qualquer um desses arquivos etl, você pode executar test.ipynb para verificar os valores de cada uma de nossas tabelas de banco de dados.








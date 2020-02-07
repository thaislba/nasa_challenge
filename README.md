# nasa_challenge (HTTP requests to NASA Kennedy Space Center WWW server)

### As respostas das questões estão disponíveis no arquivo nasa_challenge-desafioTeorico.pdf

### Tutorial para replicação do código implementado  

#### Datasets:
Os dois conjuntos de dados não estruturados possuem todas as requisições HTTP para o servidor da NASA Kennedy Space Center WWW na Flórida para os períodos de Julho e Agosto de 1995. 
Objetivo: Fazer análise exploratória dos dados com a finalidade de encontrar informações relevantes a respeito das requisições HTTP feitas na época.

#### Como utilizar:

1. Fazer download da versão 2.3.4 do Spark em http://www-eu.apache.org/dist/spark/spark-2.3.4/spark-2.3.4-bin-hadoop2.7.tgz, e descompactar.

2. Fazer clone do projeto.
   comando: git clone https://github.com/thaislba/nasa_challenge.git
   
3. Entrar na pasta nasa_challenge 

4. Mudar no arquivo deploy.sh o endereço do SPARK_HOME para apontar para o diretório em que foi feito o download do passo 1.

5. Executar script deploy.sh 
   comando: bash deploy.sh
   
### Comentários

O código irá criar cinco diretórios com as respostas do desafio (answer_1, answer_2,..., answer_5) em arquivos csv. 

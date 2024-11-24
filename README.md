# **Documentação do Projeto: Análise de Dados com Scala e Apache Spark**

## **1. Introdução**

Este projeto tem como objetivo desenvolver um sistema que demonstre a capacidade de manipulação e análise de grandes conjuntos de dados, utilizando as funcionalidades do Apache Spark dentro da linguagem funcional Scala. A proposta visa destacar a eficiência e flexibilidade do Spark em operações distribuídas, aliadas ao poder expressivo da programação funcional para realizar transformações complexas em dados estruturados.

A escolha do Apache Spark se justifica por ser uma das ferramentas mais robustas e amplamente utilizadas para processamento de grandes volumes de dados, oferecendo suporte nativo à integração com Scala. Essa combinação permite que o sistema aproveite ao máximo o paralelismo e a eficiência de execução em clusters, ao mesmo tempo que mantém a expressividade e a concisão da linguagem funcional.

As linguagens funcionais, como Scala, desempenham um papel crucial no tratamento e análise de dados devido à sua abordagem declarativa e ao suporte nativo para operações como map, filter, e reduce. Essas características facilitam a escrita de código mais legível, modular e adequado ao processamento de grandes conjuntos de dados. Além disso, a imutabilidade, amplamente utilizada em paradigmas funcionais, contribui para evitar efeitos colaterais e tornar os algoritmos mais previsíveis e seguros, características essenciais em sistemas de análise de dados.

Essa combinação torna o paradigma funcional uma escolha ideal para enfrentar os desafios impostos pela manipulação de dados em larga escala, garantindo eficiência, clareza e confiabilidade no desenvolvimento do sistema.

---

## **2. Configuração do Ambiente**

Para a configuração do ambiente de desenvolvimento, estamos utilizando a IDE **IntelliJ IDEA**, uma ferramenta robusta e amplamente utilizada para projetos em Scala. O processo de configuração do ambiente inclui a instalação do **Scala**, do **Apache Spark** e das bibliotecas necessárias para rodar o projeto.

### **Dependências Necessárias**

As dependências principais do projeto estão definidas no arquivo `build.sbt`, que gerencia as bibliotecas e versões necessárias para compilar e rodar o sistema. As dependências do projeto são:

- **Scala 2.12.20**: Versão da linguagem Scala utilizada.
- **Apache Spark** (versão 3.5.3): Biblioteca principal para processamento de grandes volumes de dados distribuídos.
   - `spark-core`: Bibliotecas essenciais para a execução de tarefas em Spark.
   - `spark-sql`: Extensão do Spark para processamento de dados estruturados com SQL.
- **Plotly-Scala 0.8.1**: Biblioteca para visualização de dados gráficos integrada com Scala, utilizada para gerar os gráficos e relatórios de análise.

### **Compatibilidade com JDK**

O Scala 2.12.20 é compatível apenas com versões específicas do **JDK**. Para garantir a execução correta do projeto, recomenda-se usar uma das seguintes versões do JDK:

- **JDK 8 (1.8)**
- **JDK 11**

É importante destacar que o Scala 2.12 não é compatível com o **JDK 16 ou versões superiores**, portanto, caso esteja utilizando uma versão mais recente do JDK, será necessário configurar um JDK compatível (8 ou 11) para rodar o projeto corretamente.

### **Conteúdo do arquivo `build.sbt`**

```scala
ThisBuild / scalaVersion := "2.12.20"

lazy val root = (project in file("."))
  .settings(
    name := "data_analysis_spark"
  )

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.3"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.3"
libraryDependencies += "org.plotly-scala" %% "plotly-render" % "0.8.1"
```

### **Passos para Configuração**

1. **Instalação do Scala e IntelliJ IDEA**: Certifique-se de ter a versão **2.12.20** do Scala instalada. A IDE IntelliJ pode ser configurada para suportar Scala através do plugin oficial.

2. **Configuração do JDK**:
   - Caso esteja utilizando o IntelliJ IDEA, vá até as configurações do projeto e defina o **JDK 8 ou JDK 11** como JDK de execução.
   - Caso não tenha o JDK 8 ou 11, é necessário baixá-lo e configurá-lo em seu ambiente de desenvolvimento.

3. **Criação do Projeto no IntelliJ**:
   - Abra o IntelliJ e crie um novo projeto Scala.
   - No arquivo `build.sbt`, adicione as dependências mencionadas acima para garantir que todas as bibliotecas necessárias sejam baixadas e configuradas.

4. **Execução do Projeto**:
   - Após a configuração, basta rodar a aplicação dentro do IntelliJ, que já estará configurado com o Apache Spark, Scala e Plotly-Scala.

Com essas dependências e a configuração do ambiente, o sistema estará pronto para ser executado, processando grandes volumes de dados com a ajuda do Apache Spark e gerando visualizações com Plotly-Scala.

---

## **3. Leitura e Análise Inicial**

### **Leitura do Conjunto de Dados (CSV)**

O primeiro passo para realizar a análise dos dados é carregar o conjunto de dados no **Spark DataFrame**. Vamos carregar os dados do arquivo **CSV** (como o `imoveis.csv`) utilizando o Spark, que irá inferir o tipo de dados de cada coluna automaticamente.

```scala
// Criação do SparkSession
val spark = SparkSession.builder()
  .appName("Imoveis")
  .master("local")
  .getOrCreate()

// Carregando o CSV com a inferência de schema
val dfImoveis = spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("imoveis.csv")

dfImoveis.printSchema()
```

### **Renomeando as Colunas para Facilidade de Acesso**

Para facilitar a manipulação dos dados, renomeamos as colunas do DataFrame para nomes mais amigáveis. O Spark permite o uso de **colunas renomeadas**, o que facilita a manipulação do conjunto de dados nas transformações seguintes.

```scala
// Renomeando as colunas para facilitar o acesso
val dfRenomeado = dfImoveis
  .withColumnRenamed("ID", "id")
  .withColumnRenamed("Área (m²)", "area")
  .withColumnRenamed("Preço", "preco")
  .withColumnRenamed("Cidade", "cidade")
  .withColumnRenamed("Bairro", "bairro")
```

---

## **4. Transformações Funcionais**

Aqui, serão aplicadas as transformações solicitadas no conjunto de dados. Utilizaremos funções como `filter`, `select`, `groupBy`, e agregações para atender aos requisitos.

### **4.1. Imóveis com Áreas Amplas e Preços Altos**

Vamos selecionar os imóveis com área maior que **100 m²** e preço superior a **1.000.000**. A transformação será realizada utilizando a função `filter`:

```scala
// Selecionando os imóveis com área > 100 m² e preço > 1.000.000
val resultado = dfRenomeado
  .filter(col("area") > 100 && col("preco") > 1000000)
  .select("id", "cidade", "bairro", "preco")

// Exibindo os resultados
resultado.show(false)
```

### **4.2. Preço Médio de Imóveis em Todo o Conjunto de Dados**

Para calcular o preço médio de todos os imóveis, utilizamos a função `agg` com o método `avg` (média). O resultado será um único valor representando a média de preços.

```scala
// Calculando o preço médio de todos os imóveis
val precoMedio = dfRenomeado
  .agg(avg("preco"))
  .collect()(0)(0)

println(s"Preço médio dos imóveis: $precoMedio")
```

### **4.3. Imóveis Mais Próximos do Centro**

Para listar os imóveis com distância do centro inferior a **5 km**, usaremos o `filter` com a condição de distância:

```scala
// Selecionando imóveis com distância do centro < 5 km
val proximosCentro = dfRenomeado
  .filter(col("Distância do centro") < 5)
  .select("id", "cidade", "bairro", "area", "preco")

// Exibindo os resultados
proximosCentro.show(false)
```

### **4.4. Imóveis de um Valor Específico**

Para selecionar imóveis com preço entre **500.000 e 800.000**, utilizamos novamente o `filter` com a faixa de preços especificada:

```scala
// Selecionando imóveis com preço entre 500.000 e 800.000
val faixaPreco = dfRenomeado
  .filter(col("preco") >= 500000 && col("preco") <= 800000)
  .select("id", "cidade", "bairro", "quartos", "preco")

// Exibindo os resultados
faixaPreco.show(false)
```

### **4.5. Número Médio de Quartos por Imóvel**

O cálculo da média do número de quartos pode ser feito utilizando a função `avg`:

```scala
// Calculando a média do número de quartos
val mediaQuartos = dfRenomeado
  .agg(avg("Número de quartos"))
  .collect()(0)(0)

println(s"Média de quartos por imóvel: $mediaQuartos")
```

### **4.6. Imóveis com Preço Elevado e Ano de Construção Recente**

Selecionamos imóveis construídos após **2015** e com preço superior a **1.200.000**:

```scala
// Selecionando imóveis com ano de construção > 2015 e preço > 1.200.000
val recentesElevados = dfRenomeado
  .filter(col("Ano de construção") > 2015 && col("preco") > 1200000)
  .select("id", "cidade", "bairro", "Ano de construção", "preco")

// Exibindo os resultados
recentesElevados.show(false)
```

### **4.7. Imóveis com Número Específico de Banheiros**

Para selecionar imóveis com pelo menos **3 banheiros**, utilizamos o `filter`:

```scala
// Selecionando imóveis com pelo menos 3 banheiros
val banheirosEspecificos = dfRenomeado
  .filter(col("Número de banheiros") >= 3)
  .select("id", "cidade", "bairro", "Número de quartos", "preco")

// Exibindo os resultados
banheirosEspecificos.show(false)
```

### **4.8. Área Média de Imóveis em Toda a Base de Dados**

Para calcular a área média de todos os imóveis, usamos a função `avg`:

```scala
// Calculando a área média dos imóveis
val areaMedia = dfRenomeado
  .agg(avg("Área (m²)"))
  .collect()(0)(0)

println(s"Área média dos imóveis: $areaMedia m²")
```

### **4.9. Preço Médio por Bairro**

Aqui, utilizamos o `groupBy` para calcular o preço médio por bairro:

```scala
// Calculando o preço médio por bairro
val precoMedioBairro = dfRenomeado
  .groupBy("bairro")
  .agg(avg("preco").alias("preco_medio"))
  .orderBy(desc("preco_medio"))

precoMedioBairro.show(false)
```

### **4.10. Faixa de Preços por Número de Quartos (1-2, 3-4, 4+)**

Para identificar as faixas de preço por número de quartos, podemos usar uma combinação de `when` e `otherwise`:

```scala
// Categorizar imóveis por faixas de número de quartos
val faixaQuartos = dfRenomeado
  .withColumn("faixa_quartos", 
    when(col("Número de quartos") <= 2, "1-2")
    .when(col("Número de quartos").between(3, 4), "3-4")
    .otherwise("4+")
  )
  .groupBy("faixa_quartos")
  .agg(avg("preco").alias("preco_medio"))
  .orderBy("faixa_quartos")

faixaQuartos.show(false)
```

---

## **5. Relatórios e Visualizações**

Entendido! Abaixo está o texto ajustado para incluir os gráficos diretamente no README, utilizando Markdown para incorporar links clicáveis que levam ao gráfico correspondente.

---

### Relatórios e Visualizações

Durante o processo de análise dos imóveis, diversos gráficos foram gerados para visualizar e interpretar as informações dos dados. Esses gráficos podem ser acessados diretamente abaixo, cada um referente a uma questão específica.

#### Questão 1: Imóveis com Áreas Amplas e Preços Altos

**Descrição da Query**:  
Selecionamos todos os imóveis com área maior que 100 m² e preço superior a 1.000.000, e exibimos o ID, a cidade, o bairro e o preço.

**Gráfico Gerado**:  
[Gráfico de Imóveis com Áreas Amplas e Preços Altos](graficos/questao1.html)

**Análise**:  
A análise dos dados revela que os imóveis com áreas amplas e preços elevados são predominantemente encontrados em cidades com um mercado imobiliário mais valorizado, como São Paulo e Rio de Janeiro. A quantidade de imóveis em Curitiba é muito menor, o que indica que a cidade pode ter um mercado mais acessível.

---

#### Questão 2: Preço Médio de Imóveis

**Descrição da Query**:  
Calculamos o preço médio de todos os imóveis.

**Gráfico Gerado**:  
[Gráfico de Preço Médio de Imóveis](graficos/questao2.html)

**Análise**:  
O preço médio de todos os imóveis da base de dados foi em torno de 1 Milhão, com um desvio padrão de 352 Mil. 

---

#### Questão 3: Imóveis Mais Próximos do Centro

**Descrição da Query**:  
Selecionamos imóveis com distância do centro inferior a 5 km e exibimos o ID, cidade, bairro, área e preço.

**Gráfico Gerado**:  
[Gráfico de Imóveis Próximos ao Centro](graficos/questao3.html)

**Análise**:  
Observamos que a maioria dos imóveis mais próximos ao centro possui preços mais elevados, o que pode ser atribuído à maior demanda por imóveis em regiões centrais e com boa infraestrutura. Um ponto interessante é que conforme na questão, o centro das cidades de São Paulo e Rio de Janeiro também são mais caros. 
---

#### Questão 4: Imóveis de um Valor Específico

**Descrição da Query**:  
Selecionamos imóveis com preço entre 500.000 e 800.000 e exibimos o ID, cidade, bairro, número de quartos e preço.

**Gráfico Gerado**:  
[Gráfico de Imóveis na Faixa de Preço Específica](graficos/questao4.html)

**Análise**:  
O gráfico mostra que a maior quantidade de imóveis na faixa de preço entre 500.000 e 800.000 está concentrada em Belo Horizonte, com 154 empreendimentos, seguida por Porto Alegre com 107 e Curitiba com 50. Essa diferença pode indicar um mercado imobiliário mais ativo ou uma maior oferta de imóveis dentro dessa faixa de preço em Belo Horizonte. Curitiba, por outro lado, apresenta um número significativamente menor de imóveis nesta faixa, possivelmente refletindo uma menor demanda ou uma distribuição de preços diferente. A predominância em Belo Horizonte sugere que há um equilíbrio interessante entre custo e oferta para a classe média-alta.
---

#### Questão 5: Número Médio de Quartos por Imóvel

**Descrição da Query**:  
Calculamos a média do número de quartos de todos os imóveis.

**Gráfico Gerado**:  
[Gráfico do Número Médio de Quartos](graficos/questao5.html)

**Análise**:  
O gráfico indica que o número médio de quartos dos imóveis no conjunto de dados é aproximadamente 3 quartos, com um desvio padrão de 1,43. Essa variação relativamente ampla sugere que os imóveis possuem uma distribuição diversificada em relação ao número de quartos, abrangendo tanto unidades menores, adequadas para solteiros ou casais, quanto propriedades maiores, voltadas para famílias grandes. Esse desvio padrão evidencia uma heterogeneidade nos tipos de imóveis oferecidos, atendendo a diferentes públicos e faixas de renda.

- A concentração média em 3 quartos pode apontar para um padrão típico no mercado imobiliário das cidades analisadas, possivelmente refletindo a demanda predominante do mercado por imóveis de médio porte.
---

#### Questão 6: Imóveis com Preço Elevado e Ano de Construção Recente

**Descrição da Query**:  
Filtramos todos os imóveis construídos após 2015 e com preço superior a 1.200.000.

**Gráfico Gerado**:  
[Gráfico de Imóveis Recentes e Caros](graficos/questao6.html)

**Análise**:  


O gráfico exibe os **imóveis mais novos e mais caros**, construídos após 2015 e com preço superior a R$ 1.200.000, agrupados por bairro e cidade. Os bairros com maior número de propriedades que atendem a esses critérios são:

- **São Paulo, Zona Sul** com 7 imóveis,
- **Rio de Janeiro, Zona Sul** com 5 imóveis,
- **São Paulo, Zona Oeste** com 4 imóveis.

Os bairros com menor presença de imóveis dentro desse perfil são:

- **Rio de Janeiro, Zona Oeste**, com apenas 1 imóvel,
- **São Paulo, Zona Norte**, **Rio de Janeiro, Zona Norte**, **Rio de Janeiro, Centro**, **São Paulo, Zona Centro**, e **São Paulo, Zona Leste**, cada um com 2 ou 3 imóveis.

A concentração de imóveis premium em bairros como **São Paulo, Zona Sul** e **Rio de Janeiro, Zona Sul** reflete o comportamento esperado do mercado imobiliário, onde bairros centrais e de alto padrão atraem investimentos em construções modernas e de alto valor. Por outro lado, a baixa densidade em regiões como **Rio de Janeiro, Zona Oeste** pode estar associada a uma menor valorização econômica ou a um perfil menos voltado para imóveis de luxo.

Essa análise reforça como o perfil socioeconômico de cada bairro influencia a quantidade de empreendimentos premium disponíveis.

---

#### Questão 7: Imóveis com Número Específico de Banheiros

**Descrição da Query**:  
Selecionamos imóveis com pelo menos 3 banheiros.

**Gráfico Gerado**:  
[Gráfico de Imóveis com 3 ou Mais Banheiros](graficos/questao7.html)

**Análise**:

O gráfico apresenta o **preço médio dos empreendimentos com mais de 3 banheiros por cidade**, juntamente com o desvio padrão, que indica a dispersão dos preços em cada local.

- **São Paulo** lidera com o maior preço médio de $ R\$ 1.680.480,00 $ e um desvio padrão de $ R\$ 93.469,98 $, sugerindo um mercado consistente de empreendimentos de alto valor para este perfil.
- **Rio de Janeiro** segue em segundo lugar, com um preço médio de $ R\$ 1.379.179,31 $ e desvio padrão de $ R\$ 102.339,32 $, refletindo uma ligeira variação nos preços entre os imóveis analisados.
- **Curitiba** e **Porto Alegre** têm preços médios mais modestos, de $ R\$ 956.361,29 $ e $ R\$ 853.130,95 $,  respectivamente, com desvios padrão próximos de R$ 90.000,00, indicando maior uniformidade nos preços desses mercados.
- **Belo Horizonte** apresenta o menor preço médio, $ R\$ 735.413,75 $, e o desvio padrão mais baixo, $ R\$ 82.966,74 $, o que pode indicar menor presença de imóveis de luxo ou alta homogeneidade no mercado local.

A análise mostra como o mercado imobiliário se comporta em relação a empreendimentos maiores e com mais infraestrutura (mais banheiros), refletindo as características econômicas e sociais de cada cidade. **São Paulo e Rio de Janeiro** destacam-se por preços elevados e variação moderada, enquanto **Belo Horizonte**, **Curitiba** e **Porto Alegre** apresentam valores mais acessíveis e consistentes.

---

#### Questão 8: Área Média de Imóveis

**Descrição da Query**:  
Calculamos a área média de todos os imóveis.

**Gráfico Gerado**:  
[Gráfico da Área Média dos Imóveis](graficos/questao8.html)

**Análise**:

O gráfico apresenta a **área média dos imóveis** no conjunto de dados, acompanhada pelo desvio padrão.

- A área média dos imóveis é **172,17 m²**, sugerindo que a maioria dos imóveis possui um tamanho acima da média para padrões urbanos.
- O desvio padrão, **74,79 m²**, indica uma significativa dispersão nos tamanhos dos imóveis, com uma ampla variedade de áreas disponíveis.

Essa variabilidade pode ser explicada pela diversidade de tipos de imóveis incluídos no conjunto de dados, abrangendo tanto pequenas unidades residenciais quanto imóveis maiores, como casas e apartamentos de luxo.

Essa análise aponta a necessidade de segmentar os imóveis por categorias (por exemplo, apartamentos versus casas) ou por faixas de área para entender melhor as tendências e os padrões de mercado.

---

#### Questão 9: Preço Médio por Bairro

**Descrição da Query**:  
Identificamos os bairros com o preço médio mais alto ou mais baixo.

**Gráfico Gerado**:  
[Gráfico de Preço Médio por Bairro](graficos/questao9.html)

**Análise**:

O gráfico apresenta um **ranking dos bairros com os preços médios mais altos e mais baixos** entre os imóveis analisados, separados em dois grupos: os cinco mais caros e os cinco mais baratos.

- **Bairros mais baratos** (todos em Belo Horizonte):
    - Os preços médios variam de **R\$ 651.364,52** (Zona Leste) a **R$ 697.131,94** (Zona Sul).
    - A concentração dos bairros de menor preço em uma única cidade indica uma possível homogeneidade no mercado imobiliário de Belo Horizonte, com preços relativamente acessíveis em relação às outras cidades.

- **Bairros mais caros** (todos em São Paulo):
    - Os preços médios variam de **R\$ 1.532.421,88** (Zona Leste) a **R$ 1.586.978,95** (Zona Oeste).
    - A predominância de bairros de São Paulo nos valores mais altos reflete a forte valorização do mercado imobiliário na cidade, com diferenças sutis entre os bairros mais caros, sugerindo um mercado de alto padrão.

### Conclusão:
Esse contraste entre Belo Horizonte e São Paulo reforça como fatores como localização geográfica, infraestrutura e desenvolvimento econômico impactam diretamente os preços médios dos imóveis. A análise também aponta que os preços médios em São Paulo são mais do que o dobro dos de Belo Horizonte, destacando um desequilíbrio significativo entre os mercados.

---

#### Questão 10: Faixa de Preços por Número de Quartos

**Descrição da Query**:  
Identificamos faixas de preço comuns para imóveis com diferentes números de quartos.

**Gráfico Gerado**:  
[Gráfico de Faixa de Preços por Quartos](graficos/questao10.html)

**Análise**:  
Imóveis com 1-2 quartos tendem a ter preços mais baixos, enquanto imóveis com 3-4 quartos possuem preços intermediários. Imóveis com mais de 4 quartos têm preços elevados, o que reflete a maior área e a procura por espaços maiores.

---

### Conclusão

* Os gráficos interativos gerados com Plotly foram vinculados diretamente para acesso fácil. Eles oferecem uma visualização clara das tendências e padrões do mercado imobiliário nas diferentes cidades, ajudando na interpretação e tomada de decisão.
---

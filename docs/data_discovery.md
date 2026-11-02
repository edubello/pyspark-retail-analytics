# Relatório de Inspeção de Dados, Governança e Decisões Técnicas
**Projeto:** Retail Analytics Lakehouse (PySpark & Delta Lake)  
**Autor:** Eduardo de Castro Bello  
**Camada:** Raw / Ingestion Audit Report  

---

## 1. Visão Geral e Contexto da Inspeção

Antes do desenvolvimento das regras de negócio e consolidação das camadas Silver e Gold, os dados brutos oriundos das fontes operacionais foram submetidos a uma etapa de **Data Discovery (Exploração Inicial)**.

O objetivo desta auditoria foi mapear inconsistências de schema, anomalias estruturais nos arquivos brutos, problemas de integridade referencial e definir os **Quality Gates (travas de qualidade)** para evitar que dados corrompidos avancem no pipeline distribuído.

---

## 2. Diagnóstico de Anomalias Encontradas nos Dados Brutos (Raw)

| Fonte | Tipo de Arquivo | Problema Diagnosticado | Impacto no Pipeline | Estratégia de Mitigação Implementada |
| :--- | :--- | :--- | :--- | :--- |
| `products.json` | JSON | **Malformação Estrutural:** Objetos soltos separados por vírgula sem os colchetes delimitadores de array (`[...]`). | Falha na leitura nativa do leitor JSON do PySpark. | Sanitização via script no `src/ingestion.py`, envolvendo a string com colchetes antes do parse. |
| `products.json` | JSON | **Sujeira de Tipagem (`price`):** Presença de caracteres não numéricos e símbolos de moeda formatados como texto. | Impossibilidade de realizar cálculos financeiros e agregações na Gold. | Tratamento via Expressão Regular (`regexp_replace`) e *cast* explícito para `DoubleType`. |
| `sales.csv` | CSV | **Data em Formato de String:** Coluna `date` no formato brasileiro `dd/MM/yyyy`. | Impossibilidade de aplicar janelas móveis de tempo e ordenação cronológica. | Conversão explícita para o tipo nativo `DateType` via `to_date(col, 'dd/MM/yyyy')`. |
| `users.csv` | CSV | **Incompatibilidade de Schema:** Desalinhamento entre o header real (`client_id, name, email, gender`) e especificação prévia que lia `signup_date` incorretamente. | Atribuição de dados de gênero na coluna de data de cadastro. | Aplicação de `StructType` estrito alinhado exatamente às colunas reais do arquivo fonte. |

---

## 3. Data Quality Gates (Travas de Qualidade com Asserts)

Para garantir que a camada **Silver** seja a "única fonte da verdade", estabelecemos travamentos automatizados por asserções (*Quality Gates*) antes de alimentar as regras de negócio analíticas:

1. **Unicidade de Chave Primária:**
   $$\text{Vendas Totais} - \text{Vendas Distintas por } \texttt{sale\_id} = 0$$
   *Garante a ausência de transações duplicadas.*

2. **Integridade de Chaves Mandatórias:**
   Nenhuma linha na tabela de vendas pode conter `client_id` ou `product_id` nulos.

3. **Integridade Referencial (Detecção de Órfãos):**
   Uso de `left_anti` join entre a tabela de vendas e o cadastro de produtos. O resultado deve ser rigorosamente zero para evitar vendas atribuídas a produtos inexistentes no catálogo.

---

## 4. Justificativas das Decisões de Engenharia

### 4.1. Filtragem da Janela de 6 Meses (Questão Analítica Q4)
* **Decisão:** A janela temporal foi calculada de forma dinâmica utilizando a data máxima encontrada no conjunto de dados (`max(date)`), subtraindo 6 meses via `add_months`.
* **Justificativa:** Evita inconsistências causadas por *hardcoding* de datas em dados históricos e garante que o ranking reflita os 6 meses mais recentes disponíveis no dataset.

### 4.2. Classificação RFM Simplificada (Silver, Gold, Platinum - Questão Q6)
* **Decisão:** Segmentação de clientes com base em limites estatísticos sem sobreposição de fronteiras:
  * **Platinum:** $\text{Gasto Total} > \text{Média}$
  * **Gold:** $\text{Média} - \text{Desvio Padrão} < \text{Gasto Total} \le \text{Média}$
  * **Silver:** $\text{Gasto Total} \le \text{Média} - \text{Desvio Padrão}$
* **Justificativa:** Elimina ambiguidade de enquadramento nas bordas das categorias e utiliza o desvio padrão amostral (`stddev`) para ajustar os cortes de distribuição dos clientes.

### 4.3. Otimização de Joins via Broadcast
* **Decisão:** Aplicação explícita de `broadcast()` nas tabelas de dimensão `df_products` e `df_users`.
* **Justificativa:** Como os cadastros de produtos e usuários possuem menor volumetria se comparados ao fato de vendas, o *broadcast* evita a operação dispendiosa de *shuffle* através da rede no cluster Spark.
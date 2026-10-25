# Validação e limites da entrega

Data de preparação: 25/10/2026.

## Verificações realizadas

- Cliente HTTP/JSON, codificação UTF-8/base64, seleção explícita de consistência, erros sem repetição automática e reinício de conexão cobertos pelos testes em `tests/`.
- Configuração de 3 e 5 membros, redes, limites de memória e estatísticas cobertas por testes de código.
- Integração do cliente com **três processos reais de etcd v3.7.3 no Windows**: escrita via seguidor e leitura em outro membro, leitura serializável, falha de líder com novo termo, escrita com 2/3 membros, perda de quórum, leitura local disponível, falha de leitura forte/escrita sem maioria e recuperação preservando valor confirmado.
- Execução nativa das rotinas E2 e E3: avanço de índices, watch HTTP no terceiro membro, cinco medidas de put/get e visibilidade local, e `etcdctl check perf --load=s` concluídos com sucesso. As respostas e amostras estão em [validation/native-experiments.json](../validation/native-experiments.json).

O registro do teste de integração está em [validation/native-smoke.json](../validation/native-smoke.json). Ele é um teste de compatibilidade e comportamento, **não** uma execução da bateria E1-E9 no ambiente proposto. Os binários usados foram obtidos no lançamento oficial [v3.7.3](https://github.com/etcd-io/etcd/releases/tag/v3.7.3), com SHA256 do arquivo Windows conferido contra o metadado publicado. Não são distribuídos neste projeto.

## Validações que faltam

O computador usado na preparação não possui Docker nem WSL instalados. Por isso, não foi possível executar localmente:

- Provisionamento Docker Compose e suas redes.
- Partição seletiva da rede Raft e restauração de IP.
- Atraso de rede com o auxiliar `tc netem`.
- Bateria E1-E9 completa com cinco repetições e comparação N=3/N=5.
- Confirmação de transferência de snapshot nos logs do ambiente Docker.
- Demonstrações manuais de lock/elect e as demais etapas complementares descritas no guia.

Os workflows do GitHub estão preparados para testar o ambiente, mas só sua execução bem-sucedida confirma esses testes. Arquivos de workflow existentes não são evidência de execução.

## Antes da entrega acadêmica

1. Instalar os requisitos, executar `python -m lab doctor` e subir N=3.
2. Registrar hardware, versões, consumo e previsões anteriores a cada experimento.
3. Executar E1-E9 e os complementos manuais; conferir as evidências, falhas e métricas.
4. Executar E9 novamente em N=5, mantendo as condições comparáveis.
5. Preencher as lacunas da base de relatório com dados e conclusões reais; revisar as respostas com os resultados.
6. Informar integrantes, contribuições, link do GitHub e declaração de IA. Conferir o roteiro final com o professor.

Não transforme uma previsão teórica em resultado observado. Um timeout de escrita também não comprova que ela deixou de ser confirmada; verifique a chave após recuperar o cluster.

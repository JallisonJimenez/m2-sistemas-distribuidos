# Referências e mapa de fontes

Fontes primárias consultadas em 25 out. 2026. Dados em formato próximo ao padrão ABNT; revisar a padronização final e acrescentar o roteiro fornecido pelo professor como material didático. Não confundir números publicados por essas fontes com resultados do laboratório.

## Artigos e fontes dos autores

ONGARO, Diego; OUSTERHOUT, John. **In Search of an Understandable Consensus Algorithm**. In: USENIX ANNUAL TECHNICAL CONFERENCE, 2014. Proceedings [...]. USENIX Association, 2014. p. 305-319. Disponível em: [página do artigo na USENIX](https://www.usenix.org/conference/atc14/technical-sessions/presentation/ongaro). Acesso em: 25 out. 2026. Uso: log replicado, eleição e segurança; Q5, Q8, Q13, S1.

HUNT, Patrick; KONAR, Mahadev; JUNQUEIRA, Flavio P.; REED, Benjamin. **ZooKeeper: Wait-free Coordination for Internet-scale Systems**. In: USENIX ANNUAL TECHNICAL CONFERENCE, 2010. Proceedings [...]. USENIX Association, 2010. Disponível em: [página do artigo na USENIX](https://www.usenix.org/conference/usenix-atc-10/zookeeper-wait-free-coordination-internet-scale-systems). Acesso em: 25 out. 2026. Uso: comparação conceitual com ZooKeeper. Não há experimentos ZAB neste repositório.

JUNQUEIRA, Flavio P.; REED, Benjamin C.; SERAFINI, Marco. **Zab: High-performance broadcast for primary-backup systems**. In: IEEE/IFIP INTERNATIONAL CONFERENCE ON DEPENDABLE SYSTEMS AND NETWORKS, 41., 2011. Proceedings [...]. IEEE, 2011. p. 245-256. DOI: 10.1109/DSN.2011.5958223. Disponível em: [texto do artigo hospedado pela Cornell](https://www.cs.cornell.edu/courses/cs6452/2012sp/papers/zab-ieee.pdf). Acesso em: 25 out. 2026. Uso: difusão atômica, ordenação e sincronização na mudança de primário.

GILBERT, Seth; LYNCH, Nancy A. **Perspectives on the CAP Theorem**. 2012. Disponível em: [manuscrito no MIT CSAIL](https://groups.csail.mit.edu/tds/papers/Gilbert/Brewer2.pdf). Acesso em: 25 out. 2026. Uso: interpretação de consistência/disponibilidade sob partição; Q11.

ABADI, Daniel J. **Consistency Tradeoffs in Modern Distributed Database System Design**. Computer, v. 45, n. 2, p. 37-42, fev. 2012. Disponível em: [cópia mantida no site do autor](https://www.cs.umd.edu/~abadi/papers/abadi-pacelc.pdf). Acesso em: 25 out. 2026. Uso: PACELC e consistência/latência; Q11 e E9.

KLEPPMANN, Martin. **How to do distributed locking**. 8 fev. 2016. Disponível em: [artigo do autor](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html). Acesso em: 25 out. 2026. Uso: pausa longa, lease e fencing; S4. É artigo técnico do autor, não artigo revisado por pares.

## Documentação oficial do etcd

ETCD AUTHORS. **etcd API guarantees**. Versão 3.7. Disponível em: [garantias da API](https://etcd.io/docs/v3.7/learning/api_guarantees/). Acesso em: 25 out. 2026. Uso: Q6, watch e consistência.

ETCD AUTHORS. **etcd API**. Versão 3.7. Disponível em: [modelo da API](https://etcd.io/docs/v3.7/learning/api/). Acesso em: 25 out. 2026. Uso: campos das operações, leitura local, transações e leases.

ETCD AUTHORS. **Data model**. Versão 3.7. Disponível em: [MVCC e revisões](https://etcd.io/docs/v3.7/learning/data_model/). Acesso em: 25 out. 2026. Uso: Q9 e E8.

ETCD AUTHORS. **etcd client design**. Versão 3.7. Disponível em: [desenho do cliente](https://etcd.io/docs/v3.7/learning/design-client/). Acesso em: 25 out. 2026. Uso: endpoints, gRPC, balanceamento e tratamento de erro; Q4.

ETCD AUTHORS. **FAQ**. Versão 3.7. Disponível em: [quórum e tamanho do cluster](https://etcd.io/docs/v3.7/faq/). Acesso em: 25 out. 2026. Uso: Q7 e S2.

ETCD AUTHORS. **Failure modes**. Versão 3.7. Disponível em: [modos de falha](https://etcd.io/docs/v3.7/op-guide/failures/). Acesso em: 25 out. 2026. Uso: E5-E7, Q10-Q11.

ETCD AUTHORS. **Runtime reconfiguration**. Versão 3.7. Disponível em: [membros e learners](https://etcd.io/docs/v3.7/op-guide/runtime-configuration/). Acesso em: 25 out. 2026. Uso: S2; não confundir comparação de clusters com reconfiguração dinâmica.

ETCD AUTHORS. **Configuration options**. Versão 3.7. Disponível em: [parâmetros de configuração](https://etcd.io/docs/v3.7/op-guide/configuration/). Acesso em: 25 out. 2026. Uso: portas, snapshots e retenção para catch-up.

ETCD AUTHORS. **Maintenance**. Versão 3.7. Disponível em: [compactação, snapshots e manutenção](https://etcd.io/docs/v3.7/op-guide/maintenance/). Acesso em: 25 out. 2026. Uso: E7 e Q12.

ETCD AUTHORS. **Performance**. Versão 3.7. Disponível em: [latência e vazão](https://etcd.io/docs/v3.7/op-guide/performance/). Acesso em: 25 out. 2026. Uso: metodologia e limitações. A página inclui benchmarks históricos, que não medem este projeto.

ETCD AUTHORS. **API reference: concurrency**. Versão 3.7. Disponível em: [lock e election](https://etcd.io/docs/v3.7/dev-guide/api_concurrency_reference_v3/). Acesso em: 25 out. 2026. Uso: E8, Q13 e S4.

ETCD AUTHORS. **Transport security model**. Versão 3.7. Disponível em: [segurança de transporte](https://etcd.io/docs/v3.7/op-guide/security/). Acesso em: 25 out. 2026. Uso: Q14 e diferença entre laboratório e produção.

ETCD AUTHORS. **etcd versus other key-value stores**. Versão 3.7. Disponível em: [propósito e comparação](https://etcd.io/docs/v3.7/learning/why/). Acesso em: 25 out. 2026. Uso: Q1 e contexto.

ETCD AUTHORS. **etcdctl README**. Código-fonte, tag v3.7.3. Disponível em: [manual do cliente fixado](https://github.com/etcd-io/etcd/blob/v3.7.3/etcdctl/README.md). Acesso em: 25 out. 2026. Uso: sintaxe dos complementos manuais; verificar `--help` da imagem efetiva quando houver divergência.

ETCD AUTHORS. **read_only.go**. Repositório raft, branch main. Disponível em: [implementação do ReadIndex](https://github.com/etcd-io/raft/blob/main/read_only.go). Acesso em: 25 out. 2026. Uso: S3 e confirmações de leitura. A branch é móvel; registrar o commit consultado caso seja utilizada em discussão de implementação específica.

ETCD AUTHORS. **Package raft**. Módulo `go.etcd.io/raft/v3`. Disponível em: [documentação primária da biblioteca](https://pkg.go.dev/go.etcd.io/raft/v3). Acesso em: 25 out. 2026. Uso: PreVote, CheckQuorum e mensagens Protocol Buffers; consultar a versão vinculada ao binário quando discutir detalhes específicos.

ETCD AUTHORS. **Package rafthttp**. Módulo `go.etcd.io/etcd/server/v3`, versão v3.7.3 consultada. Disponível em: [documentação do transporte de pares](https://pkg.go.dev/go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp). Acesso em: 25 out. 2026. Uso: transporte HTTP das mensagens Raft; Q4.

CLOUD NATIVE COMPUTING FOUNDATION. **etcd Project Journey Report**. 25 set. 2024. Disponível em: [histórico do projeto](https://www.cncf.io/reports/etcd-project-journey-report/). Acesso em: 25 out. 2026. Uso: origem na CoreOS em 2013 e trajetória do projeto.

## Documentação oficial do ZooKeeper

APACHE SOFTWARE FOUNDATION. **ZooKeeper Programmer's Guide**. Versão 3.9.4. Disponível em: [modelo de dados e sessões](https://zookeeper.apache.org/doc/r3.9.4/zookeeperProgrammers.html). Acesso em: 25 out. 2026. Uso: znodes efêmeros e sequenciais.

APACHE SOFTWARE FOUNDATION. **ZooKeeper Recipes and Solutions**. Versão 3.9.4. Disponível em: [receitas de coordenação](https://zookeeper.apache.org/doc/r3.9.4/recipes.html). Acesso em: 25 out. 2026. Uso: combinação de nós efêmeros/sequenciais e watches em travas e eleição.

APACHE SOFTWARE FOUNDATION. **ZooKeeper Internals**. Versão 3.9.4. Disponível em: [difusão atômica e consistência](https://zookeeper.apache.org/doc/r3.9.4/zookeeperInternals.html). Acesso em: 25 out. 2026. Uso: época/contador no zxid, ordenação e limites de leituras locais e de `sync`.

## Kubernetes

KUBERNETES AUTHORS. **Kubernetes API Concepts**. Disponível em: [conceitos da API](https://kubernetes.io/docs/reference/using-api/api-concepts/). Acesso em: 25 out. 2026. Uso: semânticas de get/list e parâmetros de resourceVersion; S3. A documentação é atualizada continuamente, portanto declarar a versão do cluster quando houver teste Kubernetes.

KUBERNETES CONTRIBUTORS. **KEP 2340: Consistent Reads from Cache**. Disponível em: [proposta oficial](https://github.com/kubernetes/enhancements/blob/master/keps/sig-api-machinery/2340-Consistent-reads-from-cache/README.md). Acesso em: 25 out. 2026. Uso: cache consistente, revisão e progress notifications; S3.

## Regra de citação na redação final

Vincular cada afirmação técnica à fonte adequada e cada afirmação experimental ao arquivo da execução. Escrever “a documentação garante” quando se trata da especificação, “a hipótese prevê” antes do teste e “observamos” apenas depois de conferir a evidência real. Acrescentar versões e data de acesso efetivamente usadas pela equipe.

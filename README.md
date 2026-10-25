# M2 Sistemas Distribuídos

Estado e Coordenação com etcd/Raft.

**Tema 5 · Máquina de estados replicada chave-valor · Raft · Ênfase A: linearizável × serializável.**

Laboratório acadêmico com etcd real. Python automatiza o ambiente, consulta a API v3 e coleta evidências; o consenso é implementado pelo próprio etcd. Não há simulação de Raft.

O escopo foi conferido com o roteiro: “Hierarquia e Delegação”, NS/glue e `dig +trace` pertencem à ênfase A de DNS. Para Estado/Coordenação, a ênfase A compara leituras em clusters de 3 e 5 membros, com atraso de rede e partição.

## O que está pronto

- Configuração reproduzível de 3 ou 5 membros etcd `v3.7.3`, com duas redes e volumes persistentes.
- Cliente Python sem dependências externas, sem troca automática de endpoint e sem repetir escritas silenciosamente.
- Automação E1-E9, com procedimentos complementares documentados quando exigidos pelo roteiro.
- E9 com leituras linearizáveis e serializáveis, latência, vazão sequencial, atraso entre pares e leituras desatualizadas sob partição.
- Evidências JSON/CSV, tempos brutos, média, desvio padrão amostral e registro separado de falhas.
- Base de relatório, Q1-Q14, S1-S4, diagramas, referências e roteiro de seminário.

**O trabalho acadêmico ainda precisa dos experimentos no Docker, da análise dos resultados reais, dos dados dos integrantes e da revisão final do relatório.** Os arquivos não contêm resultados inventados. Consulte [a validação realizada](docs/validacao.md).

## Preparação no Windows

1. Instale [Python 3.10 ou superior](https://www.python.org/downloads/) e [Git](https://git-scm.com/downloads).
2. Instale [Docker Desktop com WSL 2](https://docs.docker.com/desktop/setup/install/windows-install/) e conclua o procedimento solicitado pelo instalador. Reinicie se necessário.
3. Abra o Docker Desktop e use contêineres Linux. Aguarde o mecanismo iniciar.
4. Abra um terminal **nesta pasta**, onde está este README.

Não é necessário instalar etcd, etcdctl ou pacotes Python no computador. No Windows, se `python` não funcionar, use `py -3` nos comandos abaixo. No Linux/macOS, pode ser necessário usar `python3`.

```powershell
python -m lab doctor
python -m lab up --nodes 3
python -m lab status
```

O primeiro `up` baixa a imagem. O primeiro experimento com atraso também constrói um pequeno auxiliar com `iproute2/tc`. O host precisa de acesso à Internet para esses downloads.

As redes usam `172.29.0.0/24` e `172.29.1.0/24`. Se conflitarem com VPN/redes existentes, ajuste ambos os endereços, os anúncios e a lógica de rede em `lab/core.py` antes de subir.

## Executar os experimentos

Primeiro leia [os procedimentos e as previsões](docs/experimentos.md) e registre a previsão da equipe. Execute um experimento por vez; eles alteram o mesmo cluster.

```powershell
python -m lab run E1
python -m lab run E2
python -m lab run E3 --repetitions 5
python -m lab run E4
python -m lab run E5 --repetitions 5
python -m lab run E6
python -m lab run E7 --repetitions 5
python -m lab run E8
```

O guia distingue a cobertura automática e as evidências complementares de watch, desempenho, snapshots e coordenação. Confirme essas etapas antes de marcar o requisito como concluído.

### E9: a ênfase principal

```powershell
python -m lab run E9 --repetitions 5 --reads 100 --delay-ms 25
python -m lab down
python -m lab up --nodes 5
python -m lab run E9 --repetitions 5 --reads 100 --delay-ms 25
```

São dois clusters independentes, com volumes distintos. A troca de 3 para 5 é uma comparação entre topologias, não uma demonstração de reconfiguração dinâmica. Rode somente um deles por vez, pois compartilham sub-redes e portas.

O atraso é aplicado na saída da interface de consenso de **cada** membro. `25 ms` por saída representa aproximadamente `50 ms` adicionais de ida e volta entre pares; não inclui atraso artificial no caminho cliente-servidor. Não presuma diferença de desempenho antes de medir. A vazão reportada é a do cliente sequencial em HTTP/JSON, não a capacidade máxima do etcd.

As consultas fortes sem quórum esperam timeout. Os ensaios com muitas leituras, recuperação e snapshots podem levar vários minutos. Se houver interrupção ou falha:

```powershell
python -m lab recover
```

Esse comando reinicia membros, reconecta a rede de consenso e remove o atraso. Não apaga os dados. Ao terminar:

```powershell
python -m lab down
```

`down` preserva volumes, mas remove os contêineres e seus logs. Os scripts guardam cópias das evidências em `results/`; salve também as capturas manuais antes de executar `down`. A configuração ativa está em `.lab/state.json` e os arquivos usados pelo Compose em `.lab/compose-3.json` ou `.lab/compose-5.json`. As cópias na raiz permitem revisão prévia; o gerador em `lab/core.py` é a fonte usada por `up`.

## Topologia e recursos

| Membro | Rede Raft: porta 2380 | Rede de clientes: porta 2379 | Endpoint no computador |
|---|---|---|---|
| etcd1 | 172.29.0.11 | 172.29.1.11 | http://127.0.0.1:12379 |
| etcd2 | 172.29.0.12 | 172.29.1.12 | http://127.0.0.1:13379 |
| etcd3 | 172.29.0.13 | 172.29.1.13 | http://127.0.0.1:14379 |
| etcd4, somente N=5 | 172.29.0.14 | 172.29.1.14 | http://127.0.0.1:15379 |
| etcd5, somente N=5 | 172.29.0.15 | 172.29.1.15 | http://127.0.0.1:16379 |

Cada membro tem limite de 256 MiB e 0,5 CPU. São limites, não consumo medido: 768 MiB para 3 membros e 1280 MiB para 5, além do Docker, do cliente e dos auxiliares. A rede Raft é interna; os pares escutam apenas nela, de forma que a rede dos clientes não contorne uma partição. As portas do host são publicadas apenas em `127.0.0.1`. HTTP sem autenticação serve a este laboratório local; não exponha essas portas como serviço público.

## Resultados e relatório

Cada execução cria uma pasta única em `results/`, com metadados e arquivos de evidência. A lista exata depende do experimento; as amostras ficam em `measurements.csv/json` e as estatísticas em `summary.json`. Verifique erros e denominadores da taxa de leituras desatualizadas antes de escrever conclusões.

`results/` é ignorada pelo Git para evitar publicar logs acidentalmente. Para a entrega, copie as execuções revisadas para uma pasta `evidencias/` no repositório ou adicione explicitamente as pastas selecionadas com `git add -f`. Remova dados pessoais, quando existirem; use somente as chaves fictícias do laboratório.

- [Procedimentos E1-E9](docs/experimentos.md)
- [Base de relatório e respostas](docs/relatorio-base.md)
- [Roteiro de seminário](docs/seminario.md)
- [Fontes primárias](docs/referencias.md)
- [O que foi testado](docs/validacao.md)

O roteiro indica relatório de 15 a 25 páginas, sem anexos, e entrega em 28/11/2026. A base em Markdown deve receber as medições e a análise da equipe antes da diagramação final. Há orientações divergentes sobre vídeo e duração da demonstração no PDF; o guia de seminário registra essa pendência.

## Testes do projeto

```powershell
python -m unittest discover -s tests -v
```

Os testes de código usam respostas controladas e um servidor HTTP local. Não substituem os experimentos de falha em Docker. A automação do GitHub inclui esses testes e uma verificação básica com etcd em contêineres; o ensaio E9 completo pode ser iniciado manualmente em Actions após a publicação.

## Repositório no GitHub

Destino definido: [JallisonJimenez/m2-sistemas-distribuidos](https://github.com/JallisonJimenez/m2-sistemas-distribuidos), com visibilidade pública.

Para obter uma cópia após a publicação:

```powershell
git clone https://github.com/JallisonJimenez/m2-sistemas-distribuidos.git
cd m2-sistemas-distribuidos
python -m lab doctor
```

Para enviar alterações revisadas a partir da cópia clonada:

```powershell
git add .
git commit -m "Atualiza laboratorio e documentacao"
git push origin main
```

Use sua autenticação normal do GitHub. Revise as evidências selecionadas antes de incluí-las no repositório e mantenha as pendências experimentais identificadas até a execução real.

Nenhum kit de código do professor foi anexado. Esta é uma implementação própria baseada nas exigências do roteiro e na documentação oficial; se a disciplina fornecer um kit obrigatório, compare e adapte antes da entrega. Registre a assistência de IA conforme a declaração proposta no relatório e revise o que for utilizado.

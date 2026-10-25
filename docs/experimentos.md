# Roteiro de execução e coleta de evidências

Escopo: Tema 5, etcd/Raft, ênfase A. Execute na raiz do repositório com Docker iniciado em modo de contêineres Linux. No Windows, os exemplos manuais abaixo usam PowerShell. Não há resultados preenchidos previamente.

## 1. Preparação

```powershell
python -m lab doctor
python -m lab up --nodes 3
python -m lab status
```

O primeiro comando verifica os pré-requisitos; ele não instala Docker. O `up` gera `.lab/compose-3.json`, com projeto `sd-etcd-n3`, redes `sd-etcd-n3_raft` e `sd-etcd-n3_clients`. São três membros, limite de 256 MiB e 0,5 CPU por membro. Cada execução cria uma pasta em `results/`, cujo caminho é mostrado ao terminar.

Antes de cada teste, escreva a previsão da equipe, as condições iniciais e o resultado que contrariaria sua hipótese. Preserve `metadata.json`, `measurements.json`, `measurements.csv`, `summary.json` e os arquivos específicos disponíveis. Não substitua a saída real por uma resposta esperada.

```powershell
python -m lab run E1 --repetitions 5
python -m lab run E2 --repetitions 5
python -m lab run E3 --repetitions 5
python -m lab run E4 --repetitions 5
python -m lab run E5 --repetitions 5
python -m lab run E6 --repetitions 5
python -m lab run E7 --repetitions 5
python -m lab run E8 --repetitions 5
```

Falhas esperadas de operação, como o timeout de uma leitura na minoria, são dados do experimento. Falha da automação, ausência de evidência ou condição não confirmada exigem revisão; não equivalem a teste concluído com sucesso.

## 2. O que conferir em E1-E8

| Teste | Conferência necessária | Evidência específica |
|---|---|---|
| E1 | Três membros, líder único e saúde linearizável; guardar versão e consumo. | `status.json`, `member-list.json`, `endpoint-health.txt` |
| E2 | Membro escolhido era seguidor; put confirmado; revisão e índice antes/depois. | `caminho.json`, `sequence.mmd` |
| E3 | Valores entre membros, evento watch da escrita, medidas de put/get e propagação; resultado de check perf. | `replicacao.json`, `watch.json`, `check-perf.txt` |
| E4 | Seguidor isolado com endpoint ainda acessível; escrita confirmada na maioria; leitura antiga versus timeout. | `particao.json` |
| E5 | Quadro 3/2/1 membros ativos; cinco tempos de eleição; termos aumentados; disponibilidade por operação. | `quorum.json` e amostras |
| E6 | Antigo líder isolado só dos pares; maioria com novo líder; endpoints de ambos os lados testados. | `particao.json` |
| E7 | Cinco recuperações normais e cinco com 2.500 escritas durante parada; marcador e aplicação convergentes; snapshot depende de log de recepção/aplicação. | `recuperacao.json`, `snapshots.json`, `snapshot-logs-1.txt` a `snapshot-logs-5.txt` |
| E8 | Histórico de termos/revisões automático; lock/eleição de aplicação e cinco transições exigem complemento manual abaixo. | `termos-revisoes.json`, `manual-required.json`, logs manuais de coordenação |

Anexe ao relatório as saídas que realmente sustentam cada afirmação. O diagrama de sequência é uma explicação do protocolo, e não captura de pacotes. Revisão MVCC e índice Raft são grandezas distintas. No E7, criação de snapshot pelo líder não basta para provar que o membro retornado o recebeu.

No E3, `visibility_after_ack` mede o tempo depois do reconhecimento da escrita até observar o valor numa leitura local em outra réplica. É uma medida de observabilidade após ACK, e não o tempo interno completo desde a proposta até a replicação. E1/E2 são registros de estado/caminho; passar `--repetitions 5` não transforma esses inventários em cinco medições de tempo.

## 3. E9: matriz completa de leitura

Executar o mesmo número de leituras e repetições nos dois tamanhos. O parâmetro `--reads` define a carga do lote; começar com 100 e repetir com uma carga maior se os tempos forem muito pequenos ou instáveis.

```powershell
python -m lab run E9 --repetitions 5 --reads 100 --delay-ms 25
python -m lab down
python -m lab up --nodes 5
python -m lab run E1 --repetitions 5
python -m lab run E9 --repetitions 5 --reads 100 --delay-ms 25
```

O `down` preserva os dados. N=3 e N=5 usam projetos e volumes próprios. Como as faixas de IP e portas são iguais, executar alternadamente; isso não demonstra reconfiguração dinâmica do cluster. O retorno a N=3 segue `down` e `up --nodes 3`.

| N | Leitura | Atraso por saída de interface Raft | Repetições | Média latência | Desvio | Vazão | Erros |
|---|---|---|---|---|---|---|---|
| 3 | Linearizável | 0 ms | 5 | PENDENTE | PENDENTE | PENDENTE | PENDENTE |
| 3 | Serializável | 0 ms | 5 | PENDENTE | PENDENTE | PENDENTE | PENDENTE |
| 3 | Linearizável | 25 ms | 5 | PENDENTE | PENDENTE | PENDENTE | PENDENTE |
| 3 | Serializável | 25 ms | 5 | PENDENTE | PENDENTE | PENDENTE | PENDENTE |
| 5 | Linearizável | 0 ms | 5 | PENDENTE | PENDENTE | PENDENTE | PENDENTE |
| 5 | Serializável | 0 ms | 5 | PENDENTE | PENDENTE | PENDENTE | PENDENTE |
| 5 | Linearizável | 25 ms | 5 | PENDENTE | PENDENTE | PENDENTE | PENDENTE |
| 5 | Serializável | 25 ms | 5 | PENDENTE | PENDENTE | PENDENTE | PENDENTE |

O atraso é aplicado com `tc netem` no namespace de rede do membro, somente na interface Raft identificada pela rota para outro peer. Se ambos os emissores têm 25 ms de atraso de saída, a comunicação ida/volta entre eles inclui aproximadamente 50 ms de atraso artificial, além do restante do processamento. Guarde a configuração real e qualquer erro ao instalar/remover a regra.

No teste sob partição, a referência deve ser uma escrita **confirmada antes da leitura**. Guardar valor/revisão de referência e valor/revisão observado. Calcular `leituras antigas / leituras serializáveis concluídas`, informando separadamente tentativas e erros. A taxa sob isolamento completo não representa uma estimativa de produção. O arquivo `stale.json` documenta esse cenário; `leituras.json` reúne a comparação das modalidades.

A vazão reportada pelo coletor é a do cliente e carga executados. Não a chamar de capacidade máxima saturada do etcd. Quando houver HTTP persistente, informar isso; medidas de `docker exec etcdctl` têm outro custo. As definições e fatores físicos são descritos no [guia de desempenho oficial](https://etcd.io/docs/v3.7/op-guide/performance/).

## 4. Complementos manuais e inspeção

Estes comandos permitem inspecionar ou repetir partes do experimento. Use N=3 para os exemplos. As variáveis e funções abaixo precisam existir em cada terminal que chamar `dc` ou `ctl`.

```powershell
$ComposeProject = 'sd-etcd-n3'
$ComposeFile = '.lab/compose-3.json'
function dc { docker compose -p $ComposeProject -f $ComposeFile @args }
function ctl {
    param([string]$Member)
    dc exec -T $Member /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 --command-timeout=5s @args
}
ctl etcd1 member list -w table
ctl etcd1 endpoint status --cluster -w table
ctl etcd1 endpoint health --cluster
dc stats --no-stream
```

O uso de `--cluster` aqui é para inventário. Nas observações de um membro isolado, consulte um único endpoint; a troca automática de endpoint pode esconder o efeito da falha. O [manual da versão fixada do etcdctl](https://github.com/etcd-io/etcd/blob/v3.7.3/etcdctl/README.md) documenta os comandos.

### E3: watch e referência de desempenho

No terminal A, iniciar um watch em outro membro. Observar uma revisão conhecida evita perder o evento se a escrita ocorrer antes da conexão do watch.

```powershell
ctl etcd1 put /sd/manual-watch inicial -w json
```

Copie o `revision` da resposta; no exemplo seguinte, substitua `123` pela revisão real. O parâmetro é uma revisão de início, não um valor observado fictício.

```powershell
ctl etcd3 watch /sd/manual-watch --rev=123 -w json
```

No terminal B:

```powershell
ctl etcd1 put /sd/manual-watch alterado -w json
ctl etcd2 get /sd/manual-watch -w json
ctl etcd2 get /sd/manual-watch --consistency=s -w json
ctl etcd1 check perf --load=s
```

Interrompa o watch com Ctrl+C. Preserve o evento correspondente ao put confirmado e sua revisão. O `check perf` aplica carga e pode levar mais tempo; registre sua saída integral. Para cinco medidas comparáveis, repetir a mesma carga e guardar as cinco execuções, sem misturar o resultado dessa ferramenta com a latência HTTP do coletor.

### E7: comprovar recuperação por snapshot

O Compose fixa `--snapshot-count=1000` e `--snapshot-catchup-entries=100`. O teste automático tenta gerar distância suficiente entre membro parado e maioria. Se o resultado indicar ausência de prova, repetir com mais escritas e inspecionar logs; não mudar a conclusão para “snapshot confirmado” só porque o membro voltou saudável. A [configuração oficial](https://etcd.io/docs/v3.7/op-guide/configuration/) explica os parâmetros, e [manutenção](https://etcd.io/docs/v3.7/op-guide/maintenance/) distingue manutenção de histórico e snapshots.

Exemplo de repetição manual, após consultar status e garantir etcd1/etcd2 disponíveis:

```powershell
dc stop etcd3
1..4000 | ForEach-Object {
    ctl etcd1 put /sd/snapshot-manual "valor-$_" | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Escrita não confirmada; interromper e revisar.' }
}
ctl etcd1 put /sd/snapshot-manual marcador-final -w json
$SnapshotStart = Get-Date -Format o
dc start etcd3
```

Sondar até o valor de leitura serializável em etcd3 ser `marcador-final`, conferir status/índice e salvar logs:

```powershell
ctl etcd3 get /sd/snapshot-manual --consistency=s -w json
ctl etcd3 endpoint status -w json
dc logs --since $SnapshotStart --no-color --timestamps etcd3
```

Guardar o log completo; localizar recebimento/aplicação de snapshot, não apenas o termo `snapshot` em qualquer mensagem. Registrar cinco tempos da recuperação normal e, se a equipe comparar mecanismos, cinco da variante por snapshot. Capturar o instante inicial e usar cronômetro monotônico, como no coletor; a intervenção humana limita precisão de um teste exclusivamente manual.

### E8: trava com clientes independentes

Esta demonstração usa clientes descartáveis, de modo que a falha da aplicação não derrube um servidor etcd. Dois clientes competem pela mesma trava, usando a imagem já fixada. A chave de posse só aparece quando a aquisição termina. Defina TTL de 10 segundos explicitamente; o valor padrão do kit não deve ser presumido em outra versão.

```powershell
docker run -d --name sd-lock-a --network sd-etcd-n3_clients --entrypoint /usr/local/bin/etcdctl quay.io/coreos/etcd:v3.7.3 --endpoints=http://172.29.1.11:2379 lock --ttl=10 sd-mutex
docker logs --timestamps sd-lock-a
docker run -d --name sd-lock-b --network sd-etcd-n3_clients --entrypoint /usr/local/bin/etcdctl quay.io/coreos/etcd:v3.7.3 --endpoints=http://172.29.1.12:2379 lock --ttl=10 sd-mutex
docker logs --timestamps sd-lock-b
```

Confirme que A adquiriu antes de iniciar B. O segundo deve aguardar. Em outro terminal, registre a hora e interrompa A abruptamente; uma liberação graciosa pode revogar o lease imediatamente e não mede expiração.

```powershell
Get-Date -Format o
docker kill --signal=KILL sd-lock-a
docker logs --follow --timestamps sd-lock-b
```

Quando B adquirir, registrar tempo e sua chave/revisão. Repetir cinco vezes com nomes de clientes e de trava novos, ou remover os clientes encerrados antes da repetição. Encerrar e remover apenas esses auxiliares ao terminar:

```powershell
docker rm -f sd-lock-a sd-lock-b
```

O tempo de transição pode incluir expiração, sondagem e escalonamento; não deve ser preenchido antecipadamente como exatamente 10 segundos. O fencing é tratado na S4, mas não é o experimento E9 desta ênfase. [API de concorrência](https://etcd.io/docs/v3.7/dev-guide/api_concurrency_reference_v3/).

### E8: eleição de líder da aplicação

```powershell
docker run -d --name sd-elect-a --network sd-etcd-n3_clients --entrypoint /usr/local/bin/etcdctl quay.io/coreos/etcd:v3.7.3 --endpoints=http://172.29.1.11:2379 elect sd-eleicao aplicacao-A
docker logs --timestamps sd-elect-a
docker run -d --name sd-elect-b --network sd-etcd-n3_clients --entrypoint /usr/local/bin/etcdctl quay.io/coreos/etcd:v3.7.3 --endpoints=http://172.29.1.12:2379 elect sd-eleicao aplicacao-B
docker logs --timestamps sd-elect-b
Get-Date -Format o
docker kill --signal=KILL sd-elect-a
docker logs --follow --timestamps sd-elect-b
```

A deve ser confirmada como eleita antes de iniciar B. O `elect` pode usar TTL diferente do lock configurado acima; confira `elect --help`, inspecione o lease e aguarde seu prazo real. Não aplicar `--ttl=10` ao comando sem confirmar suporte. Registrar cinco transições, líder da aplicação e líder Raft; o segundo não precisa mudar. Após a coleta:

```powershell
docker rm -f sd-elect-a sd-elect-b
```

## 5. Recuperação, interrupção e finalização

Se um teste for interrompido durante falha ou atraso, execute:

```powershell
python -m lab recover
python -m lab status
```

O `recover` tenta iniciar/reconectar membros e retirar atraso. Confirme saúde antes do teste seguinte. Ao terminar:

```powershell
python -m lab down
```

Esse comando remove os contêineres do projeto atual e preserva volumes. Os logs de contêineres devem ser coletados antes. Não usar `down -v` como rotina: ele apaga dados do laboratório. `results/` contém arquivos independentes que precisam ser preservados e revisados antes de publicar.

## 6. Análise e entrega

Para cada métrica, calcular média e desvio padrão amostral sobre cinco ou mais repetições comparáveis. O desvio da distribuição de chamadas e o desvio das médias de cinco lotes são estatísticas diferentes: nomear o que foi calculado. Preservar falhas/timeouts e informar o denominador de taxas.

Completar as previsões com confronto entre expectativa, observação e teoria. Atualizar o [relatório-base](relatorio-base.md), anexar evidências e gerar figuras com dados reais. Resultados brutos sem análise ainda não completam o trabalho; texto teórico sem execução também não.

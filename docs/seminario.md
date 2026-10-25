# Guia do seminário

Tema 5 - Estado/Coordenação com etcd/Raft; ênfase A - Linearizável x serializável.

> O roteiro apresenta conflito: R9 menciona vídeo; a seção 11 exige demonstração ao vivo de pelo menos 8 minutos, mas a tabela reserva 4 minutos para a demonstração dentro de 10 minutos totais. Confirmar com o professor a modalidade e a distribuição de tempo. Preparar o cluster e um vídeo de contingência. As sugestões abaixo não resolvem essa decisão em nome do professor.

## Roteiro de 10 minutos, caso a tabela seja adotada

| Tempo | Conteúdo | Evidência a mostrar |
|---|---|---|
| 0:00-1:00 | Problema: estado crítico compartilhado; exemplo configuração e travas; artigo Raft. | Uma chave real e a referência seminal |
| 1:00-3:00 | Topologia, dois tipos de rede, líder dinâmico e maioria 2/3. | Diagrama autoral e status |
| 3:00-7:00 | Escrita em seguidor, leitura em outro, isolamento do líder, nova eleição, erro no isolado, reconexão. | E2/E3/E6/E7 já ensaiados |
| 7:00-9:00 | Ênfase A: latência/vazão N=3/N=5, atraso e leitura antiga. | Gráficos e `stale.json` reais |
| 9:00-10:00 | Quadro-síntese, limitações e conclusão sustentada. | Resultados observados e uma limitação |

Se prevalecer a exigência de oito minutos ao vivo, integrar arquitetura e conceitos à demonstração: explicar papéis enquanto mostra status, explicar quórum durante a falha e apresentar E9 já com a matriz medida. Não iniciar milhares de escritas ou benchmarks longos durante a janela de apresentação.

## Preparação antes da aula

- Executar a bateria completa e conservar todas as pastas de evidência.
- Completar a análise, as figuras e o relatório com números reais.
- Ensaiar a apresentação cronometrada e dividir falas entre todos os integrantes.
- Baixar/construir imagens antecipadamente; evitar depender de download na aula.
- Iniciar o cluster de três membros antes da apresentação, retirar atrasos e confirmar saúde.
- Preparar terminais, status, gráfico E9 e arquivos certos sem pesquisa durante a fala.
- Gravar vídeo curto de contingência da mesma demonstração; identificar data/configuração.

## Sequência operacional curta

```powershell
python -m lab doctor
python -m lab up --nodes 3
python -m lab recover
python -m lab status
```

O uso de `recover` evita herdar uma partição do ensaio. Acompanhar os comandos relevantes em [Experimentos](experimentos.md). Para demonstração de falha, usar o membro descoberto como líder, não um nome memorizado de outra execução.

Durante a apresentação, mostrar uma escrita reconhecida e a leitura em outro membro; indicar revisão/termo. Ao isolar o líder, manter a rede de clientes acessível, observar novo líder na maioria e comparar a resposta do isolado. Reconectar e conferir estado. Guardar o E9 como comparação previamente medida, para que a turma veja tanto dados quanto interpretação.

## Perguntas que todos devem saber responder

1. Por que três membros toleram uma falha e quatro não toleram duas?
2. O que distingue termo, índice Raft e revisão MVCC?
3. Por que a leitura serializável pode responder na minoria com valor antigo?
4. Um timeout de put prova que não houve escrita?
5. O que torna a eleição Raft mais que selecionar o maior identificador?
6. Qual é a diferença entre o líder etcd e o líder eleito por uma aplicação?
7. Como o teste comprova snapshot recebido, em vez de somente recuperação por log?
8. Por que aumentar N não obriga todas as medidas a ficarem piores?
9. Onde entra o custo do cliente na latência e na vazão medidas?
10. Por que um lease sozinho não impede um cliente antigo de escrever no recurso externo?

As respostas fundamentadas estão no [relatório-base](relatorio-base.md). Na arguição, citar a evidência real que sustenta a afirmação, explicando também o que o experimento não permite concluir.

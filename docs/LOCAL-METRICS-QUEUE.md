# Fila local de métricas

`StatisticsRuntime` persiste em SQLite os eventos `Metric`, chamadas a `IMetricController.Write`, `Enqueue` e `EnqueueBatch`. O retorno confirma commit **local**, não entrega ao backend. O worker envia em lotes e remove somente após confirmação do provider. Falhas preservam o lote e aplicam espera progressiva; a fila retoma ao construir o runtime. Em Generic Host, `AddSufficitStatistics` registra hosted service para instanciá-lo no startup, mesmo sem eventos novos.

`WriteSingleAsync` e `WriteBulkAsync` preservam seu contrato de escrita **direta remota**. Produtores que desejam aceite local devem usar Enqueue/EnqueueBatch; o pacote não migra automaticamente jobs externos nem muda a semântica de suas confirmações. EnqueueBatch aceita até BatchSize métricas atomicamente. A escrita SQLite é síncrona e limitada: o chamador aguarda disco, nunca a rede. Para fluxo de telefonia sensível à latência, usar o caminho de publicação em background existente e medir o custo de fsync; não mover espera de rede para a chamada.

## Configuração

```json
{
  "Sufficit": {
    "Statistics": {
      "Queue": {
        "DirectoryPath": "/var/lib/sufficit/my-service/metrics-queue",
        "MaxDatabaseBytes": 268435456,
        "MaxMetricBytes": 262144,
        "BatchSize": 100,
        "PollSeconds": 1,
        "MaxRetrySeconds": 60
      }
    }
  }
}
```

Configurar diretório absoluto persistente, exclusivo da instância, com permissão de escrita para o serviço, fora da pasta substituída pelo deploy. Em containers, montar volume persistente. Default: LocalApplicationData/Sufficit/nome-do-executável/metrics-queue. Não compartilhar entre processos/hosts. Um lock exclusivo impede dois workers sobre o mesmo arquivo. Hosts sem Generic Host devem criar e descartar o singleton StatisticsRuntime explicitamente.

MaxDatabaseBytes limita o arquivo SQLite por páginas; reservar espaço adicional para journal de transação (potencialmente até tamanho similar ao banco) e metadados do filesystem. DELETE journal e synchronous FULL confirmam persistência local; páginas livres são reutilizadas, o arquivo não encolhe a cada envio. Não há descarte automático por idade ou capacidade. Ao lotar, a escrita falha e pendências aceitas permanecem. Nunca remover manualmente o banco para liberar espaço sem aceitar a perda dessas pendências.

O lote carregado em memória é limitado por BatchSize × MaxMetricBytes, além do payload HTTP e buffers do provider. O backlog não é carregado integralmente. BatchSize também limita a transação local de EnqueueBatch; lote com item inválido/maior que o permitido é revertido inteiro. Os parâmetros têm validação de faixa. Métricas devem ter measurement e fields; valores primitivos são preservados no disco. Payload não suportado falha no aceite.

## Semântica e operação

- Entrega pelo menos uma vez: queda após aceite remoto e antes do DELETE local causa reenvio. Timestamp, tags e valores persistidos são preservados. Timestamp ausente é preenchido em UTC no aceite, não a cada tentativa.
- Consumidor precisa tolerar reenvios. Esta fila não implementa exatamente uma vez nem valida semântica de correção de buckets no VictoriaMetrics. Não usar incrementos não idempotentes sem contrato correspondente.
- Circuit breaker aberto em VictoriaMetrics/InfluxDB lança falha; não confirma um envio descartado. Cancelamento de escrita direta propaga ao chamador.
- Fila cheia ou disco indisponível: aceite falha, sem fallback para RAM. Publicador deve tratar a falha; checkpoint só avança com aceite. A política externa de event bus pode capturar exceções: quem precisa confirmação local explícita deve usar Enqueue/EnqueueBatch.
- `GetQueueStatus()` retorna pending count, bytes do banco, item mais antigo, último envio confirmado e contadores de falhas. Os contadores de falhas reiniciam com processo; pendências e último envio sobrevivem. `GetCachedMetricsCount()` permanece compatível e agora conta pendências em disco.
- Worker único preserva ordem e pausa diante de falha. Payload corrompido ou permanentemente rejeitado mantém a fila bloqueada e gera erro; investigar sem descartar silenciosamente.
- Dispose cancela o worker e aguarda término antes de fechar SQLite; providers devem respeitar cancelamento/timeout. Não há espera por esvaziamento: pendências ficam para o próximo processo.
- Fila local não protege contra perda do disco/host. Não há broker, replicação ou persistência do estado de consolidação de chamadas.
- Nenhuma consulta ao CDR é feita por esta fila. Dashboards continuam restritos ao backend de métricas.

## Validação

```sh
dotnet run --project tests/QueueTests.csproj
dotnet build src/Sufficit.Statistics.csproj
```

Harness usa SQLite em diretórios temporários e providers locais simulados. Testa falha, restart, saída de processo sem Dispose, capacidade, rollback de lote, tipos, cancelamento, propriedade exclusiva da fila e confirmação ambígua. Não envia métricas reais.

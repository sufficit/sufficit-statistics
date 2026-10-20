# Fila persistente local de métricas

Implementação local em Sufficit.Statistics: SQLite transacional, capacidade configurável, aceite local explícito, lotes atômicos, worker com retry/backoff, retomada no startup do host, lock por instância e status de backlog. Eventos/IMetricController usam disco; métodos de escrita direta preservam confirmação remota. Circuit breaker de VictoriaMetrics e InfluxDB passa a rejeitar entrega, sem falso sucesso. Cancelamento de escrita direta propaga.

Validações executadas:
- `dotnet run --project tests/QueueTests.csproj`: passou em .NET 10; saída abrupta de processo, restart sem novo enqueue, falha/reenvio, capacidade, lote inválido com rollback, primitivos, cancelamento, lock exclusivo e identidade estável no reenvio.
- Reintrodução temporária do retorno silencioso anterior no circuito VictoriaMetrics: teste falhou com `Open circuit falsely acknowledged delivery` (exit 134). Código corrigido restaurado e suíte passou novamente.
- `dotnet build src/Sufficit.Statistics.csproj --no-restore`: passou para netstandard2.0/net7.0/net10.0. Avisos existentes de dependência transitiva System.Security.Cryptography.Pkcs 8.0.1 e nullable em projetos referenciados; não corrigidos neste escopo.
- `git diff --check`: passou.

CI adicionado para PR/push e validação antes do pack no workflow de publicação. Nenhum push, pacote publicado ou deploy executado. Não foram enviadas métricas reais; providers dos testes são simulados. A semântica real de deduplicação/correção no backend permanece fora desta mudança: a fila garante reenvio com identidade preservada, não exatamente uma vez. Para produção, configurar diretório persistente exclusivo e migrar explicitamente os produtores que desejarem aceite local.

Documentação permanente: ../LOCAL-METRICS-QUEUE.md. Plano: ../PLAN-LOCAL-METRICS-QUEUE.md. Dashboard e consolidação incremental do CDR seguem pendentes no plano do Blazor.

# Fila persistente local de métricas

Escopo autorizado: substituir backlog em memória por disco no Sufficit.Statistics; sem deploy, novo broker ou implementação do dashboard.

1. CONCLUÍDO — Fixar contrato e implementar persistência SQLite limitada, envio e configuração.
2. CONCLUÍDO — Validar reinício, falha/reenvio, limites, cancelamento e serialização; compilar targets.
3. CONCLUÍDO — Documentar configuração, limitações e registrar entrega.

Contrato: aceite após commit local; confirmação remota antes de remover; reenvio pelo menos uma vez; lote limitado; sem sucesso falso com circuito aberto. Escritas diretas existentes mantêm confirmação remota; entrada pelo event bus/IMetricController passa pela fila persistente. Fornecer enqueue explícito para produtores que queiram aceite local. A fila não torna incrementos não idempotentes seguros por si só.

Validação: testes isolados em diretório temporário com provider simulado; build netstandard2.0/net7.0/net10.0. Nenhuma métrica real enviada. Skill remota: check retornou erro de alterações locais no pacote; instruções instaladas utilizadas.

Resultado: implementação e validação locais concluídas. Consulte LOCAL-METRICS-QUEUE.md e docs/activities. Não publicado/deployado.

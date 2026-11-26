# Abertura da fila SQLite gerenciada pelo consumidor

DeferStatisticsQueueStartup permite que uma aplicação retire o hosted service que força a resolução de StatisticsRuntime na partida. O consumidor que escolher essa opção assume a abertura em segundo plano, incluindo a retomada de pendências no disco mesmo quando não chegam métricas novas. O contêiner conserva a responsabilidade pelo descarte. O comportamento padrão e os contratos de escrita durável continuam disponíveis aos demais consumidores.

O build Release dos três alvos e o pack local passaram. O harness de fila passou nos cenários de retomada, retenção sob falha, lote, cancelamento, propriedade exclusiva, quota e confirmação ambígua. A integração do FastAGI iniciou o host com o diretório bloqueado por um arquivo, concluiu o roteamento da URA e entregou o registro pelo backend simulado após remover o bloqueio.

# Consolidação da fila persistente na main

Pedido: integrar os trabalhos pendentes dos projetos Sufficit na main e sincronizar com o remoto.

Alterações locais revisadas e copiadas para worktree isolada, preservando a árvore original. Integração da fila SQLite limitada, worker de envio, configuração/hosted service, contratos e harness de recuperação/reenvio. Sem deploy manual.

Validação executada: `dotnet run --project tests/QueueTests.csproj --configuration Release` passou nos cinco grupos (queda de processo, reinício/reenvio/tipos/cancelamento/ownership, capacidade, confirmação ambígua e circuito aberto). `dotnet build src/Sufficit.Statistics.csproj --configuration Release -m:1 -nodeReuse:false` passou em todos os targets, zero erros. Warnings existentes NU1902/NU1903 de System.Security.Cryptography.Pkcs 8.0.1 registrados; não foram ocultados.

# Fechamento de fronteiras da API e compatibilidade net7

Alinhadas as oito referências Microsoft.Extensions pelo ExtensionsVersion em net7. O grafo inclui Events alinhado; EF Core/System packages e TFMs mantidos.

## Validação

- Antes: testes arquitetura/áudio/painel reproduziram 2 falhas de fronteira e 13 aprovações.
- Depois: 15 testes do mesmo conjunto aprovados; limites/detectores de arquitetura intactos.
- Rodada ampliada de API: 167 aprovados, zero falhas/skips, incluindo arquitetura global, DI, áudio, painel, agentes, filas, financeiro e MarkdownCoverage.
- dotnet build Standard -f net7.0: grafo completo aprovado (0 erros), incluindo Communication, Statistics, Events e EFData. Assets net7 confirmam Microsoft.Extensions 8.x.
- dotnet build Standard -f netstandard2.0 --no-restore: aprovado (0 erros). net10 compilado pelos testes da API.
- A primeira restauração apontou NU1605 em Events; a origem foi alinhada, sem suprimir o erro. O teste DI inicialmente não tinha a configuração obrigatória do gateway; foi fornecida configuração fictícia localhost, sem abrir conexão.

## Escopo operacional

Sem chamadas reais, publicação NuGet, aplicação de migração ou deploy nesta etapa. A resposta “sim” à pergunta de duas alternativas não definiu o escopo de produção; esclarecimento permanece necessário. Entrega autorizada de código via commit/push na main. Referências finais consolidadas na atividade de Endpoints.

## Documentação concorrente

MarkdownCoverage da API: 8 aprovados. O checker adicional de Standard teve 7 aprovações e 1 falha de navegação: inventário local inclui o plano concorrente PLAN-FREEPBX-SIP-MIRROR.md ainda sem vínculo. Não alterar, esconder ou incluir documentos de outra tarefa para fazer o checker passar. As alterações desta etapa em Standard são apenas no documento de arquitetura já indexado e na atividade histórica.

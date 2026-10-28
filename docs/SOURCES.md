# Sources

Serialization-code provenance is recorded in [THIRD_PARTY.md](../THIRD_PARTY.md).

- Polymarket public market wire protocol, heartbeat, subscription changes and lifecycle: https://docs.polymarket.com/market-data/realtime-data (raw WebSocket API section, consulted 2026-10-27).
- Polymarket Gamma market discovery and keyset pagination: https://docs.polymarket.com/market-data/discover-markets (consulted 2026-10-27).

Only public read-only endpoints are used. No authentication, financial account, private key, funding, order creation or trading functionality is present. The current adapter uses Polymarket public market-data services.

Additional protocol clarification: https://docs.polymarket.com/migrate/polymarket-v2/api-integrations.md specifies canonical identifiers by market version. The keyset schema at https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination documents the 100-record limit and omitted terminal cursor. Nullable eligibility is represented in the official SDK response types: https://github.com/Polymarket/rs-clob-client-v2/blob/main/src/gamma/types/response.rs.

The event keyset endpoint and embedded market structure are documented at https://docs.polymarket.com/api-reference/events/list-events-keyset-pagination and https://docs.polymarket.com/market-data/discover-markets. An event traversal is not claimed to independently reconcile every exchange market.

Metadata readiness and current public wire contract: https://docs.polymarket.com/market-data/market-details#market-status ; https://docs.polymarket.com/asyncapi.json ; https://docs.polymarket.com/api-reference/rate-limits ; official SDK market socket lifecycle https://github.com/Polymarket/ts-sdk/blob/6e482eb3431dfb2f8dc5b2beb7e013d13064bea5/packages/client/src/websockets/clob/market.ts . These sources do not establish `ready`/`funded` as unavailable-book exclusions or a numeric per-connection asset maximum.

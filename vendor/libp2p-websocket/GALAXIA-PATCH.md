# libp2p-websocket 0.46.0 con un parche

Copia de `libp2p-websocket` 0.46.0 (crates.io) con un solo cambio:
`tls::Config::from_rustls(client, server)` en `src/tls.rs`, para poder usar un
`ServerCertVerifier` propio (ver `src/p2p/tls.rs` en galaxIA-agent).

Motivo: el certificado del laboratorio es autofirmado con `CA:TRUE` y se usa
como certificado de servidor; webpki lo rechaza (`CaUsedAsEndEntity`). Node lo
acepta con `NODE_EXTRA_CA_CERTS`. El agente lo fija (pinning) en lugar de
validarlo como cadena. Se puede quitar este vendor cuando el laboratorio use
la PKI de `galaxIA-E2E` (raíz + intermedia + certificados de nodo) o cuando
upstream permita inyectar la configuración de rustls.

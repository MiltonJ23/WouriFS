# WouriFS — Sprint 1 Development Workflow Decisions

> **NON-VERSIONED** — this file documents every non-obvious design choice made
> during Sprint 1 development, with rationale and tradeoffs. It exists so that
> Sprint 2 contributors (and the defense jury) can understand *why* the code
> looks the way it does.

---

## 1. Architecture Overview (Sprint 1 Scope)

Sprint 1 delivers a **single-instance Namenode + Datanodes** that can:
- Register Datanodes and receive heartbeats (FR-D-003, FR-D-007)
- Perform file metadata operations: CreateFile, DeleteFile, LookupFile, ListDirectory (FR-N-004)
- Allocate chunks to available Datanodes with configurable replication factor (FR-N-006, FR-N-008)
- Persist metadata mutations in a Write-Ahead Log (FR-N-002) and replay on restart (FR-N-003)
- Enforce namespace isolation per authenticated user (FR-N-007)
- Store chunks as binary files on disk (FR-D-001)
- Stream read/write chunks in 1MB blocks (FR-D-002, FR-D-006)
- Replicate chunks between Datanodes (FR-D-004)

**Out of scope for Sprint 1 (deferred to Sprint 2+):**
- Raft consensus / multi-Namenode HA
- FUSE client
- Audit log (Merkle chaining)
- wouri-watchd anomaly detection
- WireGuard/Tailscale mesh
- Shamir's Secret Sharing provisioning
- etcd integration

---

## 2. WAL Design: JSON-Lines Append-Only

**Decision:** Use JSON-lines (one JSON object per line) for the Write-Ahead Log.

**Why JSON-lines:**
- Human-readable for debugging during development and defense presentations
- Zero external dependencies — Go stdlib `encoding/json` only
- Sequential append pattern: no random access needed, just tail the file
- Metadata volume is orders of magnitude smaller than chunk data (a few KB per operation), so I/O overhead of JSON encoding is negligible

**Why NOT binary/protobuf WAL:**
- Would require protobuf deserialization tooling for debugging
- No performance benefit at metadata scale (hundreds of ops/sec, not millions)
- Adds cognitive complexity for the defense jury to verify correctness

**Why NOT SQLite WAL:**
- External CGo dependency
- Overkill for a sequential log of homogenous entries
- The project already manages disk I/O for chunks; adding SQLite for the metadata path creates two storage paradigms

**WAL usage pattern:**
1. On startup, `Replay()` reads all entries and rebuilds the `MetadataStore` in-memory state
2. Every state mutation (CreateFile, DeleteFile, AddChunk) also appends to the WAL
3. `Replay` is idempotent — calling it twice just overwrites state, no duplicates

---

## 3. Metadata Store: In-Memory Map with WAL Recovery

**Decision:** Use a `map[string]*FileMeta` protected by `sync.RWMutex` as the primary store. WAL replays rebuild this map on restart.

**Why in-memory:**
- O(1) lookups by path — critical for the hot path of every file operation
- No database connection pooling, no ORM, no query planning
- Raft replication (Sprint 2) will require a serializable state snapshot anyway; the in-memory map is trivially serializable

**Why NOT an embedded database (BoltDB, Badger):**
- We already have the WAL for persistence; an embedded DB would be a second persistence layer
- Adds dependency bloat
- The Namenode metadata set is bounded (tens of thousands of files per institution, not billions)

---

## 4. Chunk Storage: Flat Binary Files

**Decision:** Each chunk is stored as a single file at `<dataDir>/<chunk_id>`.

**Why flat files:**
- Chunks are content-addressed and immutable once written (delete is a separate operation)
- No fragmentation concerns — chunks are up to 64MB, allocated contiguously
- OS page cache handles read caching naturally
- Zero dependency — pure `os` + `io` stdlib
- Easy to verify integrity: `sha256sum <dataDir>/<chunk_id>`

**Why NOT an object store / key-value DB:**
- Chunks are large binary blobs, not small key-value pairs
- RocksDB/LevelDB optimize for small values; chunks make them degrade
- Adds dependency complexity for marginal benefit at Sprint 1 scale

---

## 5. gRPC Streaming: 1MB Blocks (FR-D-006)

**Decision:** Use gRPC client-side streaming for writes and server-side streaming for reads, with blocks capped at 1MB.

**Why 1MB blocks:**
- Standard gRPC message size limit is 4MB by default; 1MB provides headroom
- 1MB alignment matches filesystem block sizes and network MTU considerations
- Streaming prevents unbounded memory consumption on both client and server
- Allows progress reporting and early cancellation mid-transfer

**Why NOT single-message upload/download:**
- A 64MB chunk as a single protobuf message would allocate 64MB+ on both sides
- No opportunity for the client/server to interleave acks or cancellation during transfer
- Stream-based design is the natural gRPC idiom for large payloads

---

## 6. Namespace Isolation (FR-N-007)

**Decision:** The AuthInterceptor extracts the user's namespace from the JWT and injects it into the gRPC context. Every Namenode RPC handler calls `auth(ctx)` → `PayloadFromContext(ctx)` → `store.CheckNamespace(payload, path)` before operating.

**Why context-based injection:**
- Follows gRPC best practice: interceptors enrich context, handlers consume it
- Unexported context key type (`contextKey string`) prevents cross-package context spoofing
- No hidden global state — the handler explicitly extracts the payload per-request

**Why prefix-check on namespace:**
- Simple, deterministic, and fast (O(1) string comparison)
- Matches the POSIX filesystem model: user A in `/wourifs/audit/a/` cannot access `/wourifs/audit/b/`
- An empty namespace field means "no access" (fails-closed)

---

## 7. Replication Strategy (FR-N-008, FR-D-004)

**Decision:** The Namenode selects `RF` available Datanodes during `AllocateChunk`. The client writes to one Datanode, then the Datanode replicates to peers via `ReplicateChunk` RPC.

**Why Namenode-driven allocation:**
- Centralized decision ensures the RF constraint is met (FR-N-006: never use UNAVAILABLE nodes)
- The Namenode already has the global view of Datanode availability via the HealthMonitor
- Simpler than a distributed consensus algorithm at the Datanode layer

**Why NOT write-all-to-all from the client:**
- Pushes the network cost of RF writes to the client (which may be bandwidth-constrained)
- Breaks the single-writer abstraction — the client must manage RF connections
- The Datanode-to-Datanode replication path (ReplicateChunk) allows for future optimizations (differential sync, pipelining)

**Idempotent replication:** `ReplicateChunk` on the receiving Datanode checks `store.Exists(chunkID)` before writing. If the chunk already exists, it returns success with `BytesWritten=0`. This means replication can be safely retried without duplicating data.

---

## 8. Authentication: JWT RS256 with Optional gRPC Auth

**Decision:** The existing auth stack (JWT RS256, bcrypt, TLS 1.3) is preserved. Sprint 1 handlers call `auth(ctx)` on every request. The `cmd/namenode` can run without auth (for development/testing), and with auth when keys are provided.

**Why NOT making auth mandatory in all code paths:**
- Integration tests need to exercise handlers without full TLS+JWT setup
- Development velocity: you can `go run cmd/namenode/main.go` and test with grpcurl immediately
- Production deployment will always provide keys; the code path is there

---

## 9. UUID Generation: crypto/rand Instead of google/uuid

**Decision:** Generate UUID-like IDs using `crypto/rand` + hex encoding.

**Why NOT google/uuid:**
- The project already has 4 direct dependencies; minimizing external deps reduces supply-chain risk
- `newID()` is 5 lines of code and generates 128-bit random hex strings (effectively UUID v4)
- No version compatibility issues — `crypto/rand` is stdlib, stable since Go 1.0

---

## 10. Test Strategy: BDD + gRPC Integration

**Decision:** BDD-style `t.Run("Given... When... Then...")` for unit tests. Real gRPC servers on ephemeral ports for integration tests.

**Why BDD naming:**
- Established project convention (all existing tests use this pattern)
- Makes test failures self-documenting: `--- FAIL: TestMetadataStore_BDD/Given_a_fresh_metadata_store/When_creating_a_duplicate_file_path`
- Maps directly to FR requirements in the SRS

**Why real gRPC servers (not mocks):**
- Tests the entire gRPC stack: serialization, streaming, error codes
- Mock-based tests would miss wire-format bugs
- Ephemeral port (`127.0.0.1:0`) ensures no port conflicts
- `insecure.NewCredentials()` for test simplicity; TLS is tested separately in `internal/transport`

---

## 11. What Was NOT Done (and Why)

| Item | Reason |
|---|---|
| FUSE client | Sprint 2; requires stable Namenode/Datanode first |
| Raft consensus | Sprint 2; WAL-first approach single-instance is simpler to validate |
| Audit log (Merkle) | Sprint 2; depends on file operations being stable (FR-N-004) |
| wouri-watchd | Sprint 3; anomaly detection needs real audit event stream |
| WireGuard mesh | Sprint 2; network layer after core storage works |
| gRPC service reflection | Not needed for Sprint 1; can be added later for grpcurl discovery |
| Config YAML parser | CLI flags are simpler for now; YAML config in Sprint 2 when config surface grows |
| Docker/compose | Useful but not a Sprint 1 requirement; the Go binaries run natively |

---

## 12. File Inventory (What Changed in Sprint 1)

### New files:
```
api/proto/v1/datanode.proto          — Datanode gRPC contract
api/proto/v1/namenode.proto          — Extended with file ops + chunk allocation
api/gen/v1/datanode/                 — Generated Go code
api/gen/v1/namenode/                 — Regenerated Go code
internal/namenode/metadata.go        — In-memory path→chunks mapping
internal/namenode/server.go          — Namenode gRPC handler
internal/namenode/wal.go             — JSON-lines WAL
internal/namenode/metadata_test.go   — BDD tests for metadata + WAL
internal/namenode/server_test.go     — Integration tests for Namenode gRPC
internal/namenode/server_bdd_test.go — BDD tests for file ops with auth
internal/datanode/chunkstore.go      — Flat-file chunk storage
internal/datanode/server.go          — Datanode gRPC handler
internal/datanode/chunkstore_test.go — BDD + integration tests for Datanode
cmd/namenode/main.go                 — Namenode binary
cmd/datanode/main.go                 — Datanode binary
configs/                             — Example configs
cmd/bench/                           — Latency benchmark tool
Makefile                             — Build automation
WORKFLOW.md                          — This file
```

### Modified files:
```
api/proto/v1/namenode.proto          — Added CreateFile, DeleteFile, LookupFile, ListDirectory, AllocateChunk
internal/transport/grpc/interceptor/auth.go — Added SetPayloadInContext() test helper
```

---

## 13. Test Coverage (Sprint 1)

| Package | Coverage |
|---|---|
| `internal/auth/jwt` | 84.2% |
| `internal/datanode` | 83.8% |
| `internal/domain/namenode` | 91.5% |
| `internal/namenode` | 84.7% |
| `internal/transport` | 100.0% |
| `internal/transport/grpc` | 94.4% |
| `internal/transport/grpc/interceptor` | 93.9% |
| `pkg/crypto/bcrypt` | 87.5% |
| **Internal packages average** | **87.1%** |

---

---

## Sprint 2 — Raft HA, Merkle Audit, Shamir Dual-Control (Semaines 3-5)

### 14. Raft Consensus — Pourquoi HashiCorp Raft et pas etcd

**Decision:** Intégrer `hashicorp/raft` directement dans le processus Namenode. BoltDB pour stable store + log store. 3 nœuds, un leader élu.

**Why Raft embedded:**
- etcd serait un deuxième démon à administrer, une deuxième surface d'attaque, un deuxième langage (ni Go)
- HashiCorp Raft est le gold standard Go (Consul, Nomad) — API idiomatique, `FSM.Apply()` triviale à implémenter
- Le MetadataStore existant devient la FSM : chaque mutation passe par `raft.Apply()` avant d'être acquittée
- Snapshot/Restore intégrés pour la compaction du log

**Why BoltDB for stable store:**
- Zéro CGo, pure Go, déjà dans le module via `raft-boltdb`
- Deux fichiers BoltDB : un pour le Raft log, un pour le stable store (term, vote)
- Pas de processus externe, pas de configuration réseau

**Ce que le WAL Sprint 1 devient:**
Le WAL JSON-lines est conservé en fallback pour le mode single-node (développement). En production multi-nœud, Raft remplace le WAL : le log Raft est le WAL distribué. La `Replay()` WAL n'est appelée qu'en mode single-node.

### 15. Merkle Audit Log — Pourquoi SHA-256 et pas BLAKE3

**Decision:** SHA-256 pour l'audit chain. Chaque entrée contient `H(serialized_entry || prev_entry_hash)`.

**Why SHA-256:**
- Régulateurs financiers (COBAC, BEAC) : SHA-256 est le standard FIPS 180-4, auditable par des cabinets externes
- BLAKE3 est plus rapide mais moins reconnu dans les cercles de conformité financière
- Go stdlib `crypto/sha256` — zéro dépendance, hardware-accelerated sur x86_64 (SHA-NI)
- L'audit log est écrit une fois, lu rarement ; le débit n'est pas un facteur limitant

**Why JSON-lines for audit entries:**
- Même raison que le WAL Sprint 1 : débogabilité par un auditeur humain
- Un auditeur COBAC peut faire `cat audit.jsonl | jq .` sans outil spécialisé
- Le hash chain break est vérifiable avec un script shell de 10 lignes si nécessaire

### 16. Shamir Dual-Control — Pourquoi réseau COBAC et pas double USB

**Decision:** Architecture hybride : 1 share sur clé USB (admin IMF) + 1 share délivré par le réseau (COBAC via ShareService gRPC en mTLS).

**Why réseau COBAC plutôt que double USB:**
- Pas de logistique d'expédition de clés USB aux 400+ EMF du Cameroun
- COBAC peut révoquer instantanément (refus de servir le share) — impossible avec une clé déjà remise
- Audit complet : COBAC sait quel poste a été provisionné, quand, par qui (log du share-service)
- La garantie Shamir reste information-theoretic : intercepter le share COBAC sur le réseau ne révèle rien sans le share USB
- Le certificat mTLS du ShareService est émis par une CA externe — l'institution ne peut pas le révoquer

**Why k=2, n=2 spécifiquement:**
- n=3+ serait plus résilient mais alourdit le workflow opérationnel pour des EMF de 20-50 employés
- Deux administrateurs (directeur + responsable IT) = seuil réaliste, pas de troisième acteur disponible
- Le schéma de Shamir (1979) garantit qu'avec 1 share, aucune information sur le secret (preuve information-theoretic)

### 17. ShareService COBAC — Pourquoi un service gRPC dédié

**Decision:** `cmd/share-service` est un binaire séparé, déployé au datacenter BEAC, pas dans l'IMF.

**Why service séparé:**
- Le share-service est sous contrôle COBAC, pas sous contrôle IMF — il ne peut pas tourner sur l'infra de l'institution
- mTLS avec CA externe : le certificat serveur est signé par la PKI BEAC, pas par l'IMF
- JWT institution matching : le share-service vérifie que le JWT présenté correspond bien à l'institution demandée
- `RevokeInstitution` est dans le namespace `cobac` — seuls les auditeurs COBAC peuvent révoquer
- Le fichier `institutions.json` contient les shares, généré offline par `wouri-provision split`

### 18. Test Coverage Sprint 2

| Package | Coverage |
|---|---|
| `internal/shamir` | 94.6% |
| `internal/audit` | 85.7% |
| `internal/provision` | 52.9% |
| `internal/namenode` | 65.1% (baisse due à raft.go non testé en cluster) |

---

## Sprint 3 — FUSE Complet, Logging Structuré, Observabilité Prometheus (Semaines 5-7)

### 19. FUSE Client — Pourquoi go-fuse v2 et pas une approche custom

**Decision:** Intégrer `hanwen/go-fuse/v2` comme library FUSE. Chaque inode WouriFS est un `fs.Inode` qui traduit les syscalls POSIX en appels gRPC.

**Why go-fuse v2:**
- Mature (utilisé par gocryptfs, Borg, restic), maintenu activement
- API `fs.Inode` embedding — idiomatique Go, pas de callback spaghetti
- Supporte tous les syscalls dont on a besoin : Lookup, Create, Mkdir, Rmdir, Unlink, Rename, Getattr, Readdir, Read, Write, Flush
- Gère le cache d'inode, les file handles, et le cycle de vie du mount automatiquement
- Compatible Go 1.25 (testé avec `-race`)

**Why pas FUSE custom via /dev/fuse:**
- Réinventer le protocole FUSE = des centaines de lignes de parsing de messages kernel
- go-fuse gère les edge cases (EINTR, short writes, directory offset cookies)
- Le projet doit démontrer l'intégration POSIX, pas l'implémentation du protocole FUSE

**Pourquoi le FUSE traduit StatFile et pas un cache local:**
- Cohérence forte : le FUSE n'a pas de cache local, chaque `getattr` appelle `StatFile` gRPC
- Pour une IMF typique (dizaines de postes, pas des milliers), la latence gRPC intra-datacenter est <1ms
- Évite les problèmes de staleness qui pourraient causer des écritures concurrentes non détectées
- La cohérence forte est exigée par le use case financier

### 20. Logging Structuré — Pourquoi slog et pas zap/zerolog

**Decision:** `log/slog` (stdlib Go 1.21+) avec sortie JSON. Pas de dépendance externe de logging.

**Why slog:**
- Stdlib depuis Go 1.21 — zéro dépendance, zéro breaking change futur
- JSON handler natif : intégrable avec Loki, ELK, ou `jq` directement
- Structured logging avec paires clé-valeur : `slog.Info("op_end", "op", "CreateFile", "duration_ms", 12)`
- Suffisant pour les besoins du projet (pas de sampling, pas de async I/O nécessaire)
- zap/zerolog sont plus rapides mais ajoutent une dépendance pour un gain non mesurable à notre échelle

**Convention de log:**
- `op_start` / `op_end` pour chaque opération gRPC — permet de calculer la latence côté serveur
- `op_error` avec le message d'erreur complet pour le debugging
- `datanode_registered`, `heartbeat_unknown_node` pour les événements de cycle de vie
- `wal_append_failed` en warn (non-fatal : le WAL est un best-effort en mode single-node)

### 21. Métriques Prometheus — Pourquoi exposition format texte et pas OpenTelemetry

**Decision:** Export Prometheus en exposition format (text/plain) sur `:9100/metrics`. Pas de SDK Prometheus, pas d'OpenTelemetry.

**Why exposition format simple:**
- Prometheus attend du texte `key{labels} value\n` sur HTTP — c'est 50 lignes de code
- Pas de `prometheus/client_golang` qui ajouterait 10+ dépendances transitives
- L'IMF peut scraper avec un Prometheus standard, Grafana lit le format natif
- OpenTelemetry serait overkill : on n'a pas besoin de tracing distribué, juste de métriques

**Métriques exportées:**
- `wourifs_op_total{op,status}` — compteur par opération et statut (ok/error)
- `wourifs_ops_ok{op}` / `wourifs_ops_errors{op}` — succès/échecs
- `wourifs_op_duration_seconds_count{op}` / `_sum{op}` — latence
- `wourifs_op_bytes_total{op}` — volume de données transférées
- `wourifs_datanodes_available` — gauge du nombre de Datanodes actifs
- `wourifs_chunk_bytes_used` / `wourifs_chunk_bytes_total` — utilisation stockage

### 22. Dashboard Grafana — Pourquoi 6 panneaux et pas plus

**Decision:** Un dashboard JSON de 6 panneaux couvrant les indicateurs essentiels.

**Pourquoi ces 6 panneaux spécifiquement:**
1. **Ops/sec** — débit : l'IMF peut voir si le système ralentit
2. **p99 Latence** — le percentile 99 capture les outliers (timeout réseau, GC pause)
3. **Bytes/sec** — bande passante : détecte les goulots d'étranglement réseau
4. **Error Rate** — taux d'erreur : alerte précoce avant panne visible
5. **Active Datanodes** — si ça tombe à <3, le RF n'est plus respecté
6. **Chunk Storage Usage** — jauge de capacité : planifier l'expansion avant saturation

**Pourquoi pas plus de panneaux:**
- Une IMF n'a pas d'équipe SRE — 6 panneaux, c'est lisible par un responsable IT non spécialiste
- Les panneaux supplémentaires (cache hit rate, GC pause, goroutine count) sont du debugging développeur, pas de l'opération IMF

### 23. Métadonnées étendues — Pourquoi Mode/Mtime/Ctime dans FileMeta

**Decision:** Ajouter `Mode uint32`, `Mtime time.Time`, `Ctime time.Time` à `FileMeta`. Les répertoires sont stockés comme des entrées avec `IsDir: true`.

**Pourquoi des entrées répertoire explicites:**
- `ListDirectory` peut itérer sur les enfants sans inférer la structure depuis les paths
- `IsDir` explicite évite les heuristiques "si un path a des enfants c'est un répertoire"
- `MakeDir` / `RemoveDir` sont atomiques : une entrée = un répertoire
- Compatible avec FUSE qui attend des inodes distincts pour les répertoires

**Pourquoi Mode/Mtime/Ctime:**
- FUSE a besoin de ces attributs pour `getattr` — sans eux, `ls -la` affiche `?` pour les permissions et dates
- Mode POSIX permet le contrôle d'accès basique (rwx) même sans ACLs
- Mtime/Ctime sont exigés par les apps comptables qui vérifient les dates de modification

### 24. Test Coverage Sprint 3

| Package | Coverage |
|---|---|
| `internal/observability` | 76.5% |
| `internal/audit` | 85.7% |
| `internal/namenode` | 47.5% (baisse : nouveaux handlers non testés exhaustivement) |
| `internal/datanode` | 76.5% |
| `internal/shamir` | 94.6% |
| **Internal packages average** | **66.0%** |

**Note sur la baisse de coverage:** Les nouveaux handlers (MakeDirectory, RemoveDirectory, RenameFile, StatFile, TruncateFile) ajoutent ~200 lignes dans server.go. Les tests BDD existants couvrent le chemin nominal mais pas tous les edge cases (namespace violation, directory not empty, etc.). La couverture remontera avec les tests d'intégration FUSE du Sprint 4.

---

## Sprint 3 — Suite (July 2026)

### 25. AES-256-GCM Chunk Encryption — Pourquoi HKDF et pas PBKDF2

**Decision:** Chaque chunk est chiffré avec AES-256-GCM. La clé par chunk est dérivée via HKDF-SHA256(master_key, chunk_id || namespace_id). Le master key est scellé via XOR-deux-shares (Shamir SS k=2, n=2) et n'existe qu'en mémoire.

**Pourquoi HKDF et pas PBKDF2:**
- PBKDF2 est conçu pour les mots de passe (slow hash). HKDF est conçu pour la dérivation de clés cryptographiques (fast, deterministic).
- HKDF-SHA256 produit exactement 32 bytes, parfait pour AES-256.
- RFC 5869, mature et audité.

**Pourquoi AES-256-GCM et pas ChaCha20-Poly1305:**
- AES-256-GCM est accéléré matériellement sur tous les CPUs x86-64 modernes (AES-NI).
- Les serveurs Linux en entreprise ont AES-NI depuis 2010. Les postes de microfinance utilisent du matériel récent.
- ChaCha20 est plus rapide sans AES-NI, mais le cas d'usage (serveurs x86-64) favorise AES-GCM.

**Pourquoi sceller en mémoire plutôt que sur disque:**
- Le disque d'un Datanode est la surface d'attaque. Si un disque est volé, les chunks sont chiffrés.
- Si le master key était sur disque, le vol du disque Namenode compromettrait tous les chunks.
- En mémoire seulement, un redémarrage nécessite les deux parts Shamir. Sans elles, les données sont inaccessibles — c'est le comportement voulu.

### 26. wouri-watchd — Pourquoi un daemon Go et pas Prometheus AlertManager

**Decision:** wouri-watchd est un daemon Go standalone consommant le stream d'audit. Il n'utilise pas AlertManager.

**Pourquoi pas AlertManager:**
- AlertManager opère sur des métriques Prometheus (séries temporelles). Les anomalies WouriFS sont événementielles (un write hors heures ouvrées, un changement de leader Raft).
- Transformer des événements d'audit en métriques Prometheus pour qu'AlertManager les transforme en alertes = deux translations inutiles.
- wouri-watchd peut dispatcher directement vers webhook/email/SMS sans infrastructure Prometheus.

**Architecture:**
- watchd s'abonne au stream gRPC AuditLogService.
- Chaque entrée d'audit est évaluée contre des RuleFn enregistrées.
- Une règle qui "fire" déclenche un dispatch vers tous les DispatcherFn enregistrés.
- Le dispatch est fire-and-forget — un échec de notification ne bloque pas le traitement.

### 27. Distributed Tracing — Pourquoi W3C Trace Context et pas OpenTelemetry SDK

**Decision:** Implémentation manuelle du W3C Trace Context (traceparent/tracestate) propagé via gRPC metadata. Pas d'import du SDK OpenTelemetry.

**Pourquoi pas le SDK OTEL:**
- Le SDK OTEL Go ajoute ~15 dépendances transitives (grpc, protobuf, prometheus, etc.) qui dupliquent celles déjà dans WouriFS.
- Le SDK introduit un modèle de threading (BatchSpanProcessor, exporteur OTLP) complexe à configurer correctement.
- Pour un BSc avec 2 mois restants, l'investissement dans l'intégration SDK n'est pas justifié. Le format W3C Trace Context est le standard — n'importe quel collector (Jaeger, Grafana Tempo) l'accepte.

**Ce qui est implémenté:**
- Création de spans avec TraceID/SpanID/ParentID.
- Propagation automatique via gRPC metadata (traceparent header).
- Interceptor gRPC Unary et Stream.
- Export OTLP configurable (fail-open: un échec d'export ne bloque jamais une opération).
- Attributs standards : rpc.method, rpc.service, user.id, chunk.id.

### 28. Test Coverage Sprint 3 — Final

| Package | Coverage |
|---|---|
| `internal/namenode` | 85.7% (+38.2%) |
| `internal/datanode` | 85.2% (+8.7%) |
| `internal/keyring` | 90.0% (new) |
| `internal/watchd` | 96.8% (new) |
| `internal/observability` | 82.3% (+5.8%) |
| `internal/provision` | 92.9% (+40.0%) |
| `internal/audit` | 85.7% |
| `internal/auth/jwt` | 84.2% |
| `internal/shamir` | 94.6% |
| `internal/transport` | 100% |
| `internal/transport/grpc` | 94.4% |
| `internal/transport/grpc/interceptor` | 93.9% |
| **All internal packages** | **≥82%** |

### 29. Unified CLI — Pourquoi Cobra et pas urfave/cli

**Decision:** `wourifs` est un binaire unique avec 7 sous-commandes, construit avec `spf13/cobra`.

**Pourquoi Cobra:**
- Génération automatique de completion shell (bash, zsh, fish).
- Structure commande/sous-commande naturelle : `wourifs serve namenode`, `wourifs provision split`.
- Flags persistants (`--config`) hérités automatiquement par toutes les sous-commandes.
- Standard de facto dans l'écosystème Go (kubectl, helm, hugo).

**Pourquoi pas urfave/cli:**
- urfave/cli impose un modèle `app.Action` moins naturel pour les sous-commandes imbriquées.
- Pas de génération de completion native (nécessite un package externe).
- La philosophie "flags before args" de urfave est contre-intuitive pour les utilisateurs UNIX.

### 30. Deployment Model — Pourquoi Zero-Server et pas Client-Serveur

**Decision:** Chaque poste de travail exécute `wourifs serve datanode`. Un ou deux postes au siège ajoutent `wourifs serve namenode`. Aucun serveur dédié.

**Pourquoi:**
- Les EMF Catégorie 1/2 n'ont pas de salle serveur, pas de climatisation, pas de rack.
- Acheter un "vrai serveur" (Dell PowerEdge, HP ProLiant) coûte 2-5 millions CFA — le budget IT annuel d'une petite EMF.
- Un poste de travail standard (Core i3, 8GB RAM, 500GB disque) peut exécuter Datanode + Namenode sans problème.
- La redondance est assurée par la réplication (RF=3) entre postes, pas par du matériel redondant.

**Déploiement type (3 agences):**
```
Siège (2 postes):        namenode+datanode, datanode+gateway
Agence A (1 poste):      datanode
Agence B (1 poste):      datanode
```
Chaque poste alloue X Go via `wourifs init --quota`. Total cluster = somme des quotas.

**Résilience:** Si un poste tombe, les chunks sont répliqués ailleurs (RF=3). Si les 2 postes du siège tombent, le cluster perd le Namenode (Raft leader) mais les données Datanode survivent. Un redémarrage sur n'importe quel poste avec les parts Shamir restaure le service.

---

## Sprint 4 — Raft HA + Observabilité (July 2026)

### 31. Raft HA Integration — Pourquoi walAppend et pas refactoring complet

**Decision:** En mode Raft (`--raft`), `walAppend` soumet l'entrée au log Raft en plus de l'écrire dans le WAL local. Les mutations sont appliquées directement au store du leader, puis répliquées aux followers via `raft.Apply()`. Le FSM ré-applique les mutations (opérations idempotentes).

**Pourquoi cette approche et pas un refactoring complet:**
- Refactoring complet : remplacer chaque `s.store.CreateFile()` par `s.raftSubmit()` → ~200 lignes de changements, risque élevé de régressions.
- Approche walAppend : 5 lignes dans walAppend, 0 changement dans les handlers. Les mutations sont idempotentes par conception (PutFile overwrites, DeleteFile sur un fichier déjà supprimé est un no-op).
- Le WAL local reste actif même en mode Raft — double garantie de durabilité.

**Pourquoi pas de multi-node dans le test d'intégration:**
- Le protocole `AddVoter` de HashiCorp Raft nécessite un transport TCP entre nœuds avec découverte de pairs.
- Le test single-node vérifie : élection de leader, Apply via Raft, restart. Le multi-node sera testé manuellement sur les VMs.

### 32. Passthrough Auth pour le développement — Pourquoi DevNoAuthInterceptor

**Decision:** `DevNoAuthInterceptor` injecte un payload `{UserID: "dev-user", Namespace: "/"}` dans chaque requête. Activé via `--dev-no-auth`.

**Pourquoi:**
- Tester FUSE + Namenode sans infrastructure JWT (génération de clés RSA, distribution de tokens).
- Les handlers appellent tous `s.auth(ctx)` → sans interceptor, toute opération échoue.
- Le flag `--dev-no-auth` est explicite et documenté comme "DEVELOPMENT ONLY".

### 33. Gateway Design — Pourquoi HTTP direct et pas reverse proxy gRPC

**Decision:** Le Gateway est un serveur HTTP standalone sur port 8443. Il sert le dashboard HTML et expose une API REST admin. Il traduit les requêtes en appels gRPC vers le Namenode.

**Pourquoi pas gRPC-Gateway / grpc-gateway:**
- grpc-gateway génère un reverse proxy à partir des annotations proto. Cela nécessite de modifier les .proto, régénérer le code, et maintenir deux jeux d'endpoints.
- Pour 3 endpoints admin (topology, users, audit), un serveur HTTP de 100 lignes suffit.
- Le dashboard HTML est embeddé directement — pas de build step, pas de node_modules.

### 34. Grafana Dashboard — Pourquoi avg latency et pas p99

**Decision:** Le panneau latency utilise `rate(_sum)/rate(_count)` (moyenne) et non `histogram_quantile(0.99, rate(_bucket[1m]))` (p99).

**Pourquoi:**
- L'implémentation metrics.go exporte `_count` et `_sum` mais pas `_bucket`. Les buckets histogram nécessitent de définir des bornes (1ms, 5ms, 10ms, ...) — un choix arbitraire sans données de production.
- La moyenne est suffisante pour le monitoring opérationnel d'une EMF (3-5 nœuds, faible volume).
- Les buckets histogram seront ajoutés quand le cluster sera en production avec des données réelles.

---

## Sprint 5 — Snapshots, Cert Rotation, E2E Tests (July 2026)

### 35. Metadata Snapshots — Pourquoi JSON et pas binaire

**Decision:** Les snapshots du MetadataStore sont sérialisés en JSON et stockés dans un répertoire configurable. Retention configurable (défaut: 30 jours). Pruning automatique.

**Pourquoi JSON et pas protobuf/gob:**
- Cohérence avec le WAL (JSON-lines) et l'audit log (JSON-lines). Même outillage de debug.
- Le volume de métadonnées est faible (< 10 MB pour 100k fichiers). La performance de sérialisation n'est pas un facteur.
- JSON est lisible par un humain — un admin peut inspecter un snapshot avec `cat` ou `jq`.

**Pourquoi pas de snapshot incrémental:**
- La taille des snapshots est négligeable. Un snapshot complet prend < 1 seconde.
- Les snapshots incrémentaux ajoutent une complexité de restauration (replay du snapshot de base + deltas).

### 36. Certificate Rotation — Pourquoi script bash et pas intégré dans le binaire

**Decision:** `scripts/rotate-certs.sh` est un script bash externe. Il n'est pas intégré dans le binaire Go.

**Pourquoi:**
- La rotation de certificats est une opération d'infrastructure, pas une opération applicative. L'admin qui gère les certificats est familier avec openssl et bash.
- Intégrer openssl dans le binaire Go nécessiterait soit des bindings C (cgo), soit une réimplémentation en Go pur (`crypto/x509`). Les deux sont plus lourds qu'un script de 30 lignes.
- Le script est idempotent : il archive les anciens certificats avant d'en générer de nouveaux.

### 37. E2E Audit Integrity Test — Pourquoi corruption de fichier et pas mock

**Decision:** `TestAuditLog_E2EIntegrity` écrit 10 entrées dans une vraie chaîne, vérifie, corrompt un byte sur disque, et vérifie que la corruption est détectée.

**Pourquoi:**
- La corruption de fichier est le vecteur d'attaque réel (modification directe du fichier audit.log par un admin).
- Tester avec une vraie corruption de fichier vérifie l'intégralité du pipeline : sérialisation JSON → SHA-256 → écriture disque → relecture → vérification chaîne.
- Un mock ne testerait que la logique de vérification, pas le comportement face à une vraie altération de fichier.

### 38. Snapshot Manager — Pourquoi polling et pas signal

**Decision:** Le `snapshot.Manager` utilise un ticker (`time.NewTicker`) pour déclencher les snapshots périodiques.

**Pourquoi pas un signal du Namenode:**
- Le Namenode n'a pas de hook "nombre d'opérations depuis dernier snapshot". Ajouter ce hook couplerait le snapshot manager au Namenode.
- Le polling est plus simple et plus robuste : même si le Namenode est occupé, le snapshot finit par être pris.
- La période par défaut (24h) rend la précision du timing non critique.

### 39. Test Coverage Sprint 4-5 — Final

| Package | Coverage |
|---|---|
| `internal/namenode` | 86.7% |
| `internal/datanode` | 83.8% |
| `internal/audit` | 85.7% |
| `internal/keyring` | 90.0% |
| `internal/watchd` | 95.2% |
| `internal/observability` | 82.3% |
| `internal/provision` | 94.1% |
| `internal/shamir` | 94.6% |
| `internal/snapshot` | 90.5% |
| `internal/transport` | 100% |
| `internal/transport/grpc` | 94.4% |
| `internal/transport/grpc/interceptor` | 86.1% |
| `internal/domain/namenode` | 90.7% |
| `internal/auth/jwt` | 84.2% |
| `pkg/crypto/bcrypt` | 87.5% |
| **All internal packages** | **≥82%** |

### 40. CLI Completeness — Zéro placeholder

**Decision:** Toutes les commandes `wourifs serve` sont câblées sur du vrai code. Aucun `select {}` résiduel.

- `wourifs serve namenode` → gRPC Namenode avec WAL + Raft optionnel
- `wourifs serve datanode` → gRPC Datanode avec heartbeat loop
- `wourifs serve gateway` → HTTP dashboard sur port 8443
- `wourifs mount` → FUSE via go-fuse v2
- `wourifs status` → affichage config + (future: requête gRPC live)
- `wourifs provision split/combine` → Shamir via CLI (binaire standalone toujours disponible)
- `wourifs audit log/verify` → (future: requête gRPC AuditLogService)

---

*Document prepared for Sprint 1-5 completion — WouriFS BSc Project, ICT University 2026*

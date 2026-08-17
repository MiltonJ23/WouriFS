package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/MiltonJ23/WouriFS/api/gen/v1/namenode"
	"github.com/MiltonJ23/WouriFS/internal/observability"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

/*
 * Gateway — admin dashboard and REST API server.
 *
 * Serves the embedded dashboard HTML and exposes a small admin API.
 * Translates browser requests into gRPC calls to the Namenode cluster.
 * Branch employees never interact with this — they use the FUSE mount.
 */
func serveGatewayCmd() *cobra.Command {
	var nnAddr string

	cmd := &cobra.Command{
		Use:   "gateway",
		Short: "Start the Gateway admin dashboard server",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := LoadConfig(configPath)
			if err != nil {
				return err
			}
			if !cfg.HasRole("gateway") {
				return fmt.Errorf("node %q missing role 'gateway'", cfg.Node.ID)
			}

			pipeline, obsShutdown := setupObservability(cfg, "gateway")
			defer obsShutdown()
			gwLogger := observability.NewLogger(slog.LevelInfo)
			if pipeline != nil {
				gwLogger = observability.NewLoggerWithOtel(slog.LevelInfo, pipeline.LoggerProvider)
			}

			addr := fmt.Sprintf("127.0.0.1:%d", cfg.Network.NamenodePort)
			if nnAddr != "" {
				addr = nnAddr
			}
			if len(cfg.Network.NamenodePeers) > 0 {
				addr = cfg.Network.NamenodePeers[0]
			}

			mux := http.NewServeMux()

			// Dashboard HTML
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/" {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				w.Header().Set("X-Content-Type-Options", "nosniff")
				w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'")
				w.Write(dashboardHTML)
			})

			// Admin API — topology
			mux.HandleFunc("/api/v1/admin/topology", func(w http.ResponseWriter, r *http.Request) {
				conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					http.Error(w, "gateway unreachable", http.StatusBadGateway)
					return
				}
				defer conn.Close()

				client := pb.NewNameNodeServiceClient(conn)
				rctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
				defer cancel()

				resp, err := client.ListDirectory(rctx, &pb.ListDirectoryRequest{})
				if err != nil {
					http.Error(w, "namenode error", http.StatusBadGateway)
					return
				}

				// For now, return registered datanodes from the store.
				// The proper ListNodes RPC will be available after proto regeneration.
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{
					"nodes":        []interface{}{},
					"entries":      len(resp.Entries),
					"namenode":     addr,
					"gateway_node": cfg.Node.ID,
				})
			})

			httpSrv := &http.Server{Addr: fmt.Sprintf(":%d", cfg.Network.GatewayPort), Handler: mux}

			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			go func() {
				<-sigCh
				log.Println("[gateway] shutting down...")
				httpSrv.Shutdown(context.Background())
			}()

			log.Printf("[gateway] %s listening on :%d (namenode=%s)", cfg.Node.ID, cfg.Network.GatewayPort, addr)
			gwLogger.Info("gateway_started", "node", cfg.Node.ID, "port", cfg.Network.GatewayPort, "namenode", addr)
			if err := httpSrv.ListenAndServe(); err != http.ErrServerClosed {
				return err
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&nnAddr, "namenode", "n", "", "override Namenode address (host:port)")
	return cmd
}

// dashboardHTML is the embedded admin SPA. In production this is read from
// web/dashboard.html via embed.FS; the standalone file is embedded here
// as a fallback for development.
var dashboardHTML = []byte(`<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>WouriFS — Administration</title>
<style>
:root{--green:#22c55e;--red:#ef4444;--bg:#0f172a;--card:#1e293b;--text:#e2e8f0;--muted:#94a3b8}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;padding:24px}
h1{font-size:1.5rem;margin-bottom:4px}.sub{color:var(--muted);font-size:.875rem;margin-bottom:24px}
nav{display:flex;gap:16px;margin-bottom:24px;border-bottom:1px solid #334155;padding-bottom:12px}
nav a{color:var(--muted);text-decoration:none;padding:8px 0;border-bottom:2px solid transparent;font-weight:600}
nav a.active,nav a:hover{color:var(--text);border-bottom-color:var(--green)}
section{display:none}section.active{display:block}
.nodes{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:16px}
.card{background:var(--card);border-radius:12px;padding:20px;border:1px solid #334155}
.card-header{display:flex;align-items:center;gap:10px;margin-bottom:12px}
.dot{width:12px;height:12px;border-radius:50%}.dot.online{background:var(--green);box-shadow:0 0 8px var(--green)}.dot.offline{background:var(--red)}
.node-id{font-weight:700;font-size:1.1rem}.roles{display:flex;gap:6px;margin-bottom:12px;flex-wrap:wrap}
.role{background:#334155;padding:2px 10px;border-radius:999px;font-size:.75rem;font-weight:600;text-transform:uppercase}
.info{display:flex;flex-direction:column;gap:6px;font-size:.85rem;color:var(--muted)}
.info span{font-family:monospace;color:var(--text)}
.storage-label{display:flex;justify-content:space-between;margin-top:10px;font-size:.8rem}
.bar{height:6px;background:#334155;border-radius:3px;margin-top:4px;overflow:hidden}
.bar-fill{height:100%;background:var(--green);border-radius:3px;transition:width .5s}
.bar-fill.warn{background:#eab308}.bar-fill.critical{background:var(--red)}
.status-line{font-size:.75rem;margin-top:10px;display:flex;align-items:center;gap:6px}
.badge{padding:2px 8px;border-radius:4px;font-weight:600;font-size:.7rem;text-transform:uppercase}
.badge.online{background:#14532d;color:var(--green)}.badge.offline{background:#7f1d1d;color:var(--red)}
th{text-align:left;color:var(--muted);font-weight:600;padding:8px 12px;border-bottom:1px solid #334155}
td{padding:8px 12px;border-bottom:1px solid #1e293b;font-size:.85rem;font-family:monospace}
</style>
</head>
<body>
<h1>WouriFS — Administration</h1>
<div class="sub">Cluster oversight &middot; Audit inspection &middot; User provisioning</div>
<nav>
<a href="#topology" class="active" onclick="showTab('topology')">Topologie</a>
<a href="#audit" onclick="showTab('audit');loadAudit()">Audit Log</a>
</nav>
<section id="topology" class="active"><div class="nodes" id="nodes"><div style="color:var(--muted);padding:40px">Chargement...</div></div></section>
<section id="audit"><div id="auditTable"></div></section>
<script>
function showTab(t){document.querySelectorAll('section').forEach(s=>s.classList.remove('active'));document.getElementById(t).classList.add('active');document.querySelectorAll('nav a').forEach(a=>a.classList.remove('active'));document.querySelector('nav a[href="#'+t+'"]').classList.add('active')}
async function loadTopology(){
 try{
  const r=await fetch('/api/v1/admin/topology');const d=await r.json();
  document.getElementById('nodes').innerHTML='<div class="sub">Namenode: '+d.namenode+' &middot; Entries: '+d.entries+'</div>'
 }catch(e){document.getElementById('nodes').innerHTML='<div style="color:var(--red);padding:40px">Gateway unreachable</div>'}
}
function loadAudit(){document.getElementById('auditTable').innerHTML='<div style="color:var(--muted);padding:40px">Audit log viewer — connecter au Namenode pour charger</div>'}
loadTopology();setInterval(loadTopology,10000);
</script>
</body>
</html>`)

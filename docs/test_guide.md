# WouriFS — Guide de Tests et Validation

Document de référence pour la validation fonctionnelle de WouriFS v3.0.
Chaque test inclut la commande exacte et le résultat attendu.

---

## 1. Préparation des VMs

### 1.1 Configuration cible

| Machine | Rôle | IP Tailscale | CPU | RAM | Disque alloué |
|---|---|---|---|---|---|
| VM1 (siège) | namenode + datanode + gateway | 100.x.y.1 | 2 cores | 2 GB | 10 GB |
| VM2 (agence A) | datanode | 100.x.y.2 | 1 core | 1 GB | 5 GB |
| VM3 (agence B, optionnel) | datanode | 100.x.y.3 | 1 core | 1 GB | 5 GB |

### 1.2 Installation — VM1 (siège)

```bash
# Prérequis
sudo apt update && sudo apt install -y golang-go git libfuse3-dev curl

# Cloner
git clone https://github.com/MiltonJ23/WouriFS.git
cd WouriFS
go build ./cmd/wourifs/

# ☑ Check 1 — le binaire existe
ls -lh ./wourifs
# → -rwxr-xr-x ... ~25M  ./wourifs

# ☑ Check 2 — le CLI répond
./wourifs --help
# → Affiche les commandes: init, serve, mount, provision, status, audit

# Init
./wourifs init -f
#   Node ID: siege-1
#   Roles: namenode,datanode,gateway
#   Quota: 10
#   Network: tailscale (ou lan si VMs en bridge)

# ☑ Check 3 — le fichier de config existe
cat /etc/wourifs/wourifs.yaml
# → Affiche la configuration YAML
```

### 1.3 Installation — VM2 (agence A)

```bash
# Mêmes prérequis + clone + build

./wourifs init -f
#   Node ID: agence-a
#   Roles: datanode
#   Quota: 5
#   Network: tailscale (ou lan)
#   → Éditer /etc/wourifs/wourifs.yaml :
#     network.namenode_peers: ["100.x.y.1:9000"]
```

### 1.4 Installation — VM3 (agence B, optionnel)

```bash
# Identique à VM2
#   Node ID: agence-b
#   network.namenode_peers: ["100.x.y.1:9000"]
```

---

## 2. Infrastructure Checks

### 2.1 Démarrage du cluster

```bash
# === VM1 ===
./wourifs serve namenode --dev-no-auth &
# → [namenode] siege-1 listening on 0.0.0.0:9000 (rf=3, mode=standalone)

# ☑ Check 4 — le namenode écoute
curl -s http://localhost:9102/metrics 2>/dev/null | grep wourifs || echo "(metrics port pas encore bindé — normal, le serveur metrics est sur un port séparé)"

./wourifs serve datanode &
# → [datanode] siege-1 listening on 0.0.0.0:9100

./wourifs serve gateway &
# → [gateway] siege-1 listening on :8443

# === VM2 ===
./wourifs serve datanode &
# → [datanode] agence-a listening on 0.0.0.0:9100
# → datanode_registered id=agence-a

# === VM3 (optionnel) ===
./wourifs serve datanode &
# → [datanode] agence-b listening on 0.0.0.0:9100
# → datanode_registered id=agence-b
```

### 2.2 Check — Dashboard admin

```bash
# Depuis ton Mac, navigateur :
open http://100.x.y.1:8443
# → Page "WouriFS — Administration" avec onglets Topologie et Audit Log
```

### 2.3 Check — Heartbeat

```bash
# Sur VM1, laisser tourner 30 secondes, puis vérifier :
# Le namenode log doit montrer des heartbeats réguliers
# (vérifier via les logs du terminal namenode)
```

---

## 3. Tests FUSE — Opérations POSIX

### 3.1 Montage

```bash
# === VM2 (ou VM1 si libfuse3 installée) ===
sudo mkdir -p /mnt/wourifs
sudo ./wourifs mount -n 127.0.0.1:9000 /mnt/wourifs &
# → WouriFS mounted at /mnt/wourifs (namenode 127.0.0.1:9000)

# ☑ Check 7 — le montage est actif
mount | grep wourifs
# → wourifs on /mnt/wourifs type fuse.wourifs (rw, ...)
```

### 3.2 Opérations de base

```bash
# ☑ Test 1 — Écrire un fichier
echo "Bonjour WouriFS" | sudo tee /mnt/wourifs/hello.txt
# → Bonjour WouriFS

# ☑ Test 2 — Lire un fichier
cat /mnt/wourifs/hello.txt
# → Bonjour WouriFS

# ☑ Test 3 — Taille du fichier
ls -la /mnt/wourifs/hello.txt
# → -rw-r--r-- 1 root root 15 ... /mnt/wourifs/hello.txt

# ☑ Test 4 — Créer un répertoire
sudo mkdir /mnt/wourifs/clients
ls -ld /mnt/wourifs/clients
# → drwxr-xr-x ... /mnt/wourifs/clients

# ☑ Test 5 — Créer un fichier dans un sous-répertoire
echo "Dupont,50000" | sudo tee /mnt/wourifs/clients/registre.csv
cat /mnt/wourifs/clients/registre.csv
# → Dupont,50000

# ☑ Test 6 — Renommer
sudo mv /mnt/wourifs/hello.txt /mnt/wourifs/bonjour.txt
cat /mnt/wourifs/bonjour.txt
# → Bonjour WouriFS
ls /mnt/wourifs/hello.txt 2>&1
# → No such file or directory

# ☑ Test 7 — Supprimer un fichier
sudo rm /mnt/wourifs/bonjour.txt
ls /mnt/wourifs/bonjour.txt 2>&1
# → No such file or directory

# ☑ Test 8 — Supprimer un répertoire vide
sudo mkdir /mnt/wourifs/tempdir
sudo rmdir /mnt/wourifs/tempdir
ls /mnt/wourifs/tempdir 2>&1
# → No such file or directory

# ☑ Test 9 — Truncate (vider un fichier)
echo "données à effacer" | sudo tee /mnt/wourifs/vidage.txt
sudo truncate -s 0 /mnt/wourifs/vidage.txt
cat /mnt/wourifs/vidage.txt
# → (rien)
ls -la /mnt/wourifs/vidage.txt
# → -rw-r--r-- 1 root root 0 ... /mnt/wourifs/vidage.txt

# ☑ Test 10 — Arborescence complète
sudo mkdir -p /mnt/wourifs/agence-a/clients
sudo mkdir /mnt/wourifs/agence-a/comptes
sudo mkdir /mnt/wourifs/agence-b/clients
echo "Martin,120000" | sudo tee /mnt/wourifs/agence-a/clients/jean.csv
echo "Dubois,80000"  | sudo tee /mnt/wourifs/agence-b/clients/marie.csv
ls -R /mnt/wourifs/
# → Affiche l'arborescence complète avec tous les fichiers
```

---

## 4. Tests Métier — Scénarios EMF Réels

### 4.1 Scénario — Gestion des clients

```bash
# L'employé de l'agence A crée le registre clients
cat <<'EOF' | sudo tee /mnt/wourifs/agence-a/clients/registre.csv
ID,Nom,Prenom,DateNaissance,Telephone,Adresse,DateInscription
001,Dupont,Jean,1985-03-12,699887766,Yaoundé,2026-01-15
002,Martin,Marie,1990-07-22,677112233,Douala,2026-01-20
EOF

# ☑ Check — le fichier est visible depuis le siège (VM1)
ssh VM1 "cat /mnt/wourifs/agence-a/clients/registre.csv"
# → Même contenu

# ☑ Check — ajout d'un nouveau client
echo "003,Ngono,Paul,1988-11-05,655443322,Bafoussam,2026-02-01" | \
  sudo tee -a /mnt/wourifs/agence-a/clients/registre.csv
wc -l /mnt/wourifs/agence-a/clients/registre.csv
# → 4 (3 clients + 1 en-tête)
```

### 4.2 Scénario — Comptes épargne

```bash
# Créer un fichier de suivi des comptes
cat <<'EOF' | sudo tee /mnt/wourifs/agence-a/comptes/epargne.csv
IDClient,NumeroCompte,Solde,DateOuverture
001,EP-001-2026,150000,2026-01-15
002,EP-002-2026,250000,2026-01-20
EOF

# ☑ Test — mise à jour d'un solde (dépôt)
# Simulé : l'employé ouvre le CSV, le modifie dans Excel, sauvegarde
# En ligne de commande :
sudo sed -i 's/001,EP-001-2026,150000/001,EP-001-2026,200000/' \
  /mnt/wourifs/agence-a/comptes/epargne.csv
grep "001" /mnt/wourifs/agence-a/comptes/epargne.csv
# → 001,EP-001-2026,200000,2026-01-15

# ☑ Check — le siège voit la mise à jour
ssh VM1 "grep '001' /mnt/wourifs/agence-a/comptes/epargne.csv"
# → 001,EP-001-2026,200000,2026-01-15
```

### 4.3 Scénario — Crédits

```bash
sudo mkdir /mnt/wourifs/agence-a/credits
cat <<'EOF' | sudo tee /mnt/wourifs/agence-a/credits/portefeuille.csv
IDCredit,IDClient,Montant,Taux,DureeMois,Mensualite,ResteDu,Statut
CR-001,001,500000,12,12,47073,500000,Actif
CR-002,002,1000000,10,24,45685,850000,Actif
EOF

# ☑ Test — enregistrement d'un remboursement
sudo sed -i 's/CR-001,001,500000,12,12,47073,500000,Actif/CR-001,001,500000,12,12,47073,452927,Actif/' \
  /mnt/wourifs/agence-a/credits/portefeuille.csv
grep "CR-001" /mnt/wourifs/agence-a/credits/portefeuille.csv
# → ResteDu=452927 (une mensualité payée)
```

### 4.4 Scénario — Volume (écriture massive)

```bash
# ☑ Test — fichier de 50 MB
dd if=/dev/urandom of=/mnt/wourifs/gros_fichier.bin bs=1M count=50 2>&1
# → 50+0 records in / 50+0 records out / 52428800 bytes
ls -lh /mnt/wourifs/gros_fichier.bin
# → -rw-r--r-- 1 root root 50M ... /mnt/wourifs/gros_fichier.bin

# ☑ Test — 100 petits fichiers (simule une journée de travail)
for i in $(seq 1 100); do
  echo "Transaction $i: OK" | sudo tee /mnt/wourifs/agence-a/comptes/trans_$(date +%s)_$i.csv > /dev/null
done
ls /mnt/wourifs/agence-a/comptes/ | wc -l
# → ~102 (epargne.csv + 100 transactions)
```

---

## 5. Tests d'Audit et d'Intégrité

### 5.1 Vérification de la chaîne d'audit

```bash
# ☑ Test — le journal d'audit existe
ls -lh /var/lib/wourifs/siege-1/namenode/audit.jsonl 2>/dev/null || \
  echo "(le chemin exact dépend de la config — vérifier le data_dir dans wourifs.yaml)"

# ☑ Test — vérifier l'intégrité de la chaîne
./wourifs audit verify
# → (nécessite l'accès au fichier audit — actuellement le endpoint gRPC n'est pas exposé par le CLI)
# Alternative : vérifier directement le fichier
cat /var/lib/wourifs/siege-1/namenode/audit.jsonl | wc -l
# → Nombre d'entrées d'audit (> 0 si des opérations ont été faites)
```

---

## 6. Tests de Résilience

### 6.1 Perte d'un Datanode

```bash
# ☑ Test — tuer le datanode VM2
# === Sur VM2 ===
kill %1  # ou pkill wourifs

# === Sur VM1 — écrire un fichier ===
echo "après crash VM2" | sudo tee /mnt/wourifs/resilience_test.txt
cat /mnt/wourifs/resilience_test.txt
# → après crash VM2 (le fichier est stocké sur VM1 et VM3)

# === Relancer VM2 ===
./wourifs serve datanode &
# → [datanode] agence-a listening...
# → datanode_registered id=agence-a
# → Le fichier resilience_test.txt est toujours accessible
```

### 6.2 Redémarrage du Namenode

```bash
# ☑ Test — tuer et relancer le namenode
# === Sur VM1 ===
kill %1  # tuer namenode

# Les opérations FUSE échouent
echo "test" | sudo tee /mnt/wourifs/test_crash.txt 2>&1
# → Input/output error

# Relancer
./wourifs serve namenode --dev-no-auth &
# → [namenode] siege-1 listening on 0.0.0.0:9000
# → WAL replay restaure les métadonnées

echo "après redémarrage" | sudo tee /mnt/wourifs/test_restart.txt
cat /mnt/wourifs/test_restart.txt
# → après redémarrage
cat /mnt/wourifs/resilience_test.txt
# → après crash VM2 (les données d'avant le crash sont intactes)
```

---

## 7. Tests d'Observabilité

### 7.1 Métriques Prometheus

```bash
# ☑ Test — endpoint métriques accessible
curl -s http://localhost:9102/metrics
# → Affiche les métriques wourifs_op_total, wourifs_ops_ok, etc.

# ☑ Test — compteur d'opérations
curl -s http://localhost:9102/metrics | grep wourifs_op_total
# → wourifs_op_total{op="CreateFile",status="ok"} 5
# → wourifs_op_total{op="DeleteFile",status="ok"} 2
```

### 7.2 Dashboard Admin

```bash
# Depuis le navigateur :
# http://100.x.y.1:8443
# ☑ L'onglet "Topologie" affiche les nœuds enregistrés
# ☑ L'onglet "Audit Log" est visible
```

### 7.3 Status CLI

```bash
./wourifs status
# → Cluster: wourifs
# → Node: siege-1
# → Roles: [namenode datanode gateway]
# → Repl factor: 3 (quorum 2)
# → Data dir: /var/lib/wourifs/siege-1
# → Metrics: :9102 (enabled=true)
# → Tracing: (rate=1.00, enabled=true)
```

---

## 8. Checklist Récapitulative

| # | Test | Résultat Attendu | ✅/❌ |
|---|---|---|---|
| 1 | Binaire `wourifs` compile | Fichier ~25M | |
| 2 | `wourifs --help` | 7 commandes listées | |
| 3 | `wourifs.yaml` créé | Fichier YAML valide | |
| 4 | Namenode écoute | `[namenode] ... listening on 0.0.0.0:9000` | |
| 5 | Datanode s'enregistre | `datanode_registered id=...` | |
| 6 | Dashboard admin | Page HTML sur :8443 | |
| 7 | FUSE mount | `mount \| grep wourifs` | |
| 8 | Écriture simple | `echo > file` → `cat file` | |
| 9 | Création répertoire | `mkdir` → `ls -ld` | |
| 10 | Renommage | `mv` → ancien nom disparu, nouveau existe | |
| 11 | Suppression fichier | `rm` → `ls` donne "No such file" | |
| 12 | Truncate | `truncate -s 0` → fichier vide | |
| 13 | Arborescence profonde | `mkdir -p a/b/c` → `ls -R` correct | |
| 14 | Gros fichier (50 MB) | `dd` → 52428800 bytes écrits | |
| 15 | 100 petits fichiers | Tous créés sans erreur | |
| 16 | Scénario clients CSV | Fichier visible depuis VM1 | |
| 17 | Scénario épargne | Mise à jour de solde visible depuis VM1 | |
| 18 | Scénario crédit | Remboursement enregistré | |
| 19 | Crash Datanode | Données intactes, fichier accessible | |
| 20 | Redémarrage Namenode | WAL replay, données restaurées | |
| 21 | Métriques Prometheus | `curl :9102/metrics` → compteurs | |
| 22 | `wourifs status` | Informations correctes | |

---

*Document préparé pour la validation fonctionnelle de WouriFS v3.0 — ICT University, 2026.*

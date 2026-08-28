# Batch-Datenarchitektur für Energielastprognosen

Dieses Repository implementiert Phase 2 des IU-Portfolioprojekts `DLMDWWDE02`. Die Pipeline
überführt den UCI-Datensatz **Individual Household Electric Power Consumption** in geprüfte,
versionierte und ML-bereite Batch-Daten.

## Architektur

```text
UCI -> Ingestion API -> HDFS Raw -> Spark -> HDFS Curated -> ML-ready Snapshot
             ^                         ^
             +------ Airflow DAG ------+
```

- **Airflow 3.1.7** orchestriert Monatsläufe, Retries und Quality Gates.
- **Python 3.12** lädt und extrahiert wertgetreue Monatsbatches, berechnet SHA-256 und erzeugt
  ein Manifest.
- **Hadoop/HDFS 3.5.0** speichert Raw-, Curated-, ML-ready-, Governance- und Quarantänedaten.
- **Spark 3.5.7** typisiert, validiert, bereinigt, aggregiert und erzeugt Features.
- **PostgreSQL 16** speichert ausschließlich Airflow-Metadaten.

Die ML-Anwendung selbst ist entsprechend der Aufgabenstellung nicht Bestandteil des Projekts.

## Voraussetzungen

- Docker Desktop: <https://www.docker.com/products/docker-desktop/>
- Docker Compose v2.14 oder neuer (in Docker Desktop enthalten)
- Git: <https://git-scm.com/downloads>
- Für die spätere Remote-Abgabe ein GitHub-Konto: <https://github.com/signup>

Für Airflow, Spark, Hadoop und den UCI-Download ist keine Registrierung erforderlich. Docker
Hub kann ohne Anmeldung genutzt werden; ein Konto ist nur bei anonymen Pull-Limits nötig:
<https://hub.docker.com/signup>.

Empfohlen sind mindestens 8 GB für Docker Desktop. Standardmäßig startet nur ein Spark-Worker;
der zweite Worker ist ein bewusst zuschaltbarer Skalierungstest.

## Schnellstart unter Windows/PowerShell

```powershell
.\scripts\init-env.ps1
$env:GIT_COMMIT = (git rev-parse --short HEAD 2>$null)
.\scripts\build-images.ps1
docker compose up --no-build -d
docker compose ps
```

Die Airflow-Oberfläche ist anschließend ausschließlich lokal unter
<http://127.0.0.1:8080> erreichbar. Benutzername ist standardmäßig `admin`; das zufällig
generierte Passwort steht in der lokalen, von Git ignorierten `.env`-Datei.

Der DAG heißt `power_forecast_monthly`. Er ist nach der Initialisierung absichtlich pausiert.
Ein isolierter Smoke-Test lässt sich ohne Aktivierung des historischen Catch-ups ausführen:

```powershell
docker compose exec airflow-scheduler airflow dags test power_forecast_monthly 2006-12-01 `
  -c '{"year": 2006, "month": 12}'
```

Erst danach sollte der historische Catch-up-Lauf aktiviert werden. Der DAG verarbeitet wegen
`max_active_runs=1` höchstens einen Monat gleichzeitig.

## Skalierungstest

```powershell
docker compose --profile scale up -d spark-worker-2
docker compose ps
```

Der Transformationscode bleibt unverändert. Zwei Worker und zwei HDFS-DataNodes demonstrieren
horizontale Skalierung beziehungsweise Blockreplikation. Da alle Container auf demselben Laptop
laufen, ist dies keine physische Hochverfügbarkeit und kein externes Backup.

## Tests

```powershell
.\scripts\check.ps1
```

Der Check validiert die Compose-Datei, baut ein isoliertes Python-3.12-Testimage und führt die
Unit-Tests aus. Ein vollständiger Integrationstest benötigt die laufende Docker-Umgebung und
den einmaligen Download des rund 20 MB großen UCI-Archivs.

## Datenpfade

```text
/data/raw/source=uci/year=YYYY/month=MM/batch_id=...
/data/curated/hourly/year=YYYY/month=MM/batch_id=...
/data/curated/daily/year=YYYY/month=MM/batch_id=...
/data/ml_ready/snapshot=YYYY-Qn/version=...
/data/governance/batches/batch_id=...
/data/quarantine/year=YYYY/month=MM/...
```

Ein Batch wird zuerst unter `/data/_staging` geschrieben und erst nach bestandenem Quality Gate
atomar veröffentlicht. Gleiche Eingaben erzeugen dieselbe `batch_id`; ein erneuter Lauf ist ein
nachweisbarer No-op.

## Datenqualitätsregeln

Raw-Gate:

- exakt neun erwartete Spalten,
- parsebare, monatskonforme und eindeutige Zeitstempel,
- mindestens eine Datenzeile,
- dokumentierte Zahl fehlender Messwerte.

Curated-Gate:

- keine ungültigen oder doppelten Zeitstempel,
- keine negativen Werte für `Global_active_power`,
- maximal 5 % fehlende Werte in `Global_active_power`,
- nicht leere Stunden- und Tagesaggregate.

Fehlende Messwerte bleiben in den typisierten Quelldaten erkennbar. Für Aggregate wird nur der
zuletzt **vorher** beobachtete Wert verwendet; Zukunftswerte werden nicht einbezogen. Lags und
rollierende Kennzahlen basieren ebenfalls ausschließlich auf vergangenen Zeitpunkten.

## Sicherheit und bewusste Grenzen

- Nur Airflow wird an `127.0.0.1` veröffentlicht.
- Daten- und Kontrollnetze sind intern; nur der Ingestion-Service besitzt einen Egress-Pfad zur
  UCI-Quelle.
- Kein Container erhält den Docker-Socket.
- Eigener Ingestion-Code läuft als Nicht-root mit schreibgeschütztem Root-Dateisystem.
- Der Spark-Treiber läuft als Nicht-root; nur sein begrenztes `/tmp` erlaubt native Spark-
  Bibliotheken auszuführen.
- `no-new-privileges` ist für alle langlebigen Services gesetzt.
- Passwörter liegen in `.env`, die nicht versioniert wird.
- Das lokale HDFS verwendet Simple Authentication. Für Produktion wären Kerberos, TLS, externe
  Secrets-Verwaltung, getrennte Hosts und Backups erforderlich.

Weitere Begründungen stehen in [Architekturentscheidungen](docs/architecture-decisions.md) und
[Implementierungsrisiken](docs/implementation-risks.md).

## Datenquelle und Lizenz

Hebrail, G., & Berard, A. (2006). *Individual Household Electric Power Consumption*
[Datensatz]. UCI Machine Learning Repository. <https://doi.org/10.24432/C58K54>

Der Datensatz steht unter **CC BY 4.0**. Er enthält Verbrauchsdaten eines einzelnen Haushalts;
deshalb gelten Zweckbindung, Datenminimierung und keine Anreicherung um Identitätsdaten.

## Stoppen

```powershell
docker compose down
```

`docker compose down -v` löscht zusätzlich alle persistenten Projektvolumes und sollte nur
verwendet werden, wenn die erzeugten Daten bewusst verworfen werden sollen.

# Implementierungsrisiken und Gegenmaßnahmen

| Herausforderung | Konkrete Gegenmaßnahme | Nachweis |
|---|---|---|
| Inkompatible Images oder Bibliotheken | Offizielle Images und feste Versions-Tags; Compose-Validierung und Image-Build im Testprozess | `docker compose config`, Build-Protokoll |
| Docker Compose Bake scheitert auf Windows an der Sitzungsübergabe | Projektimages einzeln mit `docker build` bauen; Compose danach mit `--no-build` starten | reproduzierbarer Einzel-Build |
| Nicht-root-Dienste können neue Docker-Volumes zunächst nicht beschreiben | Einmalige Init-Container setzen ausschließlich auf den zugehörigen Volumes minimale Besitzerrechte | erfolgreiche Cache- und HDFS-Initialisierung |
| Spark kann native Zstandard-Bibliothek auf gehärtetem `/tmp` nicht laden | `exec` nur auf dem begrenzten Spark-Driver-tmpfs erlauben; Root-Dateisystem bleibt read-only | erfolgreicher Shuffle- und End-to-End-Lauf |
| Spark-Laufzeitänderungen verursachen schwer erkennbare Kompatibilitätsfehler | Stabilen offiziellen Spark-3.5.7-Tag pinnen und echten Shuffle-Lauf testen | 367 Stunden- und 16 Tagesaggregate |
| Airflow 3 besitzt keine frühere `airflow users`-CLI | SimpleAuthManager explizit konfigurieren und Passwortdatei während der Init-Phase mit Modus `0600` erzeugen | erfolgreicher API-Server-Login und Healthcheck |
| Dienste starten in falscher Reihenfolge | Healthchecks und `depends_on` mit Zustandsbedingungen | Compose-Konfiguration, Containerstatus |
| Wiederholung erzeugt doppelte Daten | Deterministische `batch_id`, SHA-256 und existenzgeprüfte Zielpfade | Idempotenztest |
| Teilweise sichtbare Ausgaben | Staging-Pfade und atomarer HDFS-Rename | Fehler- und Wiederanlauftest |
| Fehlerhafte Quelldaten gelangen weiter | Raw- und Curated-Quality-Gates; Quarantänebericht | Qualitätsbericht in HDFS |
| Laptop verfügt über begrenzten RAM | LocalExecutor; standardmäßig nur ein Spark-Worker; zweiter Worker als Profil | Ressourcenmessung |
| Logische Replikation wird mit Backup verwechselt | Grenze ausdrücklich dokumentiert; Volumes schützen nur gegen Container-Neuerstellung | ADR-003 |
| Unnötige Angriffsfläche | Nur Airflow an localhost; interne Netze; kein Docker-Socket; Nicht-root und `no-new-privileges` | `docker compose config` |
| HDFS Simple Authentication ist nicht produktionsreif | Lokale Demonstration klar abgrenzen; Kerberos/TLS als Produktionsmaßnahme dokumentieren | ADR-005 |

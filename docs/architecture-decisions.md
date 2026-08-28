# Architekturentscheidungen

## ADR-001: Konsequente Batch-Verarbeitung

Die ML-Anwendung benötigt nur quartalsweise einen neuen Feature-Snapshot. Monatsbatches mit
quartalsweiser Veröffentlichung vermeiden daher die zusätzliche Broker-, Offset- und
Streaming-Komplexität von Kafka. Eine Streaming-Pipeline bleibt eine spätere Erweiterung.

## ADR-002: Airflow 3.1.7 mit LocalExecutor

Für die lokale Demonstration genügt der `LocalExecutor` mit PostgreSQL. Damit bleiben
Scheduling, Retries und beobachtbare Task-Zustände erhalten, während Redis und Celery-Worker
entfallen. Das reduziert den RAM-Bedarf und die Zahl der Fehlerquellen auf einem Laptop.

## ADR-003: HDFS 3.5.0 mit zwei DataNodes

Zwei DataNodes demonstrieren Blockreplikation und Knotenausfall. Da beide auf demselben
physischen Rechner laufen, ist dies ausdrücklich kein Backup und keine Host-Hochverfügbarkeit.

## ADR-004: Spark 3.5.7 im Standalone-Modus

Spark verarbeitet die Daten verteilt. Ein zweiter Worker ist über das Compose-Profil `scale`
zuschaltbar. Airflow übermittelt Jobs an die interne Spark-REST-Schnittstelle; ein privilegierter
Docker-Socket im Airflow-Container wird dadurch vermieden. Die 3.5-Linie wurde nach einem echten
Shuffle-Integrationstest als stabiler, fest gepinnter Projektstand gewählt.

## ADR-005: Sicherheitsgrenzen der lokalen Umgebung

Nur die Airflow-Oberfläche wird an `127.0.0.1` veröffentlicht. PostgreSQL, HDFS, Spark und der
Ingestion-Service sind ausschließlich in internen Docker-Netzen erreichbar. Container erhalten
`no-new-privileges`; der selbst erstellte Ingestion-Container läuft als nicht privilegierter
Benutzer und mit schreibgeschütztem Root-Dateisystem. Passwörter liegen in der ignorierten
`.env`-Datei. Das lokale HDFS verwendet Simple Authentication und ist deshalb nicht als
produktionsreife Sicherheitskonfiguration zu verstehen; produktiv wären Kerberos, TLS und eine
externe Secrets-Verwaltung erforderlich.

## ADR-006: Unveränderliche Datenzonen und atomare Veröffentlichung

Rohdaten werden wertgetreu pro Monat abgelegt. Ingestion und Spark schreiben zunächst nach
`/data/_staging`. Erst bestandene Quality Gates führen zu einem atomaren Rename in die Raw-,
Curated- oder ML-ready-Zone. Deterministische Batch-IDs und Prüfsummen verhindern Duplikate.

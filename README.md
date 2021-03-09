# Message-Queue

## Protokoll
  - Jede Node hat eine Message
  - Jede Node signiert seine Message mit privatem Schlüssel
  - Nodes tauschen Nachrichten aus (Runden basiert)
  - Runden können sich überschneiden, erfassen der Rundenordnung/-zugehörigkeit über sequentielle Signierung
  - Minimaler Heartbeat zum erfassen von Dead Nodes -> Wenn dead dann senden von z.b. 0 -> Runde abschließen

## Verteilung/Topology
  - Austausch der Messages in subnetzen entsprechend der Topology (Netzlatenz)
  - Mergesort zum Zusammenfassen von message reihenfolgen
  - Mögliche Formen B-Trea oder redundante Binärbäume
  - Festschreiben durch Multicast an Subdomäne


## Problem
  - Nutzerverwaltung / Public Key Verwaltung (kombination mit Heartbeat)
  -> Public Key überall speichern
  - Korruptes Subnetz
  -> Mittelgroße Subnetze mit redundaten Verbindungen
  - Privacy Aspekt -> Wenn möglich nur Vor- und Nachgänger bekannt
  - Nur Metadaten überall Vorhalten
  - Content mit Verteiltem Filesystem via Hashes identifizieren
  - Route vorhalten für Live-Event
  - Daten replizieren, sodass sie immer mehrfach vorhanden sind
  - Absichern von Subdomänen durch zusammenfügen der Schlüssel aller in der Subdomäne enthaltenden Knoten

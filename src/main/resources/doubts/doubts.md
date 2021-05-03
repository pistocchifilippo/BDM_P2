# Doubts to ask at the first meeting session
- Do we have to load files into Mongo/HBase/HDFS and work on top of these systems or shall we work and process each file locally?
  - If so, even after performing data cleaning and reconciliation it is just necessary store data on local files?
- What abstraction do we have to use to describe pipes? (The one with graph at page 103?)
- Shall we join data in order to have optimised queries????

## Data cleaning and reconciliation
- Reconciliation = JOIN?
  - Store the reconciliated file on disk after the process?
- Data cleaning = Remove useless information in the records?

## KPI(s)
- Building Age: Correlate between the rental price and the average building oldness of the neighbourhood.
- Incident: Correlate the number (and even the kind) of accident to a neighbourhood.


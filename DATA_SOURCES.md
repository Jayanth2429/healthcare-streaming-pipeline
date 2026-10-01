# Data sources

## MIMIC-IV Clinical Database Demo v2.2

- Publisher: MIT Laboratory for Computational Physiology / PhysioNet
- Dataset: MIMIC-IV Clinical Database Demo, version 2.2
- Project page: https://physionet.org/content/mimic-iv-demo/2.2/
- DOI: https://doi.org/10.13026/dp1f-ex47
- Tables used: `hosp/admissions.csv.gz`, `hosp/transfers.csv.gz`
- Access: open access
- License: Open Data Commons Open Database License (ODbL) v1.0
- Raw data committed to this repository: **No**

The demo contains a deidentified subset of 100 patients and uses the same schema structure as MIMIC-IV. This project downloads the source files directly from PhysioNet at runtime and converts selected rows into a normalized event contract for streaming replay.

Historical timestamps are preserved from the source data, but the files are replayed as a stream only for engineering demonstration purposes. This project does not claim that the source was originally delivered through Kafka.

### Required citation

Johnson, A., Bulgarelli, L., Pollard, T., Horng, S., Celi, L. A., & Mark, R. (2023). *MIMIC-IV Clinical Database Demo (version 2.2).* PhysioNet. https://doi.org/10.13026/dp1f-ex47

Please also follow PhysioNet's citation guidance on the dataset page.

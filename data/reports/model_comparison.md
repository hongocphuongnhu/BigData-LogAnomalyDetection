| method | k | precision | recall | f1 | pr_auc | fpr | recall_brute_force | recall_sqli | recall_scan | recall_ddos |
|---|---|---|---|---|---|---|---|---|---|---|
| Rule (ngưỡng) | - | 1.000 | 0.368 | 0.538 | nan | 0.000 | 0.900 | 1.000 | 1.000 | 0.337 |
| KMeans + CBLOF · core | 2 | 0.720 | 0.998 | 0.836 | 0.998 | 0.003 | 1.000 | 0.900 | 1.000 | 1.000 |
| KMeans (tâm cụm riêng) · core | 2 | 0.221 | 0.211 | 0.216 | 0.157 | 0.006 | 1.000 | 0.900 | 1.000 | 0.172 |
| BisectingKMeans + CBLOF · core | 2 | 0.752 | 0.998 | 0.858 | 0.998 | 0.003 | 1.000 | 0.900 | 1.000 | 1.000 |
| KMeans + CBLOF · extended | 2 | 0.751 | 0.998 | 0.857 | 0.998 | 0.003 | 1.000 | 0.900 | 1.000 | 1.000 |
| BisectingKMeans + CBLOF · extended | 2 | 0.736 | 0.998 | 0.848 | 0.998 | 0.003 | 1.000 | 0.900 | 1.000 | 1.000 |

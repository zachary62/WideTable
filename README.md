# Treant: Lightweight Materialization for Fast Dashboards Over Joins


[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Dashboards are vital in modern business intelligence tools, providing non-technical users with an interface to access comprehensive business data. With the rise of cloud technology, there is an increased number of data sources to provide enriched contexts for various analytical tasks, leading to a demand for interactive dashboards over a large number of joins. Nevertheless, joins are among the most expensive operations in DBMSes, making the support of interactive dashboard over joins challenging.

In this paper, we present Treant, a dashboard accelerator for queries over large joins. Treant uses factorized query execution to handle aggregation queries over large joins, which alone is still insufficient for interactive speeds. To address this, we exploit the incremental nature of user interactions using Calibrated Junction Hypertree (CJT), a novel data structure that applies lightweight materialization of the intermediates during factorized execution. CJT ensures that the work needed to compute a query is proportional to how different it is from previous query, rather than the overall complexity. Treant manages CJTs to share work between queries and performs materialization offline or during user "think-times." Implemented as a middleware that rewrites SQL, Treant is portable to any SQL-based DBMS. Our experiments on single node and cloud DBMSes show that Treant improves dashboard interactions by two orders of magnitude, and provides 10x improvement for ML augmentation compared to SOTA factorized ML system. 

Please find the demo usage at https://anonymous.4open.science/r/WideTable-627D/src/demo.ipynb

# Medical Abstract Classification with DistilBERT

A deep learning project fine-tuning DistilBERT for multi-class sentence classification on biomedical research abstracts. The model classifies sentences from PubMed RCT (Randomized Controlled Trial) abstracts into their structural roles — Background, Objective, Methods, Results, and Conclusions — enabling automated structured summarization of medical literature.

Built as a graduate deep learning project (DL677) at NJIT Ying Wu College of Computing, 2025.

## Project Report

The full methodology, results, and analysis are documented in the milestone report below:

[![Page 1](https://github.com/DanielAyer/DistilBERT/raw/main/readme_images/1.png)](readme_images/1.png)
[![Page 2](https://github.com/DanielAyer/DistilBERT/raw/main/readme_images/2.png)](readme_images/2.png)
[![Page 3](https://github.com/DanielAyer/DistilBERT/raw/main/readme_images/3.png)](readme_images/3.png)
[![Page 4](https://github.com/DanielAyer/DistilBERT/raw/main/readme_images/4.png)](readme_images/4.png)
[![Page 5](https://github.com/DanielAyer/DistilBERT/raw/main/readme_images/5.png)](readme_images/5.png)
[![Page 6](https://github.com/DanielAyer/DistilBERT/raw/main/readme_images/6.png)](readme_images/6.png)
[![Page 7](https://github.com/DanielAyer/DistilBERT/raw/main/readme_images/7.png)](readme_images/7.png)

## Overview

Structured abstracts in biomedical literature organize sentences into labeled sections (Background, Objective, Methods, Results, Conclusions). Automatically identifying these roles enables downstream tasks like literature search, evidence extraction, and clinical decision support.

This project fine-tunes [DistilBERT](https://huggingface.co/distilbert-base-uncased) — a lightweight, distilled version of BERT — on the [PubMed 200k RCT dataset](https://github.com/Franck-Dernoncourt/pubmed-rct) for sentence-level classification within these abstracts.

## Dataset

**PubMed 200k RCT** — a large dataset of ~200,000 abstracts from PubMed randomized controlled trial papers, with each sentence labeled by its rhetorical role:

| Label | Description |
|-------|-------------|
| BACKGROUND | Context and motivation |
| OBJECTIVE | Research question or aim |
| METHODS | Study design and procedures |
| RESULTS | Findings and measurements |
| CONCLUSIONS | Interpretation and implications |

## Approach

**Base model:** `distilbert-base-uncased` from Hugging Face — chosen for its balance of performance and computational efficiency. DistilBERT retains ~97% of BERT's language understanding capability at 40% fewer parameters, making it practical to fine-tune on consumer hardware.

**Fine-tuning strategy:**
- Tokenization with DistilBERT's WordPiece tokenizer
- Classification head added on top of the `[CLS]` token representation
- Training on the PubMed RCT training split
- Evaluation on held-out test set

**Pipeline:**
```
Raw abstract sentences
        ↓
DistilBERT tokenizer
        ↓
DistilBERT encoder (fine-tuned)
        ↓
[CLS] token representation
        ↓
Linear classification head
        ↓
Predicted label (BACKGROUND / OBJECTIVE / METHODS / RESULTS / CONCLUSIONS)
```

## Tech Stack

- **Python 3**
- **PyTorch** — model training and inference
- **Hugging Face Transformers** — DistilBERT model and tokenizer
- **Jupyter Notebook** — experimentation and analysis
- **Pandas / NumPy** — data processing
- **Matplotlib / Seaborn** — results visualization

## Repository Structure

```
DistilBERT/
├── data/pubmed-rct-master/          # PubMed RCT dataset
├── processing/                       # Data preprocessing scripts
├── readme_images/                    # Project report pages
├── visuals/                          # Results visualizations
├── dl_677.ipynb                      # Main project notebook
├── dl_677_distilbert_based_model.ipynb   # Model architecture notebook
├── dl_677_distilbert_based_train.ipynb   # Training notebook
├── dl_677_model.ipynb                # Baseline model notebook
└── Milestone_3_base.pdf              # Full project report
```

## Context

This project was completed as Milestone 3 of DL677 (Deep Learning) at NJIT Ying Wu College of Computing, Spring 2025, in collaboration with a fellow student.

The task is drawn from the paper:
> Dernoncourt, F., & Lee, J. Y. (2017). [PubMed 200k RCT: a Dataset for Sequential Sentence Classification in Medical Abstracts](https://arxiv.org/abs/1710.06071). *EMNLP 2017*.

## Author

**Daniel Ayer**  
M.S. Artificial Intelligence Candidate, NJIT  
[LinkedIn](https://linkedin.com/in/danielayer) · [GitHub](https://github.com/DanielAyer) · [Hugging Face](https://huggingface.co/danielayer)

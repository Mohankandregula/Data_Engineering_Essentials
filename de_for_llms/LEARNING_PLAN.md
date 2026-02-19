# Data Engineering for Large Models — Learning Plan

> **Source:** https://datascale-ai.github.io/data_engineering_book/en/
> **Started:** February 2026
> **Approach:** Read chapter → Understand every concept → Build hands-on project → Move on
> **Goal:** Master data engineering for LLMs through practical, real-world exercises

---

## What This Book Is About (Plain English)

When companies like OpenAI build ChatGPT or Google builds Gemini, the **model architecture** (the "brain design") is mostly the same across companies. What makes one model smarter than another is the **data** it was trained on.

This book teaches you how to:
- Collect massive amounts of text/images/video from the internet
- Clean it (remove spam, duplicates, garbage)
- Format it for training AI models
- Create synthetic (AI-generated) training data
- Build RAG systems (letting AI "look up" information)

**Why this matters:** Every AI company needs data engineers who understand this pipeline. It's the highest-demand skill gap in AI right now.

---

## Learning Approach

For each chapter:
1. **Read** — Go through the chapter, understand every technical term
2. **Discuss** — Break down the "why" behind each concept with real examples
3. **Build** — Hands-on mini-project applying the concepts
4. **Review** — Summarize key takeaways before moving on

---

## Part 1: Infrastructure & Core Concepts (Foundation)

### Chapter 1: Data Revolution in the LLM Era
**What you'll learn:** Why data quality matters more than model size
**Key concepts to master:**
- Scaling Laws — the math behind "bigger model = smarter model" (and why it's incomplete)
- Pre-training vs Fine-tuning vs RLHF vs RAG — the 4 stages of making an AI model
- Data quality vs quantity tradeoff — why 7B tokens of clean data beats 70B tokens of garbage
- The "data funnel" — how 100PB of raw web data becomes 1PB of training data

**Hands-on:** Analyze a real open-source dataset (FineWeb sample) — measure noise ratio, see what "dirty data" looks like vs "clean data"

**Estimated time:** 2-3 hours

---

### Chapter 2: Data Infrastructure Selection
**What you'll learn:** How to pick the right tools for storing and processing massive data
**Key concepts to master:**
- Object Storage (S3/MinIO) — "cloud hard drives" for massive data
- Parquet vs JSONL vs WebDataset — different file formats and when to use each
- Spark vs Ray — two engines for processing data at scale
- Data Version Control (DVC/LakeFS) — "Git for datasets"
- Compression algorithms — Snappy, LZ4, ZSTD and when to use each

**Hands-on:**
- Set up MinIO locally (mini version of S3)
- Convert a JSON dataset to Parquet, measure size difference
- Process data with both Spark and Ray, compare the experience
- Initialize DVC for dataset versioning

**Estimated time:** 4-5 hours

---

## Part 2: Text Pre-training Data Engineering (The Core)

### Chapter 3: Data Acquisition
**What you'll learn:** How to get training data from the internet at scale
**Key concepts to master:**
- Common Crawl — the world's largest free web archive (3-5PB added monthly)
- Web scraping with Trafilatura — extracting clean text from HTML
- Specialized data: GitHub code, ArXiv papers, Wikipedia

**Hands-on:**
- Download and parse a Common Crawl WARC file (a slice of the internet)
- Extract clean text from 1000 web pages using Trafilatura
- Build a mini scraper for a specific domain

**Estimated time:** 4-5 hours

---

### Chapter 4: Cleaning & Deduplication
**What you'll learn:** How to remove garbage from your data
**Key concepts to master:**
- Heuristic filtering — simple rules (length, language, special chars)
- MinHash + LSH — fuzzy deduplication at scale (finding "almost identical" documents)
- Exact deduplication — finding identical documents via hashing
- PII removal — stripping personal information (emails, phone numbers, SSNs)
- Perplexity scoring — using a language model to score "how natural" text is

**Hands-on:**
- Build a cleaning pipeline: filter short/garbage text → deduplicate → remove PII
- Implement MinHash deduplication on a sample dataset
- Measure before/after quality metrics

**Estimated time:** 5-6 hours

---

### Chapter 5: Tokenization & Serialization
**What you'll learn:** How text becomes numbers that AI models can read
**Key concepts to master:**
- BPE (Byte Pair Encoding) — the algorithm behind GPT's tokenizer
- SentencePiece — Google's tokenizer library
- Vocabulary construction — building a custom tokenizer
- Data mixing — combining different data sources in the right proportions

**Hands-on:**
- Train a BPE tokenizer from scratch on the cleaned data
- Compare tokenization of English vs code vs mixed content
- Experiment with vocabulary sizes and their effects

**Estimated time:** 4-5 hours

---

## Part 3: Multimodal Data Engineering

### Chapter 6: Image-Text Pair Processing
**What you'll learn:** How to prepare image+text data for models like GPT-4V
**Key concepts to master:**
- CLIP scores — measuring how well an image matches its caption
- Image quality filtering — resolution, aspect ratio, NSFW detection
- LAION dataset — the largest open image-text dataset

**Hands-on:**
- Download a sample of LAION, filter by CLIP score
- Build an image-text cleaning pipeline

**Estimated time:** 4-5 hours

---

### Chapter 7: Recaptioning
**What you'll learn:** Why web alt-text is bad and how to generate better captions
**Key concepts to master:**
- Alt-text limitations — web image descriptions are mostly garbage
- Synthetic captioning — using AI to generate better image descriptions
- OCR enhancement — extracting text from images

**Hands-on:**
- Compare original alt-text vs AI-generated captions on sample images
- Build a recaptioning pipeline using an open model

**Estimated time:** 3-4 hours

---

### Chapter 8: Video & Audio Data
**What you'll learn:** Processing video and audio for multimodal models
**Key concepts to master:**
- Video frame extraction and keyframe selection
- Video tokenization strategies
- Audio-visual alignment

**Hands-on:**
- Extract keyframes from sample videos
- Align audio transcripts with video segments

**Estimated time:** 3-4 hours

---

## Part 4: Alignment & Synthetic Data Engineering

### Chapter 9: Instruction Fine-tuning Data (SFT)
**What you'll learn:** How to create data that teaches AI to follow instructions
**Key concepts to master:**
- SFT (Supervised Fine-Tuning) — teaching a model to be a helpful assistant
- Instruction diversity — covering many types of tasks
- Prompt engineering for data generation
- Chain-of-Thought (CoT) — teaching models to "think step by step"

**Hands-on:**
- Create an SFT dataset: generate diverse instruction-response pairs
- Implement quality filtering for instruction data

**Estimated time:** 4-5 hours

---

### Chapter 10: Synthetic Data
**What you'll learn:** Using AI to generate training data for AI
**Key concepts to master:**
- "Textbook quality" synthetic data (Microsoft Phi approach)
- Code and math synthesis
- Avoiding model collapse — when AI training data creates feedback loops

**Hands-on:**
- Generate a synthetic "textbook" dataset for a specific topic
- Compare model performance on synthetic vs real data

**Estimated time:** 4-5 hours

---

### Chapter 11: Human Preference Data (RLHF/DPO)
**What you'll learn:** How to make AI outputs align with what humans prefer
**Key concepts to master:**
- RLHF — Reinforcement Learning from Human Feedback
- DPO — Direct Preference Optimization (simpler alternative)
- Annotation platforms and quality control
- RLAIF — using AI to generate preference data

**Hands-on:**
- Create a preference dataset (chosen vs rejected responses)
- Set up a simple annotation workflow

**Estimated time:** 3-4 hours

---

## Part 5: Application-level Data Engineering

### Chapter 12: RAG Data Pipeline
**What you'll learn:** Building the data layer for Retrieval-Augmented Generation
**Key concepts to master:**
- Document parsing (PDF, HTML, Word)
- Chunking strategies — how to split documents into retrievable pieces
- Embeddings and vector databases
- Retrieval optimization

**Hands-on:**
- Build a complete RAG pipeline: parse PDFs → chunk → embed → store in vector DB → query
- Compare different chunking strategies

**Estimated time:** 5-6 hours

---

### Chapter 13: Multimodal RAG
**What you'll learn:** RAG with images, tables, and mixed content
**Key concepts to master:**
- Cross-modal retrieval
- ColPali architecture — vision-language retrieval

**Hands-on:**
- Build a multimodal RAG system that can answer questions about documents with images/tables

**Estimated time:** 4-5 hours

---

## Part 6: Capstone Projects (Apply Everything)

### Project 1: Building Mini-C4 Pre-training Set
Build a complete pre-training dataset from scratch: crawl → clean → deduplicate → tokenize

### Project 2: Domain Expert SFT
Create a domain-specific fine-tuning dataset (legal/medical/finance)

### Project 3: Building LLaVA Multimodal Instruction Set
Create image-text instruction data for a multimodal model

### Project 4: Synthetic Math/Code Textbook
Generate AI-written textbook-quality training data

### Project 5: Multimodal RAG Financial Report Assistant
Build a production RAG system that understands financial reports with charts/tables

---

## Folder Structure

```
de_for_llms/
├── LEARNING_PLAN.md          # This file
├── PROGRESS.md               # Track completion status
├── chapters/
│   ├── ch01_data_revolution/     # Notes + exercises per chapter
│   ├── ch02_infrastructure/
│   ├── ch03_data_acquisition/
│   ├── ch04_cleaning_dedup/
│   ├── ch05_tokenization/
│   ├── ch06_image_text/
│   ├── ch07_recaptioning/
│   ├── ch08_video_audio/
│   ├── ch09_sft_data/
│   ├── ch10_synthetic_data/
│   ├── ch11_preference_data/
│   ├── ch12_rag_pipeline/
│   └── ch13_multimodal_rag/
├── projects/
│   ├── p1_mini_c4/
│   ├── p2_domain_sft/
│   ├── p3_llava_instruct/
│   ├── p4_synthetic_textbook/
│   └── p5_mm_rag_finance/
├── data/                     # Downloaded datasets and samples
├── notebooks/                # Jupyter notebooks for exploration
└── scripts/                  # Reusable processing scripts
```

---

## Estimated Timeline

| Part | Chapters | Estimated Hours | Pace (2h/day) |
|------|----------|-----------------|---------------|
| Part 1: Foundation | Ch 1-2 | 6-8 hours | ~4 days |
| Part 2: Text Data | Ch 3-5 | 13-16 hours | ~8 days |
| Part 3: Multimodal | Ch 6-8 | 10-13 hours | ~6 days |
| Part 4: Alignment | Ch 9-11 | 11-14 hours | ~7 days |
| Part 5: Applications | Ch 12-13 | 9-11 hours | ~5 days |
| Part 6: Capstone | Projects 1-5 | 20-30 hours | ~12 days |
| **Total** | | **69-92 hours** | **~6 weeks at 2h/day** |

---

## Prerequisites

**Required:**
- Python (proficient)
- SQL (proficient)
- AWS basics (S3, Glue)
- Data pipeline experience
- Git

**New concepts covered in this plan:**
- Tokenizers (BPE, SentencePiece)
- Vector databases
- MinHash/LSH for deduplication
- CLIP scores, RLHF, DPO

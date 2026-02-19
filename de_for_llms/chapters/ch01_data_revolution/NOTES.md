# Chapter 1: Data Revolution in the LLM Era — Notes

## Key Concepts

### Scaling Laws (OpenAI, 2020)
- Model performance follows **power law** relationships with Parameters (N), Dataset size (D), Compute (C)
- Power law = you need exponentially more resources for linear improvement
- Turned LLM development from "alchemy" to "engineering science"

### Chinchilla Optimal Ratio (DeepMind, 2022)
- ~20 tokens per parameter is optimal
- 70B model with 1.4T tokens beats 280B model with 300B tokens
- **Lesson:** Most models were over-parameterized and under-trained on data

### Phi Series — Quality > Scale (Microsoft, 2023)
- 1.3B model trained on 7B synthetic tokens beat 10B+ models on code tasks
- GPT-4-generated structured tutorials = perfectly clean training data
- **Lesson:** Data quality can completely override scaling laws

### The 4-Stage LLM Data Lifecycle
1. **Pre-training** — Learn language from massive unlabeled text (TB-level, trillions of tokens)
2. **SFT** — Learn to follow instructions from instruction-response pairs (GB-level)
3. **RLHF/DPO** — Align with human preferences from chosen/rejected pairs (tens of thousands)
4. **RAG** — Look up real-time info from external knowledge bases (enterprise data)

### The Data Funnel
- 100PB raw → 30PB (dedup) → 5PB (quality filter) → 1PB (pre-training) → 10GB (SFT)
- 99% of the internet is not suitable for training
- Plan crawl, storage, and pipelines for 100x your target

### Multimodal Complexity
- Text-only: UTF-8 strings, TB storage, mature tooling
- Multimodal: JPEG/PNG/MP4/WAV, PB storage, cross-modal alignment needed
- Quality metrics: CLIP score, aesthetic score, OCR accuracy, ASR precision

### Copyright Compliance
- Track source metadata (URL, timestamp, license) for every data point
- Filter for permissive licenses (CC0, CC-BY)
- Respect robots.txt
- Strip PII (emails, phone numbers, SSNs)

### Data Processing ROI
- Processing costs can match training costs
- 1 hour of data engineering can save 10 hours of wasted training (20x ROI)
- Deduplication: train on LESS data, spend LESS money, get BETTER results

## Common Misconceptions to Avoid
1. "More data is always better" — 80-95% is garbage. Quality first.
2. "Process data once and done" — It's iterative. Use version control.
3. "Only pre-training data matters" — Each stage needs quality management.
4. "Open-source data is ready to use" — It's raw material, needs secondary filtering.

## Key Vocabulary
| Term | Meaning |
|------|---------|
| **Token** | A piece of text (~¾ of a word). "Hello world" = 2 tokens |
| **Parameters** | The model's learnable weights (its "brain size") |
| **Power law** | Doubling input gives fixed % improvement (diminishing returns) |
| **Pre-training** | Teaching a model language from raw text |
| **SFT** | Supervised Fine-Tuning — teaching a model to follow instructions |
| **RLHF** | Reinforcement Learning from Human Feedback — aligning with human preferences |
| **DPO** | Direct Preference Optimization — simpler alternative to RLHF |
| **RAG** | Retrieval-Augmented Generation — letting models "look up" information |
| **Common Crawl** | World's largest free web archive (3-5PB added monthly) |
| **PII** | Personally Identifiable Information (emails, SSNs, phone numbers) |
| **Perplexity** | How "surprised" a model is by text (lower = more natural) |
| **CLIP score** | How well an image matches its text caption |
| **Synthetic data** | AI-generated training data (e.g., GPT-4 writing tutorials) |
| **Deduplication** | Removing identical or near-identical documents |

## Papers to Know
1. "Scaling Laws for Neural Language Models" (Kaplan et al., 2020) — The original scaling laws
2. "Training Compute-Optimal Large Language Models" (Hoffmann et al., 2022) — Chinchilla paper
3. "Textbooks Are All You Need" (Gunasekar et al., 2023) — Phi-1, synthetic data proof

## Open-Source Datasets Referenced
- **FineWeb** — HuggingFace, ~15T tokens, English web data
- **RedPajama** — Together AI's LLaMA training data reproduction
- **The Pile** — 800GB, 22 curated sources
- **RefinedWeb** — Cleaned Common Crawl by Falcon team

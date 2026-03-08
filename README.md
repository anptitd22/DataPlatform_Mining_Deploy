# Data_Mining

#Project Summarization
This project builds a multimodal Retrieval-Augmented Generation (RAG) system over Wikipedia that supports queries using text, images, or both. Wikipedia text and images are processed through a Spark ETL pipeline, embedded using a CLIP multimodal model, and stored in Milvus vector database for semantic retrieval. Retrieved content is then provided to a Small Language Model (SLM) to generate concise answers or summaries. The system can be further improved through finetuning with synthetically generated data.

# How to run
- Step1: Run project

```bash
tilt up
```

- If you shut down docker compose

```bash
tilt down
```

or

```bash
tilt down --delete-volumes
```

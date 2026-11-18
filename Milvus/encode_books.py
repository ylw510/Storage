#!/usr/bin/env python3
"""Encode book titles with SentenceTransformer, then PCA → dim=2 for the teaching series.

Full 384-d vectors are real model outputs; 2-d is only for readable figures/tables.
Re-run after editing BOOKS / QUERY_TEXT.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.decomposition import PCA
from sklearn.preprocessing import Normalizer

BOOKS = [
    {"id": 1, "title": "Milvus Internals", "category": "tech", "year": 2024},
    {"id": 2, "title": "Vector Search", "category": "tech", "year": 2023},
    {"id": 3, "title": "The Great Novel", "category": "fiction", "year": 2020},
    {"id": 4, "title": "Database Systems", "category": "tech", "year": 2022},
    {"id": 5, "title": "Poetry of Spring", "category": "fiction", "year": 2021},
    {"id": 6, "title": "HNSW Paper", "category": "tech", "year": 2016},
    {"id": 7, "title": "Mystery Island", "category": "fiction", "year": 2019},
    {"id": 8, "title": "Knowhere Guide", "category": "tech", "year": 2025},
]

QUERY_TEXT = "Milvus HNSW vector search"
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
DIM = 2
RND = 42


def main() -> None:
    titles = [b["title"] for b in BOOKS]
    model = SentenceTransformer(MODEL_NAME)
    full = model.encode(titles, normalize_embeddings=True)
    q_full = model.encode([QUERY_TEXT], normalize_embeddings=True)

    pca = PCA(n_components=DIM, random_state=RND)
    emb2 = pca.fit_transform(full)
    q2 = pca.transform(q_full)
    emb2 = Normalizer(norm="l2").fit_transform(emb2)
    q2 = Normalizer(norm="l2").transform(q2)

    rows = []
    for book, v2, vf in zip(BOOKS, emb2, full):
        rows.append(
            {
                **book,
                "embedding": [round(float(x), 4) for x in v2],
                "embedding_full_dim": int(vf.shape[0]),
                "embedding_full_head": [round(float(x), 4) for x in vf[:8]],
            }
        )

    qv = [round(float(x), 4) for x in q2[0]]

    def l2(a, b):
        a, b = np.asarray(a, float), np.asarray(b, float)
        return float(np.linalg.norm(a - b))

    dists = sorted(
        ((r["id"], l2(qv, r["embedding"]), r["category"], r["title"]) for r in rows),
        key=lambda t: (t[1], t[0]),
    )

    # filtered search truth: category==tech, id!=4
    filtered = [t for t in dists if t[2] == "tech" and t[0] != 4]
    top3 = filtered[:3]

    out = {
        "model": MODEL_NAME,
        "pipeline": f"{MODEL_NAME} (normalize) → PCA({DIM}, random_state={RND}) → L2 Normalizer",
        "note": "Docs use embedding (2-d PCA). embedding_full_* is the raw MiniLM vector.",
        "query_text": QUERY_TEXT,
        "query_embedding": qv,
        "explained_variance_ratio": [round(float(x), 4) for x in pca.explained_variance_ratio_],
        "books": rows,
        "l2_to_query": [
            {"id": i, "l2": round(d, 4), "category": c, "title": t} for i, d, c, t in dists
        ],
        "search_filter_tech_delete_4_top3": [
            {"id": i, "l2": round(d, 4), "title": t} for i, d, c, t in top3
        ],
    }

    path = Path(__file__).with_name("books_embeddings.json")
    path.write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(out, indent=2))
    print(f"\nWrote {path}")


if __name__ == "__main__":
    main()
